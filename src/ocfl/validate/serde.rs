use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryFrom;
use std::fmt::Formatter;
use std::rc::Rc;
use std::str::FromStr;

use chrono::{DateTime, Local};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::de::{DeserializeSeed, Error as SerdeError, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use uriparse::URI;

use crate::ocfl::bimap::PathBiMap;
use crate::ocfl::digest::HexDigest;
use crate::ocfl::inventory::{Inventory, User, Version};
use crate::ocfl::serde::{
    ADDRESS_FIELD, CONTENT_DIRECTORY_FIELD, CREATED_FIELD, DIGEST_ALGORITHM_FIELD, FIXITY_FIELD,
    HEAD_FIELD, ID_FIELD, INVENTORY_FIELDS, MANIFEST_FIELD, MESSAGE_FIELD, NAME_FIELD, STATE_FIELD,
    TYPE_FIELD, USER_FIELD, USER_FIELDS, VERSIONS_FIELD, VERSION_FIELDS,
};
use crate::ocfl::validate::{ErrorCode, ParseResult, ValidationResult, WarnCode};
use crate::ocfl::{ContentPath, DigestAlgorithm, LogicalPath, VersionNum};

// TODO things to validate externally:
//      1. object id
//      2. type
//      3. head
//      4. content dir
//      5. correct version

static MD5_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{32}$"#).unwrap());
static SHA1_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{40}$"#).unwrap());
static SHA256_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{64}$"#).unwrap());
static SHA512_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{128}$"#).unwrap());
static BLAKE2B_160_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{40}$"#).unwrap());
static BLAKE2B_256_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{64}$"#).unwrap());
static BLAKE2B_384_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{96}$"#).unwrap());
static BLAKE2B_512_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{128}$"#).unwrap());
static SHA512_256_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{64}$"#).unwrap());

impl<'de> Deserialize<'de> for ParseResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            Id,
            Type,
            DigestAlgorithm,
            Head,
            ContentDirectory,
            Manifest,
            Versions,
            Fixity,
            Unknown,
        }

        struct FieldSeed<'a>(&'a ValidationResult);

        impl<'de, 'a> DeserializeSeed<'de> for FieldSeed<'a> {
            type Value = Field;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor<'a>(&'a ValidationResult);

                impl<'de, 'a> Visitor<'de> for FieldVisitor<'a> {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                        formatter.write_str("an OCFL inventory field")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                    where
                        E: SerdeError,
                    {
                        match value {
                            ID_FIELD => Ok(Field::Id),
                            TYPE_FIELD => Ok(Field::Type),
                            DIGEST_ALGORITHM_FIELD => Ok(Field::DigestAlgorithm),
                            HEAD_FIELD => Ok(Field::Head),
                            CONTENT_DIRECTORY_FIELD => Ok(Field::ContentDirectory),
                            MANIFEST_FIELD => Ok(Field::Manifest),
                            VERSIONS_FIELD => Ok(Field::Versions),
                            FIXITY_FIELD => Ok(Field::Fixity),
                            _ => {
                                unknown_field(value, self.0);
                                Ok(Field::Unknown)
                            }
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor(self.0))
            }
        }

        struct InventoryVisitor;

        impl<'de> Visitor<'de> for InventoryVisitor {
            type Value = ParseResult;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("an OCFL inventory object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let result = ValidationResult::new();

                let mut id = None;
                let mut type_declaration = None;
                let mut digest_algorithm = None;
                let mut head: Option<VersionNum> = None;
                let mut content_directory = None;
                let mut manifest = None;
                let mut versions = None;
                let mut fixity: Option<HashMap<String, HashMap<String, Vec<String>>>> = None;

                let mut id_failed = false;
                let mut type_failed = false;
                let mut digest_failed = false;
                let mut head_failed = false;
                let mut manifest_failed = false;
                let mut versions_failed = false;

                let mut data = DigestsAndPaths::new();

                loop {
                    let key = match map.next_key_seed(FieldSeed(&result)) {
                        Ok(None) => break,
                        Ok(Some(key)) => key,
                        Err(e) => {
                            result.error(
                                ErrorCode::E033,
                                format!(
                                    "Inventory contains invalid field: {}",
                                    // TODO verify what this produces
                                    e.to_string()
                                ),
                            );
                            continue;
                        }
                    };

                    match key {
                        Field::Id => {
                            if id.is_some() {
                                duplicate_field(ID_FIELD, &result);
                            } else {
                                match map.next_value::<&str>() {
                                    Ok(value) => {
                                        if URI::try_from(value).is_err() {
                                            result.warn(
                                                WarnCode::W005,
                                                format!("Inventory field 'id' should be a URI. Found: {}", value));
                                        }
                                        id = Some(value.to_string());
                                    }
                                    Err(_) => {
                                        result.error(
                                            ErrorCode::E037,
                                            "Inventory field 'id' must be a string".to_string(),
                                        );
                                        id_failed = true;
                                    }
                                }
                            }
                        }
                        Field::Type => {
                            if type_declaration.is_some() {
                                duplicate_field(TYPE_FIELD, &result);
                            } else {
                                match map.next_value() {
                                    Ok(value) => type_declaration = Some(value),
                                    Err(_) => {
                                        result.error(
                                            ErrorCode::E038,
                                            "Inventory field 'type' must be a URI".to_string(),
                                        );
                                        type_failed = true;
                                    }
                                }
                            }
                        }
                        Field::DigestAlgorithm => {
                            if digest_algorithm.is_some() {
                                duplicate_field(DIGEST_ALGORITHM_FIELD, &result);
                            } else {
                                match map.next_value::<&str>() {
                                    Ok(value) => match DigestAlgorithm::from_str(value) {
                                        Ok(algorithm) => {
                                            if algorithm != DigestAlgorithm::Sha512
                                                && algorithm != DigestAlgorithm::Sha256
                                            {
                                                result.error(
                                                        ErrorCode::E025,
                                                        format!("Inventory field 'digestAlgorithm' must be 'sha512' or 'sha256. Found: {}", value),
                                                    );
                                                digest_failed = true;
                                            } else {
                                                if algorithm == DigestAlgorithm::Sha256 {
                                                    result.warn(
                                                            WarnCode::W004,
                                                            format!("Inventory field 'digestAlgorithm' should be 'sha512'. Found: {}", value),
                                                        );
                                                }
                                                digest_algorithm = Some(algorithm);
                                            }
                                        }
                                        Err(_) => {
                                            result.error(
                                                    ErrorCode::E025,
                                                    format!("Inventory field 'digestAlgorithm' must be 'sha512' or 'sha256. Found: {}", value),
                                                );
                                            digest_failed = true;
                                        }
                                    },
                                    Err(_) => {
                                        result.error(
                                            ErrorCode::E033,
                                            "Inventory field 'digestAlgorithm' must be a string"
                                                .to_string(),
                                        );
                                        digest_failed = true;
                                    }
                                }
                            }
                        }
                        Field::Head => {
                            if head.is_some() {
                                duplicate_field(HEAD_FIELD, &result);
                            } else {
                                match map.next_value::<&str>() {
                                    Ok(value) => {
                                        match VersionNum::try_from(value) {
                                            Ok(num) => head = Some(num),
                                            Err(_) => {
                                                // TODO this is not the right code https://github.com/OCFL/spec/issues/532
                                                result.error(
                                                    ErrorCode::E011,
                                                    format!("Inventory field 'head' must be a valid version number. Found: {}", value),
                                                );
                                                head_failed = true;
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        result.error(
                                            ErrorCode::E040,
                                            "Inventory field 'head' must be a string".to_string(),
                                        );
                                        head_failed = true;
                                    }
                                }
                            }
                        }
                        Field::ContentDirectory => {
                            if content_directory.is_some() {
                                duplicate_field(CONTENT_DIRECTORY_FIELD, &result);
                            } else {
                                match map.next_value::<&str>() {
                                    Ok(value) => {
                                        if value.eq(".") || value.eq("..") {
                                            result.error(ErrorCode::E018,
                                                         format!("Inventory field 'contentDirectory' cannot equal '.' or '..'. Found: {}", value));
                                        } else if value.contains('/') {
                                            result.error(ErrorCode::E017,
                                                         format!("Inventory field 'contentDirectory' cannot contain '/'. Found: {}", value));
                                        } else {
                                            content_directory = Some(value.to_string());
                                        }
                                    }
                                    Err(_) => {
                                        result.error(
                                            ErrorCode::E033,
                                            "Inventory field 'contentDirectory' must be a string"
                                                .to_string(),
                                        );
                                    }
                                }
                            }
                        }
                        Field::Manifest => {
                            if manifest.is_some() {
                                duplicate_field(MANIFEST_FIELD, &result);
                            } else {
                                match map.next_value_seed(ManifestSeed {
                                    data: &mut data,
                                    result: &result,
                                }) {
                                    Ok(value) => manifest = Some(value),
                                    Err(_) => {
                                        result.error(
                                            ErrorCode::E033,
                                            "Inventory field 'manifest' must be an object"
                                                .to_string(),
                                        );
                                        manifest_failed = true;
                                    }
                                }
                            }
                        }
                        Field::Versions => {
                            if versions.is_some() {
                                duplicate_field(VERSIONS_FIELD, &result);
                            } else {
                                match map.next_value_seed(VersionsSeed {
                                    data: &mut data,
                                    result: &result,
                                }) {
                                    Ok(value) => versions = Some(value),
                                    Err(_) => {
                                        result.error(
                                            ErrorCode::E044,
                                            "Inventory field 'versions' must be an object"
                                                .to_string(),
                                        );
                                        versions_failed = true;
                                    }
                                }
                            }
                        }
                        Field::Fixity => {
                            if fixity.is_some() {
                                duplicate_field(FIXITY_FIELD, &result);
                            } else {
                                match map.next_value() {
                                    Ok(value) => fixity = Some(value),
                                    Err(_) => {
                                        result.error(
                                            ErrorCode::E057,
                                            "Inventory field 'fixity' is not structured correctly"
                                                .to_string(),
                                        );
                                    }
                                }
                            }
                        }
                        // TODO do I need to explicitly skip the value when unknown?
                        Field::Unknown => (),
                    }
                }

                if id.is_none() && !id_failed {
                    missing_inv_field(ID_FIELD, &result);
                }
                if type_declaration.is_none() && !type_failed {
                    missing_inv_field(TYPE_FIELD, &result);
                }
                if digest_algorithm.is_none() && !digest_failed {
                    missing_inv_field(DIGEST_ALGORITHM_FIELD, &result);
                }
                if head.is_none() && !head_failed {
                    missing_inv_field(HEAD_FIELD, &result);
                }
                if manifest.is_none() && !manifest_failed {
                    missing_inv_field_2(MANIFEST_FIELD, &result);
                }
                if versions.is_none() && !versions_failed {
                    missing_inv_field_2(VERSIONS_FIELD, &result);
                }

                if let (Some(head), Some(versions)) = (&head, &versions) {
                    if !versions.contains_key(head) {
                        result.error(
                            ErrorCode::E010,
                            format!("Inventory field 'versions' is missing version '{}'", head),
                        )
                    }

                    if let Some(highest_version) = versions.keys().rev().next() {
                        if head != highest_version {
                            result.error(
                                ErrorCode::E040,
                                format!(
                                    "Inventory field 'head' references '{}' but '{}' was expected",
                                    head, highest_version
                                ),
                            );
                        }
                    }
                }

                if let (Some(algorithm), Some(manifest)) = (digest_algorithm, &manifest) {
                    for (digest, _) in manifest.iter_id_paths() {
                        if !validate_digest(algorithm, (**digest).as_ref()) {
                            result.error(
                                ErrorCode::E096,
                                format!("Inventory manifest contains invalid digest: {}", digest),
                            );
                        }
                    }
                }

                // TODO validate that every manifest entry is in a version state: https://github.com/OCFL/spec/issues/537

                if let (Some(manifest), Some(versions)) = (&manifest, &versions) {
                    for (num, version) in versions {
                        for (_, digest) in version.state_iter() {
                            if !manifest.contains_id(digest) {
                                result.error(
                                    ErrorCode::E050,
                                    format!("Inventory version {} state contains a digest not present in the manifest. Found: {}",
                                            num, digest),
                                );
                            }
                        }
                    }
                }

                validate_fixity(&fixity, &manifest, &result);

                if result.has_errors() {
                    Ok(ParseResult::Error(result))
                } else {
                    Ok(ParseResult::Ok(
                        result,
                        Inventory::new(
                            id.unwrap(),
                            type_declaration.unwrap(),
                            digest_algorithm.unwrap(),
                            head.unwrap(),
                            content_directory,
                            manifest.unwrap(),
                            versions.unwrap(),
                            fixity,
                        )
                        .unwrap(),
                    ))
                }
            }
        }

        deserializer.deserialize_struct("Inventory", INVENTORY_FIELDS, InventoryVisitor)
    }
}

struct VersionsSeed<'a, 'b> {
    data: &'a mut DigestsAndPaths<'b>,
    result: &'a ValidationResult,
}

impl<'de: 'b, 'a, 'b> DeserializeSeed<'de> for VersionsSeed<'a, 'b> {
    type Value = BTreeMap<VersionNum, Version>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VersionsVisitor<'a, 'b> {
            data: &'a mut DigestsAndPaths<'b>,
            result: &'a ValidationResult,
        }

        impl<'de: 'b, 'a, 'b> Visitor<'de> for VersionsVisitor<'a, 'b> {
            type Value = BTreeMap<VersionNum, Version>;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("a map of OCFL version objects")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut versions = BTreeMap::new();
                let mut invalid_versions = HashSet::new();

                loop {
                    match map.next_key() {
                        Ok(None) => break,
                        Ok(Some(version_num)) => {
                            let num = match VersionNum::try_from(version_num) {
                                Ok(num) => Some(num),
                                Err(_) => {
                                    self.result.error(
                                        ErrorCode::E046,
                                        format!("Inventory field 'versions' contains an invalid version number. Found: {}", version_num),
                                    );
                                    None
                                }
                            };

                            match map.next_value_seed(VersionSeed {
                                data: self.data,
                                result: self.result,
                                version: version_num,
                            }) {
                                Ok(version) => {
                                    if let Some(num) = num {
                                        versions.insert(num, version);
                                    }
                                }
                                Err(_) => {
                                    if let Some(num) = num {
                                        invalid_versions.insert(num);
                                    }
                                    self.result.error(
                                        ErrorCode::E047,
                                        "Inventory field 'versions' contains a version that is not an object"
                                            .to_string(),
                                    );
                                }
                            }
                        }
                        Err(_) => {
                            self.result.error(
                                ErrorCode::E046,
                                "Inventory field 'versions' contains a key that is not a string"
                                    .to_string(),
                            );
                        }
                    }
                }

                validate_version_nums(&versions, &invalid_versions, self.result);

                Ok(versions)
            }
        }

        deserializer.deserialize_map(VersionsVisitor {
            data: self.data,
            result: self.result,
        })
    }
}

struct VersionSeed<'a, 'b, 'c> {
    data: &'a mut DigestsAndPaths<'b>,
    result: &'a ValidationResult,
    version: &'c str,
}

impl<'de: 'b, 'a, 'b, 'c> DeserializeSeed<'de> for VersionSeed<'a, 'b, 'c> {
    type Value = Version;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            Created,
            State,
            User,
            Message,
            Unknown,
        }

        struct FieldSeed<'a, 'b> {
            result: &'a ValidationResult,
            version: &'b str,
        }

        impl<'de, 'a, 'b> DeserializeSeed<'de> for FieldSeed<'a, 'b> {
            type Value = Field;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor<'a, 'b> {
                    result: &'a ValidationResult,
                    version: &'b str,
                }

                impl<'de, 'a, 'b> Visitor<'de> for FieldVisitor<'a, 'b> {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                        formatter.write_str("an OCFL version field")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                    where
                        E: SerdeError,
                    {
                        match value {
                            CREATED_FIELD => Ok(Field::Created),
                            STATE_FIELD => Ok(Field::State),
                            MESSAGE_FIELD => Ok(Field::Message),
                            USER_FIELD => Ok(Field::User),
                            _ => {
                                unknown_version_field(value, self.version, self.result);
                                Ok(Field::Unknown)
                            }
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor {
                    result: self.result,
                    version: self.version,
                })
            }
        }

        struct VersionVisitor<'a, 'b, 'c> {
            data: &'a mut DigestsAndPaths<'b>,
            result: &'a ValidationResult,
            version: &'c str,
        }

        impl<'de: 'b, 'a, 'b, 'c> Visitor<'de> for VersionVisitor<'a, 'b, 'c> {
            type Value = Version;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("an OCFL version object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut created = None;
                let mut message = None;
                let mut user = None;
                let mut state: Option<PathBiMap<LogicalPath>> = None;

                let mut created_failed = false;
                let mut state_failed = false;
                let mut message_failed = false;
                let mut user_failed = false;

                loop {
                    let key = match map.next_key_seed(FieldSeed {
                        result: self.result,
                        version: self.version,
                    }) {
                        Ok(None) => break,
                        Ok(Some(key)) => key,
                        Err(_) => {
                            self.result.error(
                                ErrorCode::E033,
                                format!(
                                    "Inventory version {} contains an invalid field",
                                    self.version
                                ),
                            );
                            continue;
                        }
                    };

                    match key {
                        Field::Created => {
                            if created.is_some() {
                                duplicate_version_field(CREATED_FIELD, self.version, self.result);
                            } else {
                                match map.next_value::<&str>() {
                                    Ok(value) => match DateTime::parse_from_rfc3339(value) {
                                        Ok(value) => created = Some(value.with_timezone(&Local)),
                                        Err(_) => {
                                            self.result.error(ErrorCode::E049,
                                                              format!("Inventory version {} field 'created' must be an RFC3339 formatted date. Found: {}",
                                                                      self.version, value));
                                            created_failed = true;
                                        }
                                    },
                                    Err(_) => {
                                        self.result.error(ErrorCode::E049,
                                                          format!("Inventory version {} field 'created' must be a string", self.version));
                                        created_failed = true;
                                    }
                                }
                            }
                        }
                        Field::State => {
                            if state.is_some() {
                                duplicate_version_field(STATE_FIELD, self.version, self.result);
                            } else {
                                match map.next_value_seed(StateSeed {
                                    data: self.data,
                                    result: self.result,
                                    version: self.version,
                                }) {
                                    Ok(value) => state = Some(value),
                                    Err(_) => {
                                        self.result.error(ErrorCode::E050,
                                                          format!("Inventory version {} field 'state' must be an object", self.version));
                                        state_failed = true;
                                    }
                                }
                            }
                        }
                        Field::User => {
                            if user.is_some() {
                                duplicate_version_field(USER_FIELD, self.version, self.result);
                            } else {
                                match map.next_value() {
                                    Ok(value) => user = Some(value),
                                    Err(_) => {
                                        self.result.error(ErrorCode::E054,
                                                          format!("Inventory version {} field 'user' must be an object", self.version));
                                        user_failed = true;
                                    }
                                }
                            }
                        }
                        Field::Message => {
                            if message.is_some() {
                                duplicate_version_field(MESSAGE_FIELD, self.version, self.result);
                            } else {
                                match map.next_value() {
                                    Ok(value) => message = Some(value),
                                    Err(_) => {
                                        self.result.error(ErrorCode::E094,
                                                          format!("Inventory version {} field 'message' must be a string", self.version));
                                        message_failed = true;
                                    }
                                }
                            }
                        }
                        Field::Unknown => (),
                    }
                }

                if created.is_none() && !created_failed {
                    missing_version_field(CREATED_FIELD, self.version, self.result);
                }
                if state.is_none() && !state_failed {
                    missing_version_field(STATE_FIELD, self.version, self.result);
                }
                if message.is_none() && !message_failed {
                    missing_version_field_warn(MESSAGE_FIELD, self.version, self.result);
                }
                if user.is_none() && !user_failed {
                    missing_version_field_warn(USER_FIELD, self.version, self.result);
                }

                Ok(Version::new(
                    created.unwrap(),
                    state.unwrap(),
                    message,
                    user,
                ))
            }
        }

        deserializer.deserialize_struct(
            "Version",
            VERSION_FIELDS,
            VersionVisitor {
                data: self.data,
                result: self.result,
                version: self.version,
            },
        )
    }
}

struct ManifestSeed<'a, 'b> {
    data: &'a mut DigestsAndPaths<'b>,
    result: &'a ValidationResult,
}

impl<'de: 'b, 'a, 'b> DeserializeSeed<'de> for ManifestSeed<'a, 'b> {
    type Value = PathBiMap<ContentPath>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ManifestVisitor<'a, 'b> {
            data: &'a mut DigestsAndPaths<'b>,
            result: &'a ValidationResult,
        }

        impl<'de: 'b, 'a, 'b> Visitor<'de> for ManifestVisitor<'a, 'b> {
            type Value = PathBiMap<ContentPath>;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("an OCFL inventory manifest map")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut manifest = PathBiMap::with_capacity(map.size_hint().unwrap_or(0));
                let mut all_paths = HashSet::with_capacity(map.size_hint().unwrap_or(0));

                loop {
                    match map.next_key() {
                        Ok(None) => break,
                        Ok(Some(digest)) => match map.next_value::<Vec<&str>>() {
                            Ok(paths) => {
                                let mut content_paths = Vec::with_capacity(paths.len());

                                for path in paths {
                                    if path.starts_with('/') || path.ends_with('/') {
                                        self.result.error(ErrorCode::E100,
                                                              format!("Inventory manifest key '{}' contains a content path with a leading/trailing '/'. Found: {}",
                                                                      digest, path));
                                    } else {
                                        match ContentPath::try_from(path) {
                                            Ok(content_path) => content_paths.push(content_path),
                                            Err(_) => {
                                                self.result.error(ErrorCode::E099,
                                                                      format!("Inventory manifest key '{}' contains a content path containing an illegal path part. Found: {}",
                                                                              digest, path));
                                            }
                                        }
                                    }

                                    if all_paths.contains(path) {
                                        self.result.error(ErrorCode::E101,
                                                          format!("Inventory manifest contains a duplicate content path. Found: {}",
                                                                  path));
                                    } else {
                                        all_paths.insert(path);
                                    }
                                }

                                let path_refs: Vec<Rc<ContentPath>> =
                                    content_paths.into_iter().map(Rc::new).collect();
                                let digest_ref = self.data.insert_digest(digest);

                                if manifest.contains_id(&digest_ref) {
                                    self.result.error(
                                        ErrorCode::E096,
                                        format!(
                                            "Inventory manifest contains a duplicate key '{}'",
                                            digest
                                        ),
                                    );
                                }

                                manifest.insert_multiple_rc(digest_ref, path_refs);
                            }
                            Err(_) => {
                                self.result.error(ErrorCode::E092,
                                                      format!("Inventory manifest key '{}' must reference an array of strings", digest));
                            }
                        },
                        Err(_) => {
                            self.result.error(
                                ErrorCode::E096,
                                "Inventory manifest contains a key that is not a string"
                                    .to_string(),
                            );
                        }
                    }
                }

                validate_non_conflicting(&all_paths, |path, part| {
                    self.result.error(
                        ErrorCode::E101,
                        format!("Inventory manifest contains a path, '{}', that conflicts with another path '{}'",
                                path, part));
                });

                Ok(manifest)
            }
        }

        deserializer.deserialize_map(ManifestVisitor {
            data: self.data,
            result: self.result,
        })
    }
}

struct StateSeed<'a, 'b, 'c> {
    data: &'a mut DigestsAndPaths<'b>,
    result: &'a ValidationResult,
    version: &'c str,
}

impl<'de: 'b, 'a, 'b, 'c> DeserializeSeed<'de> for StateSeed<'a, 'b, 'c> {
    type Value = PathBiMap<LogicalPath>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StateVisitor<'a, 'b, 'c> {
            data: &'a mut DigestsAndPaths<'b>,
            result: &'a ValidationResult,
            version: &'c str,
        }

        impl<'de: 'b, 'a, 'b, 'c> Visitor<'de> for StateVisitor<'a, 'b, 'c> {
            type Value = PathBiMap<LogicalPath>;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("an OCFL version state map")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut state = PathBiMap::with_capacity(map.size_hint().unwrap_or(0));
                let mut all_paths = HashSet::with_capacity(map.size_hint().unwrap_or(0));

                loop {
                    match map.next_key() {
                        Ok(None) => break,
                        Ok(Some(digest)) => match map.next_value::<Vec<&str>>() {
                            Ok(paths) => {
                                let digest_ref = self.data.insert_digest(digest);
                                let mut path_refs = Vec::with_capacity(paths.len());

                                for path in paths {
                                    if path.starts_with('/') || path.ends_with('/') {
                                        self.result.error(ErrorCode::E053,
                                                              format!("Inventory version {} state key '{}' contains a logical path with a leading/trailing '/'. Found: {}",
                                                                      self.version, digest, path));
                                    } else {
                                        match self.data.insert_path::<A::Error>(path) {
                                            Ok(logical_path) => path_refs.push(logical_path),
                                            Err(_) => {
                                                self.result.error(ErrorCode::E052,
                                                                      format!("Inventory version {} state key '{}' contains a logical path containing an illegal path part. Found: {}",
                                                                              self.version, digest, path));
                                            }
                                        }
                                    }

                                    if all_paths.contains(path) {
                                        self.result.error(ErrorCode::E095,
                                                          format!("Inventory version {} state contains a duplicate logical path. Found: {}",
                                                                  self.version, path));
                                    } else {
                                        all_paths.insert(path);
                                    }
                                }

                                state.insert_multiple_rc(digest_ref, path_refs);
                            }
                            Err(_) => {
                                self.result.error(ErrorCode::E051,
                                                      format!("Inventory version {} state key '{}' must reference an array of strings",
                                                              self.version, digest));
                            }
                        },
                        Err(_) => {
                            self.result.error(
                                ErrorCode::E050,
                                format!("Inventory version {} state contains a key that is not a string", self.version));
                        }
                    }
                }

                validate_non_conflicting(&all_paths, |path, part| {
                    self.result.error(
                        ErrorCode::E095,
                        format!("Inventory version {} state contains a path, '{}', that conflicts with another path '{}'",
                                self.version, path, part));
                });

                Ok(state)
            }
        }

        deserializer.deserialize_map(StateVisitor {
            data: self.data,
            result: self.result,
            version: self.version,
        })
    }
}

struct UserSeed<'a, 'b> {
    result: &'a ValidationResult,
    version: &'b str,
}

impl<'de, 'a, 'b> DeserializeSeed<'de> for UserSeed<'a, 'b> {
    type Value = User;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            Name,
            Address,
            Unknown,
        }

        struct FieldSeed<'a, 'b> {
            result: &'a ValidationResult,
            version: &'b str,
        }

        impl<'de, 'a, 'b> DeserializeSeed<'de> for FieldSeed<'a, 'b> {
            type Value = Field;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor<'a, 'b> {
                    result: &'a ValidationResult,
                    version: &'b str,
                }

                impl<'de, 'a, 'b> Visitor<'de> for FieldVisitor<'a, 'b> {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                        formatter.write_str("an OCFL user field")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                    where
                        E: SerdeError,
                    {
                        match value {
                            NAME_FIELD => Ok(Field::Name),
                            ADDRESS_FIELD => Ok(Field::Address),
                            _ => {
                                unknown_version_field(value, self.version, self.result);
                                Ok(Field::Unknown)
                            }
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor {
                    result: self.result,
                    version: self.version,
                })
            }
        }

        struct UserVisitor<'a, 'b> {
            result: &'a ValidationResult,
            version: &'b str,
        }

        impl<'de, 'a, 'b> Visitor<'de> for UserVisitor<'a, 'b> {
            type Value = User;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("an OCFL user object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut name = None;
                let mut address = None;

                let mut name_failed = false;
                let mut address_failed = false;

                loop {
                    let key = match map.next_key_seed(FieldSeed {
                        result: self.result,
                        version: self.version,
                    }) {
                        Ok(None) => break,
                        Ok(Some(key)) => key,
                        Err(_) => {
                            self.result.error(
                                ErrorCode::E033,
                                format!(
                                    "Inventory version {} user contains an invalid field",
                                    self.version
                                ),
                            );
                            continue;
                        }
                    };

                    match key {
                        Field::Name => {
                            if name.is_some() {
                                duplicate_version_field(NAME_FIELD, self.version, self.result);
                            } else {
                                match map.next_value() {
                                    Ok(value) => name = Some(value),
                                    Err(_) => {
                                        self.result.error(ErrorCode::E033,
                                                          format!("Inventory version {} user field 'name' must be a string", self.version));
                                        name_failed = true;
                                    }
                                }
                            }
                        }
                        Field::Address => {
                            if address.is_some() {
                                duplicate_version_field(ADDRESS_FIELD, self.version, self.result);
                            } else {
                                match map.next_value::<&str>() {
                                    Ok(value) => {
                                        if URI::try_from(value).is_err() {
                                            self.result.warn(WarnCode::W008,
                                                              format!("Inventory version {} user field 'address' should be a URI. Found: {}",
                                                                      self.version, value));
                                        }
                                        address = Some(value.to_string());
                                    }
                                    Err(_) => {
                                        self.result.error(ErrorCode::E033,
                                                          format!("Inventory version {} user field 'address' must be a string", self.version));
                                        address_failed = true;
                                    }
                                }
                            }
                        }
                        Field::Unknown => (),
                    }
                }

                if name.is_none() && !name_failed {
                    self.result.error(
                        ErrorCode::E054,
                        format!(
                            "Inventory version '{}' is missing required field '{}'",
                            self.version, NAME_FIELD
                        ),
                    );
                }
                if address.is_none() && !address_failed {
                    self.result.warn(
                        WarnCode::W008,
                        format!(
                            "Inventory version '{}' is missing recommended field '{}'",
                            self.version, ADDRESS_FIELD
                        ),
                    );
                }

                Ok(User::new(name.unwrap(), address))
            }
        }

        deserializer.deserialize_struct(
            "User",
            USER_FIELDS,
            UserVisitor {
                result: self.result,
                version: self.version,
            },
        )
    }
}

#[derive(Debug)]
struct DigestsAndPaths<'a> {
    digests: HashMap<&'a str, Rc<HexDigest>>,
    paths: HashMap<&'a str, Rc<LogicalPath>>,
}

impl<'a> DigestsAndPaths<'a> {
    fn new() -> Self {
        Self {
            digests: HashMap::new(),
            paths: HashMap::new(),
        }
    }

    fn insert_digest(&mut self, digest: &'a str) -> Rc<HexDigest> {
        self.digests
            .entry(digest)
            .or_insert_with(|| Rc::new(digest.into()))
            .clone()
    }

    fn insert_path<E>(&mut self, path: &'a str) -> Result<Rc<LogicalPath>, E>
    where
        E: SerdeError,
    {
        match self.paths.entry(path) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(vacant) => {
                let path =
                    LogicalPath::try_from(path).map_err(|e| SerdeError::custom(e.to_string()))?;
                let path_rc = Rc::new(path);
                let clone = path_rc.clone();
                vacant.insert(path_rc);
                Ok(clone)
            }
        }
    }
}

fn validate_version_nums(
    versions: &BTreeMap<VersionNum, Version>,
    invalid_versions: &HashSet<VersionNum>,
    result: &ValidationResult,
) {
    let mut padding = None;
    let mut consistent_padding = true;
    let mut next_version = VersionNum::v1();

    for version in versions.keys() {
        match padding {
            None => padding = Some(version.width),
            Some(padding) => {
                if consistent_padding && padding != version.width {
                    consistent_padding = false;
                }
            }
        }

        if *version != next_version && !invalid_versions.contains(version) {
            result.error(
                ErrorCode::E010,
                format!(
                    "Inventory field 'versions' is missing version '{}'",
                    next_version
                ),
            );
        }

        next_version = next_version.next().unwrap();
    }

    if !consistent_padding {
        result.error(
            ErrorCode::E013,
            "Inventory field 'versions' contains inconsistently padded version numbers".to_string(),
        );
    }
}

fn validate_fixity(
    fixity: &Option<HashMap<String, HashMap<String, Vec<String>>>>,
    manifest: &Option<PathBiMap<ContentPath>>,
    result: &ValidationResult,
) {
    if let Some(fixity) = fixity {
        for (algorithm, fixity_manifest) in fixity {
            if let Ok(algorithm) = DigestAlgorithm::from_str(algorithm) {
                for digest in fixity_manifest.keys() {
                    if !validate_digest(algorithm, digest) {
                        result.error(
                            ErrorCode::E057,
                            format!(
                                "Inventory fixity block '{}' contains invalid digest. Found: {}",
                                algorithm.to_string(),
                                digest
                            ),
                        );
                    }
                }
            }

            let mut all_paths = Vec::with_capacity(fixity_manifest.len());
            let mut all_digests = Vec::with_capacity(fixity_manifest.len());

            if let Some(manifest) = manifest {
                for (digest, paths) in fixity_manifest {
                    if all_digests.contains(&digest) {
                        result.error(
                            ErrorCode::E097,
                            format!("Inventory fixity block '{}' contains a duplicate digest. Found: {}",
                                    algorithm.to_string(), digest),
                        );
                    } else {
                        all_digests.push(digest);
                    }

                    for path in paths {
                        if all_paths.contains(&path) {
                            result.error(
                                ErrorCode::E101,
                                format!("Inventory fixity block '{}' contains a duplicate content path. Found: {}",
                                        algorithm.to_string(), path),
                            );

                            continue;
                        }

                        all_paths.push(path);

                        if let Ok(content_path) = ContentPath::try_from(path) {
                            if !manifest.contains_path(&content_path) {
                                result.error(
                                    ErrorCode::E057,
                                    format!("Inventory fixity block '{}' contains a content path not present in the manifest. Found: {}",
                                            algorithm.to_string(), path),
                                );
                            }
                        } else {
                            result.error(
                                ErrorCode::E099,
                                format!("Inventory fixity block '{}' contains a content path containing an illegal path part. Found: {}",
                                        algorithm.to_string(), path),
                            );
                        }
                    }
                }
            }
        }
    }
}

fn validate_non_conflicting<F>(paths: &HashSet<&str>, error: F)
where
    F: Fn(&str, &str),
{
    for path in paths {
        let mut part = *path;
        while let Some(index) = part.rfind('/') {
            part = &part[0..index];
            if paths.contains(part) {
                error(path, part);
                break;
            }
        }
    }
}

fn validate_digest(algorithm: DigestAlgorithm, digest: &str) -> bool {
    match algorithm {
        DigestAlgorithm::Md5 => MD5_REGEX.is_match(digest),
        DigestAlgorithm::Sha1 => SHA1_REGEX.is_match(digest),
        DigestAlgorithm::Sha256 => SHA256_REGEX.is_match(digest),
        DigestAlgorithm::Sha512 => SHA512_REGEX.is_match(digest),
        DigestAlgorithm::Sha512_256 => SHA512_256_REGEX.is_match(digest),
        DigestAlgorithm::Blake2b512 => BLAKE2B_512_REGEX.is_match(digest),
        DigestAlgorithm::Blake2b160 => BLAKE2B_160_REGEX.is_match(digest),
        DigestAlgorithm::Blake2b256 => BLAKE2B_256_REGEX.is_match(digest),
        DigestAlgorithm::Blake2b384 => BLAKE2B_384_REGEX.is_match(digest),
    }
}

fn duplicate_field(field: &str, result: &ValidationResult) {
    result.error(
        ErrorCode::E033,
        format!("Inventory contains duplicate field '{}'", field),
    );
}

fn duplicate_version_field(field: &str, version: &str, result: &ValidationResult) {
    result.error(
        ErrorCode::E033,
        format!(
            "Inventory version '{}' contains duplicate field '{}'",
            version, field
        ),
    );
}

fn unknown_field(field: &str, result: &ValidationResult) {
    result.error(
        ErrorCode::E102,
        format!("Inventory contains unknown field '{}'", field),
    );
}

fn unknown_version_field(field: &str, version: &str, result: &ValidationResult) {
    result.error(
        ErrorCode::E102,
        format!(
            "Inventory version '{}' contains unknown field '{}'",
            version, field
        ),
    );
}

fn missing_inv_field(field: &str, result: &ValidationResult) {
    result.error(
        ErrorCode::E036,
        format!("Inventory is missing required field '{}'", field),
    );
}

fn missing_inv_field_2(field: &str, result: &ValidationResult) {
    result.error(
        ErrorCode::E041,
        format!("Inventory is missing required field '{}'", field),
    );
}

fn missing_version_field(field: &str, version: &str, result: &ValidationResult) {
    result.error(
        ErrorCode::E048,
        format!(
            "Inventory version '{}' is missing required field '{}'",
            version, field
        ),
    );
}

fn missing_version_field_warn(field: &str, version: &str, result: &ValidationResult) {
    result.warn(
        WarnCode::W007,
        format!(
            "Inventory version '{}' is missing recommended field '{}'",
            version, field
        ),
    );
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::ocfl::validate::ParseResult;
    use crate::ocfl::{Result, RocflError};

    #[test]
    fn asdf() -> Result<()> {
        let json = json!({
            "id": "test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            "head": "v1",
            "contentDirectory": "content",
            "manifest": {
                "fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455": [
                    "v1/content/file1.txt"
                ]
            },
            "versions": {
                "v1": {
                    "created": "2021-09-05T20:36:50.923505656-05:00",
                    "state": {
                        "fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455": [
                            "file1.txt"
                        ]
                    },
                    "message": "initial commit",
                    "user": {
                        "name": "Peter Winckles",
                        "address": "mailto:me@example.com"
                    }
                }
            },
            "fixity": {
                "md5": {
                    "184f84e28cbe75e050e9c25ea7f2e939": [
                        "v1/content/file1.txt"
                    ]
                }
            }
        }).to_string();

        let result: ParseResult = serde_json::from_str(&json)?;
        // TODO add a print method that takes the object id and object relative path to the inv
        println!("{:?}", result);

        Ok(())
    }
}

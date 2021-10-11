use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::convert::TryFrom;
use std::fmt::Formatter;
use std::rc::Rc;
use std::str::FromStr;

use chrono::{DateTime, Local};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::de::{DeserializeSeed, Error as SerdeError, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use uriparse::URI;

use crate::ocfl::bimap::PathBiMap;
use crate::ocfl::digest::HexDigest;
use crate::ocfl::inventory::{Inventory, User, Version};
use crate::ocfl::serde::{
    ADDRESS_FIELD, CONTENT_DIRECTORY_FIELD, CREATED_FIELD, DIGEST_ALGORITHM_FIELD, FIXITY_FIELD,
    HEAD_FIELD, ID_FIELD, INVENTORY_FIELDS, MANIFEST_FIELD, MESSAGE_FIELD, NAME_FIELD, STATE_FIELD,
    TYPE_FIELD, USER_FIELD, USER_FIELDS, VERSIONS_FIELD, VERSION_FIELDS,
};
use crate::ocfl::validate::{ErrorCode, ParseResult, ParseValidationResult, WarnCode};
use crate::ocfl::{ContentPath, DigestAlgorithm, LogicalPath, VersionNum};

const ERROR_MARKER: &str = "ROCFL";

static MD5_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{32}$"#).unwrap());
static SHA1_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{40}$"#).unwrap());
static SHA256_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{64}$"#).unwrap());
static SHA512_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{128}$"#).unwrap());
static BLAKE2B_160_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{40}$"#).unwrap());
static BLAKE2B_256_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{64}$"#).unwrap());
static BLAKE2B_384_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{96}$"#).unwrap());
static BLAKE2B_512_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{128}$"#).unwrap());
static SHA512_256_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^[a-fA-F0-9]{64}$"#).unwrap());

thread_local!(static PARSE_RESULT: RefCell<ParseValidationResult> = RefCell::new(ParseValidationResult::new()));

/// Deserializes and validates the inventory json. If the inventory is valid, an `Inventory` object
/// is returned; otherwise a list of the problems encountered is returned.
pub(super) fn parse(bytes: &[u8]) -> ParseResult {
    fn take_result() -> ParseValidationResult {
        PARSE_RESULT.with(|result| result.take())
    }

    match serde_json::from_slice::<OptionWrapper<Inventory>>(bytes) {
        Ok(OptionWrapper(Some(inventory))) => ParseResult::Ok(take_result(), inventory),
        Ok(_) => ParseResult::Error(take_result()),
        Err(e) => {
            let result = take_result();
            if !e.is_data() {
                result.error(
                    ErrorCode::E033,
                    format!("Inventory could not be parsed: {}", e),
                );
            }
            ParseResult::Error(result)
        }
    }
}

#[derive(Debug)]
struct OptionWrapper<T>(Option<T>);

impl<'de> Deserialize<'de> for OptionWrapper<Inventory> {
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

        struct FieldSeed<'a>(&'a ParseValidationResult);

        impl<'de, 'a> DeserializeSeed<'de> for FieldSeed<'a> {
            type Value = Field;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor<'a>(&'a ParseValidationResult);

                impl<'de, 'a> Visitor<'de> for FieldVisitor<'a> {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                        formatter.write_str(ERROR_MARKER)
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

        struct InventoryVisitor<'a> {
            result: &'a ParseValidationResult,
        }

        impl<'de, 'a> Visitor<'de> for InventoryVisitor<'a> {
            type Value = Option<Inventory>;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str(ERROR_MARKER)
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut id = None;
                let mut type_declaration = None;
                let mut digest_algorithm = None;
                let mut head: Option<VersionNum> = None;
                let mut content_directory = None;
                let mut manifest = None;
                let mut versions = None;
                let mut fixity: Option<HashMap<String, HashMap<String, Vec<String>>>> = None;

                let mut digest_failed = false;
                let mut head_failed = false;
                let mut manifest_failed = false;
                let mut versions_failed = false;

                let mut data = DigestsAndPaths::new();

                loop {
                    let key = match map.next_key_seed(FieldSeed(self.result))? {
                        None => break,
                        Some(key) => key,
                    };

                    match key {
                        Field::Id => {
                            if id.is_some() {
                                duplicate_field(ID_FIELD, self.result);
                                map.next_value::<Value>()?;
                            } else {
                                match map.next_value::<&str>() {
                                    Ok(value) => {
                                        if URI::try_from(value).is_err() {
                                            self.result.warn(
                                                WarnCode::W005,
                                                format!(
                                                    "Inventory 'id' should be a URI. Found: {}",
                                                    value
                                                ),
                                            );
                                        }
                                        id = Some(value.to_string());
                                        self.result.object_id(id.as_ref().unwrap())
                                    }
                                    Err(e) => {
                                        self.result.error(
                                            ErrorCode::E037,
                                            "Inventory 'id' must be a string".to_string(),
                                        );
                                        return Err(e);
                                    }
                                }
                            }
                        }
                        Field::Type => {
                            if type_declaration.is_some() {
                                duplicate_field(TYPE_FIELD, self.result);
                                map.next_value::<Value>()?;
                            } else {
                                match map.next_value() {
                                    Ok(value) => type_declaration = Some(value),
                                    Err(e) => {
                                        self.result.error(
                                            ErrorCode::E038,
                                            "Inventory 'type' must be a URI".to_string(),
                                        );
                                        return Err(e);
                                    }
                                }
                            }
                        }
                        Field::DigestAlgorithm => {
                            if digest_algorithm.is_some() {
                                duplicate_field(DIGEST_ALGORITHM_FIELD, self.result);
                                map.next_value::<Value>()?;
                            } else {
                                match map.next_value::<&str>() {
                                    Ok(value) => match DigestAlgorithm::from_str(value) {
                                        Ok(algorithm) => {
                                            if algorithm != DigestAlgorithm::Sha512
                                                && algorithm != DigestAlgorithm::Sha256
                                            {
                                                self.result.error(
                                                        ErrorCode::E025,
                                                        format!("Inventory 'digestAlgorithm' must be 'sha512' or 'sha256. Found: {}", value),
                                                    );
                                                digest_failed = true;
                                            } else {
                                                if algorithm == DigestAlgorithm::Sha256 {
                                                    self.result.warn(
                                                            WarnCode::W004,
                                                            format!("Inventory 'digestAlgorithm' should be 'sha512'. Found: {}", value),
                                                        );
                                                }
                                                digest_algorithm = Some(algorithm);
                                            }
                                        }
                                        Err(_) => {
                                            self.result.error(
                                                    ErrorCode::E025,
                                                    format!("Inventory 'digestAlgorithm' must be 'sha512' or 'sha256. Found: {}", value),
                                                );
                                            digest_failed = true;
                                        }
                                    },
                                    Err(e) => {
                                        self.result.error(
                                            ErrorCode::E033,
                                            "Inventory 'digestAlgorithm' must be a string"
                                                .to_string(),
                                        );
                                        return Err(e);
                                    }
                                }
                            }
                        }
                        Field::Head => {
                            if head.is_some() {
                                duplicate_field(HEAD_FIELD, self.result);
                                map.next_value::<Value>()?;
                            } else {
                                match map.next_value::<&str>() {
                                    Ok(value) => {
                                        match VersionNum::try_from(value) {
                                            Ok(num) => head = Some(num),
                                            Err(_) => {
                                                // TODO this is not the right code https://github.com/OCFL/spec/issues/532
                                                self.result.error(
                                                    ErrorCode::E011,
                                                    format!("Inventory 'head' must be a valid version number. Found: {}", value),
                                                );
                                                head_failed = true;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        self.result.error(
                                            ErrorCode::E040,
                                            "Inventory 'head' must be a string".to_string(),
                                        );
                                        return Err(e);
                                    }
                                }
                            }
                        }
                        Field::ContentDirectory => {
                            if content_directory.is_some() {
                                duplicate_field(CONTENT_DIRECTORY_FIELD, self.result);
                                map.next_value::<Value>()?;
                            } else {
                                match map.next_value::<&str>() {
                                    Ok(value) => {
                                        if value.eq(".") || value.eq("..") {
                                            self.result.error(ErrorCode::E018,
                                                         format!("Inventory 'contentDirectory' cannot equal '.' or '..'. Found: {}", value));
                                        } else if value.contains('/') {
                                            self.result.error(ErrorCode::E017,
                                                         format!("Inventory 'contentDirectory' cannot contain '/'. Found: {}", value));
                                        } else {
                                            content_directory = Some(value.to_string());
                                        }
                                    }
                                    Err(e) => {
                                        self.result.error(
                                            ErrorCode::E033,
                                            "Inventory 'contentDirectory' must be a string"
                                                .to_string(),
                                        );
                                        return Err(e);
                                    }
                                }
                            }
                        }
                        Field::Manifest => {
                            if manifest.is_some() {
                                duplicate_field(MANIFEST_FIELD, self.result);
                                map.next_value::<Value>()?;
                            } else {
                                match map.next_value_seed(ManifestSeed {
                                    data: &mut data,
                                    result: self.result,
                                }) {
                                    Ok(value) => manifest = Some(value),
                                    Err(e) => {
                                        if e.to_string().contains(ERROR_MARKER) {
                                            self.result.error(
                                                ErrorCode::E033,
                                                "Inventory 'manifest' must be an object"
                                                    .to_string(),
                                            );
                                            manifest_failed = true;
                                        } else {
                                            return Err(e);
                                        }
                                    }
                                }
                            }
                        }
                        Field::Versions => {
                            if versions.is_some() {
                                duplicate_field(VERSIONS_FIELD, self.result);
                                map.next_value::<Value>()?;
                            } else {
                                match map.next_value_seed(VersionsSeed {
                                    data: &mut data,
                                    result: self.result,
                                }) {
                                    Ok(value) => versions = Some(value),
                                    Err(e) => {
                                        if e.to_string().contains(ERROR_MARKER) {
                                            self.result.error(
                                                ErrorCode::E044,
                                                "Inventory 'versions' must be an object"
                                                    .to_string(),
                                            );
                                            versions_failed = true;
                                        } else {
                                            return Err(e);
                                        }
                                    }
                                }
                            }
                        }
                        Field::Fixity => {
                            if fixity.is_some() {
                                duplicate_field(FIXITY_FIELD, self.result);
                                map.next_value::<Value>()?;
                            } else {
                                match map.next_value() {
                                    Ok(value) => fixity = Some(value),
                                    Err(_) => {
                                        self.result.error(
                                            ErrorCode::E057,
                                            "Inventory 'fixity' must be a map of maps of arrays"
                                                .to_string(),
                                        );
                                    }
                                }
                            }
                        }
                        Field::Unknown => {
                            map.next_value::<Value>()?;
                        }
                    }
                }

                if id.is_none() {
                    missing_inv_field(ID_FIELD, self.result);
                }
                if type_declaration.is_none() {
                    missing_inv_field(TYPE_FIELD, self.result);
                }
                if digest_algorithm.is_none() && !digest_failed {
                    missing_inv_field(DIGEST_ALGORITHM_FIELD, self.result);
                }
                if head.is_none() && !head_failed {
                    missing_inv_field(HEAD_FIELD, self.result);
                }
                if manifest.is_none() && !manifest_failed {
                    missing_inv_field_2(MANIFEST_FIELD, self.result);
                }
                if versions.is_none() && !versions_failed {
                    missing_inv_field_2(VERSIONS_FIELD, self.result);
                }

                if let Some(versions) = &versions {
                    if versions.nums.is_empty() {
                        self.result.error(
                            ErrorCode::E008,
                            "Inventory does not contain any valid versions".to_string(),
                        );
                    }
                }

                if let (Some(head), Some(versions)) = (&head, &versions) {
                    if !versions.nums.contains(head) {
                        self.result.error(
                            ErrorCode::E010,
                            format!("Inventory 'versions' is missing version '{}'", head),
                        )
                    }

                    if let Some(highest_version) = versions.nums.iter().rev().next() {
                        if head != highest_version {
                            self.result.error(
                                ErrorCode::E040,
                                format!(
                                    "Inventory 'head' references '{}' but '{}' was expected",
                                    head, highest_version
                                ),
                            );
                        }
                    }
                }

                if let (Some(algorithm), Some(manifest)) = (digest_algorithm, &manifest) {
                    for (digest, _) in manifest.manifest.iter_id_paths() {
                        if !validate_digest(algorithm, (**digest).as_ref()) {
                            self.result.error(
                                ErrorCode::E096,
                                format!("Inventory manifest contains invalid digest: {}", digest),
                            );
                        }
                    }
                }

                // TODO validate that every manifest entry is in a version state: https://github.com/OCFL/spec/issues/537

                if let (Some(manifest), Some(versions)) = (&manifest, &versions) {
                    for (num, version) in &versions.map {
                        for (_, digest) in version.state_iter() {
                            if !manifest.digests.contains(&(**digest).as_ref()) {
                                self.result.error(
                                    ErrorCode::E050,
                                    format!("Inventory version {} state contains a digest that is not present in the manifest. Found: {}",
                                            num, digest),
                                );
                            }
                        }
                    }
                }

                validate_fixity(&fixity, &manifest, self.result);

                if self.result.has_errors() {
                    Ok(None)
                } else {
                    Ok(Some(
                        Inventory::new(
                            id.unwrap(),
                            type_declaration.unwrap(),
                            digest_algorithm.unwrap(),
                            head.unwrap(),
                            content_directory,
                            manifest.unwrap().manifest,
                            versions.unwrap().map,
                            fixity,
                        )
                        .unwrap(),
                    ))
                }
            }
        }

        PARSE_RESULT.with(|result| {
            match deserializer.deserialize_struct(
                "Inventory",
                INVENTORY_FIELDS,
                InventoryVisitor {
                    result: &result.borrow(),
                },
            ) {
                Ok(Some(inventory)) => Ok(OptionWrapper(Some(inventory))),
                Ok(None) => Ok(OptionWrapper(None)),
                Err(e) => Err(e),
            }
        })
    }
}

struct VersionsResult {
    map: BTreeMap<VersionNum, Version>,
    /// These are the version numbers as found in the json that are parsable
    nums: BTreeSet<VersionNum>,
}

struct VersionsSeed<'a, 'b> {
    data: &'a mut DigestsAndPaths<'b>,
    result: &'a ParseValidationResult,
}

impl<'de: 'b, 'a, 'b> DeserializeSeed<'de> for VersionsSeed<'a, 'b> {
    type Value = VersionsResult;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VersionsVisitor<'a, 'b> {
            data: &'a mut DigestsAndPaths<'b>,
            result: &'a ParseValidationResult,
        }

        impl<'de: 'b, 'a, 'b> Visitor<'de> for VersionsVisitor<'a, 'b> {
            type Value = VersionsResult;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str(ERROR_MARKER)
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut versions = BTreeMap::new();
                let mut all_versions = BTreeSet::new();

                loop {
                    match map.next_key()? {
                        None => break,
                        Some(version_num) => {
                            let num = match VersionNum::try_from(version_num) {
                                Ok(num) => {
                                    all_versions.insert(num);
                                    Some(num)
                                }
                                Err(_) => {
                                    self.result.error(
                                        ErrorCode::E046,
                                        format!("Inventory 'versions' contains an invalid version number. Found: {}", version_num),
                                    );
                                    None
                                }
                            };

                            match map.next_value_seed(VersionSeed {
                                data: self.data,
                                result: self.result,
                                version: version_num,
                            }) {
                                Ok(Some(version)) => {
                                    if let Some(num) = num {
                                        versions.insert(num, version);
                                    }
                                }
                                Ok(None) => (),
                                Err(e) => {
                                    if e.to_string().contains(ERROR_MARKER) {
                                        self.result.error(
                                            ErrorCode::E047,
                                            "Inventory 'versions' contains a version that is not an object"
                                                .to_string(),
                                        );
                                    } else {
                                        return Err(e);
                                    }
                                }
                            }
                        }
                    }
                }

                validate_version_nums(&all_versions, self.result);

                Ok(VersionsResult {
                    map: versions,
                    nums: all_versions,
                })
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
    result: &'a ParseValidationResult,
    version: &'c str,
}

impl<'de: 'b, 'a, 'b, 'c> DeserializeSeed<'de> for VersionSeed<'a, 'b, 'c> {
    type Value = Option<Version>;

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
            result: &'a ParseValidationResult,
            version: &'b str,
        }

        impl<'de, 'a, 'b> DeserializeSeed<'de> for FieldSeed<'a, 'b> {
            type Value = Field;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor<'a, 'b> {
                    result: &'a ParseValidationResult,
                    version: &'b str,
                }

                impl<'de, 'a, 'b> Visitor<'de> for FieldVisitor<'a, 'b> {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                        formatter.write_str(ERROR_MARKER)
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
            result: &'a ParseValidationResult,
            version: &'c str,
        }

        impl<'de: 'b, 'a, 'b, 'c> Visitor<'de> for VersionVisitor<'a, 'b, 'c> {
            type Value = Option<Version>;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str(ERROR_MARKER)
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
                let mut user_failed = false;

                loop {
                    let key = match map.next_key_seed(FieldSeed {
                        result: self.result,
                        version: self.version,
                    })? {
                        None => break,
                        Some(key) => key,
                    };

                    match key {
                        Field::Created => {
                            if created.is_some() {
                                duplicate_version_field(CREATED_FIELD, self.version, self.result);
                                map.next_value::<Value>()?;
                            } else {
                                match map.next_value::<&str>() {
                                    Ok(value) => match DateTime::parse_from_rfc3339(value) {
                                        Ok(value) => created = Some(value.with_timezone(&Local)),
                                        Err(_) => {
                                            self.result.error(ErrorCode::E049,
                                                              format!("Inventory version {} 'created' must be an RFC3339 formatted date. Found: {}",
                                                                      self.version, value));
                                            created_failed = true;
                                        }
                                    },
                                    Err(e) => {
                                        self.result.error(
                                            ErrorCode::E049,
                                            format!(
                                                "Inventory version {} 'created' must be a string",
                                                self.version
                                            ),
                                        );
                                        return Err(e);
                                    }
                                }
                            }
                        }
                        Field::State => {
                            if state.is_some() {
                                duplicate_version_field(STATE_FIELD, self.version, self.result);
                                map.next_value::<Value>()?;
                            } else {
                                match map.next_value_seed(StateSeed {
                                    data: self.data,
                                    result: self.result,
                                    version: self.version,
                                }) {
                                    Ok(value) => state = Some(value),
                                    Err(e) => {
                                        if e.to_string().contains(ERROR_MARKER) {
                                            self.result.error(ErrorCode::E050,
                                                              format!("Inventory version {} 'state' must be an object", self.version));
                                            state_failed = true;
                                        } else {
                                            return Err(e);
                                        }
                                    }
                                }
                            }
                        }
                        Field::User => {
                            if user.is_some() {
                                duplicate_version_field(USER_FIELD, self.version, self.result);
                                map.next_value::<Value>()?;
                            } else {
                                match map.next_value_seed(UserSeed {
                                    result: self.result,
                                    version: self.version,
                                }) {
                                    Ok(Some(value)) => user = Some(value),
                                    Ok(None) => {
                                        user_failed = true;
                                    }
                                    Err(e) => {
                                        if e.to_string().contains(ERROR_MARKER) {
                                            self.result.error(
                                                ErrorCode::E054,
                                                format!(
                                                    "Inventory version {} 'user' must be an object",
                                                    self.version
                                                ),
                                            );
                                            user_failed = true;
                                        } else {
                                            return Err(e);
                                        }
                                    }
                                }
                            }
                        }
                        Field::Message => {
                            if message.is_some() {
                                duplicate_version_field(MESSAGE_FIELD, self.version, self.result);
                                map.next_value::<Value>()?;
                            } else {
                                match map.next_value() {
                                    Ok(value) => message = Some(value),
                                    Err(e) => {
                                        self.result.error(
                                            ErrorCode::E094,
                                            format!(
                                                "Inventory version {} 'message' must be a string",
                                                self.version
                                            ),
                                        );
                                        return Err(e);
                                    }
                                }
                            }
                        }
                        Field::Unknown => {
                            map.next_value::<Value>()?;
                        }
                    }
                }

                if created.is_none() && !created_failed {
                    missing_version_field(CREATED_FIELD, self.version, self.result);
                }
                if state.is_none() && !state_failed {
                    missing_version_field(STATE_FIELD, self.version, self.result);
                }
                if message.is_none() {
                    missing_version_field_warn(MESSAGE_FIELD, self.version, self.result);
                }
                if user.is_none() && !user_failed {
                    missing_version_field_warn(USER_FIELD, self.version, self.result);
                }

                if let (Some(created), Some(state)) = (created, state) {
                    Ok(Some(Version::new(created, state, message, user)))
                } else {
                    Ok(None)
                }
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

struct ManifestResult<'a> {
    manifest: PathBiMap<ContentPath>,
    digests: HashSet<&'a str>,
}

struct ManifestSeed<'a, 'b> {
    data: &'a mut DigestsAndPaths<'b>,
    result: &'a ParseValidationResult,
}

impl<'de: 'b, 'a, 'b> DeserializeSeed<'de> for ManifestSeed<'a, 'b> {
    type Value = ManifestResult<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ManifestVisitor<'a, 'b> {
            data: &'a mut DigestsAndPaths<'b>,
            result: &'a ParseValidationResult,
        }

        impl<'de: 'b, 'a, 'b> Visitor<'de> for ManifestVisitor<'a, 'b> {
            type Value = ManifestResult<'de>;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str(ERROR_MARKER)
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut manifest = PathBiMap::with_capacity(map.size_hint().unwrap_or(0));
                let mut all_paths = HashSet::with_capacity(map.size_hint().unwrap_or(0));
                let mut digests = HashSet::with_capacity(map.size_hint().unwrap_or(0));

                loop {
                    match map.next_key()? {
                        None => break,
                        Some(digest) => {
                            digests.insert(digest);
                            match map.next_value::<Vec<&str>>() {
                                Ok(paths) => {
                                    let mut content_paths = Vec::with_capacity(paths.len());

                                    for path in paths {
                                        if path.starts_with('/') || path.ends_with('/') {
                                            self.result.error(ErrorCode::E100,
                                                              format!("Inventory manifest key '{}' contains a path with a leading/trailing '/'. Found: {}",
                                                                      digest, path));
                                        } else {
                                            match ContentPath::try_from(path) {
                                                Ok(content_path) => {
                                                    content_paths.push(content_path)
                                                }
                                                Err(_) => {
                                                    self.result.error(ErrorCode::E099,
                                                                      format!("Inventory manifest key '{}' contains a path containing an illegal path part. Found: {}",
                                                                              digest, path));
                                                }
                                            }
                                        }

                                        if all_paths.contains(path) {
                                            self.result.error(ErrorCode::E101,
                                                          format!("Inventory manifest contains duplicate path '{}'",
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
                                Err(e) => {
                                    self.result.error(ErrorCode::E092,
                                                      format!("Inventory manifest key '{}' must reference an array of strings", digest));
                                    return Err(e);
                                }
                            }
                        }
                    }
                }

                validate_non_conflicting(&all_paths, |path, part| {
                    self.result.error(
                        ErrorCode::E101,
                        format!("Inventory manifest contains a path, '{}', that conflicts with another path, '{}'",
                                path, part));
                });

                Ok(ManifestResult { manifest, digests })
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
    result: &'a ParseValidationResult,
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
            result: &'a ParseValidationResult,
            version: &'c str,
        }

        impl<'de: 'b, 'a, 'b, 'c> Visitor<'de> for StateVisitor<'a, 'b, 'c> {
            type Value = PathBiMap<LogicalPath>;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str(ERROR_MARKER)
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut state = PathBiMap::with_capacity(map.size_hint().unwrap_or(0));
                let mut all_paths = HashSet::with_capacity(map.size_hint().unwrap_or(0));

                loop {
                    match map.next_key()? {
                        None => break,
                        Some(digest) => match map.next_value::<Vec<&str>>() {
                            Ok(paths) => {
                                let digest_ref = self.data.insert_digest(digest);
                                let mut path_refs = Vec::with_capacity(paths.len());

                                for path in paths {
                                    if path.starts_with('/') || path.ends_with('/') {
                                        self.result.error(ErrorCode::E053,
                                                              format!("In inventory version {}, state key '{}' contains a path with a leading/trailing '/'. Found: {}",
                                                                      self.version, digest, path));
                                    } else {
                                        match self.data.insert_path::<A::Error>(path) {
                                            Ok(logical_path) => path_refs.push(logical_path),
                                            Err(_) => {
                                                self.result.error(ErrorCode::E052,
                                                                      format!("In inventory version {}, state key '{}' contains a path containing an illegal path part. Found: {}",
                                                                              self.version, digest, path));
                                            }
                                        }
                                    }

                                    if all_paths.contains(path) {
                                        self.result.error(ErrorCode::E095,
                                                          format!("In inventory version {}, state contains duplicate path '{}'",
                                                                  self.version, path));
                                    } else {
                                        all_paths.insert(path);
                                    }
                                }

                                state.insert_multiple_rc(digest_ref, path_refs);
                            }
                            Err(e) => {
                                self.result.error(ErrorCode::E051,
                                                      format!("In inventory version {}, state key '{}' must reference an array of strings",
                                                              self.version, digest));
                                return Err(e);
                            }
                        },
                    }
                }

                validate_non_conflicting(&all_paths, |path, part| {
                    self.result.error(
                        ErrorCode::E095,
                        format!("In inventory version {}, state contains a path, '{}', that conflicts with another path, '{}'",
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
    result: &'a ParseValidationResult,
    version: &'b str,
}

impl<'de, 'a, 'b> DeserializeSeed<'de> for UserSeed<'a, 'b> {
    type Value = Option<User>;

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
            result: &'a ParseValidationResult,
            version: &'b str,
        }

        impl<'de, 'a, 'b> DeserializeSeed<'de> for FieldSeed<'a, 'b> {
            type Value = Field;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor<'a, 'b> {
                    result: &'a ParseValidationResult,
                    version: &'b str,
                }

                impl<'de, 'a, 'b> Visitor<'de> for FieldVisitor<'a, 'b> {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                        formatter.write_str(ERROR_MARKER)
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
            result: &'a ParseValidationResult,
            version: &'b str,
        }

        impl<'de, 'a, 'b> Visitor<'de> for UserVisitor<'a, 'b> {
            type Value = Option<User>;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str(ERROR_MARKER)
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut name = None;
                let mut address = None;

                loop {
                    let key = match map.next_key_seed(FieldSeed {
                        result: self.result,
                        version: self.version,
                    })? {
                        None => break,
                        Some(key) => key,
                    };

                    match key {
                        Field::Name => {
                            if name.is_some() {
                                duplicate_version_field(NAME_FIELD, self.version, self.result);
                                map.next_value::<Value>()?;
                            } else {
                                match map.next_value() {
                                    Ok(value) => name = Some(value),
                                    Err(e) => {
                                        self.result.error(
                                            ErrorCode::E033,
                                            format!(
                                                "Inventory version {} user 'name' must be a string",
                                                self.version
                                            ),
                                        );
                                        return Err(e);
                                    }
                                }
                            }
                        }
                        Field::Address => {
                            if address.is_some() {
                                duplicate_version_field(ADDRESS_FIELD, self.version, self.result);
                                map.next_value::<Value>()?;
                            } else {
                                match map.next_value::<&str>() {
                                    Ok(value) => {
                                        if URI::try_from(value).is_err() {
                                            self.result.warn(WarnCode::W009,
                                                              format!("Inventory version {} user 'address' should be a URI. Found: {}",
                                                                      self.version, value));
                                        }
                                        address = Some(value.to_string());
                                    }
                                    Err(e) => {
                                        self.result.error(ErrorCode::E033,
                                                          format!("Inventory version {} user 'address' must be a string", self.version));
                                        return Err(e);
                                    }
                                }
                            }
                        }
                        Field::Unknown => {
                            map.next_value::<Value>()?;
                        }
                    }
                }

                if name.is_none() {
                    self.result.error(
                        ErrorCode::E054,
                        format!(
                            "Inventory version '{}' is missing required key '{}'",
                            self.version, NAME_FIELD
                        ),
                    );
                }
                if address.is_none() {
                    self.result.warn(
                        WarnCode::W008,
                        format!(
                            "Inventory version '{}' is missing recommended key '{}'",
                            self.version, ADDRESS_FIELD
                        ),
                    );
                }

                if let Some(name) = name {
                    Ok(Some(User::new(name, address)))
                } else {
                    Ok(None)
                }
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

fn validate_version_nums(version_nums: &BTreeSet<VersionNum>, result: &ParseValidationResult) {
    let mut padding = None;
    let mut consistent_padding = true;
    let mut next_version = VersionNum::v1();

    for version in version_nums {
        match padding {
            None => padding = Some(version.width),
            Some(padding) => {
                if consistent_padding && padding != version.width {
                    consistent_padding = false;
                }
            }
        }

        if *version != next_version {
            while next_version < *version {
                result.error(
                    ErrorCode::E010,
                    format!("Inventory 'versions' is missing version '{}'", next_version),
                );
                next_version = next_version.next().unwrap();
            }
        }

        next_version = next_version.next().unwrap();
    }

    if !consistent_padding {
        result.error(
            ErrorCode::E013,
            "Inventory 'versions' contains inconsistently padded version numbers".to_string(),
        );
    }

    if let Some(padding) = padding {
        if padding > 0 {
            result.warn(
                WarnCode::W001,
                "Contains zero-padded version numbers".to_string(),
            );
        }
    }
}

fn validate_fixity(
    fixity: &Option<HashMap<String, HashMap<String, Vec<String>>>>,
    manifest: &Option<ManifestResult>,
    result: &ParseValidationResult,
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
                                algorithm, digest
                            ),
                        );
                    }
                }
            }

            let mut all_paths = HashSet::with_capacity(fixity_manifest.len());
            let mut all_digests = HashSet::with_capacity(fixity_manifest.len());

            if let Some(manifest) = manifest {
                for (digest, paths) in fixity_manifest {
                    let digest = digest.to_ascii_lowercase();
                    if all_digests.contains(&digest) {
                        result.error(
                            ErrorCode::E097,
                            format!(
                                "Inventory fixity block '{}' contains duplicate digest '{}'",
                                algorithm, digest
                            ),
                        );
                    } else {
                        all_digests.insert(digest);
                    }

                    for path in paths {
                        if all_paths.contains(&path) {
                            result.error(
                                ErrorCode::E101,
                                format!(
                                    "Inventory fixity block '{}' contains duplicate path '{}'",
                                    algorithm, path
                                ),
                            );

                            continue;
                        }

                        all_paths.insert(path);

                        if path.starts_with('/') || path.ends_with('/') {
                            result.error(ErrorCode::E100,
                                              format!("Inventory fixity block '{}' contains a path with a leading/trailing '/'. Found: {}",
                                                      algorithm, path));
                        } else if let Ok(content_path) = ContentPath::try_from(path) {
                            if !manifest.manifest.contains_path(&content_path) {
                                result.error(
                                    ErrorCode::E057,
                                    format!("Inventory fixity block '{}' contains a path not present in the manifest. Found: {}",
                                            algorithm, path),
                                );
                            }
                        } else {
                            result.error(
                                ErrorCode::E099,
                                format!("Inventory fixity block '{}' contains a path containing an illegal path part. Found: {}",
                                        algorithm, path),
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

fn duplicate_field(field: &str, result: &ParseValidationResult) {
    result.error(
        ErrorCode::E033,
        format!("Inventory contains duplicate key '{}'", field),
    );
}

fn duplicate_version_field(field: &str, version: &str, result: &ParseValidationResult) {
    result.error(
        ErrorCode::E033,
        format!(
            "Inventory version '{}' contains duplicate key '{}'",
            version, field
        ),
    );
}

fn unknown_field(field: &str, result: &ParseValidationResult) {
    result.error(
        ErrorCode::E102,
        format!("Inventory contains unknown key '{}'", field),
    );
}

fn unknown_version_field(field: &str, version: &str, result: &ParseValidationResult) {
    result.error(
        ErrorCode::E102,
        format!(
            "Inventory version '{}' contains unknown key '{}'",
            version, field
        ),
    );
}

fn missing_inv_field(field: &str, result: &ParseValidationResult) {
    result.error(
        ErrorCode::E036,
        format!("Inventory is missing required key '{}'", field),
    );
}

fn missing_inv_field_2(field: &str, result: &ParseValidationResult) {
    result.error(
        ErrorCode::E041,
        format!("Inventory is missing required key '{}'", field),
    );
}

fn missing_version_field(field: &str, version: &str, result: &ParseValidationResult) {
    result.error(
        ErrorCode::E048,
        format!(
            "Inventory version '{}' is missing required key '{}'",
            version, field
        ),
    );
}

fn missing_version_field_warn(field: &str, version: &str, result: &ParseValidationResult) {
    result.warn(
        WarnCode::W007,
        format!(
            "Inventory version '{}' is missing recommended key '{}'",
            version, field
        ),
    );
}

#[cfg(test)]
mod tests {
    use crate::ocfl::validate::serde::parse;
    use crate::ocfl::validate::{ParseResult, ParseValidationResult};
    use crate::ocfl::{ErrorCode, ProblemLocation, ValidationError, ValidationWarning, WarnCode};

    #[test]
    fn head_wrong_type() {
        let json = r###"{
            "id": "urn:example:test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            "head": false,
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
        }"###;

        match parse(json.as_bytes()) {
            ParseResult::Ok(_, _) => panic!("Expected parse failure"),
            ParseResult::Error(result) => {
                has_error(
                    ErrorCode::E040,
                    "Inventory 'head' must be a string",
                    &result,
                );
                error_count(1, &result);
                warning_count(0, &result);
            }
        }
    }

    #[test]
    fn head_object() {
        let json = r###"{
            "id": "urn:example:test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            "head": {"a": 1, "b", 2},
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
        }"###;

        match parse(json.as_bytes()) {
            ParseResult::Ok(_, _) => panic!("Expected parse failure"),
            ParseResult::Error(result) => {
                has_error(
                    ErrorCode::E040,
                    "Inventory 'head' must be a string",
                    &result,
                );
                error_count(1, &result);
                warning_count(0, &result);
            }
        }
    }

    #[test]
    fn bad_key() {
        let json = r###"{
            "id": "urn:example:test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            1: "v1",
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
        }"###;

        match parse(json.as_bytes()) {
            ParseResult::Ok(_, _) => panic!("Expected parse failure"),
            ParseResult::Error(result) => {
                has_error(
                    ErrorCode::E033,
                    "Inventory could not be parsed: key must be a string at line 5 column 13",
                    &result,
                );
                error_count(1, &result);
                warning_count(0, &result);
            }
        }
    }

    #[test]
    fn duplicate_key() {
        let json = r###"{
            "id": "urn:example:test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "id": "urn:example:test",
            "digestAlgorithm": "sha512",
            "digestAlgorithm": "sha512",
            "head": "v1",
            "head": "v1",
            "contentDirectory": "content",
            "contentDirectory": "content",
            "manifest": {
                "fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455": [
                    "v1/content/file1.txt"
                ]
            },
            "manifest": {
                "fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455": [
                    "v1/content/file1.txt"
                ]
            },
            "versions": {
                "v1": {
                    "created": "2021-09-05T20:36:50.923505656-05:00",
                    "created": "2021-09-05T20:36:50.923505656-05:00",
                    "state": {
                        "fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455": [
                            "file1.txt"
                        ]
                    },
                    "state": {
                        "fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455": [
                            "file1.txt"
                        ]
                    },
                    "message": "initial commit",
                    "message": "initial commit",
                    "user": {
                        "name": "Peter Winckles",
                        "name": "Peter Winckles",
                        "address": "mailto:me@example.com",
                        "address": "mailto:me@example.com"
                    },
                    "user": {
                        "name": "Peter Winckles",
                        "address": "mailto:me@example.com"
                    }
                }
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
            },
            "fixity": {
                "md5": {
                    "184f84e28cbe75e050e9c25ea7f2e939": [
                        "v1/content/file1.txt"
                    ]
                }
            }
        }"###;

        match parse(json.as_bytes()) {
            ParseResult::Ok(_, _) => panic!("Expected parse failure"),
            ParseResult::Error(result) => {
                has_error(
                    ErrorCode::E033,
                    "Inventory contains duplicate key 'id'",
                    &result,
                );
                has_error(
                    ErrorCode::E033,
                    "Inventory contains duplicate key 'type'",
                    &result,
                );
                has_error(
                    ErrorCode::E033,
                    "Inventory contains duplicate key 'digestAlgorithm'",
                    &result,
                );
                has_error(
                    ErrorCode::E033,
                    "Inventory contains duplicate key 'head'",
                    &result,
                );
                has_error(
                    ErrorCode::E033,
                    "Inventory contains duplicate key 'contentDirectory'",
                    &result,
                );
                has_error(
                    ErrorCode::E033,
                    "Inventory contains duplicate key 'manifest'",
                    &result,
                );
                has_error(
                    ErrorCode::E033,
                    "Inventory contains duplicate key 'versions'",
                    &result,
                );
                has_error(
                    ErrorCode::E033,
                    "Inventory contains duplicate key 'fixity'",
                    &result,
                );
                has_error(
                    ErrorCode::E033,
                    "Inventory version 'v1' contains duplicate key 'created'",
                    &result,
                );
                has_error(
                    ErrorCode::E033,
                    "Inventory version 'v1' contains duplicate key 'state'",
                    &result,
                );
                has_error(
                    ErrorCode::E033,
                    "Inventory version 'v1' contains duplicate key 'message'",
                    &result,
                );
                has_error(
                    ErrorCode::E033,
                    "Inventory version 'v1' contains duplicate key 'name'",
                    &result,
                );
                has_error(
                    ErrorCode::E033,
                    "Inventory version 'v1' contains duplicate key 'address'",
                    &result,
                );
                has_error(
                    ErrorCode::E033,
                    "Inventory version 'v1' contains duplicate key 'user'",
                    &result,
                );
                error_count(14, &result);
                warning_count(0, &result);
            }
        }
    }

    #[test]
    fn manifest_wrong_key_type() {
        let json = r###"{
            "id": "urn:example:test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            "head": "v1",
            "contentDirectory": "content",
            "manifest": {
                1: [
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
        }"###;

        match parse(json.as_bytes()) {
            ParseResult::Ok(_, _) => panic!("Expected parse failure"),
            ParseResult::Error(result) => {
                has_error(
                    ErrorCode::E033,
                    "Inventory could not be parsed: key must be a string at line 8 column 17",
                    &result,
                );
                error_count(1, &result);
                warning_count(0, &result);
            }
        }
    }

    #[test]
    fn manifest_wrong_type() {
        let json = r###"{
            "id": "urn:example:test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            "head": "v1",
            "contentDirectory": "content",
            "manifest": false,
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
        }"###;

        match parse(json.as_bytes()) {
            ParseResult::Ok(_, _) => panic!("Expected parse failure"),
            ParseResult::Error(result) => {
                has_error(
                    ErrorCode::E033,
                    "Inventory 'manifest' must be an object",
                    &result,
                );
                error_count(1, &result);
                warning_count(0, &result);
            }
        }
    }

    #[test]
    fn unknown_key() {
        let json = r###"{
            "id": "urn:example:test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            "head": "v1",
            "contentDirectory": "content",
            "bogus": "key",
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
        }"###;

        match parse(json.as_bytes()) {
            ParseResult::Ok(_, _) => panic!("Expected parse failure"),
            ParseResult::Error(result) => {
                has_error(
                    ErrorCode::E102,
                    "Inventory contains unknown key 'bogus'",
                    &result,
                );
                error_count(1, &result);
                warning_count(0, &result);
            }
        }
    }

    #[test]
    fn state_invalid_type() {
        let json = r###"{
            "id": "urn:example:test",
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
                        "fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455": {"a": 1}
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
        }"###;

        match parse(json.as_bytes()) {
            ParseResult::Ok(_, _) => panic!("Expected parse failure"),
            ParseResult::Error(result) => {
                has_error(ErrorCode::E051, "In inventory version v1, state key 'fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455' must reference an array of strings", &result);
                error_count(1, &result);
                warning_count(0, &result);
            }
        }
    }

    #[test]
    fn fixity_invalid_type() {
        let json = r###"{
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
                    "184f84e28cbe75e050e9c25ea7f2e939": "v1/content/file1.txt"
                }
            },
            "id": "urn:example:test"
        }"###;

        match parse(json.as_bytes()) {
            ParseResult::Ok(_, _) => panic!("Expected parse failure"),
            ParseResult::Error(result) => {
                has_error(
                    ErrorCode::E057,
                    "Inventory 'fixity' must be a map of maps of arrays",
                    &result,
                );
                error_count(1, &result);
                warning_count(0, &result);
            }
        }
    }

    #[test]
    fn user_address_invalid_type() {
        let json = r###"{
            "id": "urn:example:test",
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
                        "address": {"a": 1}
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
        }"###;

        match parse(json.as_bytes()) {
            ParseResult::Ok(_, _) => panic!("Expected parse failure"),
            ParseResult::Error(result) => {
                has_error(
                    ErrorCode::E033,
                    "Inventory version v1 user 'address' must be a string",
                    &result,
                );
                error_count(1, &result);
                warning_count(0, &result);
            }
        }
    }

    #[test]
    fn user_wrong_type() {
        let json = r###"{
            "id": "urn:example:test",
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
                    "user": false
                }
            },
            "fixity": {
                "md5": {
                    "184f84e28cbe75e050e9c25ea7f2e939": [
                        "v1/content/file1.txt"
                    ]
                }
            }
        }"###;

        match parse(json.as_bytes()) {
            ParseResult::Ok(_, _) => panic!("Expected parse failure"),
            ParseResult::Error(result) => {
                has_error(
                    ErrorCode::E054,
                    "Inventory version v1 'user' must be an object",
                    &result,
                );
                error_count(1, &result);
                warning_count(0, &result);
            }
        }
    }

    #[test]
    fn manifest_invalid_path_type() {
        let json = r###"{
            "id": "urn:example:test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            "head": "v1",
            "contentDirectory": "content",
            "manifest": {
                "fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455": "v1/content/file1.txt"
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
        }"###;

        match parse(json.as_bytes()) {
            ParseResult::Ok(_, _) => panic!("Expected parse failure"),
            ParseResult::Error(result) => {
                has_error(ErrorCode::E092, "Inventory manifest key 'fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455' must reference an array of strings", &result);
                error_count(1, &result);
                warning_count(0, &result);
            }
        }
    }

    #[test]
    fn manifest_invalid_path_object() {
        let json = r###"{
            "id": "urn:example:test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            "head": "v1",
            "contentDirectory": "content",
            "manifest": {
                "fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455": {"a": 1}
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
        }"###;

        match parse(json.as_bytes()) {
            ParseResult::Ok(_, _) => panic!("Expected parse failure"),
            ParseResult::Error(result) => {
                has_error(ErrorCode::E092, "Inventory manifest key 'fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455' must reference an array of strings", &result);
                error_count(1, &result);
                warning_count(0, &result);
            }
        }
    }

    #[test]
    fn invalid_version_type() {
        let json = r###"{
            "id": "urn:example:test",
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
                "v1": "version"
            },
            "fixity": {
                "md5": {
                    "184f84e28cbe75e050e9c25ea7f2e939": [
                        "v1/content/file1.txt"
                    ]
                }
            }
        }"###;

        match parse(json.as_bytes()) {
            ParseResult::Ok(_, _) => panic!("Expected parse failure"),
            ParseResult::Error(result) => {
                has_error(
                    ErrorCode::E047,
                    "Inventory 'versions' contains a version that is not an object",
                    &result,
                );
                error_count(1, &result);
                warning_count(0, &result);
            }
        }
    }

    #[test]
    fn invalid_version_object() {
        let json = r###"{
            "id": "urn:example:test",
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
                    "message": false,
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
        }"###;

        match parse(json.as_bytes()) {
            ParseResult::Ok(_, _) => panic!("Expected parse failure"),
            ParseResult::Error(result) => {
                has_error(
                    ErrorCode::E094,
                    "Inventory version v1 'message' must be a string",
                    &result,
                );
                error_count(1, &result);
                warning_count(0, &result);
            }
        }
    }

    #[test]
    fn empty_json() {
        let json = r###"{}"###;

        match parse(json.as_bytes()) {
            ParseResult::Ok(_, _) => panic!("Expected parse failure"),
            ParseResult::Error(result) => {
                has_error(
                    ErrorCode::E036,
                    "Inventory is missing required key 'id'",
                    &result,
                );
                has_error(
                    ErrorCode::E036,
                    "Inventory is missing required key 'type'",
                    &result,
                );
                has_error(
                    ErrorCode::E036,
                    "Inventory is missing required key 'digestAlgorithm'",
                    &result,
                );
                has_error(
                    ErrorCode::E036,
                    "Inventory is missing required key 'head'",
                    &result,
                );
                has_error(
                    ErrorCode::E041,
                    "Inventory is missing required key 'manifest'",
                    &result,
                );
                has_error(
                    ErrorCode::E041,
                    "Inventory is missing required key 'versions'",
                    &result,
                );
                error_count(6, &result);
                warning_count(0, &result);
            }
        }
    }

    fn error_count(count: usize, result: &ParseValidationResult) {
        let errors = result.errors.borrow();
        assert_eq!(
            count,
            errors.len(),
            "Expected {} errors; found {}: {:?}",
            count,
            errors.len(),
            errors
        )
    }

    fn has_error(code: ErrorCode, message: &str, result: &ParseValidationResult) {
        let errors = result.errors.borrow();
        assert!(
            errors.contains(&ValidationError::new(
                ProblemLocation::ObjectRoot,
                code,
                message.to_string()
            )),
            "Expected errors to contain code={}; msg='{}'. Found: {:?}",
            code,
            message,
            errors
        );
    }

    fn warning_count(count: usize, result: &ParseValidationResult) {
        let warnings = result.warnings.borrow();
        assert_eq!(
            count,
            warnings.len(),
            "Expected {} warnings; found {}: {:?}",
            count,
            warnings.len(),
            warnings
        )
    }

    #[allow(dead_code)]
    fn has_warning(code: WarnCode, message: &str, result: &ParseValidationResult) {
        let warnings = result.warnings.borrow();
        assert!(
            warnings.contains(&ValidationWarning::new(
                ProblemLocation::ObjectRoot,
                code,
                message.to_string()
            )),
            "Expected warnings to contain code={}; msg='{}'. Found: {:?}",
            code,
            message,
            warnings
        );
    }
}

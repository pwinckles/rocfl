//! This module provides custom deserialization for [Inventories](Inventory) that is able to
//! dedup all digests and logical paths, greatly reducing the memory used, and marginally increasing
//! the deserialization speed.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::rc::Rc;

use serde::de::{DeserializeSeed, Error as SerdeError, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::ocfl::bimap::PathBiMap;
use crate::ocfl::digest::HexDigest;
use crate::ocfl::inventory::{Inventory, Version};
use crate::ocfl::{ContentPath, LogicalPath, VersionNum};

pub const ID_FIELD: &str = "id";
pub const TYPE_FIELD: &str = "type";
pub const DIGEST_ALGORITHM_FIELD: &str = "digestAlgorithm";
pub const HEAD_FIELD: &str = "head";
pub const CONTENT_DIRECTORY_FIELD: &str = "contentDirectory";
pub const MANIFEST_FIELD: &str = "manifest";
pub const VERSIONS_FIELD: &str = "versions";
pub const FIXITY_FIELD: &str = "fixity";
pub const INVENTORY_FIELDS: &[&str] = &[
    ID_FIELD,
    TYPE_FIELD,
    DIGEST_ALGORITHM_FIELD,
    HEAD_FIELD,
    CONTENT_DIRECTORY_FIELD,
    MANIFEST_FIELD,
    VERSIONS_FIELD,
    FIXITY_FIELD,
];

pub const CREATED_FIELD: &str = "created";
pub const STATE_FIELD: &str = "state";
pub const USER_FIELD: &str = "user";
pub const MESSAGE_FIELD: &str = "message";
pub const VERSION_FIELDS: &[&str] = &[CREATED_FIELD, STATE_FIELD, USER_FIELD, MESSAGE_FIELD];

pub const NAME_FIELD: &str = "name";
pub const ADDRESS_FIELD: &str = "address";
pub const USER_FIELDS: &[&str] = &[NAME_FIELD, ADDRESS_FIELD];

impl<'de> Deserialize<'de> for Inventory {
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
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
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
                            _ => Err(SerdeError::unknown_field(value, INVENTORY_FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct InventoryVisitor;

        impl<'de> Visitor<'de> for InventoryVisitor {
            type Value = Inventory;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("an OCFL inventory object")
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
                let mut fixity = None;

                let mut data = DigestsAndPaths::new();

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Id => {
                            if id.is_some() {
                                return Err(SerdeError::duplicate_field(ID_FIELD));
                            }
                            id = Some(map.next_value()?);
                        }
                        Field::Type => {
                            if type_declaration.is_some() {
                                return Err(SerdeError::duplicate_field(TYPE_FIELD));
                            }
                            type_declaration = Some(map.next_value()?);
                        }
                        Field::DigestAlgorithm => {
                            if digest_algorithm.is_some() {
                                return Err(SerdeError::duplicate_field(DIGEST_ALGORITHM_FIELD));
                            }
                            digest_algorithm = Some(map.next_value()?);
                        }
                        Field::Head => {
                            if head.is_some() {
                                return Err(SerdeError::duplicate_field(HEAD_FIELD));
                            }
                            head = Some(map.next_value()?);
                        }
                        Field::ContentDirectory => {
                            if content_directory.is_some() {
                                return Err(SerdeError::duplicate_field(CONTENT_DIRECTORY_FIELD));
                            }
                            content_directory = Some(map.next_value()?);
                        }
                        Field::Manifest => {
                            if manifest.is_some() {
                                return Err(SerdeError::duplicate_field(MANIFEST_FIELD));
                            }
                            manifest = Some(map.next_value_seed(ManifestSeed { data: &mut data })?);
                        }
                        Field::Versions => {
                            if versions.is_some() {
                                return Err(SerdeError::duplicate_field(VERSIONS_FIELD));
                            }
                            versions = Some(map.next_value_seed(VersionsSeed { data: &mut data })?);
                        }
                        Field::Fixity => {
                            if fixity.is_some() {
                                return Err(SerdeError::duplicate_field(FIXITY_FIELD));
                            }
                            fixity = Some(map.next_value()?);
                        }
                    }
                }

                if id.is_none() {
                    return Err(SerdeError::missing_field(ID_FIELD));
                }
                if type_declaration.is_none() {
                    return Err(SerdeError::missing_field(TYPE_FIELD));
                }
                if digest_algorithm.is_none() {
                    return Err(SerdeError::missing_field(DIGEST_ALGORITHM_FIELD));
                }
                if head.is_none() {
                    return Err(SerdeError::missing_field(HEAD_FIELD));
                }
                if manifest.is_none() {
                    return Err(SerdeError::missing_field(MANIFEST_FIELD));
                }
                if versions.is_none() {
                    return Err(SerdeError::missing_field(VERSIONS_FIELD));
                }

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
                .map_err(|e| SerdeError::custom(e.to_string()))
            }
        }

        deserializer.deserialize_struct("Inventory", INVENTORY_FIELDS, InventoryVisitor)
    }
}

struct VersionsSeed<'a, 'b> {
    data: &'a mut DigestsAndPaths<'b>,
}

impl<'de: 'b, 'a, 'b> DeserializeSeed<'de> for VersionsSeed<'a, 'b> {
    type Value = BTreeMap<VersionNum, Version>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VersionsVisitor<'a, 'b> {
            data: &'a mut DigestsAndPaths<'b>,
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

                while let Some((version_num, version)) =
                    map.next_entry_seed(PhantomData, VersionSeed { data: self.data })?
                {
                    versions.insert(version_num, version);
                }

                Ok(versions)
            }
        }

        deserializer.deserialize_map(VersionsVisitor { data: self.data })
    }
}

struct VersionSeed<'a, 'b> {
    data: &'a mut DigestsAndPaths<'b>,
}

impl<'de: 'b, 'a, 'b> DeserializeSeed<'de> for VersionSeed<'a, 'b> {
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
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
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
                            _ => Err(SerdeError::unknown_field(value, VERSION_FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct VersionVisitor<'a, 'b> {
            data: &'a mut DigestsAndPaths<'b>,
        }

        impl<'de: 'b, 'a, 'b> Visitor<'de> for VersionVisitor<'a, 'b> {
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

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Created => {
                            if created.is_some() {
                                return Err(SerdeError::duplicate_field(CREATED_FIELD));
                            }
                            created = Some(map.next_value()?);
                        }
                        Field::State => {
                            if state.is_some() {
                                return Err(SerdeError::duplicate_field(STATE_FIELD));
                            }
                            state = Some(map.next_value_seed(StateSeed { data: self.data })?);
                        }
                        Field::User => {
                            if user.is_some() {
                                return Err(SerdeError::duplicate_field(USER_FIELD));
                            }
                            user = Some(map.next_value()?);
                        }
                        Field::Message => {
                            if message.is_some() {
                                return Err(SerdeError::duplicate_field(MESSAGE_FIELD));
                            }
                            message = Some(map.next_value()?);
                        }
                    }
                }

                if created.is_none() {
                    return Err(SerdeError::missing_field(CREATED_FIELD));
                }
                if state.is_none() {
                    return Err(SerdeError::missing_field(STATE_FIELD));
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
            VersionVisitor { data: self.data },
        )
    }
}

struct ManifestSeed<'a, 'b> {
    data: &'a mut DigestsAndPaths<'b>,
}

impl<'de: 'b, 'a, 'b> DeserializeSeed<'de> for ManifestSeed<'a, 'b> {
    type Value = PathBiMap<ContentPath>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ManifestVisitor<'a, 'b> {
            data: &'a mut DigestsAndPaths<'b>,
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

                while let Some((digest, paths)) = map.next_entry::<&str, Vec<ContentPath>>()? {
                    let path_refs = paths.into_iter().map(Rc::new).collect();
                    manifest.insert_multiple_rc(self.data.insert_digest(digest), path_refs);
                }

                Ok(manifest)
            }
        }

        deserializer.deserialize_map(ManifestVisitor { data: self.data })
    }
}

struct StateSeed<'a, 'b> {
    data: &'a mut DigestsAndPaths<'b>,
}

impl<'de: 'b, 'a, 'b> DeserializeSeed<'de> for StateSeed<'a, 'b> {
    type Value = PathBiMap<LogicalPath>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StateVisitor<'a, 'b> {
            data: &'a mut DigestsAndPaths<'b>,
        }

        impl<'de: 'b, 'a, 'b> Visitor<'de> for StateVisitor<'a, 'b> {
            type Value = PathBiMap<LogicalPath>;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("an OCFL version state map")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut state = PathBiMap::with_capacity(map.size_hint().unwrap_or(0));

                while let Some((digest, paths)) = map.next_entry::<&str, Vec<&str>>()? {
                    let digest_ref = self.data.insert_digest(digest);
                    let mut path_refs = Vec::with_capacity(paths.len());

                    for path in paths {
                        path_refs.push(self.data.insert_path(path)?);
                    }

                    state.insert_multiple_rc(digest_ref, path_refs);
                }

                Ok(state)
            }
        }

        deserializer.deserialize_map(StateVisitor { data: self.data })
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

#[cfg(test)]
mod tests {
    use std::convert::{TryFrom, TryInto};
    use std::rc::Rc;

    use chrono::DateTime;
    use serde_json::json;

    use crate::ocfl::inventory::Inventory;
    use crate::ocfl::{ContentPath, DigestAlgorithm, LogicalPath, RocflError, VersionNum};

    #[test]
    fn deserialize_dedup() -> Result<(), RocflError> {
        let json = json!({
            "id": "test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            "head": "v2",
            "contentDirectory": "content",
            "manifest": {
                "fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455": [
                    "v1/content/file1.txt"
                ],
                "ab0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455": [
                    "v2/content/file2.txt"
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
                    "user": {
                        "name": "Peter Winckles",
                        "address": "mailto:me@example.com"
                    }
                },
                "v2": {
                    "created": "2021-09-05T20:36:50.923505656-05:00",
                    "state": {
                        "fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455": [
                            "file1.txt"
                        ],
                        "ab0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455": [
                            "file2.txt"
                        ]
                    },
                    "user": {
                        "name": "Peter Winckles",
                        "address": "mailto:me@example.com"
                    }
                }
            }
        }).to_string();

        let inv: Inventory = serde_json::from_str(&json)?;

        let v1 = inv.get_version(1.try_into()?)?;
        let v2 = inv.get_version(2.try_into()?)?;

        assert_eq!(
            6,
            Rc::strong_count(v1.lookup_digest(&"file1.txt".try_into()?).unwrap())
        );
        assert_eq!(
            4,
            Rc::strong_count(v2.lookup_digest(&"file2.txt".try_into()?).unwrap())
        );
        assert_eq!(
            5,
            Rc::strong_count(v1.resolve_glob("*", false)?.iter().next().unwrap())
        );
        assert_eq!(
            3,
            Rc::strong_count(v2.resolve_glob("file2.txt", false)?.iter().next().unwrap())
        );

        Ok(())
    }

    #[test]
    fn all_fields_mapped() -> Result<(), RocflError> {
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

        let inv: Inventory = serde_json::from_str(&json)?;

        assert_eq!("test", inv.id);
        assert_eq!("https://ocfl.io/1.0/spec/#inventory", inv.type_declaration);
        assert_eq!(DigestAlgorithm::Sha512, inv.digest_algorithm);
        assert_eq!(VersionNum::v1(), inv.head);
        assert_eq!("content", inv.content_directory.as_ref().unwrap());
        assert!(inv.contains_content_path(&ContentPath::try_from("v1/content/file1.txt")?));
        assert!(inv.versions.contains_key(&VersionNum::v1()));

        let version = inv.versions.get(&VersionNum::v1()).unwrap();

        assert_eq!(
            DateTime::parse_from_rfc3339("2021-09-05T20:36:50.923505656-05:00").unwrap(),
            version.created
        );
        assert_eq!("initial commit", version.message.as_ref().unwrap());
        assert_eq!(
            "Peter Winckles",
            version.user.as_ref().unwrap().name.as_ref().unwrap()
        );
        assert_eq!(
            "mailto:me@example.com",
            version.user.as_ref().unwrap().address.as_ref().unwrap()
        );
        assert_eq!("fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455",
                   (**version.lookup_digest(&LogicalPath::try_from("file1.txt")?).unwrap()).as_ref());

        Ok(())
    }

    #[test]
    fn all_fields_mapped_minimal() -> Result<(), RocflError> {
        let json = json!({
            "id": "test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            "head": "v1",
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
                    }
                }
            }
        }).to_string();

        let inv: Inventory = serde_json::from_str(&json)?;

        assert_eq!("test", inv.id);
        assert_eq!("https://ocfl.io/1.0/spec/#inventory", inv.type_declaration);
        assert_eq!(DigestAlgorithm::Sha512, inv.digest_algorithm);
        assert_eq!(VersionNum::v1(), inv.head);
        assert!(inv.contains_content_path(&ContentPath::try_from("v1/content/file1.txt")?));
        assert!(inv.versions.contains_key(&VersionNum::v1()));

        let version = inv.versions.get(&VersionNum::v1()).unwrap();

        assert_eq!(
            DateTime::parse_from_rfc3339("2021-09-05T20:36:50.923505656-05:00").unwrap(),
            version.created
        );
        assert_eq!("fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455",
                   (**version.lookup_digest(&LogicalPath::try_from("file1.txt")?).unwrap()).as_ref());

        Ok(())
    }

    #[test]
    #[should_panic(expected = "duplicate field `id`")]
    fn duplicate_id_field() {
        let json = r#"{
            "id": "test",
            "id": "test2",
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
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "duplicate field `type`")]
    fn duplicate_type_field() {
        let json = r#"{
            "id": "test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "type": "https://ocfl.io/1.1/spec/#inventory",
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
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "duplicate field `digestAlgorithm`")]
    fn duplicate_algorithm_field() {
        let json = r#"{
            "id": "test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            "digestAlgorithm": "sha256",
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
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "duplicate field `head`")]
    fn duplicate_head_field() {
        let json = r#"{
            "id": "test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            "head": "v1",
            "head": "v2",
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
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "duplicate field `contentDirectory`")]
    fn duplicate_content_field() {
        let json = r#"{
            "id": "test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            "head": "v1",
            "contentDirectory": "content",
            "contentDirectory": "content_dir",
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
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "duplicate field `manifest`")]
    fn duplicate_manifest_field() {
        let json = r#"{
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
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "duplicate field `versions`")]
    fn duplicate_versions_field() {
        let json = r#"{
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
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "duplicate field `created`")]
    fn duplicate_created_field() {
        let json = r#"{
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
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "duplicate field `state`")]
    fn duplicate_state_field() {
        let json = r#"{
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
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "duplicate field `message`")]
    fn duplicate_message_field() {
        let json = r#"{
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
                    "message": "initial commit",
                    "user": {
                        "name": "Peter Winckles",
                        "address": "mailto:me@example.com"
                    }
                }
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "duplicate field `user`")]
    fn duplicate_user_field() {
        let json = r#"{
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
                    },
                    "user": {
                        "name": "Peter Winckles",
                        "address": "mailto:me@example.com"
                    }
                }
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "missing field `id`")]
    fn missing_id_field() {
        let json = r#"{
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
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "missing field `type`")]
    fn missing_type_field() {
        let json = r#"{
            "id": "test",
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
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "missing field `digestAlgorithm`")]
    fn missing_algorithm_field() {
        let json = r#"{
            "id": "test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
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
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "missing field `head`")]
    fn missing_head_field() {
        let json = r#"{
            "id": "test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
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
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "missing field `manifest`")]
    fn missing_manifest_field() {
        let json = r#"{
            "id": "test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            "head": "v1",
            "contentDirectory": "content",
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
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "missing field `versions`")]
    fn missing_versions_field() {
        let json = r#"{
            "id": "test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            "head": "v1",
            "contentDirectory": "content",
            "manifest": {
                "fb0d38126bb990e2fd0edae87bf58e7a69e85a652b67cb9db30b32c138750377f6c3e1bb2f45588aeb0db1509f3562107f896b47d5b2c8972809e42e6bb68455": [
                    "v1/content/file1.txt"
                ]
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "missing field `created`")]
    fn missing_created_field() {
        let json = r#"{
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
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "missing field `state`")]
    fn missing_state_field() {
        let json = r#"{
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
                    "message": "initial commit",
                    "user": {
                        "name": "Peter Winckles",
                        "address": "mailto:me@example.com"
                    }
                }
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }

    #[test]
    #[should_panic(expected = "unknown field `bogus`")]
    fn unknown_field() {
        let json = r#"{
            "id": "test",
            "type": "https://ocfl.io/1.0/spec/#inventory",
            "digestAlgorithm": "sha512",
            "head": "v1",
            "contentDirectory": "content",
            "bogus": "123",
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
            }
        }"#;

        serde_json::from_str::<Inventory>(&json).unwrap();
    }
}

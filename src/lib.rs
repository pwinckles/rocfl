mod fs;

use std::collections::{HashMap};
use anyhow::{Result};
use chrono::{Local, DateTime};
use serde::Deserialize;
use thiserror::Error;
use std::convert::TryFrom;
use lazy_static::lazy_static;
use regex::Regex;
use grep::regex::{RegexMatcher};
use core::fmt;
use serde::export::Formatter;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::path::Path;

pub use self::fs::FsOcflRepo;

const OBJECT_MARKER: &str = "0=ocfl_object_1.0";
const INVENTORY_FILE: &str = "inventory.json";

lazy_static! {
        static ref VERSION_REGEX: Regex = Regex::new(r#"^v\d+$"#).unwrap();
        static ref OBJECT_ID_MATCHER: RegexMatcher = RegexMatcher::new(r#""id"\s*:\s*"([^"]+)""#).unwrap();
    }

pub trait OcflRepo {

    fn list_objects(&self) -> Result<Box<dyn Iterator<Item=Result<OcflObjectVersion>>>>;

    fn get_object(&self, object_id: &str, version: Option<VersionId>) -> Result<Option<OcflObjectVersion>>;

}

#[derive(Deserialize, Debug)]
#[serde(try_from = "&str")]
pub struct VersionId {
    pub version_num: u32,
    pub version_str: String,
}

impl VersionId {

    // TODO breaks 0-padding
    fn previous(&self) -> Result<VersionId, RocflError> {
        VersionId::try_from(self.version_num - 1)
    }

    // TODO breaks 0-padding
    fn next(&self) -> Result<VersionId, RocflError> {
        VersionId::try_from(self.version_num + 1)
    }

}

impl TryFrom<&str> for VersionId {
    type Error = RocflError;

    fn try_from(version: &str) -> Result<Self, Self::Error> {
        if !VERSION_REGEX.is_match(version) {
            return Err(RocflError::IllegalArgument(format!("Invalid version {}", version)));
        }

        match version[1..].parse::<u32>() {
            Ok(num) => {
                if num < 1 {
                    return Err(RocflError::IllegalArgument(format!("Invalid version {}", version)));
                }

                Ok(Self {
                    version_num: num,
                    version_str: version.to_string(),
                })
            },
            Err(_) => return Err(RocflError::IllegalArgument(format!("Invalid version {}", version)))
        }
    }
}

impl TryFrom<u32> for VersionId {
    type Error = RocflError;

    fn try_from(version: u32) -> Result<Self, Self::Error> {
        if version < 1 {
            return Err(RocflError::IllegalArgument(format!("Invalid version number {}", version)));
        }

        Ok(Self {
            version_num: version,
            version_str: format!("v{}", version),
        })
    }
}

impl Clone for VersionId {
    fn clone(&self) -> Self {
        Self {
            version_num: self.version_num.clone(),
            version_str: self.version_str.clone(),
        }
    }
}

impl fmt::Display for VersionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.version_str)
    }
}

impl PartialEq for VersionId {
    fn eq(&self, other: &Self) -> bool {
        self.version_num == other.version_num
    }
}

impl Eq for VersionId {}

impl Hash for VersionId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.version_num.hash(state)
    }
}

impl PartialOrd for VersionId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VersionId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.version_num.cmp(&other.version_num)
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Inventory {
    id: String,
    #[serde(rename = "type")]
    type_declaration: String,
    digest_algorithm: String,
    head: VersionId,
    content_directory: Option<String>,
    manifest: HashMap<String, Vec<String>>,
    versions: HashMap<VersionId, Version>,
    fixity: Option<HashMap<String, HashMap<String, Vec<String>>>>,
}

#[derive(Deserialize, Debug)]
struct Version {
    created: DateTime<Local>,
    state: HashMap<String, Vec<String>>,
    message: Option<String>,
    user: Option<User>
}

#[derive(Deserialize, Debug)]
struct User {
    name: Option<String>,
    address: Option<String>
}

impl Inventory {

    // TODO fill in more validations
    // TODO have a shallow and a deep validation
    pub fn validate(&self) -> Result<(), RocflError> {
        if !self.versions.contains_key(&self.head) {
            return Err(RocflError::CorruptObject {
                object_id: self.id.clone(),
                message: format!("HEAD version {} was not found", self.head),
            })
        }
        Ok(())
    }

}

pub struct OcflObjectVersion {
    pub id: String,
    pub version: VersionId,
    pub root: String,
    pub created: DateTime<Local>,
    pub state: HashMap<String, FileDetails>,
    // TODO more fields
}

pub struct FileDetails {
    pub digest: String,
    pub content_path: String,
    pub storage_path: String,
    // TODO see about making this a reference
    pub last_update: VersionDetails,
}

pub struct VersionDetails {
    pub version: VersionId,
    pub created: DateTime<Local>,
}

impl OcflObjectVersion {

    fn new<P: AsRef<Path>>(root: P, version: &VersionId, inventory: &Inventory) -> Result<Self> {
        let state = construct_state(&root, &version, inventory)?;

        Ok(Self {
            id: inventory.id.clone(),
            version: version.clone(),
            root: root.as_ref().to_str().unwrap_or_default().to_string(),
            created: ensure_version(version, inventory)?.created.clone(),
            state
        })
    }

}

fn construct_state<P: AsRef<Path>>(object_root: P, target: &VersionId, inventory: &Inventory) -> Result<HashMap<String, FileDetails>> {
    let mut state = HashMap::new();

    let target_version = ensure_version(target, inventory)?;
    let mut target_path_map = invert_path_map(&target_version.state);

    let mut current_version_id = (*target).clone();
    let mut current = target_version;

    while !target_path_map.is_empty() {
        let mut found: Vec<String> = vec![];

        if current_version_id.version_num == 1 {
            for (target_path, target_digest) in target_path_map.into_iter() {
                let content_path = lookup_content_path(&target_digest, inventory)?.to_string();
                state.insert(target_path, FileDetails {
                    storage_path: object_root.as_ref().join(&content_path).to_str().unwrap_or_default().to_string(),
                    content_path,
                    digest: target_digest,
                    last_update: VersionDetails {
                        version: current_version_id.clone(),
                        created: current.created.clone()
                    }
                });
            }

            break;
        }

        let previous_version_id = current_version_id.previous()?;
        let previous = ensure_version(&previous_version_id, inventory)?;
        let mut previous_path_map = invert_path_map(&previous.state);

        for (target_path, target_digest) in target_path_map.iter() {
            let entry = previous_path_map.remove_entry(target_path);

            if entry.is_none() || entry.unwrap().1 != *target_digest {
                found.push(target_path.clone());
                let content_path = lookup_content_path(&target_digest, inventory)?.to_string();
                state.insert(target_path.clone(), FileDetails {
                    digest: target_digest.clone(),
                    storage_path: object_root.as_ref().join(&content_path).to_str().unwrap_or_default().to_string(),
                    content_path,
                    last_update: VersionDetails {
                        version: current_version_id.clone(),
                        created: current.created.clone()
                    }
                });
            }
        }

        current_version_id = previous_version_id;
        current = previous;

        for path in found {
            target_path_map.remove(&path);
        }
    }

    Ok(state)
}

fn ensure_version<'a, 'b>(version: &'b VersionId, inventory: &'a Inventory) -> Result<&'a Version> {
    match inventory.versions.get(version) {
        Some(v) => Ok(v),
        None => Err(RocflError::NotFound(format!("Object {} version {}", inventory.id, version)).into())
    }
}

fn invert_path_map(map: &HashMap<String, Vec<String>>) -> HashMap<String, String> {
    let mut inverted = HashMap::new();

    for (digest, paths) in map {
        for path in paths {
            inverted.insert(path.clone(), digest.clone());
        }
    }

    inverted
}

fn lookup_content_path<'a>(digest: &'a str, inventory: &'a Inventory) -> Result<&'a str> {
    match inventory.manifest.get(digest) {
        Some(paths) => {
            match paths.first() {
                Some(path) => Ok(path.as_str()),
                None => Err(RocflError::CorruptObject {
                    object_id: inventory.id.clone(),
                    message: format!("Digest {} is not mapped to any content paths", digest)
                }.into())
            }
        },
        None => Err(RocflError::CorruptObject {
            object_id: inventory.id.clone(),
            message: format!("Digest {} not found in manifest", digest)
        }.into())
    }
}

#[derive(Error, Debug)]
pub enum RocflError {
    #[error("Object {object_id} is corrupt: {message}")]
    CorruptObject {
        object_id: String,
        message: String,
    },
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Illegal argument: {0}")]
    IllegalArgument(String)
}

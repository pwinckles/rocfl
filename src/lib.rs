mod fs;

use std::collections::{HashMap, BTreeMap};
use anyhow::{Result, anyhow};
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

pub use self::fs::FsOcflRepo;
use std::rc::Rc;

const OBJECT_MARKER: &str = "0=ocfl_object_1.0";
const ROOT_INVENTORY_FILE: &str = "inventory.json";
const MUTABLE_HEAD_INVENTORY_FILE: &str = "extensions/0004-mutable-head/head/inventory.json";

lazy_static! {
    static ref VERSION_REGEX: Regex = Regex::new(r#"^v\d+$"#).unwrap();
    static ref OBJECT_ID_MATCHER: RegexMatcher = RegexMatcher::new(r#""id"\s*:\s*"([^"]+)""#).unwrap();
}

pub trait OcflRepo {

    // TODO consider changing this to only return object level details to avoid needless version processing
    fn list_objects(&self, filter_glob: Option<&str>) -> Result<Box<dyn Iterator<Item=Result<ObjectVersion>>>>;

    fn get_object(&self, object_id: &str, version: Option<VersionId>) -> Result<Option<ObjectVersion>>;

    fn list_object_versions(&self, object_id: &str) -> Result<Option<Vec<VersionDetails>>>;

    fn list_file_versions(&self, object_id: &str, path: &str) -> Result<Option<Vec<VersionDetails>>>;

}

#[derive(Deserialize, Debug)]
#[serde(try_from = "&str")]
pub struct VersionId {
    pub version_num: u32,
    pub width: usize,
}

impl VersionId {

    fn previous(&self) -> Result<VersionId> {
        if self.version_num - 1 < 1 {
            return Err(anyhow!("Versions cannot be less than 1"));
        }

        Ok(Self {
            version_num: self.version_num - 1,
            width: self.width,
        })
    }

    #[allow(dead_code)]
    fn next(&self) -> Result<VersionId> {
        let max = match self.width {
            0 => usize::MAX,
            _ => (10 * (self.width - 1)) - 1
        };

        if self.version_num + 1 > max as u32 {
            return Err(anyhow!("Version cannot be greater than {}", max));
        }

        Ok(Self {
            version_num: self.version_num + 1,
            width: self.width,
        })
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

                let width = match version.starts_with("v0") {
                    true => version.len() - 1,
                    false => 0
                };

                Ok(Self {
                    version_num: num,
                    width,
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
            width: 0,
        })
    }
}

impl Clone for VersionId {
    fn clone(&self) -> Self {
        Self {
            version_num: self.version_num,
            width: self.width,
        }
    }
}

impl fmt::Display for VersionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "v{:0width$}", self.version_num, width = self.width)
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
    versions: BTreeMap<VersionId, Version>,
    fixity: Option<HashMap<String, HashMap<String, Vec<String>>>>,

    // This field is not in the inventory json file and must be added after deserialization
    #[serde(skip)]
    object_root: String,
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

    fn get_version(&self, version: &VersionId) -> Result<&Version> {
        match self.versions.get(version) {
            Some(v) => Ok(v),
            None => Err(RocflError::NotFound(format!("Object {} version {}", self.id, version)).into())
        }
    }

    fn remove_version(&mut self, version: &VersionId) -> Result<Version> {
        match self.versions.remove(version) {
            Some(v) => Ok(v),
            None => Err(RocflError::NotFound(format!("Object {} version {}", self.id, version)).into())
        }
    }

    fn lookup_content_path<'a>(&'a self, digest: &'a str) -> Result<&'a str> {
        match self.manifest.get(digest) {
            Some(paths) => {
                match paths.first() {
                    Some(path) => Ok(path.as_str()),
                    None => Err(RocflError::CorruptObject {
                        object_id: self.id.clone(),
                        message: format!("Digest {} is not mapped to any content paths", digest)
                    }.into())
                }
            },
            None => Err(RocflError::CorruptObject {
                object_id: self.id.clone(),
                message: format!("Digest {} not found in manifest", digest)
            }.into())
        }
    }

}

#[derive(Debug)]
pub struct ObjectVersion {
    pub id: String,
    pub object_root: String,
    pub digest_algorithm: String,
    pub version_details: VersionDetails,
    pub state: HashMap<String, FileDetails>,
}

#[derive(Debug)]
pub struct FileDetails {
    pub digest: Rc<String>,
    pub content_path: String,
    pub storage_path: String,
    pub last_update: Rc<VersionDetails>,
}

#[derive(Debug)]
pub struct VersionDetails {
    pub version: VersionId,
    pub created: DateTime<Local>,
    pub user_name: Option<String>,
    pub user_address: Option<String>,
    pub message: Option<String>,
}

impl ObjectVersion {
    fn from_inventory(mut inventory: Inventory, version_id: Option<&VersionId>) -> Result<Self> {
        let version_id = match version_id {
            Some(version) => version.clone(),
            None => inventory.head.clone(),
        };

        let version = inventory.get_version(&version_id)?;
        let version_details = VersionDetails::new(&version_id, version);

        let state = ObjectVersion::construct_state(&version_id, &mut inventory)?;

        Ok(Self {
            id: inventory.id,
            object_root: inventory.object_root,
            digest_algorithm: inventory.digest_algorithm,
            version_details,
            state
        })
    }

    fn construct_state(target: &VersionId, inventory: &mut Inventory) -> Result<HashMap<String, FileDetails>> {
        let mut state = HashMap::new();

        let mut current_version_id = (*target).clone();
        let mut current_version = inventory.remove_version(target)?;
        let mut target_path_map = invert_path_map(current_version.state);
        current_version.state = HashMap::new();

        while !target_path_map.is_empty() {
            let mut not_found = HashMap::new();
            let version_details = Rc::new(VersionDetails::from_version(current_version_id, current_version));

            // No versions left to compare to; any remaining files were last updated here
            if version_details.version.version_num == 1 {
                for (target_path, target_digest) in target_path_map.into_iter() {
                    let content_path = inventory.lookup_content_path(&target_digest)?.to_string();
                    state.insert(target_path, FileDetails::new(content_path,
                                                               target_digest,
                                                               &inventory.object_root,
                                                               Rc::clone(&version_details)));
                }

                break;
            }

            let previous_version_id = version_details.version.previous()?;
            let mut previous_version = inventory.remove_version(&previous_version_id)?;
            let mut previous_path_map = invert_path_map(previous_version.state);
            previous_version.state = HashMap::new();

            for (target_path, target_digest) in target_path_map.into_iter() {
                let entry = previous_path_map.remove_entry(&target_path);

                if entry.is_none() || entry.unwrap().1 != target_digest {
                    let content_path = inventory.lookup_content_path(&target_digest)?.to_string();
                    state.insert(target_path, FileDetails::new(content_path,
                                                               target_digest,
                                                               &inventory.object_root,
                                                               Rc::clone(&version_details)));
                } else {
                    not_found.insert(target_path, target_digest);
                }
            }

            current_version_id = previous_version_id;
            current_version = previous_version;

            target_path_map = not_found;
        }

        Ok(state)
    }
}

impl FileDetails {
    fn new(content_path: String, digest: Rc<String>, object_root: &str, version_details: Rc<VersionDetails>) -> Self {
        Self {
            storage_path: format!("{}/{}", object_root, content_path),
            content_path,
            digest,
            last_update: version_details,
        }
    }
}

impl VersionDetails {
    fn new(version_id: &VersionId, version: &Version) -> Self {
        let (user, address) = match &version.user {
            Some(user) => (user.name.clone(), user.address.clone()),
            None => (None, None)
        };

        Self {
            version: version_id.clone(),
            created: version.created.clone(),
            user_name: user,
            user_address: address,
            message: version.message.clone()
        }
    }

    fn from_version(version_id: VersionId, version: Version) -> Self {
        let (user, address) = match version.user {
            Some(user) => (user.name, user.address),
            None => (None, None)
        };

        Self {
            version: version_id,
            created: version.created,
            user_name: user,
            user_address: address,
            message: version.message,
        }
    }
}

fn invert_path_map(map: HashMap<String, Vec<String>>) -> HashMap<String, Rc<String>> {
    let mut inverted = HashMap::new();

    for (digest, paths) in map.into_iter() {
        let digest = Rc::new(digest);
        for path in paths.into_iter() {
            inverted.insert(path, Rc::clone(&digest));
        }
    }

    inverted
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

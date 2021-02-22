use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

use crate::ocfl::digest::{DigestAlgorithm, HexDigest};
use crate::ocfl::error::{Result, RocflError, not_found};
use crate::ocfl::{VersionNum, InventoryPath};
use crate::ocfl::bimap::PathBiMap;
use std::rc::Rc;
use crate::ocfl::consts::DEFAULT_CONTENT_DIR;

// TODO need to lock down all of these public members

/// OCFL inventory serialization object
#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Inventory {
    pub id: String,
    #[serde(rename = "type")]
    pub type_declaration: String,
    pub digest_algorithm: DigestAlgorithm,
    pub head: VersionNum,
    pub content_directory: Option<String>,
    // TODO look into deduping all HexDigests and InventoryPaths using a deserialize seed
    pub manifest: PathBiMap,
    pub versions: BTreeMap<VersionNum, Version>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fixity: Option<HashMap<String, HashMap<String, Vec<String>>>>,

    #[serde(skip)]
    pub object_root: String,
}

/// OCFL version serialization object
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Version {
    pub created: DateTime<Local>,
    pub state: PathBiMap,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<User>,

    /// All of the logical path parts that should be treated as directories
    #[serde(skip)]
    pub virtual_dirs: Option<HashSet<String>>,
}

/// OCFL user serialization object
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct User {
    pub name: Option<String>,
    pub address: Option<String>
}

impl Inventory {
    /// Creates a new HEAD version, copying over the state of the previous HEAD.
    pub fn create_staging_head(&mut self) -> Result<()> {
        let version_num = self.head.next()?;
        let version = self.head_version().clone_staged();
        self.versions.insert(version_num, version);
        self.head = version_num;

        Ok(())
    }

    /// Returns the HEAD version
    pub fn head_version(&self) -> &Version {
        // The head version must exist because we look for it when the Inventory is deserialized
        self.versions.get(&self.head).unwrap()
    }

    /// Returns a reference to the specified version or an error if it does not exist.
    pub fn get_version(&self, version_num: VersionNum) -> Result<&Version> {
        match self.versions.get(&version_num) {
            Some(v) => Ok(v),
            None => Err(not_found(&self.id, Some(version_num)))
        }
    }

    /// Removes and returns the specified version from the inventory, or an error if it does not exist.
    pub fn remove_version(&mut self, version_num: VersionNum) -> Result<Version> {
        match self.versions.remove(&version_num) {
            Some(v) => Ok(v),
            None => Err(not_found(&self.id, Some(version_num)))
        }
    }

    /// Returns the first content path associated with the specified digest, or an error if it does
    /// not exist.
    pub fn content_path_for_digest(&self, digest: &HexDigest) -> Result<&Rc<InventoryPath>> {
        match self.manifest.get_paths(digest) {
            Some(paths) => {
                match paths.iter().next() {
                    Some(path) => Ok(path),
                    None => Err(RocflError::CorruptObject {
                        object_id: self.id.clone(),
                        message: format!("Digest {} is not mapped to any content paths", digest)
                    })
                }
            }
            None => Err(RocflError::CorruptObject {
                object_id: self.id.clone(),
                message: format!("Digest {} not found in manifest", digest)
            })
        }
    }

    /// Returns the content path for the logical path, or a `NotFound` error if the path
    /// is not found.
    pub fn content_path_for_logical_path(&self,
                                         logical_path: &InventoryPath,
                                         version_num: Option<VersionNum>) -> Result<&Rc<InventoryPath>> {
        let version_num = version_num.unwrap_or(self.head);
        let version = self.get_version(version_num)?;

        let digest = match version.lookup_digest(&logical_path) {
            Some(digest) => digest,
            None => return Err(RocflError::NotFound(
                format!("Path {} not found in object {} version {}",
                        logical_path, self.id, version_num)))
        };

        self.content_path_for_digest(digest)
    }

    pub fn defaulted_content_dir(&self) -> &str {
        match &self.content_directory {
            Some(dir) => dir.as_str(),
            None => DEFAULT_CONTENT_DIR,
        }
    }

    /// Performs a spot check on the inventory to see if it appears valid. This is not an
    /// exhaustive check, and does not guarantee that the inventory is valid.
    pub fn validate(&self) -> Result<()> {
        if !self.versions.contains_key(&self.head) {
            return Err(RocflError::CorruptObject {
                object_id: self.id.clone(),
                message: format!("HEAD version {} was not found", self.head),
            })
        }
        Ok(())
    }
}

impl Version {
    /// Create a new Version initialized with values for staging
    pub fn new_staged() -> Self {
        Self::staged_version(PathBiMap::new())
    }

    /// Creates a new Version with a cloned state and staging meta
    pub fn clone_staged(&self) -> Self {
        Self::staged_version(self.state.clone())
    }

    fn staged_version(state: PathBiMap) -> Self {
        Self {
            created: Local::now(),
            message: Some("Staging new version".to_string()),
            user: Some(User {
                name: Some("rocfl".to_string()),
                address: Some("https://github.com/pwinckles/rocfl".to_string()),
            }),
            state,
            virtual_dirs: None,
        }
    }

    /// Returns a reference to the digest associated to a logical path, or None if the logical
    /// path does not exist in the version's state.
    pub fn lookup_digest(&self, logical_path: &InventoryPath) -> Option<&Rc<HexDigest>> {
        self.state.get_id(logical_path)
    }
}

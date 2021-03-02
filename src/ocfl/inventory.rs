use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryInto;
use std::mem;
use std::rc::Rc;

use chrono::{DateTime, Local};
use globset::GlobBuilder;
use log::error;
use once_cell::unsync::OnceCell;
use serde::{Deserialize, Serialize};

use crate::ocfl::bimap::{IntoIter, Iter, PathBiMap};
use crate::ocfl::consts::{DEFAULT_CONTENT_DIR, INVENTORY_TYPE};
use crate::ocfl::digest::{DigestAlgorithm, HexDigest};
use crate::ocfl::error::{not_found, Result, RocflError};
use crate::ocfl::{Diff, InventoryPath, VersionNum};

const STAGING_MESSAGE: &str = "Staging new version";
const ROCFL_USER: &str = "rocfl";
const ROCFL_ADDRESS: &str = "https://github.com/pwinckles/rocfl";

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
    manifest: PathBiMap,
    pub versions: BTreeMap<VersionNum, Version>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fixity: Option<HashMap<String, HashMap<String, Vec<String>>>>,

    #[serde(skip)]
    pub object_root: String,
}

/// Used to construct new inventories. This is not currently a general purposes builder. It is
/// focused on building new inventories for staging.
pub struct InventoryBuilder {
    id: String,
    type_declaration: String,
    digest_algorithm: DigestAlgorithm,
    head: VersionNum,
    content_directory: String,
    manifest: PathBiMap,
    versions: BTreeMap<VersionNum, Version>,
    object_root: String,
}

/// OCFL version serialization object
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Version {
    pub created: DateTime<Local>,
    state: PathBiMap,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<User>,

    /// All of the logical path parts that should be treated as directories
    #[serde(skip)]
    virtual_dirs: OnceCell<HashSet<InventoryPath>>,
}

/// OCFL user serialization object
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct User {
    pub name: Option<String>,
    pub address: Option<String>,
}

impl Inventory {
    /// Returns a new inventory builder
    pub fn builder(object_id: &str) -> InventoryBuilder {
        InventoryBuilder::new(object_id)
    }

    /// Creates a new HEAD version, copying over the state of the previous HEAD.
    pub fn create_staging_head(&mut self) -> Result<()> {
        let version_num = self.head.next()?;
        let version = self.head_version().clone_staged();
        self.versions.insert(version_num, version);
        self.head = version_num;

        Ok(())
    }

    /// Returns true if the HEAD version is equal to 1
    pub fn is_new(&self) -> bool {
        self.head.number == 1
    }

    /// Returns a reference to the HEAD version
    pub fn head_version(&self) -> &Version {
        // The head version must exist because we look for it when the Inventory is deserialized
        self.versions.get(&self.head).unwrap()
    }

    /// Returns a mutable reference to the HEAD version
    pub fn head_version_mut(&mut self) -> &mut Version {
        // The head version must exist because we look for it when the Inventory is deserialized
        self.versions.get_mut(&self.head).unwrap()
    }

    /// Returns a reference to the specified version or an error if it does not exist.
    pub fn get_version(&self, version_num: VersionNum) -> Result<&Version> {
        match self.versions.get(&version_num) {
            Some(v) => Ok(v),
            None => Err(not_found(&self.id, Some(version_num))),
        }
    }

    /// Removes and returns the specified version from the inventory, or an error if it does not exist.
    pub fn remove_version(&mut self, version_num: VersionNum) -> Result<Version> {
        match self.versions.remove(&version_num) {
            Some(v) => Ok(v),
            None => Err(not_found(&self.id, Some(version_num))),
        }
    }

    /// Returns the first content path associated with the specified digest, or an error if it does
    /// not exist.
    pub fn content_path_for_digest(&self, digest: &HexDigest) -> Result<&Rc<InventoryPath>> {
        match self.manifest.get_paths(digest) {
            Some(paths) => match paths.iter().next() {
                Some(path) => Ok(path),
                None => Err(RocflError::CorruptObject {
                    object_id: self.id.clone(),
                    message: format!("Digest {} is not mapped to any content paths", digest),
                }),
            },
            None => Err(RocflError::CorruptObject {
                object_id: self.id.clone(),
                message: format!("Digest {} not found in manifest", digest),
            }),
        }
    }

    /// Returns the content path for the logical path, or a `NotFound` error if the path
    /// is not found.
    pub fn content_path_for_logical_path(
        &self,
        logical_path: &InventoryPath,
        version_num: Option<VersionNum>,
    ) -> Result<&Rc<InventoryPath>> {
        let version_num = version_num.unwrap_or(self.head);
        let version = self.get_version(version_num)?;

        let digest = match version.lookup_digest(&logical_path) {
            Some(digest) => digest,
            None => {
                return Err(RocflError::NotFound(format!(
                    "Path {} not found in object {} version {}",
                    logical_path, self.id, version_num
                )))
            }
        };

        self.content_path_for_digest(digest)
    }

    /// Returns the diffs of two versions. An error is returned if either of the specified versions
    /// does not exist. If only one version is specified, then the diff is between the specified
    /// version and the version before it.
    pub fn diff_versions(&self, left: Option<VersionNum>, right: VersionNum) -> Result<Vec<Diff>> {
        if let Some(left) = left {
            if left == right {
                return Ok(Vec::new());
            }
        }

        let left = match left {
            Some(left) => Some(self.get_version(left)?),
            None => {
                if right.number > 1 {
                    Some(self.get_version(right.previous().unwrap())?)
                } else {
                    None
                }
            }
        };

        Ok(self.get_version(right)?.diff(left))
    }

    /// Dedups all of the content paths that were added in the most recent version. All of the
    /// paths that are removed from the manifest are returned.
    pub fn dedup_head(&mut self) -> Vec<Rc<InventoryPath>> {
        let mut removed = Vec::new();
        let prefix = format!("{}/", self.head.to_string());

        let mut matches: HashMap<Rc<HexDigest>, HashSet<Rc<InventoryPath>>> = HashMap::new();

        for (digest, paths) in self.manifest.iter_id_paths() {
            if paths.len() > 1 {
                for path in paths {
                    if path.as_ref().as_ref().starts_with(&prefix) {
                        matches
                            .entry(digest.clone())
                            .or_insert_with(HashSet::new)
                            .insert(path.clone());
                    }
                }
            }
        }

        for (digest, paths) in matches {
            let total = self.manifest.get_paths(&digest).unwrap().len();

            if total == paths.len() {
                // All of the paths were added in this version; remove all but one
                let mut iter = paths.into_iter().peekable();
                while let Some(path) = iter.next() {
                    if iter.peek().is_some() {
                        self.manifest.remove_path(&path);
                        removed.push(path);
                    }
                }
            } else {
                // There's a copy in an earlier version; remove them all
                for path in paths {
                    self.manifest.remove_path(&path);
                    removed.push(path);
                }
            }
        }

        removed
    }

    /// Adds a file to the manifest and version state of the HEAD version.
    ///
    /// If the digest already exists in the manifest, an additional entry for it is added.
    /// Content paths are NOT deduped until the version is committed.
    ///
    /// If the logical path already exists in the version, then the existing file is overwritten.
    pub fn add_file_to_head(
        &mut self,
        digest: HexDigest,
        logical_path: InventoryPath,
    ) -> Result<()> {
        let digest_rc = match self.manifest.get_id_rc(&digest) {
            Some(digest_rc) => digest_rc.clone(),
            None => Rc::new(digest),
        };

        let content_path = self.new_content_path_head(&logical_path)?;
        self.manifest
            .insert_rc(digest_rc.clone(), Rc::new(content_path));

        let version = match self.versions.get_mut(&self.head) {
            Some(version) => version,
            None => return Err(not_found(&self.id, Some(self.head))),
        };

        version.add_file(digest_rc, logical_path);

        Ok(())
    }

    /// Returns a new content path for the specified logical path, assuming a direct one-to-one
    /// mapping of logical path to content path.
    pub fn new_content_path_head(&self, logical_path: &InventoryPath) -> Result<InventoryPath> {
        self.new_content_path(self.head, logical_path)
    }

    /// Returns a new content path for the specified logical path, assuming a direct one-to-one
    /// mapping of logical path to content path.
    pub fn new_content_path(
        &self,
        version_num: VersionNum,
        logical_path: &InventoryPath,
    ) -> Result<InventoryPath> {
        // TODO this is not correct for the mutable HEAD
        // TODO should any other path cleanup be performed?
        format!(
            "{}/{}/{}",
            version_num.to_string(),
            self.defaulted_content_dir(),
            logical_path.as_ref()
        )
        .try_into()
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
            });
        }
        Ok(())
    }
}

impl InventoryBuilder {
    pub fn new(object_id: &str) -> Self {
        Self {
            id: object_id.to_string(),
            type_declaration: INVENTORY_TYPE.to_string(),
            digest_algorithm: DigestAlgorithm::Sha512,
            head: VersionNum::new(1, 0),
            content_directory: DEFAULT_CONTENT_DIR.to_string(),
            manifest: PathBiMap::new(),
            versions: BTreeMap::new(),
            object_root: "".to_string(),
        }
    }

    pub fn with_digest_algorithm(mut self, digest_algorithm: DigestAlgorithm) -> Self {
        self.digest_algorithm = digest_algorithm;
        self
    }

    pub fn with_head(mut self, head: VersionNum) -> Self {
        self.head = head;
        self
    }

    pub fn with_content_directory(mut self, content_directory: &str) -> Self {
        self.content_directory = content_directory.to_string();
        self
    }

    pub fn build(mut self) -> Result<Inventory> {
        self.versions.insert(self.head, Version::new_staged());

        let inventory = Inventory {
            id: self.id,
            type_declaration: self.type_declaration,
            digest_algorithm: self.digest_algorithm,
            head: self.head,
            content_directory: Some(self.content_directory),
            manifest: self.manifest,
            versions: self.versions,
            fixity: None,
            object_root: self.object_root,
        };

        inventory.validate()?;

        Ok(inventory)
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
            message: Some(STAGING_MESSAGE.to_string()),
            user: Some(User {
                name: Some(ROCFL_USER.to_string()),
                address: Some(ROCFL_ADDRESS.to_string()),
            }),
            state,
            virtual_dirs: OnceCell::default(),
        }
    }

    pub fn update_meta(
        &mut self,
        name: &Option<String>,
        address: &Option<String>,
        message: &Option<String>,
    ) {
        self.message = message.clone();
        self.user = match name {
            Some(name) => Some(User::new(name.clone(), address.clone())),
            None => None,
        };
        self.created = Local::now();
    }

    /// Returns a consuming iterator for the version's state
    pub fn state_into_iter(&mut self) -> IntoIter {
        self.remove_state().into_iter()
    }

    /// Returns non-consuming iterator for the version's state
    pub fn state_iter(&self) -> Iter {
        self.state.iter()
    }

    /// Removes a logical path from the version's state
    pub fn remove_file(
        &mut self,
        path: &InventoryPath,
    ) -> Option<(Rc<InventoryPath>, Rc<HexDigest>)> {
        // must invalidate the virtual dirs
        if self.virtual_dirs.get().is_some() {
            self.virtual_dirs = OnceCell::default();
        }
        self.state.remove_path(path)
    }

    /// Moves the current state map out, replacing it when an empty state
    pub fn remove_state(&mut self) -> PathBiMap {
        if self.virtual_dirs.get().is_some() {
            self.virtual_dirs = OnceCell::default();
        }
        mem::replace(&mut self.state, PathBiMap::new())
    }

    /// Returns a reference to the digest associated to a logical path, or None if the logical
    /// path does not exist in the version's state.
    pub fn lookup_digest(&self, logical_path: &InventoryPath) -> Option<&Rc<HexDigest>> {
        self.state.get_id(logical_path)
    }

    /// Returns true if the specified path exists as either a logical file or virtual directory
    pub fn exists(&self, path: &InventoryPath) -> bool {
        self.is_file(&path) || self.is_dir(path)
    }

    /// Returns true if the specified path is a logical file
    pub fn is_file(&self, path: &InventoryPath) -> bool {
        self.state.contains_path(path)
    }

    // Returns true if the specified path is a virtual directory
    pub fn is_dir(&self, path: &InventoryPath) -> bool {
        self.get_virtual_dirs().contains(path)
    }

    /// Returns an error if the specified path conflicts with the existing state.
    /// A path conflicts if it a portion of the path is interpreted as both a directory
    /// and a file.
    pub fn validate_non_conflicting(&self, path: &InventoryPath) -> Result<()> {
        if self.is_dir(&path) {
            return Err(RocflError::IllegalState(format!(
                "Conflicting logical path {}: This path is already in use as a directory",
                path
            )));
        }

        foreach_dir(&path, |dir| {
            if self.is_file(&dir) {
                return Err(RocflError::IllegalState(format!(
                    "Conflicting logical path {}: The path part {} is an existing logical file",
                    path, dir
                )));
            }
            Ok(())
        })
    }

    pub fn resolve_glob(&self, glob: &str, recursive: bool) -> Result<HashSet<Rc<InventoryPath>>> {
        let mut matches = HashSet::new();

        // Logical paths do not have leading slashes
        let mut glob = glob.trim_start_matches('/');

        // Select the object root directory
        if glob.is_empty() {
            glob = "*";
        }

        let matcher = GlobBuilder::new(glob)
            .literal_separator(true)
            .backslash_escape(true)
            .build()?
            .compile_matcher();

        for (path, _digest) in self.state.iter() {
            if matcher.is_match(path.as_ref().as_ref()) {
                matches.insert(path.clone());
            }
        }

        if recursive {
            for dir in self.get_virtual_dirs() {
                if matcher.is_match(dir.as_ref()) {
                    let prefix = format!("{}/", dir);
                    for (path, _digest) in self.state.iter() {
                        if path.as_ref().as_ref().starts_with(&prefix) {
                            matches.insert(path.clone());
                        }
                    }
                }
            }
        }

        Ok(matches)
    }

    /// Computes a diff between the versions. This version is the right-hand version and the
    /// other version is the left hand version. If the other version is None, then all of
    /// this version's paths are returned as Adds.
    pub fn diff(&self, other: Option<&Version>) -> Vec<Diff> {
        let mut diffs = Vec::new();
        let mut deletes: HashMap<Rc<HexDigest>, Vec<Rc<InventoryPath>>> = HashMap::new();

        if let Some(left) = other {
            let mut seen = HashSet::with_capacity(left.state.len());

            for (path, left_digest) in left.state_iter() {
                match self.lookup_digest(&path) {
                    None => {
                        deletes
                            .entry(left_digest.clone())
                            .or_insert_with(Vec::new)
                            .push(path.clone());
                    }
                    Some(right_digest) => {
                        seen.insert(path.clone());
                        if left_digest != right_digest {
                            diffs.push(Diff::Modified(path.clone()))
                        }
                    }
                }
            }

            let mut renames: HashMap<Rc<HexDigest>, Diff> = HashMap::new();

            for (path, digest) in self.state_iter() {
                if seen.contains(path) {
                    continue;
                }

                if let Some(original) = deletes.remove(digest) {
                    let mut renamed = Vec::new();
                    renamed.push(path.clone());
                    renames.insert(digest.clone(), Diff::Renamed { original, renamed });
                } else if let Some(Diff::Renamed {
                    original: _,
                    renamed,
                }) = renames.get_mut(digest)
                {
                    renamed.push(path.clone());
                } else {
                    diffs.push(Diff::Added(path.clone()));
                }
            }

            for (_digest, deletes) in deletes {
                for delete in deletes {
                    diffs.push(Diff::Deleted(delete));
                }
            }

            for (_digest, mut rename) in renames {
                if let Diff::Renamed { original, renamed } = &mut rename {
                    original.sort_unstable();
                    renamed.sort_unstable();
                }
                diffs.push(rename);
            }
        } else {
            for (path, _digest) in self.state_iter() {
                diffs.push(Diff::Added(path.clone()));
            }
        }

        diffs
    }

    /// Adds a new logical path to the version, and updates the virtual directory set, if needed.
    /// This path MUST be added to the inventory manifest separately for the inventory to be valid.
    fn add_file(&mut self, digest: Rc<HexDigest>, logical_path: InventoryPath) {
        if let Some(dirs) = self.virtual_dirs.get_mut() {
            if let Err(e) = foreach_dir(&logical_path, |dir| {
                dirs.insert(dir);
                Ok(())
            }) {
                // This should be impossible
                error!("{}", e)
            }
        }
        self.state.insert_rc(digest, Rc::new(logical_path));
    }

    /// Initializes a HashSet containing all of the virtual directories within a version.
    fn get_virtual_dirs(&self) -> &HashSet<InventoryPath> {
        self.virtual_dirs.get_or_init(|| {
            let mut dirs: HashSet<InventoryPath> = HashSet::with_capacity(self.state.len());
            for (path, _) in self.state.iter() {
                if let Err(e) = foreach_dir(path, |dir| {
                    dirs.insert(dir);
                    Ok(())
                }) {
                    // This should be impossible
                    error!("{}", e)
                }
            }
            // Add the root path
            dirs.insert("/".try_into().unwrap());
            dirs
        })
    }
}

impl User {
    pub fn new(name: String, address: Option<String>) -> Self {
        Self {
            name: Some(name),
            address,
        }
    }
}

/// Executes the `consumer` on every virtual directory that is part of the input path.
fn foreach_dir<F: FnMut(InventoryPath) -> Result<()>>(
    path: &InventoryPath,
    mut consumer: F,
) -> Result<()> {
    let mut parts = path.parts();
    let mut dir = String::new();
    let mut current = parts.next();
    let mut next = parts.next();

    while next.is_some() {
        if !dir.is_empty() {
            dir.push('/');
        }
        dir.push_str(current.unwrap());

        let dir = dir.as_str().try_into()?;
        consumer(dir)?;

        current = next;
        next = parts.next();
    }

    Ok(())
}

//! This library is a storage agnostic abstraction over [OCFL repositories](https://ocfl.io/).
//! It is **not** thread-safe.
//!
//! Create a new `OcflRepo` as follows:
//!
//! ```rust
//! use rocfl::ocfl::OcflRepo;
//!
//! let repo = OcflRepo::new_fs_repo("path/to/ocfl/storage/root");
//! ```

use core::fmt;
use std::{error, io};
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryFrom;
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, Metadata};
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::ops::{Deref, DerefMut};
use std::path::{self, Path, PathBuf};
use std::rc::Rc;
use std::str::FromStr;

use blake2::{Blake2b, VarBlake2b};
use chrono::{DateTime, Local};
use digest::{Digest, DynDigest, Update, VariableOutput};
use lazy_static::lazy_static;
use log::error;
use md5::Md5;
use regex::Regex;
#[cfg(feature = "s3")]
use rusoto_core::Region;
#[cfg(feature = "s3")]
use rusoto_core::region::ParseRegionError;
#[cfg(feature = "s3")]
use rusoto_core::RusotoError;
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Sha256, Sha512, Sha512Trunc256};
use strum_macros::{Display as EnumDisplay, EnumString};
use thiserror::Error;

use crate::ocfl::layout::StorageLayout;

use self::fs::FsOcflStore;
use self::layout::LayoutExtensionName;
#[cfg(feature = "s3")]
use self::s3::S3OcflStore;

mod fs;
#[cfg(feature = "s3")]
mod s3;
pub mod layout;

const REPO_NAMASTE_FILE: &str = "0=ocfl_1.0";
const OBJECT_NAMASTE_FILE: &str = "0=ocfl_object_1.0";
const INVENTORY_FILE: &str = "inventory.json";
const OCFL_LAYOUT_FILE: &str = "ocfl_layout.json";
const OCFL_SPEC_FILE: &str = "ocfl_1.0.txt";
const EXTENSIONS_DIR: &str = "extensions";
const EXTENSIONS_CONFIG_FILE: &str = "config.json";
const OCFL_VERSION: &str = "ocfl_1.0";
const OCFL_OBJECT_VERSION: &str = "ocfl_object_1.0";
const INVENTORY_TYPE: &str = "https://ocfl.io/1.0/spec/#inventory";

const MUTABLE_HEAD_INVENTORY_FILE: &str = "extensions/0005-mutable-head/head/inventory.json";
const ROCFL_STAGING_EXTENSION: &str = "rocfl-staging";

lazy_static! {
    static ref VERSION_REGEX: Regex = Regex::new(r#"^v\d+$"#).unwrap();
}

// ================================================== //
//             public structs+enums+traits            //
// ================================================== //

/// Interface for interacting with an OCFL repository
pub struct OcflRepo {
    /// For local filesystem repos, this is the storage root. TBD for S3.
    root: PathBuf,
    store: Box<dyn OcflStore>,
    staging: RefCell<Option<FsOcflStore>>,
}

/// Represents an [OCFL object version](https://ocfl.io/1.0/spec/#version-directories).
#[derive(Deserialize, Serialize,  Debug, Copy, Clone)]
#[serde(try_from = "&str")]
#[serde(into = "String")]
pub struct VersionNum {
    pub number: u32,
    pub width: u32,
}

/// Represents a version of an OCFL object
#[derive(Debug, Eq, PartialEq)]
pub struct ObjectVersion {
    /// The object's ID
    pub id: String,
    /// The path from the storage root to the object root
    pub object_root: String,
    /// The algorithm used to calculate digests (sha512 or sha256)
    pub digest_algorithm: DigestAlgorithm,
    /// Metadata about the version
    pub version_details: VersionDetails,
    /// A map of files (logical paths) in the version to details about the files.
    pub state: HashMap<String, FileDetails>,
}

/// Details about a file in an OCFL object
#[derive(Debug, Eq, PartialEq)]
pub struct FileDetails {
    /// The file's digest
    pub digest: Rc<String>,
    /// The digest algorithm
    pub digest_algorithm: DigestAlgorithm,
    /// The path to the file relative the object root
    pub content_path: String,
    /// The path to the file relative the storage root
    pub storage_path: String,
    /// The version metadata for when the file was last updated
    pub last_update: Rc<VersionDetails>,
}

/// Metadata about a version
#[derive(Debug, Eq, PartialEq)]
pub struct VersionDetails {
    /// The version number of the version
    pub version_num: VersionNum,
    /// When the version was created
    pub created: DateTime<Local>,
    /// The name of the person who created the version
    pub user_name: Option<String>,
    /// The address of the person who created the version
    pub user_address: Option<String>,
    /// A description of the version
    pub message: Option<String>,
}

/// Similar to `ObjectVersion`, except it does not contain the state map.
#[derive(Debug, Eq, PartialEq)]
pub struct ObjectVersionDetails {
    /// The object's ID
    pub id: String,
    /// The path from the storage root to the object root
    pub object_root: String,
    /// The algorithm used to calculate digests (sha512 or sha256)
    pub digest_algorithm: DigestAlgorithm,
    /// Metadata about the version
    pub version_details: VersionDetails,
}

/// Enum of all valid digest algorithms
#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Copy, Clone, EnumString, EnumDisplay)]
pub enum DigestAlgorithm {
    #[serde(rename = "md5")]
    #[strum(serialize = "md5")]
    Md5,
    #[serde(rename = "sha1")]
    #[strum(serialize = "sha1")]
    Sha1,
    #[serde(rename = "sha256")]
    #[strum(serialize = "sha256")]
    Sha256,
    #[serde(rename = "sha512")]
    #[strum(serialize = "sha512")]
    Sha512,
    #[serde(rename = "sha512/256")]
    #[strum(serialize = "sha512/256")]
    Sha512_256,
    #[serde(rename = "blake2b-512")]
    #[strum(serialize = "blake2b-512")]
    Blake2b512,
    #[serde(rename = "blake2b-160")]
    #[strum(serialize = "blake2b-160")]
    Blake2b160,
    #[serde(rename = "blake2b-256")]
    #[strum(serialize = "blake2b-256")]
    Blake2b256,
    #[serde(rename = "blake2b-384")]
    #[strum(serialize = "blake2b-384")]
    Blake2b384,
}

/// Reader wrapper that calculates a digest while reading
pub struct DigestReader<R: Read> {
    digest: Box<dyn DynDigest>,
    inner: R,
}

/// Represents a change to a file
#[derive(Debug, Eq, PartialEq)]
pub struct Diff {
    /// The type of change
    pub diff_type: DiffType,
    /// The affected logical path
    pub path: String,
}

/// Represents a type of change
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DiffType {
    Added,
    Modified,
    Deleted,
}

pub type Result<T, E = RocflError> = core::result::Result<T, E>;

/// Application errors
#[derive(Error)]
pub enum RocflError {
    #[error("Object {object_id} is corrupt: {message}")]
    CorruptObject {
        object_id: String,
        message: String,
    },

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Illegal argument: {0}")]
    IllegalArgument(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Illegal state: {0}")]
    IllegalState(String),

    #[error("{0}")]
    General(String),

    #[error("{0}")]
    Io(io::Error),

    #[error("{0}")]
    Wrapped(Box<dyn error::Error>),
}

impl Debug for RocflError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl From<io::Error> for RocflError {
    fn from(e: io::Error) -> Self {
        RocflError::Io(e)
    }
}

impl From<globset::Error> for RocflError {
    fn from(e: globset::Error) -> Self {
        RocflError::Wrapped(Box::new(e))
    }
}

impl From<ParseRegionError> for RocflError {
    fn from(e: ParseRegionError) -> Self {
        RocflError::Wrapped(Box::new(e))
    }
}

impl From<serde_json::Error> for RocflError {
    fn from(e: serde_json::Error) -> Self {
        RocflError::Wrapped(Box::new(e))
    }
}

impl<T: error::Error + 'static> From<RusotoError<T>> for RocflError {
    fn from(e: RusotoError<T>) -> Self {
        RocflError::Wrapped(Box::new(e))
    }
}

// TODO remove this
pub trait Validate {
    fn validate(&self) -> Result<()>;
}

// ================================================== //
//                   public impls+fns                 //
// ================================================== //

impl OcflRepo {
    /// Creates a new `OcflRepo` instance backed by the local filesystem. `storage_root` is the
    /// location of the OCFL repository to open. The OCFL repository must already exist.
    pub fn new_fs_repo<P: AsRef<Path>>(storage_root: P) -> Result<Self> {
        Ok(Self {
            root: PathBuf::from(storage_root.as_ref()),
            store: Box::new(FsOcflStore::new(storage_root)?),
            staging: RefCell::new(None),
        })
    }

    /// Initializes a new `OcflRepo` instance backed by the local filesystem. The OCFL repository
    /// most not already exist.
    pub fn init_fs_repo<P: AsRef<Path>>(root: P, layout: StorageLayout) -> Result<Self> {
        Ok(Self {
            root: PathBuf::from(root.as_ref()),
            store: Box::new(FsOcflStore::init(root, layout)?),
            staging: RefCell::new(None),
        })
    }

    /// Creates a new `OcflRepo` instance backed by S3. `prefix` used to specify a virtual
    /// sub directory within a bucket that the OCFL repository is rooted in.
    #[cfg(feature = "s3")]
    pub fn new_s3_repo(region: Region, bucket: &str, prefix: Option<&str>) -> Result<Self> {
        Ok(Self {
            // TODO this is not correct
            root: PathBuf::from("."),
            store: Box::new(S3OcflStore::new(region, bucket, prefix)?),
            staging: RefCell::new(None),
        })
    }

    /// Returns an iterator that iterate through all of the objects in an OCFL repository.
    /// Objects are lazy-loaded. An optional glob pattern may be provided to filter the objects
    /// that are returned.
    ///
    /// The iterator return an error if it encounters a problem accessing an object. This does
    /// terminate the iterator; there are still more objects until it returns `None`.
    pub fn list_objects<'a>(&'a self, filter_glob: Option<&str>)
        -> Result<Box<dyn Iterator<Item=ObjectVersionDetails> + 'a>> {
        let inv_iter = self.store.iter_inventories(filter_glob)?;

        Ok(Box::new(InventoryAdapterIter::new(inv_iter, |inventory| {
            ObjectVersionDetails::from_inventory(inventory, None)
        })))
    }

    /// Returns a view of a version of an object. If a `VersionNum` is not specified,
    /// then the head version of the object is returned.
    ///
    /// If the object or version of the object cannot be found, then a `RocflError::NotFound`
    /// error is returned.
    pub fn get_object(&self,
                      object_id: &str,
                      version_num: Option<VersionNum>) -> Result<ObjectVersion> {
        let inventory = self.store.get_inventory(object_id)?;
        Ok(ObjectVersion::from_inventory(inventory, version_num)?)
    }

    /// Returns high-level details about an object version. This method is similar to
    /// `OcflRepo::get_object()` except that it does less processing and does not
    /// include the version's state.
    ///
    /// If the object or version of the object cannot be found, then a `RocflError::NotFound`
    /// error is returned.
    pub fn get_object_details(&self,
                              object_id: &str,
                              version_num: Option<VersionNum>) -> Result<ObjectVersionDetails> {
        let inventory = self.store.get_inventory(object_id)?;
        Ok(ObjectVersionDetails::from_inventory(inventory, version_num)?)
    }

    /// Returns a vector containing the version metadata for ever version of an object. The vector
    /// is sorted in ascending order.
    ///
    /// If the object cannot be found, then a `RocflError::NotFound` error is returned.
    pub fn list_object_versions(&self, object_id: &str) -> Result<Vec<VersionDetails>> {
        let inventory = self.store.get_inventory(object_id)?;
        let mut versions = Vec::with_capacity(inventory.versions.len());

        for (id, version) in inventory.versions {
            versions.push(VersionDetails::from_version(id, version))
        }

        Ok(versions)
    }

    /// Writes the specified file to the sink.
    ///
    /// If the file cannot be found, then a `RocflError::NotFound` error is returned.
    pub fn get_object_file(&self,
                           object_id: &str,
                           path: &str,
                           version_num: Option<VersionNum>,
                           sink: &mut dyn Write) -> Result<()> {
        self.store.get_object_file(object_id, path, version_num, sink)
    }

    /// Returns a vector contain the version metadata for every version of an object that
    /// affected the specified file. The vector is sorted in ascending order.
    ///
    /// If the object or path cannot be found, then a `RocflError::NotFound' error is returned.
    pub fn list_file_versions(&self, object_id: &str, path: &str) -> Result<Vec<VersionDetails>> {
        let inventory = self.store.get_inventory(object_id)?;

        let mut versions = Vec::new();

        let path = path.to_string();
        let mut current_digest: Option<String> = None;

        for (id, version) in inventory.versions {
            match version.lookup_digest(&path) {
                Some(digest) => {
                    if current_digest.is_none() || current_digest.as_ref().unwrap().ne(digest) {
                        current_digest = Some(digest.clone());
                        versions.push(VersionDetails::from_version(id, version));
                    }
                }
                None => {
                    if current_digest.is_some() {
                        current_digest = None;
                        versions.push(VersionDetails::from_version(id, version));
                    }
                }
            }
        }

        if versions.is_empty() {
            return Err(RocflError::NotFound(format!("Path {} not found in object {}",
                                                    path, object_id)));
        }

        Ok(versions)
    }

    /// Returns the diff of two object versions. If only one version is specified, then the diff
    /// is between the specified version and the version before it.
    ///
    /// If the object cannot be found, then a `RocflError::NotFound` error is returned.
    pub fn diff(&self,
                object_id: &str,
                left_version: Option<VersionNum>,
                right_version: VersionNum) -> Result<Vec<Diff>> {
        if left_version.is_some() && right_version.eq(&left_version.unwrap()) {
            return Ok(vec![])
        }

        let mut inventory = self.store.get_inventory(object_id)?;

        let right = inventory.remove_version(right_version)?;

        let left = match left_version {
            Some(version) => Some(inventory.remove_version(version)?),
            None => {
                if right_version.number > 1 {
                    Some(inventory.remove_version(right_version.previous().unwrap())?)
                } else {
                    None
                }
            }
        };

        let mut right_state = invert_path_map(right.state);

        let mut diffs = Vec::new();

        if let Some(left) = left {
            let left_state = invert_path_map(left.state);

            for (path, left_digest) in left_state {
                match right_state.remove(&path) {
                    None => diffs.push(Diff::deleted(path)),
                    Some(right_digest) => {
                        if left_digest.deref().ne(right_digest.deref()) {
                            diffs.push(Diff::modified(path))
                        }
                    }
                }
            }

            // TODO Renames can be detected if the same digest has both a D and an A
            for (path, _digest) in right_state {
                diffs.push(Diff::added(path))
            }
        } else {
            for (path, _digest) in right_state {
                diffs.push(Diff::added(path));
            }
        }

        Ok(diffs)
    }

    /// Stages a new OCFL object if there is not an existing object with the same ID. The object
    /// is not inserted into the repository until it is committed.
    pub fn create_object(&self,
                         object_id: &str,
                         digest_algorithm: DigestAlgorithm,
                         content_dir: &str,
                         padding_width: u32) -> Result<()> {

        let object_id = object_id.trim();

        if object_id.is_empty() {
            return Err(RocflError::IllegalArgument("Object IDs may not be blank".to_string()));
        }

        if digest_algorithm != DigestAlgorithm::Sha512
            && digest_algorithm != DigestAlgorithm::Sha256 {
            return Err(RocflError::IllegalArgument(
                format!("The inventory digest algorithm must be sha512 or sha256. Found: {}",
                        digest_algorithm.to_string())))
        }

        if content_dir.eq(".") || content_dir.eq("..") || content_dir.contains('/') {
            return Err(RocflError::IllegalArgument(
                format!("The content directory cannot equal '.' or '..' and cannot contain a '/'. Found: {}",
                        content_dir)));
        }

        match self.store.get_inventory(&object_id) {
            Err(RocflError::NotFound(_)) => (),
            Err(e) => return Err(e),
            _ => {
                return Err(RocflError::IllegalState(
                    format!("Cannot create object {} because it already exists", object_id)));
            }
        }

        let mut versions = BTreeMap::new();
        let version_num = VersionNum::new(1, padding_width);
        versions.insert(version_num, Version::new_staged());

        let inventory = Inventory {
            id: object_id.to_string(),
            type_declaration: INVENTORY_TYPE.to_string(),
            digest_algorithm,
            content_directory: Some(content_dir.to_string()),
            head: version_num,
            manifest: HashMap::new(),
            versions,
            fixity: None,
            object_root: "".to_string(),
        };

        self.create_staging_if_necessary()?;
        self.staging.borrow().as_ref().unwrap().stage_object(&inventory)
    }

    /// Copies files from outside the OCFL repository into the specified OCFL object.
    /// A destination of `/` specifies the object's root.
    ///
    /// If `force` is `false` and the copy operation attempts to write a file to a logical
    /// path where there is already a file, then the new file will **not** be copied.
    pub fn copy_files_external<P: AsRef<Path>>(&self,
                      object_id: &str,
                      src: &[P],
                      dst: &str,
                      recursive: bool,
                      force: bool) -> Result<()> {
        // TODO enforce src > 0
        // TODO enforce that the dst is legal

        self.create_staging_if_necessary()?;
        let staging_borrow = self.staging.borrow();  // This is necessary to keep it in scope
        let staging = staging_borrow.as_ref().unwrap();

        // TODO even though this is not supposed to be used concurrently, it's not a bad idea
        //      to get some sort of file lock here so that an object cannot be updated concurrently

        let mut inventory = match staging.get_inventory(&object_id) {
            Ok(inventory) => inventory,
            Err(RocflError::NotFound(_)) => {
                let mut inventory = self.store.get_inventory(&object_id)?;
                inventory.create_staging_head()?;
                // TODO is this step necessary? can I wait till after copying the files?
                staging.stage_object(&inventory)?;
                inventory
            },
            Err(e) => return Err(e),
        };

        // TODO cleanup
        for path in src.iter() {
            let path = path.as_ref();
            match std::fs::metadata(&path) {
                Err(e) => error!("Could not read file {}: {}", path.to_string_lossy(), e),
                Ok(meta) => {
                    // TODO symbolic links?
                    if meta.is_file() {
                        // TODO need to continue on error
                        let file = File::open(&path)?;
                        let mut reader = inventory.digest_algorithm.reader(file)?;

                        // TODO this path is wrong -- must determine if it is a directory
                        let mut logical_path = dst.to_string();

                        if src.len() > 1 {
                            logical_path.push('/');
                            logical_path.push_str(&path.file_name().unwrap().to_string_lossy());
                        }

                        // TODO overwrite protection
                        // TODO validate legal path

                        // TODO or should it just fail?
                        match staging.stage_file(&inventory, &mut reader, &logical_path) {
                            Ok(content_path) => {
                                // TODO make methods
                                let digest = reader.finalize_hex();
                                if !inventory.manifest.contains_key(&digest) {
                                    let mut paths = Vec::with_capacity(1);
                                    paths.push(content_path);
                                    inventory.manifest.insert(digest.clone(), paths);
                                }
                                // TODO
                                let version = inventory.versions.get_mut(&inventory.head).unwrap();
                                version.state.entry(digest)
                                    .or_insert_with(|| Vec::with_capacity(1))
                                    .push(logical_path);
                            }
                            Err(e) => error!("Failed to copy file {} to object {}: {}",
                                   &path.to_string_lossy(), &object_id, e)
                        }
                    } else if recursive {
                        // TODO walk directory
                    } else {
                        error!("Skipping directory {} because recursive copy is not enabled",
                               path.to_string_lossy());
                    }
                }
            }
        }

        // TODO need to touch the version timestamp
        staging.stage_inventory(&inventory)?;

        Ok(())
    }

    /// Copies files from inside the OCFL repository into the specified OCFL object.
    ///
    /// If `dst_object_id` is not specified, then the files are copied within the same OCFL
    /// object. If it is specified, then the files are copied between OCFL objects.
    ///
    /// The `src` parameter may be a glob pattern. `glob_literal_separator` controls whether
    /// wildcards match `/`.
    ///
    /// If `force` is `false` and the copy operation attempts to write a file to a logical
    /// path where there is already a file, then the new file will **not** be copied.
    pub fn copy_files_internal(&self,
                      src_obj_id: &str,
                      src: &[&str],
                      dst_obj_id: Option<&str>,
                      dst: &str,
                      glob_literal_separator: bool,
                      force: bool) -> Result<()> {
        // TODO leading slashes should be removed
        Ok(())
    }

    fn create_staging_if_necessary(&self) -> Result<()> {
        if self.staging.borrow().is_none() {
            let staging = FsOcflStore::init_if_needed(self.root.join(EXTENSIONS_DIR).join(ROCFL_STAGING_EXTENSION),
                                                      StorageLayout::new(LayoutExtensionName::HashedNTupleLayout, None)?)?;
            self.staging.replace(Some(staging));
        }
        Ok(())
    }
}

impl VersionNum {
    /// Creates a new VersionNum
    pub fn new(number: u32, width: u32) -> Self {
        Self {
            number,
            width,
        }
    }

    /// Returns the previous version, or an Error if the previous version is invalid (less than 1).
    pub fn previous(&self) -> Result<VersionNum> {
        if self.number - 1 < 1 {
            return Err(RocflError::IllegalState("Versions cannot be less than 1".to_string()));
        }

        Ok(Self {
            number: self.number - 1,
            width: self.width,
        })
    }

    /// Returns the next version, or an Error if the next version is invalid. Version number only
    /// have limits if they are zero-padded.
    pub fn next(&self) -> Result<VersionNum> {
        let max = match self.width {
            0 => u32::MAX,
            _ => (10 * (self.width - 1)) - 1
        };

        if self.number + 1 > max as u32 {
            return Err(RocflError::IllegalState(format!("Version cannot be greater than {}", max)));
        }

        Ok(Self {
            number: self.number + 1,
            width: self.width,
        })
    }
}

impl TryFrom<&str> for VersionNum {
    type Error = RocflError;

    /// Parses a string in the format of `v1` or `v0002` into a `VersionNum`. An error is return if
    /// the version string is invalid.
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
                    number: num,
                    width: width as u32,
                })
            }
            Err(_) => Err(RocflError::IllegalArgument(format!("Invalid version {}", version)))
        }
    }
}

impl TryFrom<u32> for VersionNum {
    type Error = RocflError;

    /// Parses a positive integer into a `VersionNum`. An error is returned if it is invalid.
    fn try_from(version: u32) -> Result<Self, Self::Error> {
        if version < 1 {
            return Err(RocflError::IllegalArgument(format!("Invalid version number {}", version)));
        }

        Ok(Self {
            number: version,
            width: 0,
        })
    }
}

impl FromStr for VersionNum {
    type Err = RocflError;

    /// This function is used when parsing command line arguments. It attempts to interpret a string
    /// as a version if it is formatted like any of these examples: `v3`, `v00009`, or `8`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match VersionNum::try_from(s) {
            Ok(v) => Ok(v),
            Err(_) => {
                match u32::from_str(s) {
                    Ok(parsed) => Ok(VersionNum::try_from(parsed)?),
                    Err(_) => Err(RocflError::IllegalArgument(format!("Invalid version number {}", s)))
                }
            },
        }
    }
}

impl fmt::Display for VersionNum {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "v{:0width$}", self.number, width = self.width as usize)
    }
}

impl From<VersionNum> for String {
    fn from(version_num: VersionNum) -> Self {
        format!("{}", version_num)
    }
}

impl PartialEq for VersionNum {
    fn eq(&self, other: &Self) -> bool {
        self.number == other.number
    }
}

impl Eq for VersionNum {}

impl Hash for VersionNum {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.number.hash(state)
    }
}

impl PartialOrd for VersionNum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VersionNum {
    fn cmp(&self, other: &Self) -> Ordering {
        self.number.cmp(&other.number)
    }
}

impl ObjectVersion {
    /// Creates an `ObjectVersion` by consuming the supplied `Inventory`.
    fn from_inventory(mut inventory: Inventory, version_num: Option<VersionNum>) -> Result<Self> {
        let version_num = match version_num {
            Some(version) => version,
            None => inventory.head,
        };

        let version = inventory.get_version(version_num)?;
        let version_details = VersionDetails::new(version_num, version);

        let state = ObjectVersion::construct_state(version_num, &mut inventory)?;

        Ok(Self {
            id: inventory.id,
            object_root: inventory.object_root,
            digest_algorithm: inventory.digest_algorithm,
            version_details,
            state
        })
    }

    fn construct_state(target: VersionNum,
                       inventory: &mut Inventory) -> Result<HashMap<String, FileDetails>> {
        let mut state = HashMap::new();

        let mut current_version_num = target;
        let mut current_version = inventory.remove_version(target)?;
        let mut target_path_map = invert_path_map(current_version.state);
        current_version.state = HashMap::new();

        while !target_path_map.is_empty() {
            let mut not_found = HashMap::new();
            let version_details = Rc::new(VersionDetails::from_version(current_version_num, current_version));

            // No versions left to compare to; any remaining files were last updated here
            if version_details.version_num.number == 1 {
                for (target_path, target_digest) in target_path_map.into_iter() {
                    let content_path = inventory.lookup_content_path_by_digest(&target_digest)?.to_string();
                    state.insert(target_path, FileDetails::new(content_path,
                                                               target_digest,
                                                               inventory.digest_algorithm,
                                                               &inventory.object_root,
                                                               Rc::clone(&version_details)));
                }

                break;
            }

            let previous_version_num = version_details.version_num.previous()?;
            let mut previous_version = inventory.remove_version(previous_version_num)?;
            let mut previous_path_map = invert_path_map(previous_version.state);
            previous_version.state = HashMap::new();

            for (target_path, target_digest) in target_path_map.into_iter() {
                let entry = previous_path_map.remove_entry(&target_path);

                if entry.is_none() || entry.unwrap().1 != target_digest {
                    let content_path = inventory.lookup_content_path_by_digest(&target_digest)?.to_string();
                    state.insert(target_path, FileDetails::new(content_path,
                                                               target_digest,
                                                               inventory.digest_algorithm,
                                                               &inventory.object_root,
                                                               Rc::clone(&version_details)));
                } else {
                    not_found.insert(target_path, target_digest);
                }
            }

            current_version_num = previous_version_num;
            current_version = previous_version;

            target_path_map = not_found;
        }

        Ok(state)
    }
}

impl FileDetails {
    fn new(content_path: String,
           digest: Rc<String>,
           digest_algorithm: DigestAlgorithm,
           object_root: &str,
           version_details: Rc<VersionDetails>) -> Self {
        Self {
            content_path: content_path.clone(),
            storage_path: join(object_root, &convert_path_separator(content_path)),
            digest,
            digest_algorithm,
            last_update: version_details,
        }
    }
}

impl VersionDetails {
    /// Creates `VersionDetails` by cloning the input.
    fn new(version_num: VersionNum, version: &Version) -> Self {
        let (user, address) = match &version.user {
            Some(user) => (user.name.clone(), user.address.clone()),
            None => (None, None)
        };

        Self {
            version_num,
            created: version.created,
            user_name: user,
            user_address: address,
            message: version.message.clone()
        }
    }

    /// Creates `VersionDetails` by consuming the input.
    fn from_version(version_num: VersionNum, version: Version) -> Self {
        let (user, address) = match version.user {
            Some(user) => (user.name, user.address),
            None => (None, None)
        };

        Self {
            version_num,
            created: version.created,
            user_name: user,
            user_address: address,
            message: version.message,
        }
    }
}

impl ObjectVersionDetails {
    /// Creates `ObjectVersionDetails` by consuming the `Inventory`.
    fn from_inventory(mut inventory: Inventory, version_num: Option<VersionNum>) -> Result<Self> {
        let version_num = match version_num {
            Some(version) => version,
            None => inventory.head,
        };

        let version = inventory.remove_version(version_num)?;
        let version_details = VersionDetails::from_version(version_num, version);

        Ok(Self {
            id: inventory.id,
            object_root: inventory.object_root,
            digest_algorithm: inventory.digest_algorithm,
            version_details,
        })
    }
}

impl DigestAlgorithm {
    /// Hashes the input and returns its hex encoded digest
    pub fn hash_hex(&self, data: impl AsRef<[u8]>) -> String {
        // This ugliness is because the variable length blake2b algorithms don't work with DynDigest
        let bytes = match self {
            DigestAlgorithm::Md5 => {
                let mut hasher = Md5::new();
                Digest::update(&mut hasher, data);
                hasher.finalize().to_vec()
            }
            DigestAlgorithm::Sha1 => {
                let mut hasher = Sha1::new();
                Digest::update(&mut hasher, data);
                hasher.finalize().to_vec()
            }
            DigestAlgorithm::Sha256 => {
                let mut hasher = Sha256::new();
                Digest::update(&mut hasher, data);
                hasher.finalize().to_vec()
            }
            DigestAlgorithm::Sha512 => {
                let mut hasher = Sha512::new();
                Digest::update(&mut hasher, data);
                hasher.finalize().to_vec()
            }
            DigestAlgorithm::Sha512_256 => {
                let mut hasher = Sha512Trunc256::new();
                Digest::update(&mut hasher, data);
                hasher.finalize().to_vec()
            }
            DigestAlgorithm::Blake2b512 => {
                let mut hasher = Blake2b::new();
                Digest::update(&mut hasher, data);
                hasher.finalize().to_vec()
            }
            DigestAlgorithm::Blake2b160 => {
                let mut hasher = VarBlake2b::new(20).unwrap();
                hasher.update(data);
                hasher.finalize_boxed().to_vec()
            }
            DigestAlgorithm::Blake2b256 => {
                let mut hasher = VarBlake2b::new(32).unwrap();
                hasher.update(data);
                hasher.finalize_boxed().to_vec()
            }
            DigestAlgorithm::Blake2b384 => {
                let mut hasher = VarBlake2b::new(48).unwrap();
                hasher.update(data);
                hasher.finalize_boxed().to_vec()
            }
        };

        hex::encode(bytes)
    }

    /// Wraps the specified reader in a `DigestReader`. Does not support blake2b because of the
    /// DynDigest problem.
    pub fn reader<R: Read>(&self, reader: R) -> Result<DigestReader<R>> {
        let digest: Box<dyn DynDigest> = match self {
            DigestAlgorithm::Md5 => Box::new(Md5::new()),
            DigestAlgorithm::Sha1 => Box::new(Sha1::new()),
            DigestAlgorithm::Sha256 => Box::new(Sha256::new()),
            DigestAlgorithm::Sha512 => Box::new(Sha512::new()),
            DigestAlgorithm::Sha512_256 => Box::new(Sha512Trunc256::new()),
            _ => return Err(RocflError::General("Blake2b is not supported for streaming digest.".to_string())),
        };

        Ok(DigestReader::new(digest, reader))
    }
}

impl<R: Read> DigestReader<R> {
    pub fn new(digest: Box<dyn DynDigest>, reader: R) -> Self {
        Self {
            digest,
            inner: reader,
        }
    }

    pub fn finalize_hex(self) -> String {
        hex::encode(self.digest.finalize().to_vec())
    }
}

impl<R: Read> Read for DigestReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let result = self.inner.read(buf)?;

        if result > 0 {
            self.digest.update(&buf);
        }

        Ok(result)
    }
}

impl Diff {
    fn added(path: String) -> Self {
        Self {
            diff_type: DiffType::Added,
            path
        }
    }
    fn modified(path: String) -> Self {
        Self {
            diff_type: DiffType::Modified,
            path
        }
    }
    fn deleted(path: String) -> Self {
        Self {
            diff_type: DiffType::Deleted,
            path
        }
    }
}

// ================================================== //
//            private structs+enums+traits            //
// ================================================== //

/// OCFL storage interface. Implementations are responsible for interacting with the physical
/// files on disk.
trait OcflStore {
    /// Returns the most recent inventory version for the specified object, or an a
    /// `RocflError::NotFound` if it does not exist.
    fn get_inventory(&self, object_id: &str) -> Result<Inventory>;

    /// Returns an iterator that iterates over every object in an OCFL repository, returning
    /// the most recent inventory of each. Optionally, a glob pattern may be provided that filters
    /// the objects that are returned by OCFL ID.
    fn iter_inventories<'a>(&'a self, filter_glob: Option<&str>)
        -> Result<Box<dyn Iterator<Item=Inventory> + 'a>>;

    /// Writes the specified file to the sink.
    ///
    /// If the file cannot be found, then a `RocflError::NotFound` error is returned.
    fn get_object_file(&self,
                       object_id: &str,
                       path: &str,
                       version_num: Option<VersionNum>,
                       sink: &mut dyn Write) -> Result<()>;
}

/// OCFL inventory serialization object
#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Inventory {
    id: String,
    #[serde(rename = "type")]
    type_declaration: String,
    digest_algorithm: DigestAlgorithm,
    head: VersionNum,
    content_directory: Option<String>,
    manifest: HashMap<String, Vec<String>>,
    versions: BTreeMap<VersionNum, Version>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fixity: Option<HashMap<String, HashMap<String, Vec<String>>>>,

    #[serde(skip)]
    object_root: String,
}

/// OCFL version serialization object
#[derive(Deserialize, Serialize, Debug, Clone)]
struct Version {
    created: DateTime<Local>,
    state: HashMap<String, Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<User>,

    /// All of the logical path parts that should be treated as directories
    #[serde(skip)]
    virtual_dirs: Option<HashSet<String>>,
}

/// OCFL user serialization object
#[derive(Deserialize, Serialize, Debug, Clone)]
struct User {
    name: Option<String>,
    address: Option<String>
}

/// ocfl_layout.json serialization object
#[derive(Deserialize, Serialize, Debug)]
struct OcflLayout {
    extension: LayoutExtensionName,
    description: String
}

/// An iterator that adapts the output of a delegate `Inventory` iterator into another type.
struct InventoryAdapterIter<'a, T> {
    iter: Box<dyn Iterator<Item=Inventory> + 'a>,
    adapter: Box<dyn Fn(Inventory) -> Result<T>>
}

// ================================================== //
//                private impls+fns                   //
// ================================================== //

impl Inventory {
    /// Creates a new HEAD version, copying over the state of the previous HEAD.
    fn create_staging_head(&mut self) -> Result<()> {
        let version_num = self.head.next()?;
        let version = self.head_version().clone_staged();
        self.versions.insert(version_num, version);
        self.head = version_num;

        Ok(())
    }

    /// Returns the HEAD version
    fn head_version(&self) -> &Version {
        // The head version must exist because we look for it when the Inventory is deserialized
        self.versions.get(&self.head).unwrap()
    }

    /// Returns a reference to the specified version or an error if it does not exist.
    fn get_version(&self, version_num: VersionNum) -> Result<&Version> {
        match self.versions.get(&version_num) {
            Some(v) => Ok(v),
            None => Err(not_found(&self.id, Some(version_num)))
        }
    }

    /// Removes and returns the specified version from the inventory, or an error if it does not exist.
    fn remove_version(&mut self, version_num: VersionNum) -> Result<Version> {
        match self.versions.remove(&version_num) {
            Some(v) => Ok(v),
            None => Err(not_found(&self.id, Some(version_num)))
        }
    }

    /// Returns the first content path associated with the specified digest, or an error if it does
    /// not exist.
    fn lookup_content_path_by_digest(&self, digest: &str) -> Result<&str> {
        match self.manifest.get(digest) {
            Some(paths) => {
                match paths.first() {
                    Some(path) => Ok(path.as_str()),
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

    fn lookup_content_path_for_logical_path(&self,
                                            logical_path: &str,
                                            version_num: Option<VersionNum>) -> Result<&str> {
        let version_num = version_num.unwrap_or(self.head);
        let version = self.get_version(version_num)?;

        let digest = match version.lookup_digest(&logical_path) {
            Some(digest) => digest,
            None => return Err(RocflError::NotFound(
                format!("Path {} not found in object {} version {}",
                        logical_path, self.id, version_num)))
        };

        self.lookup_content_path_by_digest(digest)
    }
}

impl Validate for Inventory {
    /// Performs a spot check on the inventory to see if it appears valid. This is not an
    /// exhaustive check, and does not guarantee that the inventory is valid.
    fn validate(&self) -> Result<()> {
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
    fn new_staged() -> Self {
        Self::staged_version(HashMap::new())
    }

    /// Creates a new Version with a cloned state and staging meta
    fn clone_staged(&self) -> Self {
        Self::staged_version(self.state.clone())
    }

    fn staged_version(state: HashMap<String, Vec<String>>) -> Self {
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
    fn lookup_digest(&self, logical_path: &str) -> Option<&String> {
        for (digest, paths) in &self.state {
            if paths.iter().any(|e| e == logical_path) {
                return Some(digest);
            }
        }

        None
    }
}

impl<'a, T> InventoryAdapterIter<'a, T> {
    /// Creates a new `InventoryAdapterIter` that applies the `adapter` closure to the output
    /// of every `next()` call.
    fn new(iter: Box<dyn Iterator<Item=Inventory> + 'a>,
           adapter: impl Fn(Inventory) -> Result<T> + 'a + 'static) -> Self {
        Self {
            iter,
            adapter: Box::new(adapter)
        }
    }
}

impl<'a, T> Iterator for InventoryAdapterIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => None,
            Some(inventory) => {
                match self.adapter.deref()(inventory) {
                    Ok(adapted) => Some(adapted),
                    Err(e) => {
                        error!("{:#}", e);
                        self.next()
                    }
                }
            }
        }
    }
}

/// Transforms an input map of digest to vector of paths to a map of paths to digests.
/// The original map is consumed.
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

fn join(parent: &str, child: &str) -> String {
    format!("{}{}{}", parent, path::MAIN_SEPARATOR, child)
}

fn convert_path_separator(path: String) -> String {
    if path::MAIN_SEPARATOR == '\\' {
        return path.replace("/", "\\");
    }
    path
}

/// Constructs a `RocflError::NotFound` error
fn not_found(object_id: &str, version_num: Option<VersionNum>) -> RocflError {
    match version_num {
        Some(version) => RocflError::NotFound(format!("Object {} version {}", object_id, version)),
        None => RocflError::NotFound(format!("Object {}", object_id))
    }
}

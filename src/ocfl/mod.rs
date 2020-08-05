//! This library is a storage agnostic abstraction over [OCFL repositories](https://ocfl.io/).
//! Currently, it only supports read-only operations on local filesystems.
//!
//! Create a new `OcflRepo` as follows:
//!
//! ```rust
//! use rocfl::ocfl::OcflRepo;
//!
//! let repo = OcflRepo::new_fs_repo("path/to/ocfl/storage/root");
//! ```

use core::fmt;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::ops::Deref;
use std::path::{self, Path};
use std::rc::Rc;
use std::str::FromStr;

use anyhow::{anyhow, Error, Result};
use chrono::{DateTime, Local};
use lazy_static::lazy_static;
use regex::Regex;
use rusoto_core::Region;
use serde::Deserialize;
use serde::export::Formatter;
use thiserror::Error;

use self::fs::FsOcflStore;
use self::s3::S3OcflStore;

mod fs;
mod s3;

const OBJECT_MARKER: &str = "0=ocfl_object_1.0";
const ROOT_INVENTORY_FILE: &str = "inventory.json";
const MUTABLE_HEAD_INVENTORY_FILE: &str = "extensions/0004-mutable-head/head/inventory.json";

lazy_static! {
    static ref VERSION_REGEX: Regex = Regex::new(r#"^v\d+$"#).unwrap();
}

// ================================================== //
//             public structs+enums+traits            //
// ================================================== //

/// Interface for interacting with an OCFL repository
pub struct OcflRepo {
    store: Box<dyn OcflStore>
}

/// Represents an [OCFL object version](https://ocfl.io/1.0/spec/#version-directories).
#[derive(Deserialize, Debug)]
#[serde(try_from = "&str")]
pub struct VersionNum {
    pub number: u32,
    pub width: usize,
}

/// Represents a version of an OCFL object
#[derive(Debug, Eq, PartialEq)]
pub struct ObjectVersion {
    /// The object's ID
    pub id: String,
    /// The path from the storage root to the object root
    pub object_root: String,
    /// The algorithm used to calculate digests (sha512 or sha256)
    pub digest_algorithm: String,
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
    pub digest_algorithm: Rc<String>,
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
    pub digest_algorithm: String,
    /// Metadata about the version
    pub version_details: VersionDetails,
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
#[derive(Debug, Eq, PartialEq)]
pub enum DiffType {
    Added,
    Modified,
    Deleted,
}

/// Application errors
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

// ================================================== //
//                   public impls+fns                 //
// ================================================== //

impl OcflRepo {
    /// Creates a new `OcflRepo` instance backed by the local filesystem. `storage_root` is the
    /// location of the OCFL repository to open.
    pub fn new_fs_repo<P: AsRef<Path>>(storage_root: P) -> Result<Self> {
        Ok(Self {
            store: Box::new(FsOcflStore::new(storage_root)?)
        })
    }

    /// Creates a new `OcflRepo` instance backed by S3. `prefix` used to specify a virtual
    /// sub directory within a bucket that the OCFL repository is rooted in.
    pub fn new_s3_repo(region: Region, bucket: &str, prefix: Option<&str>) -> Result<Self> {
        Ok(Self {
            store: Box::new(S3OcflStore::new(region, bucket, prefix)?)
        })
    }

    /// Returns an iterator that iterate through all of the objects in an OCFL repository.
    /// Objects are lazy-loaded. An optional glob pattern may be provided to filter the objects
    /// that are returned.
    ///
    /// The iterator return an error if it encounters a problem accessing an object. This does
    /// terminate the iterator; there are still more objects until it returns `None`.
    pub fn list_objects<'a>(&'a self, filter_glob: Option<&str>) -> Result<Box<dyn Iterator<Item=Result<ObjectVersionDetails>> + 'a>> {
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
    pub fn get_object(&self, object_id: &str, version_num: Option<&VersionNum>) -> Result<ObjectVersion> {
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
                              version_num: Option<&VersionNum>) -> Result<ObjectVersionDetails> {
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
                           version_num: Option<&VersionNum>,
                           sink: Box<&mut dyn Write>) -> Result<()> {
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
            return Err(RocflError::NotFound(format!("Path {} not found in object {}", path, object_id)).into());
        }

        Ok(versions)
    }

    /// Returns the diff of two object versions. If only one version is specified, then the diff
    /// is between the specified version and the version before it.
    ///
    /// If the object cannot be found, then a `RocflError::NotFound` error is returned.
    pub fn diff(&self, object_id: &str, left_version: Option<&VersionNum>, right_version: &VersionNum) -> Result<Vec<Diff>> {
        if left_version.is_some() && right_version.eq(left_version.unwrap()) {
            return Ok(vec![])
        }

        let mut inventory = self.store.get_inventory(object_id)?;

        let right = inventory.remove_version(&right_version)?;

        let left = match left_version {
            Some(version) => Some(inventory.remove_version(version)?),
            None => {
                if right_version.number > 1 {
                    Some(inventory.remove_version(&right_version.previous().unwrap())?)
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
}

impl VersionNum {
    /// Returns the previous version, or an Error if the previous version is invalid (less than 1).
    pub fn previous(&self) -> Result<VersionNum> {
        if self.number - 1 < 1 {
            return Err(anyhow!("Versions cannot be less than 1"));
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
            0 => usize::MAX,
            _ => (10 * (self.width - 1)) - 1
        };

        if self.number + 1 > max as u32 {
            return Err(anyhow!("Version cannot be greater than {}", max));
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
                    width,
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
    type Err = Error;

    /// This function is used when parsing command line arguments. It attempts to interpret a string
    /// as a version if it is formatted like any of these examples: `v3`, `v00009`, or `8`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match VersionNum::try_from(s) {
            Ok(v) => Ok(v),
            Err(_) => Ok(VersionNum::try_from(u32::from_str(s)?)?),
        }
    }
}

impl Clone for VersionNum {
    fn clone(&self) -> Self {
        Self {
            number: self.number,
            width: self.width,
        }
    }
}

impl fmt::Display for VersionNum {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "v{:0width$}", self.number, width = self.width)
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
    fn from_inventory(mut inventory: Inventory, version_num: Option<&VersionNum>) -> Result<Self> {
        let version_num = match version_num {
            Some(version) => version.clone(),
            None => inventory.head.clone(),
        };

        let version = inventory.get_version(&version_num)?;
        let version_details = VersionDetails::new(&version_num, version);

        let state = ObjectVersion::construct_state(&version_num, &mut inventory)?;

        Ok(Self {
            id: inventory.id,
            object_root: inventory.object_root,
            digest_algorithm: inventory.digest_algorithm,
            version_details,
            state
        })
    }

    fn construct_state(target: &VersionNum, inventory: &mut Inventory) -> Result<HashMap<String, FileDetails>> {
        let mut state = HashMap::new();

        let digest_algorithm = Rc::new(inventory.digest_algorithm.clone());
        let mut current_version_num = (*target).clone();
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
                                                               Rc::clone(&digest_algorithm),
                                                               &inventory.object_root,
                                                               Rc::clone(&version_details)));
                }

                break;
            }

            let previous_version_num = version_details.version_num.previous()?;
            let mut previous_version = inventory.remove_version(&previous_version_num)?;
            let mut previous_path_map = invert_path_map(previous_version.state);
            previous_version.state = HashMap::new();

            for (target_path, target_digest) in target_path_map.into_iter() {
                let entry = previous_path_map.remove_entry(&target_path);

                if entry.is_none() || entry.unwrap().1 != target_digest {
                    let content_path = inventory.lookup_content_path_by_digest(&target_digest)?.to_string();
                    state.insert(target_path, FileDetails::new(content_path,
                                                               target_digest,
                                                               Rc::clone(&digest_algorithm),
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
    fn new(content_path: String, digest: Rc<String>, digest_algorithm: Rc<String>,
           object_root: &str, version_details: Rc<VersionDetails>) -> Self {
        Self {
            content_path: content_path.clone(),
            // TODO this is not correct for s3
            storage_path: join(object_root, &convert_path_separator(content_path)),
            digest,
            digest_algorithm,
            last_update: version_details,
        }
    }
}

impl VersionDetails {
    /// Creates `VersionDetails` by cloning the input.
    fn new(version_num: &VersionNum, version: &Version) -> Self {
        let (user, address) = match &version.user {
            Some(user) => (user.name.clone(), user.address.clone()),
            None => (None, None)
        };

        Self {
            version_num: version_num.clone(),
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
    fn from_inventory(mut inventory: Inventory, version_num: Option<&VersionNum>) -> Result<Self> {
        let version_num = match version_num {
            Some(version) => version.clone(),
            None => inventory.head.clone(),
        };

        let version = inventory.remove_version(&version_num)?;
        let version_details = VersionDetails::from_version(version_num, version);

        Ok(Self {
            id: inventory.id,
            object_root: inventory.object_root,
            digest_algorithm: inventory.digest_algorithm,
            version_details,
        })
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
    fn iter_inventories<'a>(&'a self, filter_glob: Option<&str>) -> Result<Box<dyn Iterator<Item=Result<Inventory>> + 'a>>;

    /// Writes the specified file to the sink.
    ///
    /// If the file cannot be found, then a `RocflError::NotFound` error is returned.
    fn get_object_file(&self,
                       object_id: &str,
                       path: &str,
                       version_num: Option<&VersionNum>,
                       sink: Box<&mut dyn Write>) -> Result<()>;
}

/// OCFL inventory serialization object
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Inventory {
    id: String,
    #[serde(rename = "type")]
    type_declaration: String,
    digest_algorithm: String,
    head: VersionNum,
    content_directory: Option<String>,
    manifest: HashMap<String, Vec<String>>,
    versions: BTreeMap<VersionNum, Version>,
    fixity: Option<HashMap<String, HashMap<String, Vec<String>>>>,

    // This field is not in the inventory json file and must be added after deserialization
    #[serde(skip)]
    object_root: String,
}

/// OCFL version serialization object
#[derive(Deserialize, Debug)]
struct Version {
    created: DateTime<Local>,
    state: HashMap<String, Vec<String>>,
    message: Option<String>,
    user: Option<User>
}

/// OCFL user serialization object
#[derive(Deserialize, Debug)]
struct User {
    name: Option<String>,
    address: Option<String>
}

/// An iterator that adapts the output of a delegate `Inventory` iterator into another type.
struct InventoryAdapterIter<'a, T> {
    iter: Box<dyn Iterator<Item=Result<Inventory>> + 'a>,
    adapter: Box<dyn Fn(Inventory) -> Result<T>>
}

// ================================================== //
//                private impls+fns                   //
// ================================================== //

impl Inventory {
    // TODO fill in more validations
    // TODO have a shallow and a deep validation
    /// Performs a spot check on the inventory to see if it appears valid. This is not an
    /// exhaustive check, and does not guarantee that the inventory is valid.
    pub fn validate(&self) -> Result<()> {
        if !self.versions.contains_key(&self.head) {
            return Err(RocflError::CorruptObject {
                object_id: self.id.clone(),
                message: format!("HEAD version {} was not found", self.head),
            }.into())
        }
        Ok(())
    }

    /// Returns a reference to the specified version or an error if it does not exist.
    fn get_version(&self, version_num: &VersionNum) -> Result<&Version> {
        match self.versions.get(version_num) {
            Some(v) => Ok(v),
            None => Err(not_found(&self.id, Some(version_num)).into())
        }
    }

    /// Removes and returns the specified version from the inventory, or an error if it does not exist.
    fn remove_version(&mut self, version_num: &VersionNum) -> Result<Version> {
        match self.versions.remove(version_num) {
            Some(v) => Ok(v),
            None => Err(not_found(&self.id, Some(version_num)).into())
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
                    }.into())
                }
            }
            None => Err(RocflError::CorruptObject {
                object_id: self.id.clone(),
                message: format!("Digest {} not found in manifest", digest)
            }.into())
        }
    }

    fn lookup_content_path_for_logical_path(&self,
                                            logical_path: &str,
                                            version_num: Option<&VersionNum>) -> Result<&str> {
        let version_num = version_num.unwrap_or_else(|| &self.head);
        let version = self.get_version(&version_num)?;

        let digest = match version.lookup_digest(&logical_path) {
            Some(digest) => digest,
            None => return Err(RocflError::NotFound(
                format!("Path {} not found in object {} version {}",
                        logical_path, self.id, version_num)).into())
        };

        self.lookup_content_path_by_digest(digest)
    }
}

impl Version {
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
    fn new(iter: Box<dyn Iterator<Item=Result<Inventory>> + 'a>, adapter: impl Fn(Inventory) -> Result<T> + 'a + 'static) -> Self {
        Self {
            iter,
            adapter: Box::new(adapter)
        }
    }
}

impl<'a, T> Iterator for InventoryAdapterIter<'a, T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(inventory)) => {
                Some(self.adapter.deref()(inventory))
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
fn not_found(object_id: &str, version_num: Option<&VersionNum>) -> RocflError {
    match version_num {
        Some(version) => RocflError::NotFound(format!("Object {} version {}", object_id, version)),
        None => RocflError::NotFound(format!("Object {}", object_id))
    }
}

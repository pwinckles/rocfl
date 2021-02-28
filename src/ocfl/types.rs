use core::fmt;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::path;
use std::rc::Rc;
use std::str::{FromStr, Split};

use chrono::{DateTime, Local};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::ocfl::bimap::PathBiMap;
use crate::ocfl::digest::HexDigest;
use crate::ocfl::DigestAlgorithm;
use crate::ocfl::error::{Result, RocflError};
use crate::ocfl::inventory::{Inventory, Version};

static VERSION_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^v\d+$"#).unwrap());

/// Represents an [OCFL object version](https://ocfl.io/1.0/spec/#version-directories).
#[derive(Deserialize, Serialize, Debug, Copy, Clone)]
#[serde(try_from = "&str")]
#[serde(into = "String")]
pub struct VersionNum {
    pub number: u32,
    pub width: u32,
}

#[derive(Deserialize, Serialize, Debug, Eq, Ord, PartialOrd, PartialEq, Hash, Clone)]
/// Represents a logical or content path.
pub struct InventoryPath(String);

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
    pub state: HashMap<Rc<InventoryPath>, FileDetails>,
}

/// Details about a file in an OCFL object
#[derive(Debug, Eq, PartialEq)]
pub struct FileDetails {
    /// The file's digest
    pub digest: Rc<HexDigest>,
    /// The digest algorithm
    pub digest_algorithm: DigestAlgorithm,
    /// The path to the file relative the object root
    pub content_path: Rc<InventoryPath>,
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

/// Represents a change to a file
#[derive(Debug, Eq, PartialEq)]
pub enum Diff {
    Added(Rc<InventoryPath>),
    Modified(Rc<InventoryPath>),
    Deleted(Rc<InventoryPath>),
    Renamed {
        original: Vec<Rc<InventoryPath>>,
        renamed: Vec<Rc<InventoryPath>>,
    },
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

impl InventoryPath {
    pub fn parts(&self) -> Split<char> {
        self.0.split('/')
    }
}

// It looks like its not possible to implement `impl<T: AsRef<str> TryFrom<t>`
// https://github.com/rust-lang/rust/issues/50133

impl TryFrom<&str> for InventoryPath {
    type Error = RocflError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let trimmed = value.trim_start_matches('/').trim_end_matches('/');

        if !trimmed.is_empty() {
            let has_illegal_part = trimmed.split('/').any(|part| {
                part == "." || part == ".." || part.is_empty()
            });

            if has_illegal_part {
                return Err(RocflError::IllegalArgument(
                    format!("Paths may not contain '.', '..', or '' parts. Found: {} ",
                            value)));
            }
        }

        Ok(Self(trimmed.to_string()))
    }
}

impl TryFrom<String> for InventoryPath {
    type Error = RocflError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl TryFrom<&String> for InventoryPath {
    type Error = RocflError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl From<InventoryPath> for String {
    fn from(path: InventoryPath) -> Self {
        path.0
    }
}

impl AsRef<String> for InventoryPath {
    fn as_ref(&self) -> &String {
        &self.0
    }
}

impl Display for InventoryPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ObjectVersion {
    /// Creates an `ObjectVersion` by consuming the supplied `Inventory`.
    pub fn from_inventory(mut inventory: Inventory, version_num: Option<VersionNum>) -> Result<Self> {
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
                       inventory: &mut Inventory) -> Result<HashMap<Rc<InventoryPath>, FileDetails>> {
        let mut state = HashMap::new();

        let mut current_version_num = target;
        let mut current_version = inventory.remove_version(target)?;
        let mut target_path_map = current_version.remove_state();

        while !target_path_map.is_empty() {
            let mut not_found = PathBiMap::new();
            let version_details = Rc::new(VersionDetails::from_version(current_version_num, current_version));

            // No versions left to compare to; any remaining files were last updated here
            if version_details.version_num.number == 1 {
                for (target_path, target_digest) in target_path_map {
                    let content_path = inventory.content_path_for_digest(&target_digest)?;
                    state.insert(target_path, FileDetails::new(content_path.clone(),
                                                               target_digest,
                                                               inventory.digest_algorithm,
                                                               &inventory.object_root,
                                                               version_details.clone()));
                }

                break;
            }

            let previous_version_num = version_details.version_num.previous()?;
            let mut previous_version = inventory.remove_version(previous_version_num)?;
            let mut previous_path_map = previous_version.remove_state();

            for (target_path, target_digest) in target_path_map {
                let entry = previous_path_map.remove_path(&target_path);

                if entry.is_none() || entry.unwrap().1 != target_digest {
                    let content_path = inventory.content_path_for_digest(&target_digest)?;
                    state.insert(target_path, FileDetails::new(content_path.clone(),
                                                               target_digest,
                                                               inventory.digest_algorithm,
                                                               &inventory.object_root,
                                                               version_details.clone()));
                } else {
                    not_found.insert_rc(target_digest, target_path);
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
    pub fn new(content_path: Rc<InventoryPath>,
           digest: Rc<HexDigest>,
           digest_algorithm: DigestAlgorithm,
           object_root: &str,
           version_details: Rc<VersionDetails>) -> Self {
        let storage_path = convert_path_separator(join(object_root, content_path.as_ref().as_ref()));

        Self {
            content_path,
            storage_path,
            digest,
            digest_algorithm,
            last_update: version_details,
        }
    }
}

impl VersionDetails {
    /// Creates `VersionDetails` by cloning the input.
    pub fn new(version_num: VersionNum, version: &Version) -> Self {
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
    pub fn from_version(version_num: VersionNum, version: Version) -> Self {
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
    pub fn from_inventory(mut inventory: Inventory, version_num: Option<VersionNum>) -> Result<Self> {
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

impl Diff {
    /// This method returns the path associated with the diff. If there are multiple paths,
    /// it is the first path on the left hand side.
    pub fn path(&self) -> &Rc<InventoryPath> {
        match self {
            Diff::Added(path) => path,
            Diff::Modified(path) => path,
            Diff::Deleted(path) => path,
            Diff::Renamed { original, ..} =>
                original.first().expect("At least one renamed path should have existed"),
        }
    }
}

/// Joins to strings using the file system separator
fn join(parent: &str, child: &str) -> String {
    format!("{}{}{}", parent, path::MAIN_SEPARATOR, child)
}

/// Changes `/` to `\` on Windows
fn convert_path_separator(path: String) -> String {
    if path::MAIN_SEPARATOR == '\\' {
        return path.replace("/", "\\");
    }
    path
}

#[cfg(test)]
mod tests {
    use std::convert::{TryFrom, TryInto};

    use crate::ocfl::InventoryPath;

    #[test]
    fn create_logical_path_when_valid() {
        let value = "foo/.bar/baz.txt";
        let path: InventoryPath = value.try_into().unwrap();
        assert_eq!(value, path.0);
    }

    #[test]
    fn create_logical_path_when_root() {
        let path: InventoryPath = "/".try_into().unwrap();
        assert_eq!("", path.0);
    }

    #[test]
    fn remove_leading_and_trailing_slashes_from_logical_paths() {
        let path: InventoryPath = "//foo/bar/baz//".try_into().unwrap();
        assert_eq!("foo/bar/baz", path.0);
    }

    #[test]
    #[should_panic(expected = "Paths may not contain")]
    fn reject_logical_paths_with_empty_parts() {
        InventoryPath::try_from("foo//bar/baz").unwrap();
    }

    #[test]
    #[should_panic(expected = "Paths may not contain")]
    fn reject_logical_paths_with_single_dot() {
        InventoryPath::try_from("foo/bar/./baz").unwrap();
    }

    #[test]
    #[should_panic(expected = "Paths may not contain")]
    fn reject_logical_paths_with_double_dot() {
        InventoryPath::try_from("foo/bar/../baz").unwrap();
    }

    #[test]
    #[should_panic(expected = "Paths may not contain")]
    fn reject_logical_paths_with_double_dot_leading() {
        InventoryPath::try_from("../foo/bar/baz").unwrap();
    }
}
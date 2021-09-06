use core::fmt;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::path;
use std::path::Path;
use std::rc::Rc;
use std::str::{FromStr, Split};

use chrono::{DateTime, Local};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use VersionRef::Head;

use crate::ocfl::bimap::PathBiMap;
use crate::ocfl::consts::MUTABLE_HEAD_EXT_DIR;
use crate::ocfl::digest::HexDigest;
use crate::ocfl::error::{Result, RocflError};
use crate::ocfl::inventory::{Inventory, Version};
use crate::ocfl::VersionRef::Number;
use crate::ocfl::{util, DigestAlgorithm};

static VERSION_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"^v\d+$"#).unwrap());

/// Represents an [OCFL object version](https://ocfl.io/1.0/spec/#version-directories).
#[derive(Deserialize, Serialize, Debug, Copy, Clone)]
#[serde(try_from = "&str")]
#[serde(into = "String")]
pub struct VersionNum {
    pub number: u32,
    pub width: u32,
}

/// Represents either a specific version number or whatever the current head version is
pub enum VersionRef {
    Number(VersionNum),
    Head,
}

pub trait InventoryPath {
    /// Returns an iterable containing each segment of the path split on the `/` separator
    fn parts(&self) -> Split<char>;

    /// Returns the parent path of this path.
    fn parent(&self) -> Self;

    /// Returns the part of the logical path that's after the final `/` or the entire path if
    /// there is no `/`
    fn filename(&self) -> &str;

    /// Creates a new path by joining this path with another
    fn resolve(&self, other: &Self) -> Self;

    /// Returns true if the path ends with the given suffix
    fn ends_with(&self, suffix: &str) -> bool;

    /// Returns true if the path starts with the given prefix
    fn starts_with(&self, prefix: &str) -> bool;

    /// Returns a reference to the path represented as a `Path`
    fn as_path(&self) -> &Path;

    /// Returns a reference to the path represented as a `str`
    fn as_str(&self) -> &str;

    /// Returns true if the path is empty
    fn is_empty(&self) -> bool;
}

#[derive(Deserialize, Serialize, Debug, Eq, Ord, PartialOrd, PartialEq, Hash, Clone)]
struct InventoryPathInner(String);

/// Represents the logical path to a file in an object.
#[derive(Deserialize, Serialize, Debug, Eq, Ord, PartialOrd, PartialEq, Hash, Clone)]
#[serde(transparent)]
pub struct LogicalPath {
    inner: InventoryPathInner,
}

/// Represents a path within a version's content directory. This path must be relative the object
/// root.
#[derive(Debug, Eq, Ord, PartialOrd, PartialEq, Hash, Clone)]
pub struct ContentPath {
    inner: InventoryPathInner,
    /// The version the content path exists in. This will be a version number, except in the case
    /// when the path is in the mutable head extension
    pub version: ContentPathVersion,
}

#[derive(Debug, Eq, Ord, PartialOrd, PartialEq, Hash, Copy, Clone)]
pub enum ContentPathVersion {
    VersionNum(VersionNum),
    MutableHead,
}

/// Represents a version of an OCFL object
#[derive(Debug, Eq, PartialEq, Clone)]
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
    pub state: HashMap<Rc<LogicalPath>, FileDetails>,
}

/// Details about a file in an OCFL object
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct FileDetails {
    /// The file's digest
    pub digest: Rc<HexDigest>,
    /// The digest algorithm
    pub digest_algorithm: DigestAlgorithm,
    /// The path to the file relative the object root
    pub content_path: Rc<ContentPath>,
    /// The path to the file relative the storage root
    pub storage_path: String,
    /// The version metadata for when the file was last updated
    pub last_update: Rc<VersionDetails>,
}

/// Metadata about a version
#[derive(Debug, Eq, PartialEq, Clone)]
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
#[derive(Debug, Eq, PartialEq, Clone)]
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

/// Optional meta that may be associated with a commit
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct CommitMeta {
    /// Name of the user who created the commit
    pub(super) user_name: Option<String>,
    // URI address of the user who created the commit
    pub(super) user_address: Option<String>,
    /// Message describing the changes
    pub(super) message: Option<String>,
    /// When the commit was created
    pub(super) created: Option<DateTime<Local>>,
}

/// Represents a change to a file
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Diff {
    Added(Rc<LogicalPath>),
    Modified(Rc<LogicalPath>),
    Deleted(Rc<LogicalPath>),
    Renamed {
        original: Vec<Rc<LogicalPath>>,
        renamed: Vec<Rc<LogicalPath>>,
    },
}

impl VersionNum {
    /// Creates a new VersionNum with width 0
    pub fn new(number: u32) -> Self {
        Self { number, width: 0 }
    }

    /// Creates a new VersionNum
    pub fn with_width(number: u32, width: u32) -> Self {
        Self { number, width }
    }

    /// Returns the previous version, or an Error if the previous version is invalid (less than 1).
    pub fn previous(&self) -> Result<VersionNum> {
        if self.number - 1 < 1 {
            return Err(RocflError::IllegalState(
                "Versions cannot be less than 1".to_string(),
            ));
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
            _ => (10 * (self.width - 1)) - 1,
        };

        if self.number + 1 > max as u32 {
            return Err(RocflError::IllegalState(format!(
                "Version cannot be greater than {}",
                max
            )));
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
            return Err(RocflError::IllegalArgument(format!(
                "Invalid version {}",
                version
            )));
        }

        match version[1..].parse::<u32>() {
            Ok(num) => {
                if num < 1 {
                    return Err(RocflError::IllegalArgument(format!(
                        "Invalid version {}",
                        version
                    )));
                }

                let width = match version.starts_with("v0") {
                    true => version.len() - 1,
                    false => 0,
                };

                Ok(Self {
                    number: num,
                    width: width as u32,
                })
            }
            Err(_) => Err(RocflError::IllegalArgument(format!(
                "Invalid version {}",
                version
            ))),
        }
    }
}

impl TryFrom<u32> for VersionNum {
    type Error = RocflError;

    /// Parses a positive integer into a `VersionNum`. An error is returned if it is invalid.
    fn try_from(version: u32) -> Result<Self, Self::Error> {
        if version < 1 {
            return Err(RocflError::IllegalArgument(format!(
                "Invalid version number {}",
                version
            )));
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
            Err(_) => match u32::from_str(s) {
                Ok(parsed) => Ok(VersionNum::try_from(parsed)?),
                Err(_) => Err(RocflError::IllegalArgument(format!(
                    "Invalid version number {}",
                    s
                ))),
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

impl VersionRef {
    pub fn resolve(&self, head_num: VersionNum) -> VersionNum {
        match self {
            Number(num) => *num,
            Head => head_num,
        }
    }
}

impl From<VersionNum> for VersionRef {
    fn from(num: VersionNum) -> Self {
        Self::Number(num)
    }
}

impl From<Option<VersionNum>> for VersionRef {
    fn from(num: Option<VersionNum>) -> Self {
        num.map_or(Head, Number)
    }
}

impl InventoryPath for InventoryPathInner {
    /// Returns an iterable containing each segment of the path split on the `/` separator
    fn parts(&self) -> Split<char> {
        self.0.split('/')
    }

    /// Returns the parent path of this path.
    fn parent(&self) -> Self {
        match self.0.rfind('/') {
            Some(last_slash) => Self(self.0.as_str()[0..last_slash].into()),
            None => Self("".to_string()),
        }
    }

    /// Returns the part of the logical path that's after the final `/` or the entire path if
    /// there is no `/`
    fn filename(&self) -> &str {
        match self.0.rfind('/') {
            Some(last_slash) => &self.0.as_str()[last_slash + 1..],
            None => self.0.as_str(),
        }
    }

    /// Creates a new path by joining this path with another
    fn resolve(&self, other: &Self) -> Self {
        if self.0.is_empty() {
            other.clone()
        } else {
            Self(format!("{}/{}", self.0, other.0))
        }
    }

    /// Returns true if the path ends with the given suffix
    fn ends_with(&self, suffix: &str) -> bool {
        self.0.ends_with(suffix)
    }

    /// Returns true if the path starts with the given prefix
    fn starts_with(&self, prefix: &str) -> bool {
        self.0.starts_with(prefix)
    }

    /// Returns a reference to the path represented as a `Path`
    fn as_path(&self) -> &Path {
        self.as_ref()
    }

    /// Returns a reference to the path represented as a `str`
    fn as_str(&self) -> &str {
        self.as_ref()
    }

    /// Returns a reference to the path represented as a `Path`
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl InventoryPath for LogicalPath {
    /// Returns an iterable containing each segment of the path split on the `/` separator
    fn parts(&self) -> Split<char> {
        self.inner.parts()
    }

    /// Returns the parent path of this path.
    fn parent(&self) -> Self {
        Self {
            inner: self.inner.parent(),
        }
    }

    /// Returns the part of the logical path that's after the final `/` or the entire path if
    /// there is no `/`
    fn filename(&self) -> &str {
        self.inner.filename()
    }

    /// Creates a new path by joining this path with another
    fn resolve(&self, other: &Self) -> Self {
        Self {
            inner: self.inner.resolve(&other.inner),
        }
    }

    /// Returns true if the path ends with the given suffix
    fn ends_with(&self, suffix: &str) -> bool {
        self.inner.ends_with(suffix)
    }

    /// Returns true if the path starts with the given prefix
    fn starts_with(&self, prefix: &str) -> bool {
        self.inner.starts_with(prefix)
    }

    /// Returns a reference to the path represented as a `Path`
    fn as_path(&self) -> &Path {
        self.as_ref()
    }

    /// Returns a reference to the path represented as a `str`
    fn as_str(&self) -> &str {
        self.as_ref()
    }

    /// Returns a reference to the path represented as a `Path`
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl InventoryPath for ContentPath {
    /// Returns an iterable containing each segment of the path split on the `/` separator
    fn parts(&self) -> Split<char> {
        self.inner.parts()
    }

    /// Returns the parent path of this path.
    fn parent(&self) -> Self {
        Self {
            inner: self.inner.parent(),
            version: self.version,
        }
    }

    /// Returns the part of the logical path that's after the final `/` or the entire path if
    /// there is no `/`
    fn filename(&self) -> &str {
        self.inner.filename()
    }

    /// Creates a new path by joining this path with another
    fn resolve(&self, other: &Self) -> Self {
        Self {
            inner: self.inner.resolve(&other.inner),
            version: self.version,
        }
    }

    /// Returns true if the path ends with the given suffix
    fn ends_with(&self, suffix: &str) -> bool {
        self.inner.ends_with(suffix)
    }

    /// Returns true if the path starts with the given prefix
    fn starts_with(&self, prefix: &str) -> bool {
        self.inner.starts_with(prefix)
    }

    /// Returns a reference to the path represented as a `Path`
    fn as_path(&self) -> &Path {
        self.as_ref()
    }

    /// Returns a reference to the path represented as a `str`
    fn as_str(&self) -> &str {
        self.as_ref()
    }

    /// Returns a reference to the path represented as a `Path`
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

// It looks like its not possible to implement `impl<T: AsRef<str> TryFrom<t>`
// https://github.com/rust-lang/rust/issues/50133

impl TryFrom<&str> for InventoryPathInner {
    type Error = RocflError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let trimmed = value.trim_start_matches('/').trim_end_matches('/');

        if !trimmed.is_empty() {
            let has_illegal_part = trimmed
                .split('/')
                .any(|part| part == "." || part == ".." || part.is_empty());

            if has_illegal_part {
                return Err(RocflError::IllegalArgument(format!(
                    "Paths may not contain '.', '..', or '' parts. Found: {} ",
                    value
                )));
            }
        }

        Ok(Self(trimmed.to_string()))
    }
}

impl TryFrom<&str> for LogicalPath {
    type Error = RocflError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self {
            inner: InventoryPathInner::try_from(value)?,
        })
    }
}

impl TryFrom<&str> for ContentPath {
    type Error = RocflError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let inner = InventoryPathInner::try_from(value)?;

        // Mutable head paths do not have a version
        let version = if value.starts_with(MUTABLE_HEAD_EXT_DIR) {
            ContentPathVersion::MutableHead
        } else {
            match value.find('/') {
                Some(index) => ContentPathVersion::VersionNum(value[0..index].try_into()?),
                None => {
                    return Err(RocflError::IllegalArgument(format!(
                        "Content paths must begin with a valid version number. Found: {} ",
                        value
                    )));
                }
            }
        };

        Ok(Self { inner, version })
    }
}

impl TryFrom<String> for InventoryPathInner {
    type Error = RocflError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl TryFrom<String> for LogicalPath {
    type Error = RocflError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(Self {
            inner: InventoryPathInner::try_from(value)?,
        })
    }
}

impl TryFrom<String> for ContentPath {
    type Error = RocflError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<&String> for InventoryPathInner {
    type Error = RocflError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl TryFrom<&String> for LogicalPath {
    type Error = RocflError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Ok(Self {
            inner: InventoryPathInner::try_from(value)?,
        })
    }
}

impl TryFrom<&String> for ContentPath {
    type Error = RocflError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<Cow<'_, str>> for InventoryPathInner {
    type Error = RocflError;

    fn try_from(value: Cow<'_, str>) -> Result<Self, Self::Error> {
        value.as_ref().try_into()
    }
}

impl TryFrom<Cow<'_, str>> for LogicalPath {
    type Error = RocflError;

    fn try_from(value: Cow<'_, str>) -> Result<Self, Self::Error> {
        Ok(Self {
            inner: InventoryPathInner::try_from(value)?,
        })
    }
}

impl TryFrom<Cow<'_, str>> for ContentPath {
    type Error = RocflError;

    fn try_from(value: Cow<'_, str>) -> Result<Self, Self::Error> {
        Self::try_from(value.as_ref())
    }
}

impl From<InventoryPathInner> for String {
    fn from(path: InventoryPathInner) -> Self {
        path.0
    }
}

impl From<LogicalPath> for String {
    fn from(path: LogicalPath) -> Self {
        path.inner.0
    }
}

impl From<ContentPath> for String {
    fn from(path: ContentPath) -> Self {
        path.inner.0
    }
}

impl AsRef<str> for InventoryPathInner {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for LogicalPath {
    fn as_ref(&self) -> &str {
        self.inner.as_ref()
    }
}

impl AsRef<str> for ContentPath {
    fn as_ref(&self) -> &str {
        self.inner.as_ref()
    }
}

impl AsRef<Path> for InventoryPathInner {
    fn as_ref(&self) -> &Path {
        self.0.as_ref()
    }
}

impl AsRef<Path> for LogicalPath {
    fn as_ref(&self) -> &Path {
        self.inner.as_ref()
    }
}

impl AsRef<Path> for ContentPath {
    fn as_ref(&self) -> &Path {
        self.inner.as_ref()
    }
}

impl Display for InventoryPathInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Display for LogicalPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl Display for ContentPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl Serialize for ContentPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ContentPath {
    fn deserialize<D>(deserializer: D) -> Result<ContentPath, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(ContentPathVisitor)
    }
}

struct ContentPathVisitor;

impl<'de> Visitor<'de> for ContentPathVisitor {
    type Value = ContentPath;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a path string that is a valid OCFL content path")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        v.try_into()
            .map_err(|e: RocflError| E::custom(e.to_string()))
    }
}

impl ObjectVersion {
    /// Creates an `ObjectVersion` by consuming the supplied `Inventory`.
    pub fn from_inventory<S: AsRef<str> + Copy>(
        mut inventory: Inventory,
        version_num: VersionRef,
        object_storage_path: S,
        object_staging_path: Option<S>,
        use_backslashes: bool,
    ) -> Result<Self> {
        let version_num = version_num.resolve(inventory.head);

        let version = inventory.get_version(version_num)?;
        let version_details = VersionDetails::new(version_num, version);

        let state = ObjectVersion::construct_state(
            version_num,
            &mut inventory,
            object_storage_path,
            object_staging_path,
            use_backslashes,
        )?;

        Ok(Self {
            id: inventory.id,
            object_root: inventory.storage_path,
            digest_algorithm: inventory.digest_algorithm,
            version_details,
            state,
        })
    }

    fn construct_state<S: AsRef<str> + Copy>(
        target: VersionNum,
        inventory: &mut Inventory,
        object_storage_path: S,
        object_staging_path: Option<S>,
        use_backslashes: bool,
    ) -> Result<HashMap<Rc<LogicalPath>, FileDetails>> {
        let mut state = HashMap::new();

        let mut current_version_num = target;
        let mut current_version = inventory.remove_version(target)?;
        let mut target_path_map = current_version.remove_state();

        // This nonsense is needed to differentiate the storage paths for staged files
        let staging_version_prefix = if object_staging_path.is_some() {
            Some(format!("{}/", target))
        } else {
            None
        };

        while !target_path_map.is_empty() {
            let mut not_found = PathBiMap::new();
            let version_details = Rc::new(VersionDetails::from_version(
                current_version_num,
                current_version,
            ));

            // No versions left to compare to; any remaining files were last updated here
            if version_details.version_num.number == 1 {
                for (target_path, target_digest) in target_path_map {
                    let content_path = inventory.content_path_for_digest(
                        &target_digest,
                        current_version_num.into(),
                        Some(&target_path),
                    )?;

                    let storage_path = ObjectVersion::storage_path(
                        content_path.as_str(),
                        object_storage_path,
                        use_backslashes,
                        &staging_version_prefix,
                        &object_staging_path,
                    );

                    state.insert(
                        target_path,
                        FileDetails::new(
                            content_path.clone(),
                            storage_path,
                            target_digest,
                            inventory.digest_algorithm,
                            version_details.clone(),
                        ),
                    );
                }

                break;
            }

            let previous_version_num = version_details.version_num.previous()?;
            let mut previous_version = inventory.remove_version(previous_version_num)?;
            let mut previous_path_map = previous_version.remove_state();

            for (target_path, target_digest) in target_path_map {
                let entry = previous_path_map.remove_path(&target_path);

                if entry.is_none() || entry.unwrap().1 != target_digest {
                    let content_path = inventory.content_path_for_digest(
                        &target_digest,
                        current_version_num.into(),
                        Some(&target_path),
                    )?;

                    let storage_path = ObjectVersion::storage_path(
                        content_path.as_str(),
                        object_storage_path,
                        use_backslashes,
                        &staging_version_prefix,
                        &object_staging_path,
                    );

                    state.insert(
                        target_path,
                        FileDetails::new(
                            content_path.clone(),
                            storage_path,
                            target_digest,
                            inventory.digest_algorithm,
                            version_details.clone(),
                        ),
                    );
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

    fn storage_path<S: AsRef<str> + Copy>(
        content_path: &str,
        storage_path: S,
        use_backslashes: bool,
        staging_version_prefix: &Option<String>,
        staging_path: &Option<S>,
    ) -> String {
        if staging_version_prefix.is_some()
            && content_path.starts_with(staging_version_prefix.as_ref().unwrap())
        {
            // The content path resides in staging
            convert_path_separator(
                util::BACKSLASH_SEPARATOR,
                join(
                    util::BACKSLASH_SEPARATOR,
                    staging_path.unwrap().as_ref(),
                    content_path,
                ),
            )
        } else {
            // The content path resides in the main repo
            convert_path_separator(
                use_backslashes,
                join(use_backslashes, storage_path.as_ref(), content_path),
            )
        }
    }
}

impl FileDetails {
    pub fn new(
        content_path: Rc<ContentPath>,
        storage_path: String,
        digest: Rc<HexDigest>,
        digest_algorithm: DigestAlgorithm,
        version_details: Rc<VersionDetails>,
    ) -> Self {
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
            None => (None, None),
        };

        Self {
            version_num,
            created: version.created,
            user_name: user,
            user_address: address,
            message: version.message.clone(),
        }
    }

    /// Creates `VersionDetails` by consuming the input.
    pub fn from_version(version_num: VersionNum, version: Version) -> Self {
        let (user, address) = match version.user {
            Some(user) => (user.name, user.address),
            None => (None, None),
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
    pub fn from_inventory(mut inventory: Inventory, version_num: VersionRef) -> Result<Self> {
        let version_num = version_num.resolve(inventory.head);

        let version = inventory.remove_version(version_num)?;
        let version_details = VersionDetails::from_version(version_num, version);

        Ok(Self {
            id: inventory.id,
            object_root: inventory.storage_path,
            digest_algorithm: inventory.digest_algorithm,
            version_details,
        })
    }
}

impl Default for CommitMeta {
    fn default() -> Self {
        Self::new()
    }
}

impl CommitMeta {
    /// Creates commit meta with all values empty
    pub fn new() -> Self {
        Self {
            user_name: None,
            user_address: None,
            message: None,
            created: None,
        }
    }

    /// Sets the commit user. `name` must be provided if `address` is provided.
    pub fn with_user(mut self, name: Option<String>, address: Option<String>) -> Result<Self> {
        if address.is_some() && name.is_none() {
            return Err(RocflError::IllegalArgument(
                "User name must be set when user address is set.".to_string(),
            ));
        }
        self.user_name = name;
        self.user_address = address;
        Ok(self)
    }

    /// Sets the commit message
    pub fn with_message(mut self, message: Option<String>) -> Self {
        self.message = message;
        self
    }

    /// Sets the commit created timestamp
    pub fn with_created(mut self, created: Option<DateTime<Local>>) -> Self {
        self.created = created;
        self
    }
}

impl Diff {
    /// This method returns the path associated with the diff. If there are multiple paths,
    /// it is the first path on the left hand side.
    pub fn path(&self) -> &Rc<LogicalPath> {
        match self {
            Diff::Added(path) => path,
            Diff::Modified(path) => path,
            Diff::Deleted(path) => path,
            Diff::Renamed { original, .. } => original
                .first()
                .expect("At least one renamed path should have existed"),
        }
    }
}

/// Joins to strings using the file system separator
fn join(use_backslashes: bool, parent: &str, child: &str) -> String {
    if use_backslashes {
        format!("{}\\{}", parent, child)
    } else {
        format!("{}/{}", parent, child)
    }
}

/// Changes `/` to `\` on Windows
fn convert_path_separator(use_backslashes: bool, path: String) -> String {
    if use_backslashes && path::MAIN_SEPARATOR == '\\' {
        return path.replace("/", "\\");
    }
    path
}

#[cfg(test)]
mod tests {
    use std::convert::{TryFrom, TryInto};

    use crate::ocfl::LogicalPath;

    #[test]
    fn create_logical_path_when_valid() {
        let value = "foo/.bar/baz.txt";
        let path: LogicalPath = value.try_into().unwrap();
        assert_eq!(value, path.inner.0);
    }

    #[test]
    fn create_logical_path_when_root() {
        let path: LogicalPath = "/".try_into().unwrap();
        assert_eq!("", path.inner.0);
    }

    #[test]
    fn remove_leading_and_trailing_slashes_from_logical_paths() {
        let path: LogicalPath = "//foo/bar/baz//".try_into().unwrap();
        assert_eq!("foo/bar/baz", path.inner.0);
    }

    #[test]
    #[should_panic(expected = "Paths may not contain")]
    fn reject_logical_paths_with_empty_parts() {
        LogicalPath::try_from("foo//bar/baz").unwrap();
    }

    #[test]
    #[should_panic(expected = "Paths may not contain")]
    fn reject_logical_paths_with_single_dot() {
        LogicalPath::try_from("foo/bar/./baz").unwrap();
    }

    #[test]
    #[should_panic(expected = "Paths may not contain")]
    fn reject_logical_paths_with_double_dot() {
        LogicalPath::try_from("foo/bar/../baz").unwrap();
    }

    #[test]
    #[should_panic(expected = "Paths may not contain")]
    fn reject_logical_paths_with_double_dot_leading() {
        LogicalPath::try_from("../foo/bar/baz").unwrap();
    }
}

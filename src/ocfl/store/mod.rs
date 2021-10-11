use std::borrow::Cow;
use std::fmt::Debug;
use std::io::{Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::ocfl::error::Result;
use crate::ocfl::inventory::Inventory;
use crate::ocfl::store::layout::LayoutExtensionName;
use crate::ocfl::validate::{IncrementalValidator, ObjectValidationResult};
use crate::ocfl::{ContentPath, LogicalPath, VersionRef};

pub mod fs;
pub mod layout;
#[cfg(feature = "s3")]
pub mod s3;

/// OCFL storage interface. Implementations are responsible for interacting with the physical
/// files on disk.
pub trait OcflStore {
    /// Returns the most recent inventory version for the specified object, or an a
    /// `RocflError::NotFound` if it does not exist.
    fn get_inventory(&self, object_id: &str) -> Result<Inventory>;

    /// Returns an iterator that iterates over every object in an OCFL repository, returning
    /// the most recent inventory of each. Optionally, a glob pattern may be provided that filters
    /// the objects that are returned by OCFL ID.
    fn iter_inventories<'a>(
        &'a self,
        filter_glob: Option<&str>,
    ) -> Result<Box<dyn Iterator<Item = Inventory> + 'a>>;

    /// Writes the specified file to the sink.
    ///
    /// If the file cannot be found, then a `RocflError::NotFound` error is returned.
    fn get_object_file(
        &self,
        object_id: &str,
        path: &LogicalPath,
        version_num: VersionRef,
        sink: &mut dyn Write,
    ) -> Result<()>;

    /// Writes a new OCFL object. The contents at `object_path` must be a fully formed OCFL
    /// object that is able to be moved into place with no additional modifications.
    ///
    /// The object must not already exist.
    fn write_new_object(
        &self,
        inventory: &mut Inventory,
        src_object_path: &Path,
        object_root: Option<&str>,
    ) -> Result<()>;

    /// Writes a new version to the OCFL object. The contents at `version_path` must be a fully
    /// formed OCFL version that is able to be moved into place within the object, requiring
    /// no additional modifications.
    ///
    /// The object must already exist, and the new version must not exist.
    fn write_new_version(&self, inventory: &mut Inventory, version_path: &Path) -> Result<()>;

    /// Purges the specified object from the repository, if it exists. If it does not exist,
    /// nothing happens. Any dangling directories that were created as a result of purging
    /// the object are also removed.
    fn purge_object(&self, object_id: &str) -> Result<()>;

    /// Returns a list of all of the extension names that are associated with the object
    fn list_object_extensions(&self, object_id: &str) -> Result<Vec<String>>;

    /// Validates the specified object and returns any problems found. Err will only be returned
    /// if a non-validation problem was encountered.
    fn validate_object(
        &self,
        object_id: &str,
        fixity_check: bool,
    ) -> Result<ObjectValidationResult>;

    /// Validates the specified object at the specified path, relative the storage root, and
    /// returns any problems found. Err will only be returned if a non-validation problem was
    /// encountered.
    fn validate_object_at(
        &self,
        object_root: &str,
        fixity_check: bool,
    ) -> Result<ObjectValidationResult>;

    /// Validates the structure of an OCFL repository as well as all of the objects in the repository
    /// When `fixity_check` is `false`, then the digests of object content files are not validated.
    ///
    /// The storage root is validated immediately, and an incremental validator is returned that
    /// is used to lazily validate the rest of the repository.
    fn validate_repo<'a>(
        &'a self,
        fixity_check: bool,
    ) -> Result<Box<dyn IncrementalValidator + 'a>>;

    /// Instructs the store to gracefully stop any in-flight work and not accept any additional
    /// requests.
    fn close(&self);
}

/// Operations related to staging versions of objects
pub trait StagingStore: OcflStore {
    /// Stages an OCFL object if there is not an existing object with the same ID.
    fn stage_object(&self, inventory: &mut Inventory) -> Result<()>;

    /// Copies a file in the staging area
    fn stage_file_copy(
        &self,
        inventory: &Inventory,
        source: &mut impl Read,
        logical_path: &LogicalPath,
    ) -> Result<()>;

    /// Copies an existing staged file to a new location
    fn copy_staged_file(
        &self,
        inventory: &Inventory,
        src_content: &ContentPath,
        dst_logical: &LogicalPath,
    ) -> Result<()>;

    /// Moves a file in the staging area
    fn stage_file_move(
        &self,
        inventory: &Inventory,
        source: &impl AsRef<Path>,
        logical_path: &LogicalPath,
    ) -> Result<()>;

    /// Moves an existing staged file to a new location
    fn move_staged_file(
        &self,
        inventory: &Inventory,
        src_content: &ContentPath,
        dst_logical: &LogicalPath,
    ) -> Result<()>;

    /// Deletes staged content files.
    fn rm_staged_files(&self, inventory: &Inventory, paths: &[&ContentPath]) -> Result<()>;

    /// Deletes any staged files that are not referenced in the manifest
    fn rm_orphaned_files(&self, inventory: &Inventory) -> Result<()>;

    /// Serializes the inventory to the object's staging directory. If `finalize` is true,
    /// then the inventory file will additionally be copied into the version directory.
    fn stage_inventory(
        &self,
        inventory: &Inventory,
        finalize: bool,
        pretty_print: bool,
    ) -> Result<()>;
}

/// ocfl_layout.json serialization object
#[derive(Deserialize, Serialize, Debug)]
pub struct OcflLayout {
    extension: LayoutExtensionName,
    description: String,
}

/// Abstraction over reading files and listing directory contents. `/` _must_ be used as the file
/// path separator.
pub trait Storage {
    /// Reads the file at the specified path and writes its contents to the provided sink.
    fn read<W: Write>(&self, path: &str, sink: &mut W) -> Result<()>;

    /// Lists the contents of the specified directory. If `recursive` is `true`, then all leaf-nodes
    /// are returned. If the directory does not exist, or is empty, then an empty vector is returned.
    /// The returned paths are all relative the directory that was listed.
    fn list(&self, path: &str, recursive: bool) -> Result<Vec<Listing>>;
}

/// Represents filesystem entity
#[derive(Debug, Hash, Eq, PartialEq)]
pub enum Listing<'a> {
    /// A regular file
    File(Cow<'a, str>),
    /// A directory
    Directory(Cow<'a, str>),
    /// Anything that is not a regular file or directory, eg a symbolic link
    Other(Cow<'a, str>),
}

impl<'a> Listing<'a> {
    pub fn file(path: &str) -> Listing {
        Listing::File(Cow::Borrowed(path))
    }

    pub fn dir(path: &str) -> Listing {
        Listing::Directory(Cow::Borrowed(path))
    }
    pub fn file_owned(path: String) -> Listing<'a> {
        Listing::File(Cow::Owned(path))
    }

    pub fn dir_owned(path: String) -> Listing<'a> {
        Listing::Directory(Cow::Owned(path))
    }

    pub fn path(&self) -> &str {
        match self {
            Listing::File(path) => path,
            Listing::Directory(path) => path,
            Listing::Other(path) => path,
        }
    }
}

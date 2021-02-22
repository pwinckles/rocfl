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

use std::convert::TryInto;
use std::fmt::Debug;
use std::fs::File;
use std::io::Write;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use chrono::Local;
use log::{error, info};
use once_cell::unsync::OnceCell;
#[cfg(feature = "s3")]
use rusoto_core::Region;
use serde::{Deserialize, Serialize};

use crate::ocfl::consts::*;
use crate::ocfl::digest::HexDigest;
pub use crate::ocfl::error::{Result, RocflError};
use crate::ocfl::inventory::{Inventory, Version};
use crate::ocfl::layout::StorageLayout;

pub use self::digest::DigestAlgorithm;
use self::fs::FsOcflStore;
use self::layout::LayoutExtensionName;
#[cfg(feature = "s3")]
use self::s3::S3OcflStore;
pub use self::types::*;

pub mod error;
pub mod layout;
mod bimap;
mod consts;
mod digest;
mod fs;
mod inventory;
mod types;
#[cfg(feature = "s3")]
mod s3;

// TODO consider moving the repo stuff to `repo.rs`

/// Interface for interacting with an OCFL repository
pub struct OcflRepo {
    /// For local filesystem repos, this is the storage root. TBD for S3.
    root: PathBuf,
    store: Box<dyn OcflStore>,
    staging: OnceCell<FsOcflStore>,
}

impl OcflRepo {
    /// Creates a new `OcflRepo` instance backed by the local filesystem. `storage_root` is the
    /// location of the OCFL repository to open. The OCFL repository must already exist.
    pub fn new_fs_repo<P: AsRef<Path>>(storage_root: P) -> Result<Self> {
        Ok(Self {
            root: PathBuf::from(storage_root.as_ref()),
            store: Box::new(FsOcflStore::new(storage_root)?),
            staging: OnceCell::default(),
        })
    }

    /// Initializes a new `OcflRepo` instance backed by the local filesystem. The OCFL repository
    /// most not already exist.
    pub fn init_fs_repo<P: AsRef<Path>>(root: P, layout: StorageLayout) -> Result<Self> {
        Ok(Self {
            root: PathBuf::from(root.as_ref()),
            store: Box::new(FsOcflStore::init(root, layout)?),
            staging: OnceCell::default(),
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
            staging: OnceCell::default(),
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
                           path: &InventoryPath,
                           version_num: Option<VersionNum>,
                           sink: &mut dyn Write) -> Result<()> {
        self.store.get_object_file(object_id, path, version_num, sink)
    }

    /// Returns a vector contain the version metadata for every version of an object that
    /// affected the specified file. The vector is sorted in ascending order.
    ///
    /// If the object or path cannot be found, then a `RocflError::NotFound' error is returned.
    pub fn list_file_versions(&self,
                              object_id: &str,
                              path: &InventoryPath) -> Result<Vec<VersionDetails>> {
        let inventory = self.store.get_inventory(object_id)?;

        let mut versions = Vec::new();

        let mut current_digest: Option<Rc<HexDigest>> = None;

        for (id, version) in inventory.versions {
            match version.lookup_digest(&path) {
                Some(digest) => {
                    if current_digest.is_none() || current_digest.as_ref().unwrap().as_ref().ne(digest) {
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

        let mut right = inventory.remove_version(right_version)?;

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

        let mut diffs = Vec::new();

        if let Some(mut left) = left {
            for (path, left_digest) in left.state_into_iter() {
                match right.remove_file(&path) {
                    None => diffs.push(Diff::deleted(path)),
                    Some((_, right_digest)) => {
                        if left_digest.ne(&right_digest) {
                            diffs.push(Diff::modified(path))
                        }
                    }
                }
            }

            // TODO Renames can be detected if the same digest has both a D and an A
            for (path, _digest) in right.state_into_iter() {
                diffs.push(Diff::added(path))
            }
        } else {
            for (path, _digest) in right.state_into_iter() {
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

        let version_num = VersionNum::new(1, padding_width);

        let inventory = Inventory::builder(object_id)
            .with_digest_algorithm(digest_algorithm)
            .with_content_directory(content_dir)
            .with_head(version_num)
            .build()?;

        self.get_staging()?.stage_object(&inventory)
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
        if src.is_empty() {
            return Err(RocflError::IllegalArgument("Must provide at least one source".to_string()));
        }

        let staging = self.get_staging()?;

        // TODO even though this is not supposed to be used concurrently, it's not a bad idea
        //      to get some sort of file lock here so that an object cannot be updated concurrently

        let mut inventory = match staging.get_inventory(&object_id) {
            Ok(inventory) => inventory,
            Err(RocflError::NotFound(_)) => {
                let mut inventory = self.store.get_inventory(&object_id)?;
                inventory.create_staging_head()?;
                staging.stage_object(&inventory)?;
                inventory
            },
            Err(e) => return Err(e),
        };

        // TODO cleanup
        for path in src.iter() {
            let path = path.as_ref();

            if path.is_file() {
                // TODO need to continue on error ?
                let version = inventory.head_version();
                let mut reader = inventory.digest_algorithm.reader(File::open(&path)?)?;
                let logical_path = logical_path_for_file(&path, &dst,
                                                         src.len() > 1,
                                                         version)?;

                if version.is_file(&logical_path) {
                    if force {
                        info!("Overwriting existing file at {}", &logical_path);
                    } else {
                        return Err(RocflError::AlreadyExists(logical_path));
                    }
                }

                // TODO this has the "revision" bug.
                //  1. add `file1.txt` with content `test`
                //  2. add `file2.txt` with content `test`
                //  3. overwrite `file1.txt` with content `test2`
                //  4. `file2.txt` is now corrupt
                //  I think I need to change `add_file_to_head` to NOT dedup anything until commit

                staging.stage_file(&inventory, &mut reader, &logical_path)?;
                let digest = reader.finalize_hex();
                inventory.add_file_to_head(digest, logical_path)?;
            } else if recursive {
                // TODO walk directory
            } else {
                error!("Skipping directory {} because recursive copy is not enabled",
                       path.to_string_lossy());
            }
        }

        inventory.head_version_mut().created = Local::now();
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
                      dst: &InventoryPath,
                      glob_literal_separator: bool,
                      force: bool) -> Result<()> {
        // TODO leading slashes should be removed
        Ok(())
    }

    fn get_staging(&self) -> Result<&FsOcflStore> {
        Ok(self.staging.get_or_try_init(|| {
            FsOcflStore::init_if_needed(self.root.join(EXTENSIONS_DIR).join(ROCFL_STAGING_EXTENSION),
                                        StorageLayout::new(LayoutExtensionName::HashedNTupleLayout, None)?)
        })?)
    }
}

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
                       path: &InventoryPath,
                       version_num: Option<VersionNum>,
                       sink: &mut dyn Write) -> Result<()>;
}

/// An iterator that adapts the output of a delegate `Inventory` iterator into another type.
struct InventoryAdapterIter<'a, T> {
    iter: Box<dyn Iterator<Item=Inventory> + 'a>,
    adapter: Box<dyn Fn(Inventory) -> Result<T>>
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

/// ocfl_layout.json serialization object
#[derive(Deserialize, Serialize, Debug)]
struct OcflLayout {
    extension: LayoutExtensionName,
    description: String
}

/// Creates a logical path for a source file based on its destination.
fn logical_path_for_file(file: impl AsRef<Path>,
                         dst: &str,
                         src_is_many: bool,
                         version: &Version) -> Result<InventoryPath> {
    let logical_path = if src_is_many || dst.ends_with('/') {
        // When there are multiple source files, then the destination must be a directory
        // OR there's a single source and the destination ends with a '/', so interpret it as a dir
        logical_path_in_dst_dir(file, dst)?
    } else {
        let dst_path: InventoryPath = dst.try_into()?;

        if version.exists(&dst_path) {
            if version.is_file(&dst_path) {
                // There's a single src and the destination is an existing logical path,
                // use the destination as is
                dst_path
            } else {
                // There's a single source and the destination is an existing virtual dir,
                // interpret the destination as a directory
                logical_path_in_dst_dir(file, dst)?
            }
        } else {
            // Single source to a non-existent destination that does not end with a '/',
            // interpret destination as logical path
            dst_path
        }
    };

    version.validate_non_conflicting(&logical_path)?;

    Ok(logical_path)
}

/// Creates a logical path that uses `dst` as the parent directory and the filename of
/// `src` as the filename.
fn logical_path_in_dst_dir(src: impl AsRef<Path>, dst: &str) -> Result<InventoryPath> {
    let mut logical_path = dst.to_string();
    if !logical_path.ends_with('/') {
        logical_path.push('/');
    }
    logical_path.push_str(&src.as_ref().file_name().unwrap().to_string_lossy());
    logical_path.try_into()
}
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

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::fs::File;
use std::io::Write;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use log::error;
#[cfg(feature = "s3")]
use rusoto_core::Region;
use serde::{Deserialize, Serialize};

use crate::ocfl::consts::*;
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

/// Interface for interacting with an OCFL repository
pub struct OcflRepo {
    /// For local filesystem repos, this is the storage root. TBD for S3.
    root: PathBuf,
    store: Box<dyn OcflStore>,
    staging: RefCell<Option<FsOcflStore>>,
}

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
                                // TODO convert this to a string for now
                                let digest: String = reader.finalize_hex().into();
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

// TODO this should no longer be needed when the bimap is complete
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
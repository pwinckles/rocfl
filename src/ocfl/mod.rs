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

use std::borrow::Cow;
use std::convert::TryInto;
use std::fmt::Debug;
use std::fs::File;
use std::io::Write;
use std::ops::Deref;
use std::path;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use chrono::Local;
use log::{error, info};
use once_cell::unsync::OnceCell;
#[cfg(feature = "s3")]
use rusoto_core::Region;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

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

mod bimap;
mod consts;
mod digest;
pub mod error;
mod fs;
mod inventory;
pub mod layout;
#[cfg(feature = "s3")]
mod s3;
mod types;
mod util;

// TODO consider moving the repo stuff to `repo.rs`

/// Interface for interacting with an OCFL repository
pub struct OcflRepo {
    /// For local filesystem repos, this is the storage root. TBD for S3.
    root: PathBuf,
    // TODO experiment changing this to a generic
    store: Box<dyn OcflStore>,
    staging: OnceCell<FsOcflStore>,
}

impl OcflRepo {
    /// Creates a new `OcflRepo` instance backed by the local filesystem. `storage_root` is the
    /// location of the OCFL repository to open. The OCFL repository must already exist.
    pub fn new_fs_repo<P: AsRef<Path>>(storage_root: P) -> Result<Self> {
        // TODO need to warn about unsupported extensions
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
    pub fn list_objects<'a>(
        &'a self,
        filter_glob: Option<&str>,
    ) -> Result<Box<dyn Iterator<Item = ObjectVersionDetails> + 'a>> {
        let inv_iter = self.store.iter_inventories(filter_glob)?;

        Ok(Box::new(InventoryAdapterIter::new(inv_iter, |inventory| {
            ObjectVersionDetails::from_inventory(inventory, None)
        })))
    }

    /// Returns a list of objects that have staged changes
    pub fn list_staged_objects<'a>(
        &'a self,
    ) -> Result<Box<dyn Iterator<Item = ObjectVersionDetails> + 'a>> {
        // TODO this should NOT create staging if it does not exist
        let inv_iter = self.get_staging()?.iter_inventories(None)?;

        Ok(Box::new(InventoryAdapterIter::new(inv_iter, |inventory| {
            ObjectVersionDetails::from_inventory(inventory, None)
        })))
    }

    /// Returns a view of a version of an object. If a `VersionNum` is not specified,
    /// then the head version of the object is returned.
    ///
    /// If the object or version of the object cannot be found, then a `RocflError::NotFound`
    /// error is returned.
    pub fn get_object(
        &self,
        object_id: &str,
        version_num: Option<VersionNum>,
    ) -> Result<ObjectVersion> {
        let inventory = self.store.get_inventory(object_id)?;
        Ok(ObjectVersion::from_inventory(inventory, version_num)?)
    }

    /// Returns high-level details about an object version. This method is similar to
    /// `OcflRepo::get_object()` except that it does less processing and does not
    /// include the version's state.
    ///
    /// If the object or version of the object cannot be found, then a `RocflError::NotFound`
    /// error is returned.
    pub fn get_object_details(
        &self,
        object_id: &str,
        version_num: Option<VersionNum>,
    ) -> Result<ObjectVersionDetails> {
        let inventory = self.store.get_inventory(object_id)?;
        Ok(ObjectVersionDetails::from_inventory(
            inventory,
            version_num,
        )?)
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
    pub fn get_object_file(
        &self,
        object_id: &str,
        path: &InventoryPath,
        version_num: Option<VersionNum>,
        sink: &mut dyn Write,
    ) -> Result<()> {
        self.store
            .get_object_file(object_id, path, version_num, sink)
    }

    /// Returns a vector contain the version metadata for every version of an object that
    /// affected the specified file. The vector is sorted in ascending order.
    ///
    /// If the object or path cannot be found, then a `RocflError::NotFound' error is returned.
    pub fn list_file_versions(
        &self,
        object_id: &str,
        path: &InventoryPath,
    ) -> Result<Vec<VersionDetails>> {
        let inventory = self.store.get_inventory(object_id)?;

        let mut versions = Vec::new();

        let mut current_digest: Option<Rc<HexDigest>> = None;

        for (id, version) in inventory.versions {
            match version.lookup_digest(&path) {
                Some(digest) => {
                    if current_digest.is_none()
                        || current_digest.as_ref().unwrap().as_ref().ne(digest)
                    {
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
            return Err(RocflError::NotFound(format!(
                "Path {} not found in object {}",
                path, object_id
            )));
        }

        Ok(versions)
    }

    /// Returns the diff of two object versions. If only one version is specified, then the diff
    /// is between the specified version and the version before it.
    ///
    /// If the object cannot be found, then a `RocflError::NotFound` error is returned.
    pub fn diff(
        &self,
        object_id: &str,
        left_version: Option<VersionNum>,
        right_version: VersionNum,
    ) -> Result<Vec<Diff>> {
        self.store
            .get_inventory(object_id)?
            .diff_versions(left_version, right_version)
    }

    /// Returns all of the staged changes to the specified object, if there are any.
    pub fn diff_staged(&self, object_id: &str) -> Result<Vec<Diff>> {
        // TODO this should NOT create staging if it does not exist
        let staging = self.get_staging()?;

        match staging.get_inventory(&object_id) {
            Err(RocflError::NotFound(_)) => Ok(Vec::new()),
            Err(e) => Err(e),
            Ok(inventory) => inventory.diff_versions(None, inventory.head),
        }
    }

    /// Stages a new OCFL object if there is not an existing object with the same ID. The object
    /// is not inserted into the repository until it is committed.
    pub fn create_object(
        &self,
        object_id: &str,
        digest_algorithm: DigestAlgorithm,
        content_dir: &str,
        padding_width: u32,
    ) -> Result<()> {
        let object_id = object_id.trim();

        if object_id.is_empty() {
            return Err(RocflError::IllegalArgument(
                "Object IDs may not be blank".to_string(),
            ));
        }

        if digest_algorithm != DigestAlgorithm::Sha512
            && digest_algorithm != DigestAlgorithm::Sha256
        {
            return Err(RocflError::IllegalArgument(format!(
                "The inventory digest algorithm must be sha512 or sha256. Found: {}",
                digest_algorithm.to_string()
            )));
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
                return Err(RocflError::IllegalState(format!(
                    "Cannot create object {} because it already exists",
                    object_id
                )));
            }
        }

        let version_num = VersionNum::new(1, padding_width);

        let mut inventory = Inventory::builder(object_id)
            .with_digest_algorithm(digest_algorithm)
            .with_content_directory(content_dir)
            .with_head(version_num)
            .build()?;

        self.get_staging()?.stage_object(&mut inventory)
    }

    /// Copies files from outside the OCFL repository into the specified OCFL object.
    /// A destination of `/` specifies the object's root.
    ///
    /// If `force` is `false` and the copy operation attempts to write a file to a logical
    /// path where there is already a file, then the new file will **not** be copied.
    pub fn copy_files_external<P: AsRef<Path>>(
        &self,
        object_id: &str,
        src: &[P],
        dst: &str,
        recursive: bool,
        force: bool,
    ) -> Result<()> {
        if src.is_empty() {
            return Err(RocflError::IllegalArgument(
                "Must provide at least one source".to_string(),
            ));
        }

        let staging = self.get_staging()?;

        // TODO even though this is not supposed to be used concurrently, it's not a bad idea
        //      to get some sort of file lock here so that an object cannot be updated concurrently

        let mut inventory = match staging.get_inventory(&object_id) {
            Ok(inventory) => inventory,
            Err(RocflError::NotFound(_)) => {
                let mut inventory = self.store.get_inventory(&object_id)?;
                inventory.create_staging_head()?;
                staging.stage_object(&mut inventory)?;
                inventory
            }
            Err(e) => return Err(e),
        };

        let dst_path: InventoryPath = dst.try_into()?;

        for path in src.iter() {
            let path = path.as_ref();

            if path.is_file() {
                // TODO need to continue on error ?
                let parent = path.parent().unwrap_or_else(|| &Path::new(""));
                let logical_path = logical_path_for_file(
                    &path,
                    parent,
                    &dst,
                    src.len() > 1,
                    false,
                    &inventory.head_version(),
                )?;

                self.copy_file(&path, logical_path, force, &mut inventory)?;
            } else if recursive {
                let dst_dir_exists = inventory.head_version().is_dir(&dst_path);

                for file in WalkDir::new(&path).into_iter() {
                    let file = file?;
                    if file.path().is_file() {
                        let logical_path = logical_path_for_file(
                            &file.path(),
                            &path,
                            &dst,
                            true,
                            dst_dir_exists,
                            &inventory.head_version(),
                        )?;
                        // TODO need to continue on error ?
                        self.copy_file(file.path(), logical_path, force, &mut inventory)?;
                    }
                }
            } else {
                error!(
                    "Skipping directory {} because recursive copy is not enabled",
                    path.to_string_lossy()
                );
            }
        }

        inventory.head_version_mut().created = Local::now();
        staging.stage_inventory(&inventory, false)?;

        Ok(())
    }

    /// Commits all of an object's staged changes
    pub fn commit(
        &self,
        object_id: &str,
        user_name: &Option<String>,
        user_address: &Option<String>,
        message: &Option<String>,
    ) -> Result<()> {
        let staging = self.get_staging()?;

        let mut inventory = match staging.get_inventory(&object_id) {
            Ok(inventory) => inventory,
            Err(RocflError::NotFound(_)) => {
                // TODO should this be an error?
                return Err(RocflError::General(
                    "No staged changed found for the specified object".to_string(),
                ));
            }
            Err(e) => return Err(e),
        };

        let duplicates = inventory.dedup_head();

        inventory
            .head_version_mut()
            .update_meta(user_name, user_address, message);

        staging.stage_inventory(&inventory, true)?;
        staging.rm_staged_files(&inventory, &duplicates)?;

        if inventory.is_new() {
            let object_root = staging.object_staging_path(&inventory);
            self.store
                .write_new_object(&inventory, &object_root.as_ref().to_path_buf())?;
        } else {
            let version_root = staging.version_staging_path(&inventory);
            self.store
                .write_new_version(&inventory, &version_root.as_ref().to_path_buf())?;
        }

        staging.purge_object(object_id)?;

        Ok(())
    }

    fn copy_file(
        &self,
        file: impl AsRef<Path>,
        logical_path: InventoryPath,
        force: bool,
        inventory: &mut Inventory,
    ) -> Result<()> {
        let version = inventory.head_version();
        let mut reader = inventory.digest_algorithm.reader(File::open(&file)?)?;

        if version.is_file(&logical_path) {
            if force {
                info!("Overwriting existing file at {}", &logical_path);
            } else {
                return Err(RocflError::AlreadyExists(logical_path));
            }
        }

        info!(
            "Copying file {} into object at {}",
            file.as_ref().to_string_lossy(),
            &logical_path
        );

        self.get_staging()?
            .stage_file(&inventory, &mut reader, &logical_path)?;
        let digest = reader.finalize_hex();
        inventory.add_file_to_head(digest, logical_path)
    }

    fn get_staging(&self) -> Result<&FsOcflStore> {
        Ok(self.staging.get_or_try_init(|| {
            FsOcflStore::init_if_needed(
                self.root.join(EXTENSIONS_DIR).join(ROCFL_STAGING_EXTENSION),
                StorageLayout::new(LayoutExtensionName::HashedNTupleLayout, None)?,
            )
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
        path: &InventoryPath,
        version_num: Option<VersionNum>,
        sink: &mut dyn Write,
    ) -> Result<()>;

    /// Writes a new OCFL object. The contents at `object_path` must be a fully formed OCFL
    /// object that is able to be moved into place with no additional modifications.
    ///
    /// The object must not already exist.
    fn write_new_object(&self, inventory: &Inventory, object_path: &Path) -> Result<()>;

    /// Writes a new version to the OCFL object. The contents at `version_path` must be a fully
    /// formed OCFL version that is able to be moved into place within the object, requiring
    /// no additional modifications.
    ///
    /// The object must already exist, and the new version must not exist.
    fn write_new_version(&self, inventory: &Inventory, version_path: &Path) -> Result<()>;

    /// Purges the specified object from the repository, if it exists. If it does not exist,
    /// nothing happens. Any dangling directories that were created as a result of purging
    /// the object are also removed.
    fn purge_object(&self, object_id: &str) -> Result<()>;
}

/// An iterator that adapts the output of a delegate `Inventory` iterator into another type.
struct InventoryAdapterIter<'a, T> {
    iter: Box<dyn Iterator<Item = Inventory> + 'a>,
    adapter: Box<dyn Fn(Inventory) -> Result<T>>,
}

impl<'a, T> InventoryAdapterIter<'a, T> {
    /// Creates a new `InventoryAdapterIter` that applies the `adapter` closure to the output
    /// of every `next()` call.
    fn new(
        iter: Box<dyn Iterator<Item = Inventory> + 'a>,
        adapter: impl Fn(Inventory) -> Result<T> + 'a + 'static,
    ) -> Self {
        Self {
            iter,
            adapter: Box::new(adapter),
        }
    }
}

impl<'a, T> Iterator for InventoryAdapterIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => None,
            Some(inventory) => match self.adapter.deref()(inventory) {
                Ok(adapted) => Some(adapted),
                Err(e) => {
                    error!("{:#}", e);
                    self.next()
                }
            },
        }
    }
}

/// ocfl_layout.json serialization object
#[derive(Deserialize, Serialize, Debug)]
struct OcflLayout {
    extension: LayoutExtensionName,
    description: String,
}

/// Creates a logical path for a source file based on its destination.
fn logical_path_for_file<F, B>(
    file: F,
    base: B,
    dst: &str,
    src_is_many: bool,
    dst_dir_exists: bool,
    version: &Version,
) -> Result<InventoryPath>
where
    F: AsRef<Path>,
    B: AsRef<Path>,
{
    let logical_path = if src_is_many {
        if dst_dir_exists {
            // When multiple src and dst is existing dir, then copy into dir; x -> y/x
            let parent = base.as_ref().parent().unwrap_or_else(|| &Path::new(""));
            logical_path_in_dst_dir(file, parent, dst)?
        } else {
            // When multiple src and dst not exists, then copy to dir; x -> y
            logical_path_in_dst_dir(file, base, dst)?
        }
    } else if !src_is_many && dst.ends_with('/') {
        // When there's a single source and the destination ends with `/`, then it must be a dir
        logical_path_in_dst_dir(file, base, dst)?
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
                logical_path_in_dst_dir(file, base, dst)?
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

/// Creates a logical path that combines `dst` with the relativized `src` path.
fn logical_path_in_dst_dir<F, B>(src: F, base: B, dst: &str) -> Result<InventoryPath>
where
    F: AsRef<Path>,
    B: AsRef<Path>,
{
    let mut logical_path = dst.to_string();
    if !logical_path.ends_with('/') {
        logical_path.push('/');
    }

    let relative_path = pathdiff::diff_paths(src, base).unwrap();
    let mut relative = relative_path.to_string_lossy();

    if path::MAIN_SEPARATOR == '\\' {
        relative = Cow::Owned(relative.as_ref().replace("\\", "/"))
    }

    logical_path.push_str(relative.as_ref());
    logical_path.try_into()
}

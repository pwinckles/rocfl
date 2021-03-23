//! This library is a storage agnostic abstraction over [OCFL repositories](https://ocfl.io/).
//! It is **not** thread-safe.
//!
//! Create a new `OcflRepo` as follows:
//!
//! ```rust
//! use rocfl::ocfl::OcflRepo;
//!
//! let repo = OcflRepo::fs_repo("path/to/ocfl/storage/root");
//! ```

use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fmt::Debug;
use std::fs::File;
use std::io::Write;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use chrono::{DateTime, Local};
use log::{error, info};
use once_cell::unsync::OnceCell;
#[cfg(feature = "s3")]
use rusoto_core::Region;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

pub use self::digest::DigestAlgorithm;
use self::fs::FsOcflStore;
use self::layout::LayoutExtensionName;
#[cfg(feature = "s3")]
use self::s3::S3OcflStore;
pub use self::types::*;
use crate::ocfl::consts::*;
use crate::ocfl::digest::HexDigest;
use crate::ocfl::error::MultiError;
pub use crate::ocfl::error::{Result, RocflError};
use crate::ocfl::inventory::Inventory;
use crate::ocfl::layout::StorageLayout;

mod bimap;
mod consts;
mod digest;
pub mod error;
mod fs;
mod inventory;
pub mod layout;
#[cfg(feature = "s3")]
mod s3;
mod specs;
mod types;
mod util;

// TODO consider moving the repo stuff to `repo.rs`

/// Interface for interacting with an OCFL repository
pub struct OcflRepo {
    /// For local filesystem repos, this is the storage root. TBD for S3.
    // TODO experiment changing this to a generic
    store: Box<dyn OcflStore>,
    /// The OCFL repo that stores staged objects
    staging: OnceCell<FsOcflStore>,
    /// The path to the root of the staging repo
    staging_root: PathBuf,
    /// Indicates if the repository should convert separators to backslashes when rendering
    /// physical paths.
    use_backslashes: bool,
}

impl OcflRepo {
    /// Creates a new `OcflRepo` instance backed by the local filesystem. `storage_root` is the
    /// location of the OCFL repository to open. The OCFL repository must already exist.
    pub fn fs_repo<P: AsRef<Path>>(storage_root: P) -> Result<Self> {
        // TODO need to warn about unsupported extensions

        let mut staging_root = storage_root.as_ref().join(EXTENSIONS_DIR);
        staging_root.push(ROCFL_STAGING_EXTENSION);

        Ok(Self {
            staging_root,
            store: Box::new(FsOcflStore::new(storage_root)?),
            staging: OnceCell::default(),
            use_backslashes: util::BACKSLASH_SEPARATOR,
        })
    }

    /// Initializes a new `OcflRepo` instance backed by the local filesystem. The OCFL repository
    /// most not already exist.
    pub fn init_fs_repo<P: AsRef<Path>>(storage_root: P, layout: StorageLayout) -> Result<Self> {
        let mut staging_root = storage_root.as_ref().join(EXTENSIONS_DIR);
        staging_root.push(ROCFL_STAGING_EXTENSION);

        Ok(Self {
            staging_root,
            store: Box::new(FsOcflStore::init(storage_root, layout)?),
            staging: OnceCell::default(),
            use_backslashes: util::BACKSLASH_SEPARATOR,
        })
    }

    /// Creates a new `OcflRepo` instance backed by S3. `prefix` used to specify a
    /// sub directory within a bucket that the OCFL repository is rooted in.
    #[cfg(feature = "s3")]
    pub fn s3_repo(region: Region, bucket: &str, prefix: Option<&str>) -> Result<Self> {
        Ok(Self {
            // TODO this is not correct -- use xdg
            staging_root: PathBuf::from("."),
            store: Box::new(S3OcflStore::new(region, bucket, prefix)?),
            staging: OnceCell::default(),
            use_backslashes: false,
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

    /// Returns an iterator that iterate through all of the staged objects in an OCFL repository.
    /// Objects are lazy-loaded. An optional glob pattern may be provided to filter the objects
    /// that are returned.
    ///
    /// The iterator return an error if it encounters a problem accessing an object. This does
    /// terminate the iterator; there are still more objects until it returns `None`.
    pub fn list_staged_objects<'a>(
        &'a self,
        filter_glob: Option<&str>,
    ) -> Result<Box<dyn Iterator<Item = ObjectVersionDetails> + 'a>> {
        // TODO this should NOT create staging if it does not exist
        let inv_iter = self.get_staging()?.iter_inventories(filter_glob)?;

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
        let object_root = inventory.storage_path.clone();

        Ok(ObjectVersion::from_inventory(
            inventory,
            version_num,
            &object_root,
            None,
            self.use_backslashes,
        )?)
    }

    /// Same as `get_object()` except that it returns the staged version of an object.
    ///
    /// If the object does not have a staged version, then a `RocflError::NotFound`
    /// error is returned.
    pub fn get_staged_object(&self, object_id: &str) -> Result<ObjectVersion> {
        let staging_inventory = self.get_staged_inventory(object_id)?;
        let version = staging_inventory.head;
        let object_staging_root = staging_inventory.storage_path.clone();

        let object_storage_root = match self.store.get_inventory(object_id) {
            Ok(inventory) => Some(inventory.storage_path),
            Err(RocflError::NotFound(_)) => None,
            Err(e) => return Err(e),
        };

        let (root, staging) = if let Some(storage_root) = object_storage_root {
            (storage_root, Some(object_staging_root))
        } else {
            (object_staging_root, None)
        };

        Ok(ObjectVersion::from_inventory(
            staging_inventory,
            Some(version),
            &root,
            staging.as_ref(),
            util::BACKSLASH_SEPARATOR,
        )?)
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

    /// Same as `get_object_details()`, but for the staged version of an object.
    ///
    /// If the object does not have a staged version, then a `RocflError::NotFound`
    /// error is returned.
    pub fn get_staged_object_details(&self, object_id: &str) -> Result<ObjectVersionDetails> {
        let inventory = self.get_staged_inventory(object_id)?;
        let version = inventory.head;
        Ok(ObjectVersionDetails::from_inventory(
            inventory,
            Some(version),
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

    /// Writes the specified file from the staged version of the object to the sink.
    ///
    /// If the file cannot be found, then a `RocflError::NotFound` error is returned.
    pub fn get_staged_object_file(
        &self,
        object_id: &str,
        path: &InventoryPath,
        sink: &mut dyn Write,
    ) -> Result<()> {
        let staging = self.get_staging()?;

        let inventory = staging.get_inventory(object_id)?;
        let content_path = inventory.content_path_for_logical_path(path, None)?;

        let version_prefix = format!("{}/", inventory.head);

        if content_path.as_ref().as_ref().starts_with(&version_prefix) {
            // The content exists in staging
            self.get_staging()?
                .get_object_file(object_id, path, None, sink)
        } else {
            // The content exists in the main repo
            self.store
                .get_object_file(object_id, path, Some(inventory.head.previous()?), sink)
        }
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

    /// Completely removes the specified object from the repository. If the object doest not exist,
    /// nothing happens.
    pub fn purge_object(&self, object_id: &str) -> Result<()> {
        self.get_staging()?.purge_object(object_id)?;
        self.store.purge_object(object_id)
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

        let version_num = VersionNum::with_width(1, padding_width);

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
    pub fn copy_files_external(
        &self,
        object_id: &str,
        src: &[impl AsRef<Path>],
        dst: &str,
        recursive: bool,
    ) -> Result<()> {
        self.operate_on_external_source(
            object_id,
            src,
            dst,
            recursive,
            |file, logical_path, inventory| self.copy_file(file, logical_path, inventory),
        )
    }

    /// Copies files within an OCFL object. The source paths may be glob patterns.
    pub fn copy_files_internal(
        &self,
        object_id: &str,
        version_num: Option<VersionNum>,
        src: &[impl AsRef<str>],
        dst: &str,
        recursive: bool,
    ) -> Result<()> {
        if src.is_empty() {
            return Ok(());
        }

        // TODO abstract the before and after?

        let mut inventory = self.get_or_created_staged_inventory(object_id)?;
        let src_version_num = version_num.unwrap_or(inventory.head);

        let to_copy =
            self.resolve_internal_moves(&inventory, src_version_num, src, dst, recursive)?;

        // TODO continue on error

        let staging = self.get_staging()?;

        for (src_path, dst_path) in to_copy {
            info!(
                "Copying file {} from {} to {}",
                src_path, src_version_num, dst_path
            );

            let digest_and_path =
                lookup_staged_digest_and_content_path(&inventory, src_version_num, &src_path)?;

            // Copies of files new in the staged version must be copied on disk as well
            if let Some((digest, content_path)) = digest_and_path {
                staging.copy_staged_file(&inventory, &content_path, &dst_path)?;
                inventory.add_file_to_head(digest, dst_path)?;
            } else {
                inventory.copy_file_to_head(src_version_num, &src_path, dst_path)?;
            }
        }

        inventory.head_version_mut().created = Local::now();
        staging.stage_inventory(&inventory, false)?;

        Ok(())
    }

    /// Moves files from outside the OCFL repository into the specified OCFL object.
    /// A destination of `/` specifies the object's root.
    ///
    /// If `force` is `false` and the copy operation attempts to write a file to a logical
    /// path where there is already a file, then the new file will **not** be copied.
    pub fn move_files_external(
        &self,
        object_id: &str,
        src: &[impl AsRef<Path>],
        dst: &str,
    ) -> Result<()> {
        self.operate_on_external_source(
            object_id,
            src,
            dst,
            true,
            |file, logical_path, inventory| self.move_file(file, logical_path, inventory),
        )?;

        for path in src {
            let path = path.as_ref();
            if path.exists() && path.is_dir() {
                util::clean_dirs_down(path)?;
            }
        }

        Ok(())
    }

    /// Moves files within an OCFL object. The source paths may be glob patterns.
    pub fn move_files_internal(
        &self,
        object_id: &str,
        src: &[impl AsRef<str>],
        dst: &str,
    ) -> Result<()> {
        if src.is_empty() {
            return Ok(());
        }

        // TODO abstract the before and after?

        let mut inventory = self.get_or_created_staged_inventory(object_id)?;

        let to_copy = self.resolve_internal_moves(&inventory, inventory.head, src, dst, true)?;

        // TODO continue on error

        let staging = self.get_staging()?;

        for (src_path, dst_path) in to_copy {
            info!("Moving {} to {}", src_path, dst_path);

            let digest_and_path =
                lookup_staged_digest_and_content_path(&inventory, inventory.head, &src_path)?;

            // Moves of files new in the staged version must be moved on disk as well
            if let Some((digest, content_path)) = digest_and_path {
                staging.move_staged_file(&inventory, &content_path, &dst_path)?;
                inventory.move_new_in_head_file(digest, &src_path, dst_path)?;
            } else {
                inventory.move_file_in_head(&src_path, dst_path)?;
            }
        }

        inventory.head_version_mut().created = Local::now();
        staging.stage_inventory(&inventory, false)?;

        Ok(())
    }

    /// Removes the specified files from the staged version of the object. The files still
    /// exist in prior versions.
    pub fn remove_files<P: AsRef<str>>(
        &self,
        object_id: &str,
        paths: &[P],
        recursive: bool,
    ) -> Result<()> {
        if paths.is_empty() {
            return Ok(());
        }

        let mut inventory = self.get_or_created_staged_inventory(object_id)?;
        let version = inventory.head_version();

        let mut paths_to_remove = HashSet::new();

        for path in paths {
            paths_to_remove.extend(version.resolve_glob(path.as_ref(), recursive)?);
        }

        let staging = self.get_staging()?;

        for path in paths_to_remove {
            info!("Removing path from staged version: {}", path);
            if let Some(content_path) = inventory.remove_logical_path_from_head(&path) {
                staging.rm_staged_files(&inventory, &[&content_path])?;
            }
        }

        staging.stage_inventory(&inventory, false)?;

        Ok(())
    }

    /// Reset all staged changes for an object by dropping the object's staged version completely.
    pub fn reset_all(&self, object_id: &str) -> Result<()> {
        self.get_staging()?.purge_object(object_id)
    }

    /// Resets to specified staged changes to an object. Paths may be a glob and is resolved
    /// against both the staged version and the previous version. Matches in the staged version
    /// are treated as add/update resets and matches in the previous version are treated as
    /// remove resets.
    pub fn reset<P: AsRef<str>>(
        &self,
        object_id: &str,
        paths: &[P],
        recursive: bool,
    ) -> Result<()> {
        if !paths.is_empty() {
            let staging = self.get_staging()?;

            let mut inventory = match staging.get_inventory(&object_id) {
                Ok(inventory) => inventory,
                Err(RocflError::NotFound(_)) => return Ok(()),
                Err(e) => return Err(e),
            };

            let head = inventory.head_version();
            let (previous, previous_num) = if inventory.is_new() {
                (None, None)
            } else {
                let previous_num = inventory.head.previous()?;
                (
                    Some(inventory.get_version(previous_num)?),
                    Some(previous_num),
                )
            };

            let mut head_paths = HashSet::new();
            let mut previous_paths = HashSet::new();

            for path in paths {
                head_paths.extend(head.resolve_glob(path.as_ref(), recursive)?);
                if let Some(previous) = previous {
                    previous_paths.extend(previous.resolve_glob(path.as_ref(), recursive)?);
                }
            }

            let mut reset_updates = HashSet::new();
            let mut reset_adds = HashSet::new();

            for head_path in head_paths {
                if previous_paths.remove(&head_path) {
                    reset_updates.insert(head_path);
                } else {
                    reset_adds.insert(head_path);
                }
            }

            // Need to apply add resets first to attempt to avoid path conflicts
            for path in reset_adds {
                if let Some(content_path) = inventory.remove_logical_path_from_head(&path) {
                    staging.rm_staged_files(&inventory, &[&content_path])?;
                }
            }

            for path in reset_updates {
                if let Some(previous_num) = previous_num {
                    inventory.copy_file_to_head(previous_num, &path, path.as_ref().clone())?;
                }
            }

            // The remaining paths are deletes to reset
            for path in previous_paths {
                if let Some(previous_num) = previous_num {
                    inventory.copy_file_to_head(previous_num, &path, path.as_ref().clone())?;
                }
            }

            inventory.head_version_mut().created = Local::now();
            staging.stage_inventory(&inventory, false)?;
        }

        Ok(())
    }

    /// Commits all of an object's staged changes. If `user_address` is provided, then `user_name`
    /// must also be. If `created` is not provided, then it defaults to the current time.
    pub fn commit(
        &self,
        object_id: &str,
        user_name: Option<&str>,
        user_address: Option<&str>,
        message: Option<&str>,
        created: Option<DateTime<Local>>,
    ) -> Result<()> {
        if user_address.is_some() && user_name.is_none() {
            return Err(RocflError::IllegalArgument(
                "User name must be set when user address is set.".to_string(),
            ));
        }

        let staging = self.get_staging()?;

        let mut inventory = match staging.get_inventory(&object_id) {
            Ok(inventory) => inventory,
            Err(RocflError::NotFound(_)) => {
                // TODO should this be an error?
                return Err(RocflError::General(format!(
                    "No staged changes found for object {}",
                    object_id
                )));
            }
            Err(e) => return Err(e),
        };

        let duplicates = inventory.dedup_head();

        inventory
            .head_version_mut()
            .update_meta(user_name, user_address, message, created);

        staging.stage_inventory(&inventory, true)?;
        staging.rm_staged_files(
            &inventory,
            &duplicates
                .iter()
                .map(|p| p.as_ref())
                .collect::<Vec<&InventoryPath>>(),
        )?;
        staging.rm_orphaned_files(&inventory)?;

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

    /// Attempts to get the inventory from staging. If it is not found, it is loaded from the
    /// main repo, and moved into staging. If it is not found in the main repo, then an error is
    /// returned.
    fn get_or_created_staged_inventory(&self, object_id: &str) -> Result<Inventory> {
        let staging = self.get_staging()?;

        match staging.get_inventory(&object_id) {
            Ok(inventory) => Ok(inventory),
            Err(RocflError::NotFound(_)) => {
                let mut inventory = self.store.get_inventory(&object_id)?;
                inventory.create_staging_head()?;
                staging.stage_object(&mut inventory)?;
                Ok(inventory)
            }
            Err(e) => Err(e),
        }
    }

    /// Attempts to load the object's inventory from staging. If it does not exist,
    /// then `RocflError::NotFound` is returned.
    fn get_staged_inventory(&self, object_id: &str) -> Result<Inventory> {
        match self.get_staging()?.get_inventory(object_id) {
            Ok(inventory) => Ok(inventory),
            Err(RocflError::NotFound(_)) => Err(RocflError::NotFound(format!(
                "{} does not have a staged version.",
                object_id
            ))),
            Err(e) => Err(e),
        }
    }

    /// Iterates over every file in an input source and applies the operator to the file. This
    /// is intended to be used to copy/move files from the filesystem into staging.
    fn operate_on_external_source(
        &self,
        object_id: &str,
        src: &[impl AsRef<Path>],
        dst: &str,
        recursive: bool,
        operator: impl Fn(&Path, InventoryPath, &mut Inventory) -> Result<()>,
    ) -> Result<()> {
        if src.is_empty() {
            return Ok(());
        }

        // TODO even though this is not supposed to be used concurrently, it's not a bad idea
        //      to get some sort of file lock here so that an object cannot be updated concurrently
        // TODO will also need to handle ctrlc https://github.com/Detegr/rust-ctrlc

        let mut inventory = self.get_or_created_staged_inventory(object_id)?;

        let dst_path = dst.try_into()?;

        let dst_dir_exists = inventory.head_version().is_dir(&dst_path);
        let src_is_many = src.len() > 1;
        let dst_has_slash = dst.ends_with('/');

        let mut errors = Vec::new();

        for path in src.iter() {
            let path = path.as_ref();

            if !path.exists() {
                errors.push(format!(
                    "Failed to copy/move {}: Does not exist",
                    path.to_string_lossy()
                ));
                continue;
            }

            let mut attempt = || -> Result<()> {
                if path.is_file() {
                    let parent = path.parent().unwrap();

                    let logical_path = if dst_dir_exists || src_is_many || dst_has_slash {
                        logical_path_in_dst_dir(path, parent, dst)?
                    } else {
                        dst_path.clone()
                    };

                    inventory
                        .head_version()
                        .validate_non_conflicting(&logical_path)?;
                    operator(path, logical_path, &mut inventory)?;
                } else if recursive {
                    for file in WalkDir::new(path) {
                        let file = file?;
                        if file.file_type().is_file() {
                            let mut attempt = || -> Result<()> {
                                let logical_path = if dst_dir_exists || src_is_many {
                                    let grandparent = path.parent().unwrap_or(path);
                                    logical_path_in_dst_dir(file.path(), grandparent, dst)?
                                } else {
                                    logical_path_in_dst_dir(file.path(), path, dst)?
                                };

                                inventory
                                    .head_version()
                                    .validate_non_conflicting(&logical_path)?;
                                operator(file.path(), logical_path, &mut inventory)
                            };

                            if let Err(e) = attempt() {
                                errors.push(format!(
                                    "Failed to copy/move {}: {}",
                                    file.path().to_string_lossy(),
                                    e
                                ));
                            }
                        }
                    }
                } else {
                    errors.push(format!(
                        "Skipping directory {} because recursion is not enabled",
                        path.to_string_lossy()
                    ));
                }

                Ok(())
            };

            if let Err(e) = attempt() {
                errors.push(format!(
                    "Failed to copy/move {}: {}",
                    path.to_string_lossy(),
                    e
                ));
            }
        }

        inventory.head_version_mut().created = Local::now();
        self.get_staging()?.stage_inventory(&inventory, false)?;

        if !errors.is_empty() {
            return Err(RocflError::CopyMoveError(MultiError(errors)));
        }

        Ok(())
    }

    fn copy_file(
        &self,
        file: impl AsRef<Path>,
        logical_path: InventoryPath,
        inventory: &mut Inventory,
    ) -> Result<()> {
        let mut reader = inventory.digest_algorithm.reader(File::open(&file)?)?;

        info!(
            "Copying file {} into object at {}",
            file.as_ref().to_string_lossy(),
            logical_path
        );

        self.get_staging()?
            .stage_file_copy(&inventory, &mut reader, &logical_path)?;
        let digest = reader.finalize_hex();
        inventory.add_file_to_head(digest, logical_path)
    }

    fn move_file(
        &self,
        file: impl AsRef<Path>,
        logical_path: InventoryPath,
        inventory: &mut Inventory,
    ) -> Result<()> {
        info!(
            "Moving file {} into object at {}",
            file.as_ref().to_string_lossy(),
            logical_path
        );

        let digest = inventory
            .digest_algorithm
            .hash_hex(&mut File::open(file.as_ref())?)?;

        self.get_staging()?
            .stage_file_move(&inventory, &file, &logical_path)?;
        inventory.add_file_to_head(digest, logical_path)
    }

    /// Returns a map of source logical paths to destination logical paths that represent a source
    /// logical path being copied or moved from to the destination.
    fn resolve_internal_moves(
        &self,
        inventory: &Inventory,
        src_version_num: VersionNum,
        src: &[impl AsRef<str>],
        dst: &str,
        recursive: bool,
    ) -> Result<HashMap<Rc<InventoryPath>, InventoryPath>> {
        let mut to_move = HashMap::new();

        let dst_path = dst.try_into()?;
        let dst_dir_exists = inventory.head_version().is_dir(&dst_path);
        let src_is_many = src.len() > 1;
        let dst_has_slash = dst.ends_with('/');

        let version = inventory.get_version(src_version_num)?;

        for path in src {
            let mut has_matches = false;
            let files = version.resolve_glob(path.as_ref(), false)?;
            let many_files = files.len() > 1;

            if recursive {
                let dirs = version.resolve_glob_to_dirs(path.as_ref())?;
                let many_dirs = dirs.len() > 1;

                for dir in dirs {
                    let children = version.paths_with_prefix(dir.as_ref());
                    let many_children = children.len() > 1;

                    for file in children {
                        let logical_path = if dst_dir_exists
                            || src_is_many
                            || many_children
                            || many_dirs
                            || !files.is_empty()
                        {
                            logical_path_in_dst_dir_internal(&file, &dir.parent(), dst)?
                        } else {
                            logical_path_in_dst_dir_internal(&file, &dir, dst)?
                        };

                        has_matches = true;
                        to_move.insert(file.clone(), logical_path);
                    }
                }
            }

            for file in files {
                let logical_path = if dst_dir_exists
                    || src_is_many
                    || dst_has_slash
                    || many_files
                    || !to_move.is_empty()
                {
                    dst_path.resolve(&file.filename().try_into()?)
                } else {
                    dst_path.clone()
                };

                has_matches = true;
                to_move.insert(file.clone(), logical_path);
            }

            if !has_matches {
                error!(
                    "Object {} version {} does not contain: {}",
                    inventory.id,
                    src_version_num,
                    path.as_ref()
                );
            }
        }

        Ok(to_move)
    }

    fn get_staging(&self) -> Result<&FsOcflStore> {
        Ok(self.staging.get_or_try_init(|| {
            FsOcflStore::init_if_needed(
                &self.staging_root,
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

/// Creates a logical path that combines `dst` with the relativized `src` path.
fn logical_path_in_dst_dir(
    src: impl AsRef<Path>,
    base: impl AsRef<Path>,
    dst: &str,
) -> Result<InventoryPath> {
    let mut logical_path = dst.to_string();
    if !logical_path.ends_with('/') {
        logical_path.push('/');
    }

    let relative_path = pathdiff::diff_paths(src, base).unwrap();
    let relative_str = relative_path.to_string_lossy();

    logical_path.push_str(&util::convert_backslash_to_forward(&relative_str));
    logical_path.try_into()
}

/// Same as `logical_path_in_dst_dir()` but operates on `InventoryPath`s
fn logical_path_in_dst_dir_internal(
    src: &InventoryPath,
    base: &InventoryPath,
    dst: &str,
) -> Result<InventoryPath> {
    let mut logical_path = dst.to_string();
    if !logical_path.ends_with('/') {
        logical_path.push('/');
    }

    let base_length = if base.as_ref().is_empty() {
        0
    } else {
        base.as_ref().len() + 1
    };

    logical_path.push_str(&src.as_ref().as_str()[base_length..]);
    logical_path.try_into()
}

/// Looks up the digest of the specified logical path in the specified version, and then
/// attempts to resolve the digest to a content path within the staging directory. If it
/// is able to, then the digest and content path are returned. If it is not, nothing is
/// returned.
fn lookup_staged_digest_and_content_path(
    inventory: &Inventory,
    src_version_num: VersionNum,
    src_path: &InventoryPath,
) -> Result<Option<(HexDigest, Rc<InventoryPath>)>> {
    let staging_prefix = format!("{}/", inventory.head);

    match inventory
        .get_version(src_version_num)?
        .lookup_digest(&src_path)
    {
        Some(digest) => {
            let content_path = inventory.content_path_for_digest(&digest, None, Some(&src_path))?;

            if content_path.as_ref().as_ref().starts_with(&staging_prefix) {
                Ok(Some((digest.as_ref().clone(), content_path.clone())))
            } else {
                Ok(None)
            }
        }
        None => Err(RocflError::IllegalState(format!(
            "Failed to find digest for {}",
            src_path
        ))),
    }
}

use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{DateTime, Local};
use log::{error, info, warn};
use once_cell::sync::OnceCell;
#[cfg(feature = "s3")]
use rusoto_core::Region;
use walkdir::WalkDir;

use crate::ocfl::consts::*;
use crate::ocfl::digest::HexDigest;
use crate::ocfl::error::{MultiError, Result, RocflError};
use crate::ocfl::inventory::Inventory;
use crate::ocfl::lock::LockManager;
use crate::ocfl::store::fs::FsOcflStore;
use crate::ocfl::store::layout::{LayoutExtensionName, StorageLayout};
#[cfg(feature = "s3")]
use crate::ocfl::store::s3::S3OcflStore;
use crate::ocfl::store::{OcflStore, StagingStore};
use crate::ocfl::{
    paths, util, Diff, DigestAlgorithm, InventoryPath, ObjectVersion, ObjectVersionDetails,
    VersionDetails, VersionNum,
};

/// Interface for interacting with an OCFL repository
pub struct OcflRepo {
    /// For local filesystem repos, this is the storage root. TBD for S3.
    // TODO experiment changing this to a generic
    store: Box<dyn OcflStore + Sync + Send>,
    /// The OCFL repo that stores staged objects
    staging: OnceCell<FsOcflStore>,
    /// Locks staged objects so they cannot be concurrently modified
    staging_lock_manager: OnceCell<LockManager>,
    /// The path to the root of the staging repo
    staging_root: PathBuf,
    /// Indicates if the repository should convert separators to backslashes when rendering
    /// physical paths.
    use_backslashes: bool,
    closed: AtomicBool,
}

impl OcflRepo {
    /// Creates a new `OcflRepo` instance backed by the local filesystem. `storage_root` is the
    /// location of the OCFL repository to open. The OCFL repository must already exist.
    pub fn fs_repo(storage_root: impl AsRef<Path>) -> Result<Self> {
        let staging_root = paths::staging_extension_path(storage_root.as_ref());

        Ok(Self {
            staging_root,
            store: Box::new(FsOcflStore::new(storage_root)?),
            staging: OnceCell::default(),
            staging_lock_manager: OnceCell::default(),
            use_backslashes: util::BACKSLASH_SEPARATOR,
            closed: AtomicBool::new(false),
        })
    }

    /// Initializes a new `OcflRepo` instance backed by the local filesystem. The OCFL repository
    /// most not already exist.
    pub fn init_fs_repo(storage_root: impl AsRef<Path>, layout: StorageLayout) -> Result<Self> {
        let staging_root = paths::staging_extension_path(storage_root.as_ref());

        Ok(Self {
            staging_root,
            store: Box::new(FsOcflStore::init(storage_root, layout)?),
            staging: OnceCell::default(),
            staging_lock_manager: OnceCell::default(),
            use_backslashes: util::BACKSLASH_SEPARATOR,
            closed: AtomicBool::new(false),
        })
    }

    /// Initializes a new `OcflRepo` instance backed by S3. The OCFL repository
    /// most not already exist.
    #[cfg(feature = "s3")]
    pub fn init_s3_repo(
        region: Region,
        bucket: &str,
        prefix: Option<&str>,
        local_storage: impl AsRef<Path>,
        layout: StorageLayout,
    ) -> Result<Self> {
        let staging_root = paths::staging_extension_path(local_storage.as_ref());

        Ok(Self {
            staging_root,
            store: Box::new(S3OcflStore::init(region, bucket, prefix, layout)?),
            staging: OnceCell::default(),
            staging_lock_manager: OnceCell::default(),
            use_backslashes: false,
            closed: AtomicBool::new(false),
        })
    }

    /// Creates a new `OcflRepo` instance backed by S3. `prefix` used to specify a
    /// sub directory within a bucket that the OCFL repository is rooted in.
    #[cfg(feature = "s3")]
    pub fn s3_repo(
        region: Region,
        bucket: &str,
        prefix: Option<&str>,
        local_storage: impl AsRef<Path>,
    ) -> Result<Self> {
        let staging_root = paths::staging_extension_path(local_storage.as_ref());

        Ok(Self {
            staging_root,
            store: Box::new(S3OcflStore::new(region, bucket, prefix)?),
            staging: OnceCell::default(),
            staging_lock_manager: OnceCell::default(),
            use_backslashes: false,
            closed: AtomicBool::new(false),
        })
    }

    /// Instructs the repo to gracefully stop any in-flight work and not accept any additional
    /// requests.
    pub fn close(&self) {
        info!("Closing OCFL repository");
        self.closed.store(true, Ordering::Release);
        // TODO this should close the store too
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
        self.ensure_open()?;

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
        self.ensure_open()?;

        if !self.staging_root.exists() {
            return Ok(Box::new(Vec::new().into_iter()));
        }

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
        self.ensure_open()?;

        let inventory = self.store.get_inventory(object_id)?;
        let object_root = inventory.storage_path.clone();

        ObjectVersion::from_inventory(
            inventory,
            version_num,
            &object_root,
            None,
            self.use_backslashes,
        )
    }

    /// Same as `get_object()` except that it returns the staged version of an object.
    ///
    /// If the object does not have a staged version, then a `RocflError::NotFound`
    /// error is returned.
    pub fn get_staged_object(&self, object_id: &str) -> Result<ObjectVersion> {
        self.ensure_open()?;

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

        ObjectVersion::from_inventory(
            staging_inventory,
            Some(version),
            &root,
            staging.as_ref(),
            util::BACKSLASH_SEPARATOR,
        )
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
        self.ensure_open()?;

        let inventory = self.store.get_inventory(object_id)?;
        ObjectVersionDetails::from_inventory(inventory, version_num)
    }

    /// Same as `get_object_details()`, but for the staged version of an object.
    ///
    /// If the object does not have a staged version, then a `RocflError::NotFound`
    /// error is returned.
    pub fn get_staged_object_details(&self, object_id: &str) -> Result<ObjectVersionDetails> {
        self.ensure_open()?;

        let inventory = self.get_staged_inventory(object_id)?;
        let version = inventory.head;
        ObjectVersionDetails::from_inventory(inventory, Some(version))
    }

    /// Returns a vector containing the version metadata for ever version of an object. The vector
    /// is sorted in ascending order.
    ///
    /// If the object cannot be found, then a `RocflError::NotFound` error is returned.
    pub fn list_object_versions(&self, object_id: &str) -> Result<Vec<VersionDetails>> {
        self.ensure_open()?;

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
        self.ensure_open()?;

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
        self.ensure_open()?;

        let inventory = self.get_staged_inventory(object_id)?;
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
        self.ensure_open()?;

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
        self.ensure_open()?;

        self.store
            .get_inventory(object_id)?
            .diff_versions(left_version, right_version)
    }

    /// Returns all of the staged changes to the specified object, if there are any.
    pub fn diff_staged(&self, object_id: &str) -> Result<Vec<Diff>> {
        self.ensure_open()?;

        if !self.staging_root.exists() {
            return Ok(Vec::new());
        }

        match self.get_staging()?.get_inventory(&object_id) {
            Err(RocflError::NotFound(_)) => Ok(Vec::new()),
            Err(e) => Err(e),
            Ok(inventory) => inventory.diff_versions(None, inventory.head),
        }
    }

    /// Completely removes the specified object from the repository. If the object doest not exist,
    /// nothing happens.
    pub fn purge_object(&self, object_id: &str) -> Result<()> {
        self.ensure_open()?;

        if self.staging_root.exists() {
            self.get_staging()?.purge_object(object_id)?;
        }

        // Last chance for the user to have ctrl-c'd the operation
        if self.is_open() {
            self.store.purge_object(object_id)
        } else {
            Ok(())
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
        self.ensure_open()?;

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

        let _lock = self.get_lock_manager()?.acquire(object_id)?;

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
    pub fn copy_files_external(
        &self,
        object_id: &str,
        src: &[impl AsRef<Path>],
        dst: &str,
        recursive: bool,
    ) -> Result<()> {
        self.ensure_open()?;

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
        self.ensure_open()?;

        if src.is_empty() {
            return Ok(());
        }

        let _lock = self.get_lock_manager()?.acquire(object_id)?;

        let mut inventory = self.get_or_created_staged_inventory(object_id)?;
        let src_version_num = version_num.unwrap_or(inventory.head);
        let staging = self.get_staging()?;

        let (to_copy, mut errors) =
            self.resolve_internal_moves(&inventory, src_version_num, src, dst, recursive)?;

        for (src_path, dst_path) in to_copy {
            if self.is_closed() {
                break;
            }

            let attempt = || -> Result<()> {
                info!(
                    "Copying file {} from {} to {}",
                    src_path, src_version_num, dst_path
                );

                let digest_and_path =
                    lookup_staged_digest_and_content_path(&inventory, src_version_num, &src_path)?;

                // Copies of files new in the staged version must be copied on disk as well
                if let Some((digest, content_path)) = digest_and_path {
                    // Validate before copy to decrease the chance of failure after copying on disk
                    inventory
                        .head_version()
                        .validate_non_conflicting(&dst_path)?;
                    staging.copy_staged_file(&inventory, &content_path, &dst_path)?;
                    // Should be impossible to fail
                    inventory.add_file_to_head(digest, dst_path)
                } else {
                    inventory.copy_file_to_head(src_version_num, &src_path, dst_path)
                }
            };

            if let Err(e) = attempt() {
                errors.push(format!("Failed to copy file {}: {}", src_path, e));
            }
        }

        inventory.head_version_mut().created = Local::now();
        staging.stage_inventory(&inventory, false)?;

        if !errors.is_empty() {
            return Err(RocflError::CopyMoveError(MultiError(errors)));
        }

        Ok(())
    }

    /// Moves files from outside the OCFL repository into the specified OCFL object.
    /// A destination of `/` specifies the object's root.
    pub fn move_files_external(
        &self,
        object_id: &str,
        src: &[impl AsRef<Path>],
        dst: &str,
    ) -> Result<()> {
        self.ensure_open()?;

        self.operate_on_external_source(
            object_id,
            src,
            dst,
            true,
            |file, logical_path, inventory| self.move_file(file, logical_path, inventory),
        )?;

        if self.is_open() {
            for path in src {
                let path = path.as_ref();
                if path.exists() && path.is_dir() {
                    util::clean_dirs_down(path)?;
                }
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
        self.ensure_open()?;

        if src.is_empty() {
            return Ok(());
        }

        let _lock = self.get_lock_manager()?.acquire(object_id)?;

        let mut inventory = self.get_or_created_staged_inventory(object_id)?;
        let staging = self.get_staging()?;

        let (to_move, mut errors) =
            self.resolve_internal_moves(&inventory, inventory.head, src, dst, true)?;

        for (src_path, dst_path) in to_move {
            if self.is_closed() {
                break;
            }

            info!("Moving {} to {}", src_path, dst_path);

            let attempt = || -> Result<()> {
                let digest_and_path =
                    lookup_staged_digest_and_content_path(&inventory, inventory.head, &src_path)?;

                // Moves of files new in the staged version must be moved on disk as well
                if let Some((digest, content_path)) = digest_and_path {
                    // Validate before move to decrease the chance of failure after moving on disk
                    inventory
                        .head_version()
                        .validate_non_conflicting(&dst_path)?;
                    staging.move_staged_file(&inventory, &content_path, &dst_path)?;
                    // Should be impossible to fail
                    inventory.move_new_in_head_file(digest, &src_path, dst_path)
                } else {
                    inventory.move_file_in_head(&src_path, dst_path)
                }
            };

            if let Err(e) = attempt() {
                errors.push(format!("Failed to move file {}: {}", src_path, e));
            }
        }

        inventory.head_version_mut().created = Local::now();
        staging.stage_inventory(&inventory, false)?;

        if !errors.is_empty() {
            return Err(RocflError::CopyMoveError(MultiError(errors)));
        }

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
        self.ensure_open()?;

        if paths.is_empty() {
            return Ok(());
        }

        let _lock = self.get_lock_manager()?.acquire(object_id)?;

        let mut inventory = self.get_or_created_staged_inventory(object_id)?;
        let version = inventory.head_version();

        let mut paths_to_remove = HashSet::new();

        for path in paths {
            paths_to_remove.extend(version.resolve_glob(path.as_ref(), recursive)?);
        }

        let staging = self.get_staging()?;

        for path in paths_to_remove {
            if self.is_closed() {
                break;
            }

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
        self.ensure_open()?;

        if self.staging_root.exists() {
            self.get_staging()?.purge_object(object_id)
        } else {
            Ok(())
        }
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
        self.ensure_open()?;

        if paths.is_empty() {
            return Ok(());
        }

        let staging = self.get_staging()?;

        let _lock = self.get_lock_manager()?.acquire(object_id)?;

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

        let reset_adds = head_paths
            .into_iter()
            .filter(|path| !previous_paths.contains(path))
            .collect::<HashSet<Rc<InventoryPath>>>();

        // Need to apply add resets first to attempt to avoid path conflicts
        for path in reset_adds {
            if self.is_closed() {
                break;
            }

            if let Some(content_path) = inventory.remove_logical_path_from_head(&path) {
                staging.rm_staged_files(&inventory, &[&content_path])?;
            }
        }

        // Resetting deleted or modified files is the same
        for path in previous_paths {
            if self.is_closed() {
                break;
            }

            if let Some(previous_num) = previous_num {
                inventory.copy_file_to_head(previous_num, &path, path.as_ref().clone())?;
            }
        }

        inventory.head_version_mut().created = Local::now();
        staging.stage_inventory(&inventory, false)
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
        self.ensure_open()?;

        if user_address.is_some() && user_name.is_none() {
            return Err(RocflError::IllegalArgument(
                "User name must be set when user address is set.".to_string(),
            ));
        }

        let staging = self.get_staging()?;

        let _lock = self.get_lock_manager()?.acquire(object_id)?;

        let mut inventory = match staging.get_inventory(&object_id) {
            Ok(inventory) => inventory,
            Err(RocflError::NotFound(_)) => {
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

        // Last chance to ctrl-c before committing
        if self.is_open() {
            if inventory.is_new() {
                let object_root = PathBuf::from(&inventory.storage_path);
                self.store.write_new_object(&mut inventory, &object_root)?;
            } else {
                let version_root = paths::version_path(&inventory.storage_path, inventory.head);
                self.store
                    .write_new_version(&mut inventory, &version_root)?;
            }

            staging.purge_object(object_id)?;
        }

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

                if inventory.mutable_head {
                    return Err(RocflError::IllegalState(
                        "Cannot stage changes for object because it has an active mutable HEAD."
                            .to_string(),
                    ));
                }

                for extension in self.store.list_object_extensions(object_id)? {
                    if !SUPPORTED_EXTENSIONS.contains(&extension.as_ref()) {
                        warn!("Object {} uses unsupported extension {}. Modifying this object may have unintended consequences.",
                              object_id, extension);
                    }
                }

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
        if !self.staging_root.exists() {
            return Err(RocflError::NotFound(format!(
                "{} does not have a staged version.",
                object_id
            )));
        }

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

        let _lock = self.get_lock_manager()?.acquire(object_id)?;

        let mut inventory = self.get_or_created_staged_inventory(object_id)?;

        let dst_path = dst.try_into()?;

        let dst_dir_exists = inventory.head_version().is_dir(&dst_path);
        let src_is_many = src.len() > 1;
        let dst_has_slash = dst.ends_with('/');

        let mut errors = Vec::new();

        for path in src.iter() {
            if self.is_closed() {
                break;
            }

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
                        if self.is_closed() {
                            break;
                        }

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

        // It should be impossible for the inventory update to fail because the destination
        // paths were already validated for conflicts. It is possible the file move could fail
        // if the source files conflict, but this will not corrupt anything.
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

        // It should be impossible for the inventory update to fail because the destination
        // paths were already validated for conflicts. It is possible the file move could fail
        // if the source files conflict, but this will not corrupt anything.
        self.get_staging()?
            .stage_file_move(&inventory, &file, &logical_path)?;
        inventory.add_file_to_head(digest, logical_path)
    }

    /// Returns a map of source logical paths to destination logical paths that represent a source
    /// logical path being copied or moved from to the destination.
    #[allow(clippy::type_complexity)]
    fn resolve_internal_moves(
        &self,
        inventory: &Inventory,
        src_version_num: VersionNum,
        src: &[impl AsRef<str>],
        dst: &str,
        recursive: bool,
    ) -> Result<(HashMap<Rc<InventoryPath>, InventoryPath>, Vec<String>)> {
        let mut to_move = HashMap::new();
        let mut errors = Vec::new();

        let dst_path = dst.try_into()?;
        let dst_dir_exists = inventory.head_version().is_dir(&dst_path);
        let src_is_many = src.len() > 1;
        let dst_has_slash = dst.ends_with('/');

        let version = inventory.get_version(src_version_num)?;

        for path in src {
            let mut has_matches = false;

            let files = match version.resolve_glob(path.as_ref(), false) {
                Ok(files) => files,
                Err(e) => {
                    errors.push(format!("Failed to resolve path {}: {}", path.as_ref(), e));
                    continue;
                }
            };
            let many_files = files.len() > 1;

            if recursive {
                let dirs = match version.resolve_glob_to_dirs(path.as_ref()) {
                    Ok(dirs) => dirs,
                    Err(e) => {
                        errors.push(format!("Failed to resolve path {}: {}", path.as_ref(), e));
                        HashSet::new()
                    }
                };
                let many_dirs = dirs.len() > 1;

                for dir in dirs {
                    let children = version.paths_with_prefix(dir.as_ref());
                    let many_children = children.len() > 1;

                    for file in children {
                        let mut attempt = || -> Result<()> {
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
                            Ok(())
                        };

                        if let Err(e) = attempt() {
                            errors.push(format!("Failed to copy/move file {}: {}", file, e));
                        }
                    }
                }
            }

            for file in files {
                let mut attempt = || -> Result<()> {
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
                    Ok(())
                };

                if let Err(e) = attempt() {
                    errors.push(format!("Failed to copy/move file {}: {}", file, e));
                }
            }

            if !has_matches {
                errors.push(format!(
                    "Object {} version {} does not contain any files at {}",
                    inventory.id,
                    src_version_num,
                    path.as_ref()
                ));
            }
        }

        Ok((to_move, errors))
    }

    fn get_staging(&self) -> Result<&FsOcflStore> {
        // This is deferred so that the extension directories are only created if needed
        self.staging.get_or_try_init(|| {
            FsOcflStore::init_if_needed(
                &self.staging_root,
                StorageLayout::new(LayoutExtensionName::HashedNTupleLayout, None)?,
            )
        })
    }

    fn get_lock_manager(&self) -> Result<&LockManager> {
        // Staging must exist first
        self.get_staging()?;
        // This is deferred so that the extension directories are only created if needed
        self.staging_lock_manager
            .get_or_try_init(|| -> Result<LockManager> {
                let dir = paths::locks_extension_path(&self.staging_root);
                fs::create_dir_all(&dir)?;
                Ok(LockManager::new(dir))
            })
    }

    fn ensure_open(&self) -> Result<()> {
        if self.is_closed() {
            Err(RocflError::Closed)
        } else {
            Ok(())
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn is_open(&self) -> bool {
        !self.is_closed()
    }
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

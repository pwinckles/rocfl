//! S3 OCFL storage implementation.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::vec::IntoIter;

use bytes::Bytes;
use futures::{FutureExt, TryStreamExt};
use globset::GlobBuilder;
use log::{debug, error, info, warn};
use once_cell::sync::Lazy;
use rusoto_core::credential::{AutoRefreshingProvider, ChainProvider, ProfileProvider};
use rusoto_core::{ByteStream, Client, HttpClient, Region, RusotoError};
use rusoto_s3::{
    AbortMultipartUploadRequest, CompleteMultipartUploadRequest, CompletedMultipartUpload,
    CompletedPart, CreateMultipartUploadRequest, DeleteObjectRequest, GetObjectError,
    GetObjectRequest, ListObjectsV2Output, ListObjectsV2Request, PutObjectRequest,
    S3Client as RusotoS3Client, StreamingBody, UploadPartRequest, S3,
};
use tokio::io::AsyncReadExt;
use tokio::runtime;
use tokio::runtime::Runtime;
use walkdir::WalkDir;

use super::layout::StorageLayout;
use super::{OcflLayout, OcflStore};
use crate::ocfl::consts::*;
use crate::ocfl::error::{not_found, Result, RocflError};
use crate::ocfl::inventory::Inventory;
use crate::ocfl::paths::{join, join_with_trailing_slash};
use crate::ocfl::store::{Listing, Storage};
use crate::ocfl::validate::{IncrementalValidator, ObjectValidationResult, Validator};
use crate::ocfl::{
    paths, specs, util, InventoryPath, LayoutExtensionName, LogicalPath, VersionRef,
};

const TYPE_PLAIN: &str = "text/plain; charset=UTF-8";
const TYPE_MARKDOWN: &str = "text/markdown; charset=UTF-8";
const TYPE_JSON: &str = "application/json; charset=UTF-8";
const ROOT_NAMASTE_CONTENT: &str = "ocfl_1.0\n";

const PART_SIZE: u64 = 1024 * 1024 * 5;

static EXTENSIONS_DIR_SUFFIX: Lazy<String> = Lazy::new(|| format!("/{}", EXTENSIONS_DIR));

pub struct S3OcflStore {
    s3_client: Arc<S3Client>,
    /// Maps object IDs to paths within the storage root
    storage_layout: Option<StorageLayout>,
    validator: Validator<S3Storage>,
    // TODO this never expires entries and is only intended to be useful within the scope of the cli
    /// Caches object ID to path mappings
    id_path_cache: RwLock<HashMap<String, String>>,
    prefix: Option<String>,
    closed: Arc<AtomicBool>,
}

impl S3OcflStore {
    /// Creates a new S3OcflStore
    pub fn new(
        region: Region,
        bucket: &str,
        prefix: Option<&str>,
        profile: Option<&str>,
    ) -> Result<Self> {
        let s3_client = S3Client::new(region, bucket, prefix, profile)?;

        check_extensions(&s3_client);
        let storage_layout = load_storage_layout(&s3_client);

        let s3_client = Arc::new(s3_client);

        Ok(Self {
            validator: Validator::new(S3Storage::new(s3_client.clone())),
            s3_client,
            storage_layout,
            id_path_cache: RwLock::new(HashMap::new()),
            prefix: prefix.map(|p| util::trim_trailing_slashes(p).to_string()),
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Initializes a new OCFL repository at the specified location
    pub fn init(
        region: Region,
        bucket: &str,
        prefix: Option<&str>,
        layout: Option<StorageLayout>,
        profile: Option<&str>,
    ) -> Result<Self> {
        let s3_client = S3Client::new(region, bucket, prefix, profile)?;

        init_new_repo(&s3_client, layout.as_ref())?;

        let s3_client = Arc::new(s3_client);

        Ok(Self {
            validator: Validator::new(S3Storage::new(s3_client.clone())),
            s3_client,
            storage_layout: layout,
            id_path_cache: RwLock::new(HashMap::new()),
            prefix: prefix.map(|p| util::trim_trailing_slashes(p).to_string()),
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    /// This method first attempts to locate the path to the object using the storage layout.
    /// If it is not able to, then it scans the repository looking for the object.
    fn lookup_or_find_object_root_path(&self, object_id: &str) -> Result<String> {
        match self.get_object_root_path(object_id) {
            Some(path) => Ok(path),
            None => match self.scan_for_inventory(object_id) {
                Ok(inventory) => Ok(inventory.object_root),
                Err(e) => Err(e),
            },
        }
    }

    /// Returns the storage root relative path to the object by doing a cache look up. If
    /// the mapping was not found in the cache, then it is computed using the configured
    /// storage layout. If there is no storage layout, then `None` is returned.
    fn get_object_root_path(&self, object_id: &str) -> Option<String> {
        if let Ok(cache) = self.id_path_cache.read() {
            if let Some(object_root) = cache.get(object_id) {
                return Some(object_root.clone());
            }
        }

        if let Some(storage_layout) = &self.storage_layout {
            let object_root = storage_layout.map_object_id(object_id);

            if let Ok(mut cache) = self.id_path_cache.write() {
                cache.insert(object_id.to_string(), object_root.clone());
                return Some(object_root);
            }
        }

        None
    }

    fn scan_for_inventory(&self, object_id: &str) -> Result<Inventory> {
        info!(
            "Storage layout not configured, scanning repository to locate object {}",
            &object_id
        );

        let mut iter = InventoryIter::new_id_matching(self, object_id, self.closed.clone());

        loop {
            return match iter.next() {
                Some(Ok(inventory)) => Ok(inventory),
                Some(Err(e)) => {
                    error!("{:#}", e);
                    continue;
                }
                None => Err(not_found(object_id, None)),
            };
        }
    }

    fn parse_inventory_required(&self, object_id: &str, object_root: &str) -> Result<Inventory> {
        match self.parse_inventory(object_root)? {
            Some(inventory) => {
                if inventory.id != object_id {
                    Err(RocflError::CorruptObject {
                        object_id: object_id.to_string(),
                        message: format!(
                            "Expected object to exist at {} but found object {} instead.",
                            object_root, inventory.id
                        ),
                    })
                } else {
                    Ok(inventory)
                }
            }
            None => Err(not_found(object_id, None)),
        }
    }

    /// Parses the HEAD inventory of the OCFL object that's rooted in the specified directory.
    /// This is normally the `inventory.json` file in the object's root, but it could also be
    /// the inventory file in an extension directory, such as the mutable HEAD extension.
    fn parse_inventory(&self, object_root: &str) -> Result<Option<Inventory>> {
        let bytes = self.get_inventory_bytes(object_root)?;
        // TODO should validate hash

        if let Some((bytes, mutable_head)) = bytes {
            let mut inventory = match self.parse_inventory_bytes(&bytes) {
                Ok(inventory) => inventory,
                Err(e) => {
                    return Err(RocflError::General(format!(
                        "Failed to parse inventory in object at {}: {}",
                        object_root, e
                    )))
                }
            };
            inventory.object_root = util::trim_slashes(object_root).to_string();

            inventory.storage_path = match &self.prefix {
                Some(prefix) => join(prefix, &inventory.object_root),
                None => inventory.object_root.clone(),
            };
            inventory.mutable_head = mutable_head;

            Ok(Some(inventory))
        } else {
            Ok(None)
        }
    }

    fn parse_inventory_bytes(&self, bytes: &[u8]) -> Result<Inventory> {
        let inventory: Inventory = serde_json::from_slice(bytes)?;
        Ok(inventory)
    }

    fn get_inventory_bytes(&self, object_root: &str) -> Result<Option<(Vec<u8>, bool)>> {
        let mutable_head_inv = join(object_root, MUTABLE_HEAD_INVENTORY_FILE);

        match self.s3_client.get_object(&mutable_head_inv)? {
            Some(bytes) => {
                info!("Found mutable HEAD at {}", &mutable_head_inv);
                Ok(Some((bytes, true)))
            }
            None => {
                let inv_path = join(object_root, INVENTORY_FILE);
                match self.s3_client.get_object(&inv_path)? {
                    Some(bytes) => Ok(Some((bytes, false))),
                    None => Ok(None),
                }
            }
        }
    }

    fn upload_all_files_with_rollback(
        &self,
        dst_path: &str,
        src_dir: impl AsRef<Path>,
    ) -> Result<Vec<String>> {
        self.do_with_rollback(Vec::new(), |done: &mut Vec<String>| -> Result<()> {
            for file in WalkDir::new(src_dir.as_ref()) {
                // Want an error returned here so that we rollback
                self.ensure_open()?;

                let file = file?;
                if file.file_type().is_dir() {
                    continue;
                }

                let relative_path = pathdiff::diff_paths(file.path(), src_dir.as_ref())
                    .unwrap()
                    .to_string_lossy()
                    .to_string();
                let content_path = util::convert_backslash_to_forward(relative_path.as_ref());
                let storage_path = join(dst_path, content_path.as_ref());
                self.s3_client
                    .put_object_file(&storage_path, file.path(), None)?;
                done.push(storage_path);
            }
            Ok(())
        })
    }

    fn install_inventory_in_root_with_rollback(
        &self,
        inventory: &Inventory,
        version_path: impl AsRef<Path>,
        uploaded: Vec<String>,
    ) -> Result<()> {
        let inventory_src = paths::inventory_path(&version_path);
        let sidecar_src = paths::sidecar_path(&version_path, inventory.digest_algorithm);
        let inventory_dst = join(&inventory.object_root, INVENTORY_FILE);
        let sidecar_dst = join(
            &inventory.object_root,
            &sidecar_src.file_name().unwrap().to_string_lossy(),
        );

        self.do_with_rollback(uploaded, |done: &mut Vec<String>| -> Result<()> {
            self.s3_client
                .put_object_file(&inventory_dst, &inventory_src, Some(TYPE_JSON))?;
            done.push(inventory_dst.clone());
            self.s3_client
                .put_object_file(&sidecar_dst, &sidecar_src, Some(TYPE_PLAIN))?;
            Ok(())
        })?;

        Ok(())
    }

    fn do_with_rollback(
        &self,
        mut done: Vec<String>,
        mut callable: impl FnMut(&mut Vec<String>) -> Result<()>,
    ) -> Result<Vec<String>> {
        if let Err(e) = callable(&mut done) {
            for path in &done {
                if let Err(e2) = self.s3_client.delete_object(path) {
                    error!("Failed to rollback file {}: {}", path, e2);
                }
            }
            return Err(RocflError::General(
                format!("Failed to upload all files to S3. Successfully uploaded files were rolled back. Error: {}", e)));
        }
        Ok(done)
    }

    /// Pass through to S3 to list the contents of a path in S3
    fn list_dir(&self, path: &str) -> Result<ListResult> {
        self.s3_client.list_dir(path)
    }

    /// Returns an error if the store is closed
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
}

impl OcflStore for S3OcflStore {
    /// Returns the most recent inventory version for the specified object, or an a
    /// `RocflError::NotFound` if it does not exist.
    fn get_inventory(&self, object_id: &str) -> Result<Inventory> {
        self.ensure_open()?;

        match self.get_object_root_path(object_id) {
            Some(object_root) => self.parse_inventory_required(object_id, &object_root),
            None => self.scan_for_inventory(object_id),
        }
    }

    /// Returns an iterator that iterates over every object in an OCFL repository, returning
    /// the most recent inventory of each. Optionally, a glob pattern may be provided that filters
    /// the objects that are returned by OCFL ID.
    fn iter_inventories<'a>(
        &'a self,
        filter_glob: Option<&str>,
    ) -> Result<Box<dyn Iterator<Item = Result<Inventory>> + 'a>> {
        self.ensure_open()?;

        Ok(Box::new(match filter_glob {
            Some(glob) => InventoryIter::new_glob_matching(self, glob, self.closed.clone())?,
            None => InventoryIter::new(self, None, self.closed.clone()),
        }))
    }

    /// Writes the specified file to the sink.
    ///
    /// If the file cannot be found, then a `RocflError::NotFound` error is returned.
    fn get_object_file(
        &self,
        object_id: &str,
        path: &LogicalPath,
        version_num: VersionRef,
        sink: &mut dyn Write,
    ) -> Result<()> {
        self.ensure_open()?;

        let inventory = self.get_inventory(object_id)?;

        let content_path = inventory.content_path_for_logical_path(path, version_num)?;
        let storage_path = join(&inventory.object_root, content_path.as_str());

        self.s3_client.stream_object(&storage_path, sink)
    }

    /// Writes a new OCFL object. The contents at `object_path` must be a fully formed OCFL
    /// object that is able to be moved into place with no additional modifications.
    ///
    /// The object must not already exist.
    fn write_new_object(
        &self,
        inventory: &mut Inventory,
        src_object_path: &Path,
        object_root: Option<&str>,
    ) -> Result<()> {
        self.ensure_open()?;

        let object_root = match self.get_object_root_path(&inventory.id) {
            Some(object_root) => object_root,
            None => {
                if let Some(root) = object_root {
                    util::trim_slashes(root).to_string()
                } else {
                    return Err(RocflError::IllegalState(
                            "Cannot create object because the repository does not have a defined storage layout, and an object root path was not specified."
                                .to_string(),
                        ));
                }
            }
        };

        if !self.s3_client.list_dir(&object_root)?.is_empty() {
            return Err(RocflError::IllegalState(format!(
                "Cannot create object {} because there are existing files at {}",
                inventory.id, object_root
            )));
        }

        info!("Creating new object {}", inventory.id);

        self.upload_all_files_with_rollback(&object_root, src_object_path)?;

        inventory.storage_path = match &self.prefix {
            Some(prefix) => join(prefix, &inventory.object_root),
            None => object_root,
        };

        Ok(())
    }

    /// Writes a new version to the OCFL object. The contents at `version_path` must be a fully
    /// formed OCFL version that is able to be moved into place within the object, requiring
    /// no additional modifications.
    ///
    /// The object must already exist, and the new version must not exist.
    fn write_new_version(&self, inventory: &mut Inventory, version_path: &Path) -> Result<()> {
        self.ensure_open()?;

        if inventory.is_new() {
            return Err(RocflError::IllegalState(format!(
                "Object {} must be created before adding new versions to it.",
                inventory.id
            )));
        }

        let existing_inventory = self.get_inventory(&inventory.id)?;
        let version_str = inventory.head.to_string();

        if existing_inventory.head != inventory.head.previous().unwrap() {
            return Err(RocflError::IllegalState(format!(
                "Cannot create version {} in object {} because the current version is at {}",
                version_str,
                inventory.id,
                existing_inventory.head.to_string()
            )));
        }

        let version_dst_path = join(&existing_inventory.object_root, &version_str);

        if !self.s3_client.list_dir(&version_dst_path)?.is_empty() {
            return Err(RocflError::IllegalState(
                format!("Cannot create version {} in object {} because the version directory already exists.",
                        version_str, inventory.id)));
        }

        info!(
            "Creating version {} of object {}",
            version_str, inventory.id
        );

        let uploaded = self.upload_all_files_with_rollback(&version_dst_path, version_path)?;
        self.install_inventory_in_root_with_rollback(inventory, version_path, uploaded)?;

        inventory.storage_path = existing_inventory.storage_path;

        Ok(())
    }

    /// Purges the specified object from the repository, if it exists. If it does not exist,
    /// nothing happens. Any dangling directories that were created as a result of purging
    /// the object are also removed.
    fn purge_object(&self, object_id: &str) -> Result<()> {
        self.ensure_open()?;

        let object_root = match self.lookup_or_find_object_root_path(object_id) {
            Err(RocflError::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e),
            Ok(object_root) => object_root,
        };

        info!("Purging object {} at {}", object_id, object_root);

        let mut failed = false;

        for file in self.s3_client.list_objects(&object_root)? {
            if self.is_closed() {
                error!("Terminating purge of object {} at {}. This object will need to be cleaned up manually.",
                       object_id, object_root);
                failed = true;
                break;
            }
            if let Err(e) = self.s3_client.delete_object(&file) {
                error!("Failed to delete file {}: {}", file, e);
                failed = true;
            }
        }

        if failed {
            return Err(RocflError::CorruptObject {
                object_id: object_id.to_string(),
                message: format!(
                    "Failed to purge object at {}. This object may need to be removed manually.",
                    object_root
                ),
            });
        }

        Ok(())
    }

    /// Returns a list of all of the extension names that are associated with the object
    fn list_object_extensions(&self, object_id: &str) -> Result<Vec<String>> {
        self.ensure_open()?;

        let object_root = self.lookup_or_find_object_root_path(object_id)?;
        let extensions_dir = join(&object_root, EXTENSIONS_DIR);

        let mut extensions = Vec::new();

        let list_result = self.s3_client.list_dir(&extensions_dir)?;

        for path in list_result.directories {
            extensions.push(path[extensions_dir.len() + 1..].to_string());
        }

        Ok(extensions)
    }

    /// Validates the specified object and returns any problems found. Err will only be returned
    /// if a non-validation problem was encountered.
    fn validate_object(
        &self,
        object_id: &str,
        fixity_check: bool,
    ) -> Result<ObjectValidationResult> {
        let object_root = self.lookup_or_find_object_root_path(object_id)?;

        self.validator
            .validate_object(Some(object_id), &object_root, fixity_check)
    }

    /// Validates the specified object at the specified path, relative the storage root, and
    /// returns any problems found. Err will only be returned if a non-validation problem was
    /// encountered.
    fn validate_object_at(
        &self,
        object_root: &str,
        fixity_check: bool,
    ) -> Result<ObjectValidationResult> {
        self.validator
            .validate_object(None, object_root, fixity_check)
    }

    /// Validates the structure of an OCFL repository as well as all of the objects in the repository
    /// When `fixity_check` is `false`, then the digests of object content files are not validated.
    ///
    /// The storage root is validated immediately, and an incremental validator is returned that
    /// is used to lazily validate the rest of the repository.
    fn validate_repo<'a>(
        &'a self,
        fixity_check: bool,
    ) -> Result<Box<dyn IncrementalValidator + 'a>> {
        Ok(Box::new(self.validator.validate_repo(fixity_check)?))
    }

    /// Instructs the store to gracefully stop any in-flight work and not accept any additional
    /// requests.
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.validator.close();
    }
}

struct S3Client {
    s3_client: RusotoS3Client,
    bucket: String,
    prefix: String,
    // TODO this should ideally be externalized, but wait for new aws rust client
    runtime: Runtime,
}

struct ListResult {
    objects: Vec<String>,
    directories: Vec<String>,
}

struct InventoryIter<'a> {
    store: &'a S3OcflStore,
    dir_iters: Vec<IntoIter<String>>,
    current: RefCell<Option<IntoIter<String>>>,
    id_matcher: Option<Box<dyn Fn(&str) -> bool>>,
    closed: Arc<AtomicBool>,
}

pub struct S3Storage {
    s3_client: Arc<S3Client>,
}

impl S3Client {
    fn new(
        region: Region,
        bucket: &str,
        prefix: Option<&str>,
        profile: Option<&str>,
    ) -> Result<Self> {
        Ok(S3Client {
            s3_client: create_rusoto_client(region, profile),
            bucket: bucket.to_owned(),
            prefix: prefix.unwrap_or_default().to_owned(),
            runtime: runtime::Builder::new_multi_thread().enable_all().build()?,
        })
    }

    /// Returns all of the object keys or logical directories that are under the specified prefix.
    /// All returned keys and key parts are relative the repository prefix; not the search prefix.
    fn list_dir(&self, path: &str) -> Result<ListResult> {
        self.list_prefix(path, Some("/".to_string()))
    }

    /// Returns all of the object keys under the specified prefix. All returned keys and key parts
    /// are relative the repository prefix; not the search prefix.
    fn list_objects(&self, path: &str) -> Result<Vec<String>> {
        Ok(self.list_prefix(path, None)?.objects)
    }

    /// Returns all of the object keys or logical directories that are under the specified prefix.
    /// All returned keys and key parts are relative the repository prefix; not the search prefix.
    fn list_prefix(&self, path: &str, delimiter: Option<String>) -> Result<ListResult> {
        let prefix = join_with_trailing_slash(&self.prefix, path);

        info!("Listing S3 prefix: {}", prefix);

        let mut objects = Vec::new();
        let mut directories = Vec::new();
        let mut continuation = None;

        loop {
            let result: ListObjectsV2Output =
                self.runtime
                    .block_on(self.s3_client.list_objects_v2(ListObjectsV2Request {
                        bucket: self.bucket.clone(),
                        prefix: Some(prefix.clone()),
                        delimiter: delimiter.clone(),
                        continuation_token: continuation.clone(),
                        ..Default::default()
                    }))?;

            let prefix_offset = if self.prefix.is_empty() {
                0
            } else {
                self.prefix.len() + 1
            };

            if let Some(contents) = &result.contents {
                for object in contents {
                    objects.push(object.key.as_ref().unwrap()[prefix_offset..].to_owned());
                }
            }

            if let Some(prefixes) = &result.common_prefixes {
                for prefix in prefixes {
                    let length = prefix.prefix.as_ref().unwrap().len() - 1;
                    directories
                        .push(prefix.prefix.as_ref().unwrap()[prefix_offset..length].to_owned());
                }
            }

            if result.is_truncated.unwrap() {
                continuation = result.next_continuation_token.clone();
            } else {
                break;
            }
        }

        Ok(ListResult {
            objects,
            directories,
        })
    }

    fn get_object(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let key = join(&self.prefix, path);

        info!("Getting object from S3: {}", key);

        let result = self
            .runtime
            .block_on(self.s3_client.get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key,
                ..Default::default()
            }));

        match result {
            Ok(result) => self.runtime.block_on(async move {
                let mut buffer = Vec::new();
                result
                    .body
                    .unwrap()
                    .into_async_read()
                    .read_to_end(&mut buffer)
                    .await?;
                Ok(Some(buffer))
            }),
            Err(RusotoError::Service(GetObjectError::NoSuchKey(_e))) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn stream_object(&self, path: &str, sink: &mut dyn Write) -> Result<()> {
        let key = join(&self.prefix, path);

        info!("Streaming object from S3: {}", key);

        let result = self
            .runtime
            .block_on(self.s3_client.get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key,
                ..Default::default()
            }));

        match result {
            Ok(result) => self.runtime.block_on(async move {
                let mut reader = result.body.unwrap().into_async_read();
                let mut buf = [0; 8192];
                loop {
                    let read = reader.read(&mut buf).await?;
                    if read == 0 {
                        break;
                    }
                    sink.write_all(&buf[..read])?;
                }
                Ok(())
            }),
            Err(e) => Err(e.into()),
        }
    }

    fn delete_object(&self, path: &str) -> Result<()> {
        let key = join(&self.prefix, path);

        info!("Deleting object in S3: {}", key);

        self.runtime
            .block_on(self.s3_client.delete_object(DeleteObjectRequest {
                bucket: self.bucket.clone(),
                key,
                ..Default::default()
            }))?;

        Ok(())
    }

    fn put_object_bytes(
        &self,
        path: &str,
        content: Bytes,
        content_type: Option<&str>,
    ) -> Result<()> {
        let key = join(&self.prefix, path);

        info!("Putting object in S3: {}", key);

        self.runtime
            .block_on(self.s3_client.put_object(PutObjectRequest {
                key,
                bucket: self.bucket.clone(),
                content_length: Some(content.len() as i64),
                body: Some(ByteStream::new(futures::stream::once(async move {
                    Ok(content)
                }))),
                content_type: content_type.map(|s| s.to_string()),
                ..Default::default()
            }))?;

        Ok(())
    }

    fn put_object_file(
        &self,
        path: &str,
        file_path: impl AsRef<Path>,
        content_type: Option<&str>,
    ) -> Result<()> {
        let content_length = std::fs::metadata(&file_path)?.len();

        if content_length > PART_SIZE {
            self.multipart_put_file(path, file_path, content_length, content_type)?;
        } else {
            let key = join(&self.prefix, path);
            info!(
                "Putting {} in S3 at {}",
                file_path.as_ref().to_string_lossy(),
                key
            );

            let stream = tokio::fs::read(file_path.as_ref().to_path_buf())
                .into_stream()
                .map_ok(Bytes::from);

            self.runtime
                .block_on(self.s3_client.put_object(PutObjectRequest {
                    key,
                    bucket: self.bucket.clone(),
                    content_length: Some(content_length as i64),
                    body: Some(StreamingBody::new(stream)),
                    content_type: content_type.map(|s| s.to_string()),
                    ..Default::default()
                }))?;
        }

        Ok(())
    }

    fn multipart_put_file(
        &self,
        path: &str,
        file_path: impl AsRef<Path>,
        content_length: u64,
        content_type: Option<&str>,
    ) -> Result<()> {
        let key = join(&self.prefix, path);

        info!(
            "Initiating S3 multipart upload of {} to {}",
            file_path.as_ref().to_string_lossy(),
            key
        );

        let mut i = 1;
        let mut reader = File::open(file_path)?;
        let mut buffer = [b'a'; PART_SIZE as usize];
        let mut parts = Vec::with_capacity(((content_length / PART_SIZE) + 1) as usize);

        let upload_id = self
            .runtime
            .block_on(
                self.s3_client
                    .create_multipart_upload(CreateMultipartUploadRequest {
                        bucket: self.bucket.clone(),
                        content_type: content_type.map(|s| s.to_string()),
                        key: key.clone(),
                        ..Default::default()
                    }),
            )?
            .upload_id
            .unwrap();

        let create_upload_part = |content: Vec<u8>, part_number: i64| -> UploadPartRequest {
            UploadPartRequest {
                upload_id: upload_id.clone(),
                part_number,
                bucket: self.bucket.clone(),
                key: key.clone(),
                body: Some(content.into()),
                ..Default::default()
            }
        };

        loop {
            let read = match reader.read(&mut buffer) {
                Ok(read) => read,
                Err(e) => {
                    self.abort_multipart(&key, &upload_id);
                    return Err(e.into());
                }
            };

            if read == 0 {
                break;
            }

            debug!("Upload part {} for {}", i, read);

            let e_tag = match self.runtime.block_on(
                self.s3_client
                    .upload_part(create_upload_part(buffer[..read].to_vec(), i)),
            ) {
                Ok(result) => result.e_tag,
                Err(e) => {
                    self.abort_multipart(&key, &upload_id);
                    return Err(e.into());
                }
            };

            parts.push(CompletedPart {
                e_tag,
                part_number: Some(i),
            });

            i += 1;
        }

        debug!("Finish multipart upload for {}", key);

        self.runtime
            .block_on(
                self.s3_client
                    .complete_multipart_upload(CompleteMultipartUploadRequest {
                        bucket: self.bucket.clone(),
                        key: key.clone(),
                        multipart_upload: Some(CompletedMultipartUpload { parts: Some(parts) }),
                        upload_id: upload_id.clone(),
                        ..Default::default()
                    }),
            )?;

        Ok(())
    }

    fn abort_multipart(&self, key: &str, upload_id: &str) {
        info!("Aborting multipart upload to {}", key);
        if let Err(e) = self.runtime.block_on(self.s3_client.abort_multipart_upload(
            AbortMultipartUploadRequest {
                bucket: self.bucket.clone(),
                key: key.to_string(),
                upload_id: upload_id.to_string(),
                ..Default::default()
            },
        )) {
            error!("Failed to abort multipart upload to {}: {}", key, e);
        };
    }
}

impl ListResult {
    fn is_empty(&self) -> bool {
        self.objects.is_empty() && self.directories.is_empty()
    }
}

impl<'a> InventoryIter<'a> {
    /// Creates a new iterator that only returns objects that match the given object ID.
    fn new_id_matching(store: &'a S3OcflStore, object_id: &str, closed: Arc<AtomicBool>) -> Self {
        let o = object_id.to_string();
        InventoryIter::new(store, Some(Box::new(move |id| id == o)), closed)
    }

    /// Creates a new iterator that only returns objects with IDs that match the specified glob
    /// pattern.
    fn new_glob_matching(
        store: &'a S3OcflStore,
        glob: &str,
        closed: Arc<AtomicBool>,
    ) -> Result<Self> {
        let matcher = GlobBuilder::new(glob)
            .backslash_escape(true)
            .build()?
            .compile_matcher();
        Ok(InventoryIter::new(
            store,
            Some(Box::new(move |id| matcher.is_match(id))),
            closed,
        ))
    }

    /// Creates a new iterator that returns all objects if no `id_matcher` is provided, or only
    /// the objects the `id_matcher` returns `true` for if one is provided.
    fn new(
        store: &'a S3OcflStore,
        id_matcher: Option<Box<dyn Fn(&str) -> bool>>,
        closed: Arc<AtomicBool>,
    ) -> Self {
        Self {
            store,
            dir_iters: Vec::new(),
            current: RefCell::new(Some(vec!["".to_string()].into_iter())),
            id_matcher,
            closed,
        }
    }

    fn create_if_matches(&self, object_root: &str) -> Option<Result<Inventory>> {
        match self.store.parse_inventory(object_root) {
            Ok(Some(inventory)) => {
                if let Some(id_matcher) = &self.id_matcher {
                    if id_matcher(&inventory.id) {
                        Some(Ok(inventory))
                    } else {
                        None
                    }
                } else {
                    Some(Ok(inventory))
                }
            }
            Ok(None) => Some(Err(RocflError::NotFound(format!(
                "Expected object to exist at {}, but none found.",
                object_root
            )))),
            Err(e) => Some(Err(e)),
        }
    }
}

impl<'a> Iterator for InventoryIter<'a> {
    type Item = Result<Inventory>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.closed.load(Ordering::Acquire) {
                info!("Terminating object search");
                return None;
            }

            if self.current.borrow().is_none() && self.dir_iters.is_empty() {
                return None;
            } else if self.current.borrow().is_none() {
                self.current.replace(self.dir_iters.pop());
            }

            let entry = self.current.borrow_mut().as_mut().unwrap().next();

            match entry {
                None => {
                    self.current.replace(None);
                }
                Some(entry) => {
                    if entry.ends_with(&*EXTENSIONS_DIR_SUFFIX) {
                        continue;
                    }

                    let listing = match self.store.list_dir(&entry) {
                        Ok(listing) => listing,
                        Err(e) => return Some(Err(e)),
                    };

                    if is_object_dir(&listing.objects) {
                        match self.create_if_matches(&entry) {
                            Some(Ok(inventory)) => return Some(Ok(inventory)),
                            Some(Err(e)) => return Some(Err(e)),
                            _ => (),
                        }
                    } else {
                        self.dir_iters.push(self.current.replace(None).unwrap());
                        self.current.replace(Some(listing.directories.into_iter()));
                    }
                }
            }
        }
    }
}

impl S3Storage {
    fn new(s3_client: Arc<S3Client>) -> Self {
        Self { s3_client }
    }
}

impl Storage for S3Storage {
    /// Reads the file at the specified path and writes its contents to the provided sink.
    fn read<W: Write>(&self, path: &str, sink: &mut W) -> Result<()> {
        self.s3_client.stream_object(path, sink)
    }

    /// Lists the contents of the specified directory. If `recursive` is `true`, then all leaf-nodes
    /// are returned. If the directory does not exist, or is empty, then an empty vector is returned.
    /// The returned paths are all relative the directory that was listed.
    fn list(&self, path: &str, recursive: bool) -> Result<Vec<Listing>> {
        let prefix_len = if path.is_empty() || path.ends_with('/') {
            path.len()
        } else {
            path.len() + 1
        };

        if recursive {
            let key_parts = self.s3_client.list_objects(path)?;
            Ok(key_parts
                .iter()
                .map(|entry| Listing::file_owned(entry[prefix_len..].to_string()))
                .collect::<Vec<Listing>>())
        } else {
            let s3_result = self.s3_client.list_dir(path)?;
            let mut result =
                Vec::with_capacity(s3_result.directories.len() + s3_result.objects.len());

            s3_result
                .objects
                .iter()
                .map(|entry| Listing::file_owned(entry[prefix_len..].to_string()))
                .for_each(|entry| result.push(entry));
            s3_result
                .directories
                .iter()
                .map(|entry| Listing::dir_owned(entry[prefix_len..].to_string()))
                .for_each(|entry| result.push(entry));

            Ok(result)
        }
    }
}

fn check_extensions(s3_client: &S3Client) {
    match s3_client.list_dir(EXTENSIONS_DIR) {
        Ok(result) => {
            for entry in result.directories {
                let ext_name = &entry[EXTENSIONS_DIR.len() + 1..];
                if !SUPPORTED_EXTENSIONS.contains(ext_name) {
                    warn!(
                        "Storage root extension {} is not supported at this time",
                        ext_name
                    );
                }
            }
        }
        Err(e) => error!("Failed to list storage root extensions: {}", e),
    }
}

fn create_rusoto_client(region: Region, profile: Option<&str>) -> RusotoS3Client {
    match profile {
        Some(profile) => {
            // Client setup code copied from Rusoto -- they don't make it easy to set the profile
            let credentials_provider =
                AutoRefreshingProvider::new(ChainProvider::with_profile_provider(
                    ProfileProvider::with_default_credentials(profile)
                        .expect("failed to create profile provider"),
                ))
                .expect("failed to create credentials provider");
            let dispatcher = HttpClient::new().expect("failed to create request dispatcher");
            let client = Client::new_with(credentials_provider, dispatcher);
            RusotoS3Client::new_with_client(client, region)
        }
        None => RusotoS3Client::new(region),
    }
}

fn init_new_repo(s3_client: &S3Client, layout: Option<&StorageLayout>) -> Result<()> {
    if !s3_client.list_dir("")?.is_empty() {
        return Err(RocflError::IllegalState(
            "Cannot create new repository. Storage root must be empty".to_string(),
        ));
    }

    info!(
        "Initializing OCFL storage root in bucket {} under prefix {}",
        s3_client.bucket, s3_client.prefix
    );

    s3_client.put_object_bytes(
        REPO_NAMASTE_FILE,
        Bytes::from(ROOT_NAMASTE_CONTENT.as_bytes()),
        Some(TYPE_PLAIN),
    )?;

    s3_client.put_object_bytes(
        OCFL_SPEC_FILE,
        Bytes::from(specs::OCFL_1_0_SPEC.as_bytes()),
        Some(TYPE_PLAIN),
    )?;

    if let Some(layout) = layout {
        write_layout_config(s3_client, layout)?;
    }

    Ok(())
}

fn write_layout_config(s3_client: &S3Client, layout: &StorageLayout) -> Result<()> {
    let extension_name = layout.extension_name().to_string();

    let ocfl_layout = OcflLayout {
        extension: layout.extension_name(),
        description: format!("See specification document {}.md", extension_name),
    };

    let mut ocfl_layout_bytes = Vec::new();

    serde_json::to_writer_pretty(&mut ocfl_layout_bytes, &ocfl_layout)?;

    s3_client.put_object_bytes(
        OCFL_LAYOUT_FILE,
        Bytes::from(ocfl_layout_bytes),
        Some(TYPE_JSON),
    )?;

    s3_client.put_object_bytes(
        &format!(
            "{}/{}/{}",
            EXTENSIONS_DIR, extension_name, EXTENSIONS_CONFIG_FILE
        ),
        Bytes::from(layout.serialize()?),
        Some(TYPE_JSON),
    )?;

    let extension_spec = match layout.extension_name() {
        LayoutExtensionName::FlatDirectLayout => specs::EXT_0002_SPEC,
        LayoutExtensionName::HashedNTupleObjectIdLayout => specs::EXT_0003_SPEC,
        LayoutExtensionName::HashedNTupleLayout => specs::EXT_0004_SPEC,
        LayoutExtensionName::FlatOmitPrefixLayout => specs::EXT_0006_SPEC,
    };

    s3_client.put_object_bytes(
        &format!("{}.md", extension_name),
        Bytes::from(extension_spec),
        Some(TYPE_MARKDOWN),
    )?;

    Ok(())
}

/// Reads `ocfl_layout.json` and attempts to load the specified storage layout extension
fn load_storage_layout(s3_client: &S3Client) -> Option<StorageLayout> {
    match s3_client.get_object(OCFL_LAYOUT_FILE) {
        Ok(Some(layout)) => match serde_json::from_slice::<OcflLayout>(layout.as_slice()) {
            Ok(layout) => load_layout_extension(layout, s3_client),
            Err(e) => {
                error!("Failed to load OCFL layout: {:#}", e);
                None
            }
        },
        Ok(None) => {
            info!(
                "The OCFL repository at {}/{} does not contain an ocfl_layout.json file.",
                s3_client.bucket, s3_client.prefix
            );
            None
        }
        Err(e) => {
            error!("Failed to load OCFL layout: {:#}", e);
            None
        }
    }
}

/// Attempts to read a storage layout extension config and return configured `StorageLayout`
fn load_layout_extension(layout: OcflLayout, s3_client: &S3Client) -> Option<StorageLayout> {
    let config_path = join(
        &join(EXTENSIONS_DIR, &layout.extension.to_string()),
        EXTENSIONS_CONFIG_FILE,
    );

    match s3_client.get_object(&config_path) {
        Ok(config) => match StorageLayout::new(layout.extension, config.as_deref()) {
            Ok(storage_layout) => {
                info!(
                    "Loaded storage layout extension {}",
                    layout.extension.to_string()
                );
                Some(storage_layout)
            }
            Err(e) => {
                error!(
                    "Failed to load storage layout extension {}: {:#}",
                    layout.extension.to_string(),
                    e
                );
                None
            }
        },
        Err(e) => {
            error!(
                "Failed to load storage layout extension {}: {:#}",
                layout.extension.to_string(),
                e
            );
            None
        }
    }
}

fn is_object_dir(objects: &[String]) -> bool {
    for object in objects {
        if object.ends_with(OBJECT_NAMASTE_FILE) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::{is_object_dir, join, join_with_trailing_slash};

    #[test]
    fn join_path_when_both_empty() {
        assert_eq!(join("", ""), "");
        assert_eq!(join_with_trailing_slash("", ""), "");
    }

    #[test]
    fn join_path_when_first_empty() {
        assert_eq!(join("", "foo"), "foo");
        assert_eq!(join_with_trailing_slash("", "foo"), "foo/");
    }

    #[test]
    fn join_path_when_second_empty() {
        assert_eq!(join("foo", ""), "foo");
        assert_eq!(join_with_trailing_slash("foo", ""), "foo/");
    }

    #[test]
    fn join_path_when_first_is_only_slash() {
        assert_eq!(join("/", "foo"), "/foo");
        assert_eq!(join_with_trailing_slash("/", "foo"), "/foo/");
    }

    #[test]
    fn join_path_when_first_has_slash() {
        assert_eq!(join("foo/", "bar"), "foo/bar");
        assert_eq!(join_with_trailing_slash("foo/", "bar"), "foo/bar/");
    }

    #[test]
    fn join_path_when_both_have_slashes() {
        assert_eq!(join("/foo/", "/bar/"), "/foo/bar/");
        assert_eq!(join_with_trailing_slash("/foo/", "/bar/"), "/foo/bar/");
    }

    #[test]
    fn join_path_when_both_no_slashes() {
        assert_eq!(join("foo", "bar"), "foo/bar");
        assert_eq!(join_with_trailing_slash("foo", "bar"), "foo/bar/");
    }

    #[test]
    fn is_root_when_has_object_marker_key() {
        let objects = vec![
            "foo/bar.txt".to_string(),
            "foo/0=ocfl_object_1.0".to_string(),
        ];
        assert!(is_object_dir(&objects));
    }

    #[test]
    fn is_root_when_not_has_object_marker_key() {
        let objects = vec!["foo/bar.txt".to_string()];
        assert!(!is_object_dir(&objects));
    }
}

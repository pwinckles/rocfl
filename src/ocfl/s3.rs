//! S3 OCFL storage implementation.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::vec::IntoIter;

use globset::GlobBuilder;
use log::{error, info};
use once_cell::sync::Lazy;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{
    GetObjectError, GetObjectRequest, ListObjectsV2Output, ListObjectsV2Request,
    S3Client as RusotoS3Client, S3,
};
use tokio::io::AsyncReadExt;
use tokio::runtime::Runtime;

use super::OcflStore;
use crate::ocfl::consts::*;
use crate::ocfl::error::{not_found, Result, RocflError};
use crate::ocfl::inventory::Inventory;
use crate::ocfl::layout::StorageLayout;
use crate::ocfl::{InventoryPath, OcflLayout, VersionNum};

static EXTENSIONS_DIR_SUFFIX: Lazy<String> = Lazy::new(|| format!("/{}", EXTENSIONS_DIR));

pub struct S3OcflStore {
    s3_client: S3Client,
    /// Maps object IDs to paths within the storage root
    storage_layout: Option<StorageLayout>,
    /// Caches object ID to path mappings
    id_path_cache: RefCell<HashMap<String, String>>,
    prefix: Option<String>,
}

impl S3OcflStore {
    /// Creates a new S3OcflStore
    pub fn new(region: Region, bucket: &str, prefix: Option<&str>) -> Result<Self> {
        let s3_client = S3Client::new(region, bucket, prefix)?;
        let storage_layout = load_storage_layout(&s3_client);

        Ok(Self {
            s3_client,
            storage_layout,
            id_path_cache: RefCell::new(HashMap::new()),
            prefix: prefix.map(|p| p.to_string()),
        })
    }

    fn get_inventory_inner(&self, object_id: &str) -> Result<Inventory> {
        if let Some(storage_layout) = &self.storage_layout {
            let object_root = storage_layout.map_object_id(object_id);
            self.parse_inventory_required(object_id, &object_root)
        } else {
            info!(
                "Storage layout not configured, scanning repository to locate object {}",
                &object_id
            );

            let mut iter = InventoryIter::new_id_matching(&self, &object_id);

            match iter.next() {
                Some(inventory) => Ok(inventory),
                None => Err(not_found(&object_id, None)),
            }
        }
    }

    fn parse_inventory_optional(&self, object_root: &str) -> Option<Inventory> {
        match self.parse_inventory(object_root) {
            Ok(inventory) => inventory,
            Err(e) => {
                error!("{:#}", e);
                None
            }
        }
    }

    fn parse_inventory_required(&self, object_id: &str, object_root: &str) -> Result<Inventory> {
        match self.parse_inventory(object_root)? {
            Some(inventory) => Ok(inventory),
            None => Err(not_found(object_id, None)),
        }
    }

    /// Parses the HEAD inventory of the OCFL object that's rooted in the specified directory.
    /// This is normally the `inventory.json` file in the object's root, but it could also be
    /// the inventory file in an extension directory, such as the mutable HEAD extension.
    fn parse_inventory(&self, object_root: &str) -> Result<Option<Inventory>> {
        let bytes = self.get_inventory_bytes(&object_root)?;
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
            inventory.object_root =
                strip_leading_slash(strip_trailing_slash(object_root).as_ref()).into();

            inventory.storage_path = if let Some(prefix) = &self.prefix {
                join(prefix, &inventory.object_root)
            } else {
                inventory.object_root.clone()
            };
            inventory.mutable_head = mutable_head;

            Ok(Some(inventory))
        } else {
            Ok(None)
        }
    }

    fn parse_inventory_bytes(&self, bytes: &[u8]) -> Result<Inventory> {
        let inventory: Inventory = serde_json::from_slice(bytes)?;
        inventory.validate()?;
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

    /// Pass through to S3 to list the contents of a path in S3
    fn list_dir(&self, path: &str) -> Result<ListResult> {
        self.s3_client.list_dir(path)
    }
}

impl OcflStore for S3OcflStore {
    /// Returns the most recent inventory version for the specified object, or an a
    /// `RocflError::NotFound` if it does not exist.
    fn get_inventory(&self, object_id: &str) -> Result<Inventory> {
        let object_root = match self.id_path_cache.borrow().get(object_id) {
            Some(object_root) => Some(object_root.clone()),
            None => None,
        };

        match object_root {
            Some(object_root) => self.parse_inventory_required(object_id, &object_root),
            None => {
                let inventory = self.get_inventory_inner(&object_id)?;
                self.id_path_cache
                    .borrow_mut()
                    .insert(object_id.to_string(), inventory.object_root.clone());
                Ok(inventory)
            }
        }
    }

    /// Returns an iterator that iterates over every object in an OCFL repository, returning
    /// the most recent inventory of each. Optionally, a glob pattern may be provided that filters
    /// the objects that are returned by OCFL ID.
    fn iter_inventories<'a>(
        &'a self,
        filter_glob: Option<&str>,
    ) -> Result<Box<dyn Iterator<Item = Inventory> + 'a>> {
        Ok(Box::new(match filter_glob {
            Some(glob) => InventoryIter::new_glob_matching(&self, glob)?,
            None => InventoryIter::new(&self, None),
        }))
    }

    /// Writes the specified file to the sink.
    ///
    /// If the file cannot be found, then a `RocflError::NotFound` error is returned.
    fn get_object_file(
        &self,
        object_id: &str,
        path: &InventoryPath,
        version_num: Option<VersionNum>,
        sink: &mut dyn Write,
    ) -> Result<()> {
        let inventory = self.get_inventory(object_id)?;

        let content_path = inventory.content_path_for_logical_path(path, version_num)?;
        let storage_path = join(&inventory.object_root, content_path.as_ref().as_ref());

        self.s3_client.stream_object(&storage_path, sink)
    }

    fn write_new_object(&self, _inventory: &Inventory, _object_path: &Path) -> Result<()> {
        // TODO s3
        unimplemented!()
    }

    fn write_new_version(&self, _inventory: &Inventory, _version_path: &Path) -> Result<()> {
        // TODO s3
        unimplemented!()
    }

    fn purge_object(&self, _object_id: &str) -> Result<()> {
        // TODO s3
        unimplemented!()
    }
}

struct S3Client {
    s3_client: RusotoS3Client,
    bucket: String,
    prefix: String,
    runtime: RefCell<Runtime>,
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
}

impl S3Client {
    fn new(region: Region, bucket: &str, prefix: Option<&str>) -> Result<Self> {
        Ok(S3Client {
            s3_client: RusotoS3Client::new(region),
            bucket: bucket.to_owned(),
            prefix: prefix.unwrap_or_default().to_owned(),
            runtime: RefCell::new(Runtime::new()?),
        })
    }

    fn list_dir(&self, path: &str) -> Result<ListResult> {
        let prefix = join_with_trailing_slash(&self.prefix, &path);

        let mut objects = Vec::new();
        let mut directories = Vec::new();
        let mut continuation = None;

        loop {
            let result: ListObjectsV2Output =
                self.runtime
                    .borrow_mut()
                    .block_on(self.s3_client.list_objects_v2(ListObjectsV2Request {
                        bucket: self.bucket.clone(),
                        prefix: Some(prefix.clone()),
                        delimiter: Some("/".to_owned()),
                        continuation_token: continuation.clone(),
                        ..Default::default()
                    }))?;

            if let Some(contents) = &result.contents {
                for object in contents {
                    objects.push(object.key.as_ref().unwrap()[self.prefix.len()..].to_owned());
                }
            }

            if let Some(prefixes) = &result.common_prefixes {
                for prefix in prefixes {
                    directories
                        .push(prefix.prefix.as_ref().unwrap()[self.prefix.len()..].to_owned());
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
        let key = join(&self.prefix, &path);

        info!("Getting object from S3: {}", &key);

        let result = self
            .runtime
            .borrow_mut()
            .block_on(self.s3_client.get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key,
                ..Default::default()
            }));

        match result {
            Ok(result) => self.runtime.borrow_mut().block_on(async move {
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
        let key = join(&self.prefix, &path);

        info!("Streaming object from S3: {}", &key);

        let result = self
            .runtime
            .borrow_mut()
            .block_on(self.s3_client.get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key,
                ..Default::default()
            }));

        match result {
            Ok(result) => self.runtime.borrow_mut().block_on(async move {
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
}

impl<'a> InventoryIter<'a> {
    /// Creates a new iterator that only returns objects that match the given object ID.
    fn new_id_matching(store: &'a S3OcflStore, object_id: &str) -> Self {
        let o = object_id.to_string();
        InventoryIter::new(store, Some(Box::new(move |id| id == o)))
    }

    /// Creates a new iterator that only returns objects with IDs that match the specified glob
    /// pattern.
    fn new_glob_matching(store: &'a S3OcflStore, glob: &str) -> Result<Self> {
        let matcher = GlobBuilder::new(glob)
            .backslash_escape(true)
            .build()?
            .compile_matcher();
        Ok(InventoryIter::new(
            store,
            Some(Box::new(move |id| matcher.is_match(id))),
        ))
    }

    /// Creates a new iterator that returns all objects if no `id_matcher` is provided, or only
    /// the objects the `id_matcher` returns `true` for if one is provided.
    fn new(store: &'a S3OcflStore, id_matcher: Option<Box<dyn Fn(&str) -> bool>>) -> Self {
        Self {
            store,
            dir_iters: Vec::new(),
            current: RefCell::new(Some(vec!["".to_string()].into_iter())),
            id_matcher,
        }
    }

    fn create_if_matches(&self, object_root: &str) -> Option<Inventory> {
        match self.store.parse_inventory_optional(object_root) {
            Some(inventory) => {
                if let Some(id_matcher) = &self.id_matcher {
                    if id_matcher(&inventory.id) {
                        Some(inventory)
                    } else {
                        None
                    }
                } else {
                    Some(inventory)
                }
            }
            None => {
                error!(
                    "Expected object to exist at {}, but none found.",
                    object_root
                );
                None
            }
        }
    }
}

impl<'a> Iterator for InventoryIter<'a> {
    type Item = Inventory;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
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

                    match self.store.list_dir(&entry) {
                        Ok(listing) => {
                            if is_object_dir(&listing.objects) {
                                if let Some(inventory) = self.create_if_matches(&entry) {
                                    return Some(inventory);
                                }
                            } else {
                                self.dir_iters.push(self.current.replace(None).unwrap());
                                self.current.replace(Some(listing.directories.into_iter()));
                            }
                        }
                        Err(e) => error!("{:#}", e),
                    }
                }
            }
        }
    }
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

fn join(part1: &str, part2: &str) -> String {
    let mut joined = match part1.ends_with('/') {
        true => part1[..part1.len() - 1].to_string(),
        false => part1.to_string(),
    };

    if !part2.is_empty() {
        if (!joined.is_empty() || part1 == "/") && !part2.starts_with('/') {
            joined.push('/');
        }
        joined.push_str(part2);
    }

    joined
}

fn join_with_trailing_slash(part1: &str, part2: &str) -> String {
    let mut joined = join(part1, part2);

    if !joined.is_empty() && !joined.ends_with('/') {
        joined.push('/');
    }

    joined
}

fn strip_trailing_slash(path: &str) -> Cow<str> {
    if let Some(stripped) = path.strip_suffix('/') {
        Cow::Owned(stripped.to_string())
    } else {
        path.into()
    }
}

fn strip_leading_slash(path: &str) -> Cow<str> {
    if path.starts_with('/') {
        Cow::Owned(path[1..path.len()].to_string())
    } else {
        path.into()
    }
}

#[cfg(test)]
mod tests {
    use super::{is_object_dir, join, join_with_trailing_slash, strip_trailing_slash};

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
    fn remove_trailing_slash() {
        assert_eq!(strip_trailing_slash("/"), "");
        assert_eq!(strip_trailing_slash("/foo/bar/"), "/foo/bar");
        assert_eq!(strip_trailing_slash("/foo/bar"), "/foo/bar");
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

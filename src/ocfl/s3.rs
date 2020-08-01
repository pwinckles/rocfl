//! S3 OCFL storage implementation.

use std::cell::RefCell;
use std::ops::Deref;
use std::vec::IntoIter;

use anyhow::{anyhow, Context, Result};
use globset::GlobBuilder;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{GetObjectError, GetObjectRequest, ListObjectsV2Output, ListObjectsV2Request, S3, S3Client as RusotoS3Client};
use tokio::io::AsyncReadExt;
use tokio::runtime::Runtime;

use super::{Inventory, MUTABLE_HEAD_INVENTORY_FILE, not_found, OBJECT_MARKER, OcflStore, ROOT_INVENTORY_FILE};

// ================================================== //
//             public structs+enums+traits            //
// ================================================== //

pub struct S3OcflStore {
    s3_client: S3Client,
}

// ================================================== //
//                   public impls+fns                 //
// ================================================== //

impl S3OcflStore {
    /// Creates a new S3OcflStore
    pub fn new(region: Region, bucket: &str, prefix: Option<&str>) -> Result<Self> {
        Ok(Self {
            s3_client: S3Client::new(region, bucket, prefix)?
        })
    }
}

impl OcflStore for S3OcflStore {
    /// Returns the most recent inventory version for the specified object, or an a
    /// `RocflError::NotFound` if it does not exist.
    fn get_inventory(&self, object_id: &str) -> Result<Inventory> {
        let mut iter = InventoryIter::new_id_matching(&self.s3_client, &object_id);

        loop {
            match iter.next() {
                Some(Ok(inventory)) => return Ok(inventory),
                Some(Err(_)) => (),  // Errors are ignored because we don't know what object they're for
                None => return Err(not_found(&object_id, None).into())
            }
        }
    }

    /// Returns an iterator that iterates over every object in an OCFL repository, returning
    /// the most recent inventory of each. Optionally, a glob pattern may be provided that filters
    /// the objects that are returned by OCFL ID.
    fn iter_inventories<'a>(&'a self, filter_glob: Option<&str>) -> Result<Box<dyn Iterator<Item=Result<Inventory>> + 'a>> {
        Ok(Box::new(match filter_glob {
            Some(glob) => InventoryIter::new_glob_matching(&self.s3_client, glob)?,
            None => InventoryIter::new(&self.s3_client, None)
        }))
    }
}

// ================================================== //
//            private structs+enums+traits            //
// ================================================== //

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
    s3_client: &'a S3Client,
    dir_iters: Vec<IntoIter<String>>,
    current: RefCell<Option<IntoIter<String>>>,
    id_matcher: Option<Box<dyn Fn(&str) -> bool>>,
}

// ================================================== //
//                private impls+fns                   //
// ================================================== //

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
            let result: ListObjectsV2Output = self.runtime.borrow_mut().block_on(
                self.s3_client.list_objects_v2(ListObjectsV2Request {
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
                    directories.push(prefix.prefix.as_ref().unwrap()[self.prefix.len()..].to_owned());
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
            directories
        })
    }

    fn get_object(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let key = join(&self.prefix, &path);

        let result = self.runtime.borrow_mut().block_on(self.s3_client.get_object(GetObjectRequest {
            bucket: self.bucket.clone(),
            key,
            ..Default::default()
        }));

        match result {
            Ok(result) => {
                self.runtime.borrow_mut().block_on(async move {
                    let mut buffer = Vec::new();
                    result.body.unwrap().into_async_read().read_to_end(&mut buffer).await?;
                    Ok(Some(buffer))
                })
            }
            Err(RusotoError::Service(GetObjectError::NoSuchKey(_e))) => Ok(None),
            Err(e) => Err(e.into())
        }
    }
}

impl<'a> InventoryIter<'a> {
    /// Creates a new iterator that only returns objects that match the given object ID.
    fn new_id_matching(s3_client: &'a S3Client, object_id: &str) -> Self {
        let o = object_id.to_string();
        InventoryIter::new(s3_client, Some(Box::new(move |id| id == o)))
    }

    /// Creates a new iterator that only returns objects with IDs that match the specified glob
    /// pattern.
    fn new_glob_matching(s3_client: &'a S3Client, glob: &str) -> Result<Self> {
        let matcher = GlobBuilder::new(glob).backslash_escape(true).build()?.compile_matcher();
        Ok(InventoryIter::new(s3_client, Some(Box::new(move |id| matcher.is_match(id)))))
    }

    /// Creates a new iterator that returns all objects if no `id_matcher` is provided, or only
    /// the objects the `id_matcher` returns `true` for if one is provided.
    fn new(s3_client: &'a S3Client, id_matcher: Option<Box<dyn Fn(&str) -> bool>>) -> Self {
        Self {
            s3_client,
            dir_iters: Vec::new(),
            current: RefCell::new(Some(vec!["".to_owned()].into_iter())),
            id_matcher,
        }
    }

    fn create_if_matches(&self, object_root: &str) -> Result<Option<Inventory>> {
        let inventory = self.parse_inventory(object_root)?;

        if self.id_matcher.is_some() {
            if self.id_matcher.as_ref().unwrap().deref()(&inventory.id) {
                return Ok(Some(inventory));
            }
        } else {
            return Ok(Some(inventory));
        }

        Ok(None)
    }

    /// Parses the HEAD inventory of the OCFL object that's rooted in the specified directory.
    /// This is normally the `inventory.json` file in the object's root, but it could also be
    /// the inventory file in an extension directory, such as the mutable HEAD extension.
    fn parse_inventory(&self, object_root: &str) -> Result<Inventory> {
        let inventory_bytes = self.get_inventory_bytes(&object_root)?;
        // TODO should validate hash
        let mut inventory = self.parse_inventory_bytes(&inventory_bytes)
            .with_context(|| format!("Failed to parse inventory in object at {}", object_root))?;
        inventory.object_root = strip_trailing_slash(object_root);
        Ok(inventory)
    }

    fn parse_inventory_bytes(&self, bytes: &[u8]) -> Result<Inventory> {
        let inventory: Inventory = serde_json::from_slice(bytes)?;
        inventory.validate()?;
        Ok(inventory)
    }

    fn get_inventory_bytes(&self, object_root: &str) -> Result<Vec<u8>> {
        let mutable_head_inv = join(object_root, MUTABLE_HEAD_INVENTORY_FILE);

        match self.s3_client.get_object(&mutable_head_inv)? {
            Some(bytes) => Ok(bytes),
            None => {
                let inv_path = join(object_root, ROOT_INVENTORY_FILE);
                match self.s3_client.get_object(&inv_path)? {
                    Some(bytes) => Ok(bytes),
                    None => Err(anyhow!("Expected inventory at {} not found", inv_path))
                }
            }
        }
    }
}

impl<'a> Iterator for InventoryIter<'a> {
    type Item = Result<Inventory>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.borrow().is_none() && self.dir_iters.is_empty() {
                return None
            } else if self.current.borrow().is_none() {
                self.current.replace(self.dir_iters.pop());
            }

            let entry = self.current.borrow_mut().as_mut().unwrap().next();

            match entry {
                None => {
                    self.current.replace(None);
                }
                Some(entry) => {
                    match self.s3_client.list_dir(&entry) {
                        Ok(listing) => {
                            if is_object_dir(&listing.objects) {
                                match self.create_if_matches(&entry) {
                                    Ok(Some(inventory)) => return Some(Ok(inventory)),
                                    Ok(None) => (),
                                    Err(e) => return Some(Err(e))
                                }
                            } else {
                                self.dir_iters.push(self.current.replace(None).unwrap());
                                self.current.replace(Some(listing.directories.into_iter()));
                            }
                        }
                        Err(e) => return Some(Err(e))
                    }
                }
            }
        }
    }
}

fn is_object_dir(objects: &[String]) -> bool {
    for object in objects {
        if object.ends_with(OBJECT_MARKER) {
            return true;
        }
    }
    false
}

fn join(part1: &str, part2: &str) -> String {
    let mut joined = match part1.ends_with('/') {
        true => {
            part1[..part1.len() - 1].to_string()
        },
        false => part1.to_string()
    };

    if !part2.is_empty() {
        if (!joined.is_empty() || part1 == "/")
            && !part2.starts_with('/') {
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

fn strip_trailing_slash(path: &str) -> String {
    if path.ends_with('/') {
        path[..path.len() - 1].to_owned()
    } else {
        path.to_owned()
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
        let objects = vec!["foo/bar.txt".to_string(), "foo/0=ocfl_object_1.0".to_string()];
        assert!(is_object_dir(&objects));
    }

    #[test]
    fn is_root_when_not_has_object_marker_key() {
        let objects = vec!["foo/bar.txt".to_string()];
        assert!(!is_object_dir(&objects));
    }
}
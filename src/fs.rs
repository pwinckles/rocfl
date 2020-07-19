//! Local filesystem OCFL implementation.

use std::cell::RefCell;
use std::fs::{self, File, ReadDir};
use std::io::Read;
use std::ops::Deref;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use globset::GlobBuilder;
use grep::matcher::{Captures, Matcher};
use grep::searcher::Searcher;
use grep::searcher::sinks::UTF8;

use crate::{Diff, Inventory, invert_path_map, MUTABLE_HEAD_INVENTORY_FILE, not_found, OBJECT_ID_MATCHER, OBJECT_MARKER, ObjectVersion, ObjectVersionDetails, OcflRepo, ROOT_INVENTORY_FILE, VersionDetails, VersionNum};

/// Local filesystem OCFL repository
pub struct FsOcflRepo {
    /// The path to the OCFL storage root
    pub storage_root: PathBuf
}

impl FsOcflRepo {
    /// Creates a new FsOcflRepo
    pub fn new<P: AsRef<Path>>(storage_root: P) -> Result<FsOcflRepo> {
        let storage_root = storage_root.as_ref().to_path_buf();

        if !storage_root.exists() {
            return Err(anyhow!("Storage root {} does not exist", storage_root.to_string_lossy()));
        } else if !storage_root.is_dir() {
            return Err(anyhow!("Storage root {} is not a directory", storage_root.to_string_lossy()))
        }

        // TODO verify is an OCFL repository
        // TODO load storage layout

        Ok(FsOcflRepo {
            storage_root
        })
    }

    // TODO add this method to OcflRepo and then use it to create blanket implementations
    fn get_inventory(&self, object_id: &str) -> Result<Inventory> {
        let mut iter = InventoryIter::new_id_matching(&self.storage_root, object_id.clone())?;

        loop {
            match iter.next() {
                Some(Ok(inventory)) => {
                    return Ok(inventory)
                },
                Some(Err(_)) => (),  // Errors are ignored because we don't know what object they're for
                None => return Err(not_found(&object_id, None).into())
            }
        }
    }
}

impl OcflRepo for FsOcflRepo {
    /// Returns an iterator that iterate through all of the objects in an OCFL repository.
    /// Objects are lazy-loaded. An optional glob pattern may be provided to filter the objects
    /// that are returned.
    ///
    /// The iterator return an error if it encounters a problem accessing an object. This does
    /// terminate the iterator; there are still more objects until it returns `None`.
    fn list_objects(&self, filter_glob: Option<&str>) -> Result<Box<dyn Iterator<Item=Result<ObjectVersionDetails>>>> {
        let inv_iter = match filter_glob {
            Some(glob) => InventoryIter::new_glob_matching(&self.storage_root, glob)?,
            None => InventoryIter::new(&self.storage_root, None)?
        };

        Ok(Box::new(InventoryAdapterIter::new(inv_iter, Box::new(|inventory| {
            ObjectVersionDetails::from_inventory(inventory, None)
        }))))
    }

    /// Returns a view of a version of an object. If a [VersionNum](rocfl::VersionNum) is not specified,
    /// then the head version of the object is returned.
    ///
    /// If the object or version of the object cannot be found, then a [NotFound](rocfl::RocflError::NotFound)
    /// error is returned.
    fn get_object(&self, object_id: &str, version_num: Option<&VersionNum>) -> Result<ObjectVersion> {
        let inventory = self.get_inventory(object_id)?;
        Ok(ObjectVersion::from_inventory(inventory, version_num)?)
    }

    /// Returns high-level details about an object version. This method is similar to
    /// [get_object](rocfl::OcflRepo::get_object) except that it does less processing and does not
    /// include the version's state.
    ///
    /// If the object or version of the object cannot be found, then a [NotFound](rocfl::RocflError::NotFound)
    /// error is returned.
    fn get_object_details(&self, object_id: &str, version_num: Option<&VersionNum>) -> Result<ObjectVersionDetails> {
        let inventory = self.get_inventory(object_id)?;
        Ok(ObjectVersionDetails::from_inventory(inventory, version_num)?)
    }

    /// Returns a vector containing the version metadata for ever version of an object. The vector
    /// is sorted in ascending order.
    ///
    /// If the object cannot be found, then a [NotFound](rocfl::RocflError::NotFound) error is returned.
    fn list_object_versions(&self, object_id: &str) -> Result<Vec<VersionDetails>> {
        let inventory = self.get_inventory(object_id)?;
        let mut versions = Vec::with_capacity(inventory.versions.len());

        for (id, version) in inventory.versions {
            versions.push(VersionDetails::from_version(id, version))
        }

        Ok(versions)
    }

    /// Returns a vector contain the version metadata for every version of an object that
    /// affected the specified file. The vector is sorted in ascending order.
    ///
    /// If the object or path cannot be found, then a [NotFound](rocfl::RocflError::NotFound) error is returned.
    fn list_file_versions(&self, object_id: &str, path: &str) -> Result<Vec<VersionDetails>> {
        let inventory = self.get_inventory(object_id)?;

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
                },
                None => {
                    if current_digest.is_some() {
                        current_digest = None;
                        versions.push(VersionDetails::from_version(id, version));
                    }
                }
            }
        }

        Ok(versions)
    }

    /// Returns the diff of two object versions. If only one version is specified, then the diff
    /// is between the specified version and the version before it.
    ///
    /// If the object cannot be found, then a [NotFound](rocfl::RocflError::NotFound) error is returned.
    fn diff(&self, object_id: &str, left_version: &VersionNum, right_version: Option<&VersionNum>) -> Result<Vec<Diff>> {
        if right_version.is_some() && left_version.eq(right_version.unwrap()) {
            return Ok(vec![])
        }

        let mut inventory = self.get_inventory(object_id)?;

        let left = inventory.remove_version(&left_version)?;

        let right = match right_version {
            Some(version) => Some(inventory.remove_version(version)?),
            None => {
                if left_version.number > 1 {
                    Some(inventory.remove_version(&left_version.previous().unwrap())?)
                } else {
                    None
                }
            }
        };

        let mut left_state = invert_path_map(left.state);

        let mut diffs = Vec::new();

        if right.is_none() {
            for (path, _digest) in left_state {
                diffs.push(Diff::added(path));
            }
        } else {
            let right_state = invert_path_map(right.unwrap().state);

            for (path, right_digest) in right_state {
                match left_state.remove(&path) {
                    None => diffs.push(Diff::added(path)),
                    Some(left_digest) => {
                        if right_digest.deref().ne(left_digest.deref()) {
                            diffs.push(Diff::modified(path))
                        }
                    }
                }
            }

            for (path, _digest) in left_state {
                diffs.push(Diff::deleted(path))
            }
        }

        Ok(diffs)
    }
}

/// An iterator that adapts the out of `InventoryIter`.
struct InventoryAdapterIter<T> {
    iter: InventoryIter,
    adapter: Box<dyn Fn(Inventory) -> Result<T>>
}

impl<T> InventoryAdapterIter<T> {
    /// Creates a new `InventoryAdapterIter` that applies the `adapter` closure to the output
    /// of every `next()` call.
    fn new(iter: InventoryIter, adapter: Box<dyn Fn(Inventory) -> Result<T>>) -> Self {
        Self {
            iter,
            adapter
        }
    }
}

impl<T> Iterator for InventoryAdapterIter<T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(inventory)) => {
                Some(self.adapter.deref()(inventory))
            }
        }
    }
}

/// Iterates over ever object in an OCFL repository by walking the file tree.
struct InventoryIter {
    dir_iters: Vec<ReadDir>,
    current: RefCell<Option<ReadDir>>,
    id_matcher: Option<Box<dyn Fn(&str) -> bool>>,
}

impl InventoryIter {
    /// Creates a new iterator that only returns objects that match the given object ID.
    fn new_id_matching<P: AsRef<Path>>(root: P, object_id: &str) -> Result<Self> {
        let o = object_id.to_string();
        InventoryIter::new(root, Some(Box::new(move |id| id == o)))
    }

    /// Creates a new iterator that only returns objects with IDs that match the specified glob
    /// pattern.
    fn new_glob_matching<P: AsRef<Path>>(root: P, glob: &str) -> Result<Self> {
        let matcher = GlobBuilder::new(glob).backslash_escape(true).build()?.compile_matcher();
        InventoryIter::new(root, Some(Box::new(move |id| matcher.is_match(id))))
    }

    /// Creates a new iterator that returns all objects if no `id_matcher` is provided, or only
    /// the objects the `id_matcher` returns `true` for if one is provided.
    fn new<P: AsRef<Path>>(root: P, id_matcher: Option<Box<dyn Fn(&str) -> bool>>) -> Result<Self> {
        Ok(InventoryIter {
            dir_iters: vec![fs::read_dir(&root)?],
            current: RefCell::new(None),
            id_matcher,
        })
    }

    fn create_if_matches<P: AsRef<Path>>(&self, object_root: P) -> Result<Option<Inventory>>{
        let inventory_path = object_root.as_ref().join(ROOT_INVENTORY_FILE);

        if self.id_matcher.is_some() {
            let object_id = self.extract_object_id(&inventory_path)?;
            if self.id_matcher.as_ref().unwrap().deref()(&object_id) {
                return self.create_object_version(&object_root);
            }
        } else {
            return self.create_object_version(&object_root);
        }

        Ok(None)
    }

    fn create_object_version<P: AsRef<Path>>(&self, path: P) -> Result<Option<Inventory>> {
        match parse_inventory(&path) {
            Ok(inventory) => Ok(Some(inventory)),
            Err(e) => Err(e)
        }
    }

    fn extract_object_id<P: AsRef<Path>>(&self, path: P) -> Result<String> {
        let mut matches: Vec<String> = vec![];
        Searcher::new().search_path(&*OBJECT_ID_MATCHER, &path, UTF8(|_, line| {
            let mut captures = OBJECT_ID_MATCHER.new_captures()?;
            OBJECT_ID_MATCHER.captures(line.as_bytes(), &mut captures)?;
            matches.push(line[captures.get(1).unwrap()].to_string());
            Ok(true)
        })).with_context(|| format!("Failed to locate object ID in inventory at {}",
                        path.as_ref().to_string_lossy().to_string()))?;

        match matches.get(0) {
            Some(id) => Ok(id.to_string()),
            None => Err(anyhow!("Failed to locate object ID in inventory at {}",
                        path.as_ref().to_string_lossy().to_string()))
        }
    }
}

impl Iterator for InventoryIter {
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
                },
                Some(Err(e)) => return Some(Err(e.into())),
                Some(Ok(entry)) => {
                    match entry.file_type() {
                        Err(e) => return Some(Err(e.into())),
                        Ok(ftype) if ftype.is_dir() => {
                            let path = entry.path();

                            match is_object_root(&path) {
                                Ok(is_root) if is_root => {
                                    match self.create_if_matches(&path) {
                                        Ok(Some(object)) => return Some(Ok(object)),
                                        Ok(None) => (),
                                        Err(e) => return Some(Err(e))
                                    }
                                },
                                Ok(is_root) if !is_root => {
                                    self.dir_iters.push(self.current.replace(None).unwrap());
                                    match std::fs::read_dir(&path) {
                                        Ok(next) => {
                                            self.current.replace(Some(next));
                                        },
                                        Err(e) => return Some(Err(e.into()))
                                    }
                                },
                                Err(e) => return Some(Err(e.into())),
                                _ => panic!("This code is unreachable")
                            }
                        },
                        _ => ()
                    }
                },
            }
        }
    }
}

/// Returns true if the path contains an OCFL object root marker file
fn is_object_root<P: AsRef<Path>>(path: P) -> Result<bool> {
    for entry in std::fs::read_dir(path)? {
        let entry_path = entry?.path();
        if entry_path.is_file()
            && entry_path.file_name().unwrap_or_default() == OBJECT_MARKER {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Parses the HEAD inventory of the OCFL object that's rooted in the specified directory.
/// This is normally the `inventory.json` file in the object's root, but it could also be
/// the inventory file in an extension directory, such as the mutable HEAD extension.
fn parse_inventory<P: AsRef<Path>>(object_root: P) -> Result<Inventory> {
    let inventory_path = resolve_inventory_path(&object_root);
    // TODO should validate hash
    let mut inventory = parse_inventory_file(&inventory_path)
        .with_context(|| format!("Failed to parse inventory at {}",
                             inventory_path.to_str().unwrap_or_default()))?;
    inventory.object_root = object_root.as_ref().to_string_lossy().to_string();
    Ok(inventory)
}

fn parse_inventory_file<P: AsRef<Path>>(inventory_file: P) -> Result<Inventory> {
    let mut bytes = Vec::new();
    File::open(&inventory_file)?.read_to_end(&mut bytes)?;
    let inventory: Inventory = serde_json::from_slice(&bytes)?;
    inventory.validate()?;
    Ok(inventory)
}

fn resolve_inventory_path<P: AsRef<Path>>(object_root: P) -> PathBuf {
    let mutable_head_inv = object_root.as_ref().join(MUTABLE_HEAD_INVENTORY_FILE);
    if mutable_head_inv.exists() {
        return mutable_head_inv;
    }
    object_root.as_ref().join(ROOT_INVENTORY_FILE)
}

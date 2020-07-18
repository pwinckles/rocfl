use std::cell::RefCell;
use std::io::{Read};
use std::fs::{File, ReadDir};
use std::path::{Path, PathBuf};
use grep::searcher::Searcher;
use anyhow::{anyhow, Result, Context};
use grep::searcher::sinks::UTF8;
use grep::matcher::{Matcher, Captures};
use crate::{OcflRepo, OBJECT_MARKER, OBJECT_ID_MATCHER, Inventory, ObjectVersion, VersionId, ROOT_INVENTORY_FILE, MUTABLE_HEAD_INVENTORY_FILE, VersionDetails, not_found, ObjectVersionDetails, Diff, invert_path_map};
use globset::{GlobBuilder};
use std::ops::Deref;

pub struct FsOcflRepo {
    pub storage_root: PathBuf
}

impl FsOcflRepo {
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
    fn list_objects(&self, filter_glob: Option<&str>) -> Result<Box<dyn Iterator<Item=Result<ObjectVersionDetails>>>> {
        let inv_iter = match filter_glob {
            Some(glob) => InventoryIter::new_glob_matching(&self.storage_root, glob)?,
            None => InventoryIter::new(&self.storage_root, None)?
        };

        Ok(Box::new(InventoryAdapterIter::new(inv_iter, Box::new(|inventory| {
            ObjectVersionDetails::from_inventory(inventory, None)
        }))))
    }

    fn get_object(&self, object_id: &str, version: Option<&VersionId>) -> Result<ObjectVersion> {
        let inventory = self.get_inventory(object_id)?;
        Ok(ObjectVersion::from_inventory(inventory, version)?)
    }

    fn get_object_details(&self, object_id: &str, version: Option<&VersionId>) -> Result<ObjectVersionDetails> {
        let inventory = self.get_inventory(object_id)?;
        Ok(ObjectVersionDetails::from_inventory(inventory, version)?)
    }

    fn list_object_versions(&self, object_id: &str) -> Result<Vec<VersionDetails>> {
        let inventory = self.get_inventory(object_id)?;
        let mut versions = Vec::with_capacity(inventory.versions.len());

        for (id, version) in inventory.versions {
            versions.push(VersionDetails::from_version(id, version))
        }

        Ok(versions)
    }

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

    fn diff(&self, object_id: &str, left_version: &VersionId, right_version: Option<&VersionId>) -> Result<Vec<Diff>> {
        if right_version.is_some() && left_version.eq(right_version.unwrap()) {
            return Ok(vec![])
        }

        let mut inventory = self.get_inventory(object_id)?;

        let left = inventory.remove_version(&left_version)?;

        let right = match right_version {
            Some(version) => Some(inventory.remove_version(version)?),
            None => {
                if left_version.version_num > 1 {
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

struct InventoryAdapterIter<T> {
    iter: InventoryIter,
    adapter: Box<dyn Fn(Inventory) -> Result<T>>
}

impl<T> InventoryAdapterIter<T> {
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

struct InventoryIter {
    dir_iters: Vec<ReadDir>,
    current: RefCell<Option<ReadDir>>,
    id_matcher: Option<Box<dyn Fn(&str) -> bool>>,
}

impl InventoryIter {
    fn new_id_matching<P: AsRef<Path>>(root: P, object_id: &str) -> Result<Self> {
        let o = object_id.to_string();
        InventoryIter::new(root, Some(Box::new(move |id| id == o)))
    }

    fn new_glob_matching<P: AsRef<Path>>(root: P, glob: &str) -> Result<Self> {
        let matcher = GlobBuilder::new(glob).backslash_escape(true).build()?.compile_matcher();
        InventoryIter::new(root, Some(Box::new(move |id| matcher.is_match(id))))
    }

    fn new<P: AsRef<Path>>(root: P, id_matcher: Option<Box<dyn Fn(&str) -> bool>>) -> Result<Self> {
        Ok(InventoryIter {
            dir_iters: vec![std::fs::read_dir(&root)?],
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

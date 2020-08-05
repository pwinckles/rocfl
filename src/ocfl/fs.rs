//! Local filesystem OCFL storage implementation.

use std::cell::RefCell;
use std::fs::{self, File, ReadDir};
use std::io::{self, Read, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use globset::GlobBuilder;
use grep_matcher::{Captures, Matcher};
use grep_regex::RegexMatcher;
use grep_searcher::Searcher;
use grep_searcher::sinks::UTF8;
use lazy_static::lazy_static;

use crate::ocfl::VersionNum;

use super::{Inventory, MUTABLE_HEAD_INVENTORY_FILE, not_found, OBJECT_MARKER, OcflStore, ROOT_INVENTORY_FILE};

lazy_static! {
    static ref OBJECT_ID_MATCHER: RegexMatcher = RegexMatcher::new(r#""id"\s*:\s*"([^"]+)""#).unwrap();
}

// ================================================== //
//             public structs+enums+traits            //
// ================================================== //

/// Local filesystem OCFL repository
pub struct FsOcflStore {
    /// The path to the OCFL storage root
    storage_root: PathBuf
}

// ================================================== //
//                   public impls+fns                 //
// ================================================== //

impl FsOcflStore {
    /// Creates a new FsOcflStore
    pub fn new<P: AsRef<Path>>(storage_root: P) -> Result<Self> {
        let storage_root = storage_root.as_ref().to_path_buf();

        if !storage_root.exists() {
            return Err(anyhow!("Storage root {} does not exist", storage_root.to_string_lossy()));
        } else if !storage_root.is_dir() {
            return Err(anyhow!("Storage root {} is not a directory", storage_root.to_string_lossy()))
        }

        // TODO verify is an OCFL repository
        // TODO load storage layout

        Ok(Self {
            storage_root
        })
    }
}

impl OcflStore for FsOcflStore {
    /// Returns the most recent inventory version for the specified object, or an a
    /// `RocflError::NotFound` if it does not exist.
    fn get_inventory(&self, object_id: &str) -> Result<Inventory> {
        let mut iter = InventoryIter::new_id_matching(&self.storage_root, &object_id)?;

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
            Some(glob) => InventoryIter::new_glob_matching(&self.storage_root, glob)?,
            None => InventoryIter::new(&self.storage_root, None)?
        }))
    }

    /// Writes the specified file to the sink.
    ///
    /// If the file cannot be found, then a `RocflError::NotFound` error is returned.
    fn get_object_file(&self,
                       object_id: &str,
                       path: &str,
                       version_num: Option<&VersionNum>,
                       sink: Box<&mut dyn Write>) -> Result<()> {
        let inventory = self.get_inventory(object_id)?;

        let content_path = inventory.lookup_content_path_for_logical_path(path, version_num)?;
        let storage_path = self.storage_root.join(&inventory.object_root).join(content_path);

        let mut file = File::open(storage_path)?;

        io::copy(&mut file, *sink)?;

        Ok(())
    }
}

// ================================================== //
//            private structs+enums+traits            //
// ================================================== //

/// Iterates over ever object in an OCFL repository by walking the file tree.
struct InventoryIter {
    dir_iters: Vec<ReadDir>,
    current: RefCell<Option<ReadDir>>,
    id_matcher: Option<Box<dyn Fn(&str) -> bool>>,
}

// ================================================== //
//                private impls+fns                   //
// ================================================== //

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

    fn create_if_matches<P: AsRef<Path>>(&self, object_root: P) -> Result<Option<Inventory>> {
        let inventory_path = object_root.as_ref().join(ROOT_INVENTORY_FILE);

        if self.id_matcher.is_some() {
            let object_id = self.extract_object_id(&inventory_path)?;
            if self.id_matcher.as_ref().unwrap().deref()(&object_id) {
                return Ok(Some(parse_inventory(&object_root)?));
            }
        } else {
            return Ok(Some(parse_inventory(&object_root)?));
        }

        Ok(None)
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
                }
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
                                }
                                Ok(is_root) if !is_root => {
                                    self.dir_iters.push(self.current.replace(None).unwrap());
                                    match std::fs::read_dir(&path) {
                                        Ok(next) => {
                                            self.current.replace(Some(next));
                                        }
                                        Err(e) => return Some(Err(e.into()))
                                    }
                                }
                                Err(e) => return Some(Err(e)),
                                _ => panic!("This code is unreachable")
                            }
                        }
                        _ => ()
                    }
                }
            }
        }
    }
}

/// Returns true if the path contains an OCFL object root marker file
fn is_object_root<P: AsRef<Path>>(path: P) -> Result<bool> {
    for entry in fs::read_dir(path)? {
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

//! Local filesystem OCFL storage implementation.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{self, File, ReadDir};
use std::io::{self, Read, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use globset::GlobBuilder;
use grep_matcher::{Captures, Matcher};
use grep_regex::RegexMatcher;
use grep_searcher::Searcher;
use grep_searcher::sinks::UTF8;
use log::{error, info};
use once_cell::sync::Lazy;

use crate::ocfl::{InventoryPath, OcflLayout, VersionNum};
use crate::ocfl::consts::*;
use crate::ocfl::error::{not_found, Result, RocflError};
use crate::ocfl::inventory::Inventory;
use crate::ocfl::layout::StorageLayout;

use super::OcflStore;

static OBJECT_ID_MATCHER: Lazy<RegexMatcher> = Lazy::new(|| RegexMatcher::new(r#""id"\s*:\s*"([^"]+)""#).unwrap());

/// Local filesystem OCFL repository
pub struct FsOcflStore {
    /// The path to the OCFL storage root
    storage_root: PathBuf,
    /// Maps object IDs to paths within the storage root
    storage_layout: Option<StorageLayout>,
    /// Caches object ID to path mappings
    id_path_cache: RefCell<HashMap<String, String>>,
}

impl FsOcflStore {
    /// Creates a new FsOcflStore
    pub fn new<P: AsRef<Path>>(storage_root: P) -> Result<Self> {
        let storage_root = storage_root.as_ref().to_path_buf();

        if !storage_root.exists() {
            return Err(RocflError::IllegalState(format!(
                "Storage root {} does not exist", canonical_str(storage_root))));
        } else if !storage_root.is_dir() {
            return Err(RocflError::IllegalState(format!(
                "Storage root {} is not a directory", canonical_str(storage_root))));
        }

        let storage_layout = load_storage_layout(&storage_root);

        Ok(Self {
            storage_root,
            storage_layout,
            id_path_cache: RefCell::new(HashMap::new()),
        })
    }

    /// Initializes a new OCFL repository at the specified location
    pub fn init<P: AsRef<Path>>(root: P, layout: StorageLayout) -> Result<Self> {
        let root = root.as_ref().to_path_buf();

        init_new_repo(&root, &layout)?;

        Ok(Self {
            storage_root: root,
            storage_layout: Some(layout),
            id_path_cache: RefCell::new(HashMap::new()),
        })
    }

    /// Conditionally initializes a new OCFL repository at the specified location if one does
    /// not already exist.
    pub fn init_if_needed<P: AsRef<Path>>(root: P, layout: StorageLayout) -> Result<Self> {
        let root = root.as_ref().to_path_buf();

        if root.exists() && root.is_dir() && fs::read_dir(&root)?.next().is_some() {
            Self::new(root)
        } else {
            Self::init(root, layout)
        }
    }

    /// Stages an OCFL object if there is not an existing object with the same ID.
    pub(super) fn stage_object(&self, inventory: &Inventory) -> Result<()> {
        match self.get_inventory(&inventory.id) {
            Err(RocflError::NotFound(_)) => (),
            Err(e) => return Err(e),
            _ => {
                return Err(RocflError::IllegalState(
                    format!("Cannot create object {} because it already exists in staging",
                            inventory.id)));
            }
        }

        info!("Staging OCFL object {} {}", &inventory.id, &inventory.head);

        // If it's a new object, the object root path will not be known
        let object_root: Cow<str> = if inventory.object_root.is_empty() {
            Cow::Owned(self.require_layout()?.map_object_id(&inventory.id))
        } else {
            inventory.object_root.as_str().into()
        };

        let storage_path = self.storage_root.join(object_root.as_ref());

        // TODO existence?

        fs::create_dir_all(&storage_path)?;

        // TODO should we fail if already exists?
        writeln!(File::create(storage_path.join(OBJECT_NAMASTE_FILE))?, "{}", OCFL_OBJECT_VERSION)?;
        serde_json::to_writer(File::create(storage_path.join(INVENTORY_FILE))?, &inventory)?;

        // TODO write inventory digest?

        Ok(())
    }

    pub(super) fn stage_file<R: Read>(&self,
                           inventory: &Inventory,
                           source: &mut R,
                           logical_path: &InventoryPath) -> Result<()> {
        // TODO any validation that the staged object exist?

        let content_path = inventory.new_content_path_head(&logical_path)?;

        let storage_path = self.storage_root
            .join(&inventory.object_root)
            .join(&content_path.as_ref());

        fs::create_dir_all(storage_path.parent().unwrap())?;
        io::copy(source, &mut File::create(&storage_path)?)?;

        Ok(())
    }

    pub(super) fn stage_inventory(&self, inventory: &Inventory) -> Result<()> {
        // TODO any validation that the staged object exist?

        let storage_path = self.storage_root.join(&inventory.object_root);

        serde_json::to_writer(File::create(storage_path.join(INVENTORY_FILE))?, &inventory)?;

        // TODO sidecar?

        Ok(())
    }

    fn require_layout(&self) -> Result<&StorageLayout> {
        match &self.storage_layout {
            Some(layout) => Ok(layout),
            None => Err(RocflError::IllegalState(
                "The OCFL repository must have a defined storage layout to execute this operation.".to_string()))
        }
    }
}

impl OcflStore for FsOcflStore {
    /// Returns the most recent inventory version for the specified object, or an a
    /// `RocflError::NotFound` if it does not exist.
    fn get_inventory(&self, object_id: &str) -> Result<Inventory> {
        let object_root = match self.id_path_cache.borrow().get(object_id) {
            Some(object_root) => Some(object_root.clone()),
            None => None
        };

        match object_root {
            Some(object_root) => {
                self.get_inventory_by_path(object_id, &object_root)
            }
            None => {
                let inventory = self.get_inventory_inner(&object_id)?;
                self.id_path_cache.borrow_mut().insert(object_id.to_string(),
                                                       inventory.object_root.clone());
                Ok(inventory)
            }
        }
    }

    /// Returns an iterator that iterates over every object in an OCFL repository, returning
    /// the most recent inventory of each. Optionally, a glob pattern may be provided that filters
    /// the objects that are returned by OCFL ID.
    fn iter_inventories<'a>(&'a self, filter_glob: Option<&str>)
        -> Result<Box<dyn Iterator<Item=Inventory> + 'a>> {
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
                       path: &InventoryPath,
                       version_num: Option<VersionNum>,
                       sink: &mut dyn Write) -> Result<()> {
        let inventory = self.get_inventory(object_id)?;

        let content_path = inventory.content_path_for_logical_path(path, version_num)?;
        let storage_path = self.storage_root.join(&inventory.object_root)
            .join(content_path.as_ref().as_ref());

        let mut file = File::open(storage_path)?;
        io::copy(&mut file, sink)?;

        Ok(())
    }
}

impl FsOcflStore {
    fn get_inventory_inner(&self, object_id: &str) -> Result<Inventory> {
        if let Some(storage_layout) = &self.storage_layout {
            let object_root = storage_layout.map_object_id(object_id);
            self.get_inventory_by_path(object_id, &object_root)
        } else {
            info!("Storage layout not configured, scanning repository to locate object {}", &object_id);

            let mut iter = InventoryIter::new_id_matching(&self.storage_root, &object_id)?;

            match iter.next() {
                Some(inventory) => Ok(inventory),
                None => Err(not_found(&object_id, None))
            }
        }
    }

    fn get_inventory_by_path(&self, object_id: &str, object_root: &str) -> Result<Inventory> {
        let object_root = self.storage_root.join(object_root);

        if object_root.exists() {
            parse_inventory(&object_root, &self.storage_root)
        } else {
            Err(not_found(&object_id, None))
        }
    }
}

/// Iterates over ever object in an OCFL repository by walking the file tree.
struct InventoryIter {
    root: PathBuf,
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
            root: root.as_ref().to_path_buf(),
            current: RefCell::new(None),
            id_matcher,
        })
    }

    fn create_if_matches<P: AsRef<Path>>(&self, object_root: P) -> Option<Inventory> {
        let inventory_path = object_root.as_ref().join(INVENTORY_FILE);

        if self.id_matcher.is_some() {
            if let Some(object_id) = self.extract_object_id(&inventory_path) {
                if self.id_matcher.as_ref().unwrap().deref()(&object_id) {
                    return parse_inventory_optional(&object_root, &self.root);
                }
            }
        } else {
            return parse_inventory_optional(&object_root, &self.root);
        }

        None
    }

    fn extract_object_id<P: AsRef<Path>>(&self, path: P) -> Option<String> {
        let mut matches: Vec<String> = vec![];

        let result = Searcher::new().search_path(&*OBJECT_ID_MATCHER, &path, UTF8(|_, line| {
            let mut captures = OBJECT_ID_MATCHER.new_captures()?;
            OBJECT_ID_MATCHER.captures(line.as_bytes(), &mut captures)?;
            matches.push(line[captures.get(1).unwrap()].to_string());
            Ok(true)
        }));

        if let Err(e) = result {
            error!("Failed to locate object ID in inventory at {}: {:#}",
                  path.as_ref().to_string_lossy(), e);
            None
        } else {
            match matches.get(0) {
                Some(id) => Some(id.to_string()),
                None => {
                    error!("Failed to locate object ID in inventory at {}",
                          path.as_ref().to_string_lossy());
                    None
                }
            }
        }
    }
}

impl Iterator for InventoryIter {
    type Item = Inventory;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.borrow().is_none() && self.dir_iters.is_empty() {
                return None
            } else if self.current.borrow().is_none() {
                self.current.replace(self.dir_iters.pop());
            }

            let entry = self.current.borrow_mut().as_mut().unwrap().next();

            match entry {
                None =>  {
                    self.current.replace(None);
                },
                Some(Err(e)) => error!("{:#}", e),
                Some(Ok(entry)) => {
                    match entry.file_type() {
                        Err(e) => error!("{:#}", e),
                        Ok(ftype) if ftype.is_dir() => {
                            let path = entry.path();

                            if path.file_name().unwrap_or_default() == EXTENSIONS_DIR {
                                continue;
                            }

                            match is_object_root(&path) {
                                Ok(is_root) if is_root => {
                                    if let Some(inventory) = self.create_if_matches(&path) {
                                        return Some(inventory);
                                    }
                                }
                                Ok(is_root) if !is_root => {
                                    self.dir_iters.push(self.current.replace(None).unwrap());
                                    match fs::read_dir(&path) {
                                        Ok(next) => {
                                            self.current.replace(Some(next));
                                        }
                                        Err(e) => error!("{:#}", e)
                                    }
                                }
                                Err(e) => error!("{:#}", e),
                                _ => unreachable!()
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
            && entry_path.file_name().unwrap_or_default() == OBJECT_NAMASTE_FILE {
            return Ok(true);
        }
    }
    Ok(false)
}

fn parse_inventory_optional<A, B>(object_root: A, storage_root: B) -> Option<Inventory>
    where
        A: AsRef<Path>,
        B: AsRef<Path>,
{
    match parse_inventory(object_root, storage_root) {
        Ok(inventory) => Some(inventory),
        Err(e) => {
            error!("{:#}", e);
            None
        }
    }
}

/// Parses the HEAD inventory of the OCFL object that's rooted in the specified directory.
/// This is normally the `inventory.json` file in the object's root, but it could also be
/// the inventory file in an extension directory, such as the mutable HEAD extension.
fn parse_inventory<A, B>(object_root: A, storage_root: B) -> Result<Inventory>
    where
        A: AsRef<Path>,
        B: AsRef<Path>,
{
    let inventory_path = resolve_inventory_path(&object_root);
    // TODO should validate hash
    let mut inventory = match parse_inventory_file(&inventory_path) {
        Ok(inventory) => inventory,
        Err(e) => return Err(RocflError::General(
            format!("Failed to parse inventory at {}: {}", inventory_path.to_string_lossy(), e)))
    };

    let relative = match pathdiff::diff_paths(&object_root, &storage_root) {
        Some(relative) => relative.to_string_lossy().into(),
        None => object_root.as_ref().to_string_lossy().into(),
    };

    inventory.object_root = relative;
    Ok(inventory)
}

fn parse_inventory_file<P: AsRef<Path>>(inventory_file: P) -> Result<Inventory> {
    let bytes = file_to_bytes(inventory_file)?;
    let inventory: Inventory = serde_json::from_slice(&bytes)?;
    inventory.validate()?;
    Ok(inventory)
}

fn resolve_inventory_path<P: AsRef<Path>>(object_root: P) -> PathBuf {
    let mutable_head_inv = object_root.as_ref().join(MUTABLE_HEAD_INVENTORY_FILE);
    if mutable_head_inv.exists() {
        info!("Found mutable HEAD at {}", mutable_head_inv.to_string_lossy());
        return mutable_head_inv;
    }
    object_root.as_ref().join(INVENTORY_FILE)
}

fn load_storage_layout<P: AsRef<Path>>(storage_root: P) -> Option<StorageLayout> {
    let layout = parse_layout(&storage_root);

    match layout {
        Some(layout) => {
            let config_bytes = read_layout_config(&storage_root, &layout);
            let storage_layout = StorageLayout::new(layout.extension, config_bytes.as_deref());

            match storage_layout {
                Ok(storage_layout) => {
                    info!("Loaded storage layout extension {}", layout.extension.to_string());
                    Some(storage_layout)
                },
                Err(e) => {
                    error!("Failed to load storage layout extension {}: {:#}",
                           layout.extension.to_string(), e);
                    None
                }
            }
        },
        None => None
    }
}

/// Parses the `ocfl_layout.json` file if it exists
fn parse_layout<P: AsRef<Path>>(storage_root: P) -> Option<OcflLayout> {
    let layout_file = storage_root.as_ref().join(OCFL_LAYOUT_FILE);
    if layout_file.exists() {
        match parse_layout_file(&layout_file) {
            Ok(layout) => Some(layout),
            Err(e) => {
                error!("Failed to parse OCFL layout file at {}: {:#}",
                      layout_file.to_string_lossy(), e);
                None
            }
        }
    } else {
        info!("The OCFL repository at {} does not contain an ocfl_layout.json file.",
              canonical_str(storage_root));
        None
    }
}

fn parse_layout_file<P: AsRef<Path>>(layout_file: P) -> Result<OcflLayout> {
    let bytes = file_to_bytes(layout_file)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn read_layout_config<P: AsRef<Path>>(storage_root: P, layout: &OcflLayout) -> Option<Vec<u8>> {
    let config_file = storage_root.as_ref()
        .join(EXTENSIONS_DIR)
        .join(layout.extension.to_string())
        .join(EXTENSIONS_CONFIG_FILE);

    if config_file.exists() {
        return match file_to_bytes(&config_file) {
            Ok(bytes) => Some(bytes),
            Err(e) => {
                error!("Failed to parse OCFL storage layout extension config at {}: {:#}",
                      config_file.to_string_lossy(), e);
                None
            }
        }
    }

    info!("Storage layout configuration not found at {}", config_file.to_string_lossy());
    None
}

fn init_new_repo<P: AsRef<Path>>(root: P, layout: &StorageLayout) -> Result<()> {
    let root = root.as_ref().to_path_buf();

    if root.exists() {
        if !root.is_dir() {
            return Err(RocflError::IllegalState(format!(
                "Storage root {} is not a directory", canonical_str(root))));
        }

        if fs::read_dir(&root)?.next().is_some() {
            return Err(RocflError::IllegalState(format!(
                "Storage root {} must be empty", canonical_str(root))));
        }
    }

    info!("Initializing OCFL storage root at {}", canonical_str(&root));

    fs::create_dir_all(&root)?;

    // TODO should we fail if already exists?
    writeln!(File::create(root.join(REPO_NAMASTE_FILE))?, "{}", OCFL_VERSION)?;

    let ocfl_spec = include_str!("../../resources/main/specs/ocfl_1.0.txt");
    write!(File::create(root.join(OCFL_SPEC_FILE))?, "{}", ocfl_spec)?;

    let ocfl_layout = OcflLayout {
        extension: layout.extension_name(),
        // TODO what to do about this?
        description: layout.extension_name().to_string()
    };

    serde_json::to_writer_pretty(File::create(root.join(OCFL_LAYOUT_FILE))?, &ocfl_layout)?;

    let layout_ext_dir = root.join(EXTENSIONS_DIR).join(layout.extension_name().to_string());
    fs::create_dir_all(&layout_ext_dir)?;

    File::create(layout_ext_dir.join(EXTENSIONS_CONFIG_FILE))?.write_all(&layout.serialize()?)?;

    // TODO write extension spec

    Ok(())
}

fn file_to_bytes<P: AsRef<Path>>(file: P) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(&file)?.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn canonical_str(path: impl AsRef<Path>) -> String  {
    match fs::canonicalize(path.as_ref()) {
        Ok(path) => path.to_string_lossy().into(),
        Err(_) => path.as_ref().to_string_lossy().into(),
    }
}

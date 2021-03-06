//! Local filesystem OCFL storage implementation.

use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs::{self, File, OpenOptions, ReadDir};
use std::io::{self, Read, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use globset::GlobBuilder;
use grep_matcher::{Captures, Matcher};
use grep_regex::RegexMatcher;
use grep_searcher::sinks::UTF8;
use grep_searcher::Searcher;
use log::{error, info, warn};
use once_cell::sync::Lazy;
use walkdir::WalkDir;

use super::layout::{LayoutExtensionName, StorageLayout};
use super::{OcflLayout, OcflStore, StagingStore};
use crate::ocfl::consts::*;
use crate::ocfl::error::{not_found, Result, RocflError};
use crate::ocfl::inventory::Inventory;
use crate::ocfl::{paths, specs, util, InventoryPath, VersionNum};

static OBJECT_ID_MATCHER: Lazy<RegexMatcher> =
    Lazy::new(|| RegexMatcher::new(r#""id"\s*:\s*"([^"]+)""#).unwrap());

/// Local filesystem OCFL repository
pub struct FsOcflStore {
    /// The path to the OCFL storage root
    storage_root: PathBuf,
    /// Maps object IDs to paths within the storage root
    storage_layout: Option<StorageLayout>,
    // TODO this never expires entries and is only intended to be useful within the scope of the cli
    /// Caches object ID to path mappings
    id_path_cache: RwLock<HashMap<String, String>>,
    closed: Arc<AtomicBool>,
}

impl FsOcflStore {
    /// Creates a new FsOcflStore
    pub fn new(storage_root: impl AsRef<Path>) -> Result<Self> {
        let storage_root = storage_root.as_ref().to_path_buf();

        if !storage_root.exists() {
            return Err(RocflError::IllegalState(format!(
                "Storage root {} does not exist",
                canonical_str(storage_root)
            )));
        } else if !storage_root.is_dir() {
            return Err(RocflError::IllegalState(format!(
                "Storage root {} is not a directory",
                canonical_str(storage_root)
            )));
        }

        check_extensions(&storage_root);

        let storage_layout = load_storage_layout(&storage_root);

        Ok(Self {
            storage_root,
            storage_layout,
            id_path_cache: RwLock::new(HashMap::new()),
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Initializes a new OCFL repository at the specified location
    pub fn init(root: impl AsRef<Path>, layout: Option<StorageLayout>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();

        init_new_repo(&root, layout.as_ref())?;

        Ok(Self {
            storage_root: root,
            storage_layout: layout,
            id_path_cache: RwLock::new(HashMap::new()),
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Conditionally initializes a new OCFL repository at the specified location if one does
    /// not already exist.
    pub fn init_if_needed<P: AsRef<Path>>(root: P, layout: StorageLayout) -> Result<Self> {
        let root = root.as_ref().to_path_buf();

        if root.exists() && root.is_dir() && !util::dir_is_empty(&root)? {
            Self::new(root)
        } else {
            Self::init(root, Some(layout))
        }
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

        let mut iter =
            InventoryIter::new_id_matching(&self.storage_root, &object_id, self.closed.clone())?;

        match iter.next() {
            Some(inventory) => {
                if let Ok(mut cache) = self.id_path_cache.write() {
                    cache.insert(object_id.to_string(), inventory.object_root.clone());
                }
                Ok(inventory)
            }
            None => Err(not_found(&object_id, None)),
        }
    }

    fn get_inventory_by_path(&self, object_id: &str, object_root: &str) -> Result<Inventory> {
        let object_root = self.storage_root.join(object_root);

        if object_root.exists() {
            let inventory = parse_inventory(&object_root, &self.storage_root)?;

            if inventory.id != object_id {
                Err(RocflError::CorruptObject {
                    object_id: object_id.to_string(),
                    message: format!(
                        "Expected object to exist at {} but found object {} instead.",
                        object_root.to_string_lossy(),
                        inventory.id
                    ),
                })
            } else {
                Ok(inventory)
            }
        } else {
            Err(not_found(&object_id, None))
        }
    }

    fn copy_inventory_files(
        &self,
        inventory: &Inventory,
        from: impl AsRef<Path>,
        to: impl AsRef<Path>,
    ) -> Result<()> {
        let from_path = from.as_ref();
        let to_path = to.as_ref();

        fs::copy(
            paths::inventory_path(from_path),
            paths::inventory_path(to_path),
        )?;
        fs::copy(
            paths::sidecar_path(from_path, inventory.digest_algorithm),
            paths::sidecar_path(to_path, inventory.digest_algorithm),
        )?;

        Ok(())
    }

    fn require_layout(&self) -> Result<&StorageLayout> {
        match &self.storage_layout {
            Some(layout) => Ok(layout),
            None => Err(RocflError::IllegalState(
                "The OCFL repository must have a defined storage layout to execute this operation."
                    .to_string(),
            )),
        }
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

impl OcflStore for FsOcflStore {
    /// Returns the most recent inventory version for the specified object, or an a
    /// `RocflError::NotFound` if it does not exist.
    fn get_inventory(&self, object_id: &str) -> Result<Inventory> {
        self.ensure_open()?;

        match self.get_object_root_path(object_id) {
            Some(object_root) => self.get_inventory_by_path(object_id, &object_root),
            None => self.scan_for_inventory(&object_id),
        }
    }

    /// Returns an iterator that iterates over every object in an OCFL repository, returning
    /// the most recent inventory of each. Optionally, a glob pattern may be provided that filters
    /// the objects that are returned by OCFL ID.
    fn iter_inventories<'a>(
        &'a self,
        filter_glob: Option<&str>,
    ) -> Result<Box<dyn Iterator<Item = Inventory> + 'a>> {
        self.ensure_open()?;

        Ok(Box::new(match filter_glob {
            Some(glob) => {
                InventoryIter::new_glob_matching(&self.storage_root, glob, self.closed.clone())?
            }
            None => InventoryIter::new(&self.storage_root, None, self.closed.clone())?,
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
        self.ensure_open()?;

        let inventory = self.get_inventory(object_id)?;

        let content_path = inventory.content_path_for_logical_path(path, version_num)?;
        let mut storage_path = PathBuf::from(&inventory.storage_path);
        storage_path.push(content_path.as_ref().as_ref());

        let mut file = File::open(storage_path)?;
        io::copy(&mut file, sink)?;

        Ok(())
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

        let root_path = match self.get_object_root_path(&inventory.id) {
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

        let storage_path = self.storage_root.join(root_path);

        if storage_path.exists() {
            return Err(RocflError::IllegalState(format!(
                "Cannot create object {} because an object already exists at {}",
                inventory.id,
                storage_path.to_string_lossy()
            )));
        }

        info!("Creating new object {}", inventory.id);

        fs::create_dir_all(storage_path.parent().unwrap())?;
        fs::rename(src_object_path, &storage_path)?;

        inventory.storage_path = storage_path.to_string_lossy().into();

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

        let object_root = self.storage_root.join(&existing_inventory.object_root);
        let destination = object_root.join(&version_str);

        if destination.exists() {
            return Err(RocflError::IllegalState(
                format!("Cannot create version {} in object {} because the version directory already exists.",
                        version_str, inventory.id)));
        }

        info!(
            "Creating version {} of object {}",
            version_str, inventory.id
        );

        fs::rename(version_path, &destination)?;

        if let Err(e) = self.copy_inventory_files(&inventory, &destination, &object_root) {
            if let Err(e) = fs::rename(&destination, version_path) {
                error!("Failed to rollback version {} of object {} at {}: {}. Manual intervention may be required.",
                       version_str, inventory.id, version_path.to_string_lossy(), e);
            }

            return Err(RocflError::General(format!(
                "Failed to create new version {} of object {}: {}",
                version_str, inventory.id, e
            )));
        }

        inventory.storage_path = object_root.to_string_lossy().into();

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

        let storage_path = self.storage_root.join(&object_root);
        info!(
            "Purging object {} at {}",
            object_id,
            storage_path.to_string_lossy()
        );

        if storage_path.exists() {
            if let Err(e) = remove_dir_all::remove_dir_all(&storage_path) {
                return Err(RocflError::CorruptObject {
                    object_id: object_id.to_string(),
                    message: format!("Failed to purge object at {}. This object may need to be removed manually. Error: {}",
                                     storage_path.to_string_lossy(), e)
                });
            }
        }

        let parent = storage_path.parent().unwrap();

        if parent.exists() {
            if let Err(e) = util::clean_dirs_up(&parent) {
                error!(
                    "Failed to cleanup dangling directories at {}: {}",
                    storage_path.to_string_lossy(),
                    e
                );
            }
        }

        Ok(())
    }

    /// Returns a list of all of the extension names that are associated with the object
    fn list_object_extensions(&self, object_id: &str) -> Result<Vec<String>> {
        self.ensure_open()?;

        let object_root = self.lookup_or_find_object_root_path(object_id)?;
        let extensions_dir = paths::extensions_path(&object_root);

        let mut extensions = Vec::new();

        if extensions_dir.exists() {
            for entry in fs::read_dir(extensions_dir)? {
                extensions.push(entry?.file_name().to_string_lossy().to_string());
            }
        }

        Ok(extensions)
    }

    /// Instructs the store to gracefully stop any in-flight work and not accept any additional
    /// requests.
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
}

impl StagingStore for FsOcflStore {
    /// Stages an OCFL object if there is not an existing object with the same ID.
    fn stage_object(&self, inventory: &mut Inventory) -> Result<()> {
        match self.get_inventory(&inventory.id) {
            Err(RocflError::NotFound(_)) => (),
            Err(e) => return Err(e),
            _ => {
                return Err(RocflError::IllegalState(format!(
                    "Cannot create object {} because it already exists in staging",
                    inventory.id
                )));
            }
        }

        info!("Staging OCFL object {} {}", &inventory.id, &inventory.head);

        // Staging layout may differ from main repo
        let object_root = self.require_layout()?.map_object_id(&inventory.id);
        inventory.object_root = object_root;

        let storage_path = self.storage_root.join(&inventory.object_root);
        inventory.storage_path =
            util::convert_forwardslash_to_back(&storage_path.to_string_lossy()).into();

        fs::create_dir_all(&storage_path)?;

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(paths::object_namaste_path(&storage_path))?;

        writeln!(file, "{}", OCFL_OBJECT_VERSION)?;
        self.stage_inventory(&inventory, false, false)?;

        Ok(())
    }

    /// Copies a file in the staging area
    fn stage_file_copy(
        &self,
        inventory: &Inventory,
        source: &mut impl Read,
        logical_path: &InventoryPath,
    ) -> Result<()> {
        let content_path = inventory.new_content_path_head(&logical_path)?;

        let mut storage_path = PathBuf::from(&inventory.storage_path);
        storage_path.push(&content_path.as_ref());

        fs::create_dir_all(storage_path.parent().unwrap())?;
        io::copy(source, &mut File::create(&storage_path)?)?;

        Ok(())
    }

    /// Copies an existing staged file to a new location
    fn copy_staged_file(
        &self,
        inventory: &Inventory,
        src_content: &InventoryPath,
        dst_logical: &InventoryPath,
    ) -> Result<()> {
        let object_root = PathBuf::from(&inventory.storage_path);

        let dst_content = inventory.new_content_path_head(&dst_logical)?;

        let src_storage = object_root.join(src_content.as_ref());
        let dst_storage = object_root.join(dst_content.as_ref());

        fs::create_dir_all(dst_storage.parent().unwrap())?;
        fs::copy(&src_storage, &dst_storage)?;

        Ok(())
    }

    /// Moves a file in the staging area
    fn stage_file_move(
        &self,
        inventory: &Inventory,
        source: &impl AsRef<Path>,
        logical_path: &InventoryPath,
    ) -> Result<()> {
        let content_path = inventory.new_content_path_head(&logical_path)?;

        let mut storage_path = PathBuf::from(&inventory.storage_path);
        storage_path.push(&content_path.as_ref());

        fs::create_dir_all(storage_path.parent().unwrap())?;
        fs::rename(source, &storage_path)?;

        Ok(())
    }

    /// Moves an existing staged file to a new location
    fn move_staged_file(
        &self,
        inventory: &Inventory,
        src_content: &InventoryPath,
        dst_logical: &InventoryPath,
    ) -> Result<()> {
        let object_root = PathBuf::from(&inventory.storage_path);

        let dst_content = inventory.new_content_path_head(&dst_logical)?;

        let src_storage = object_root.join(src_content.as_ref());
        let dst_storage = object_root.join(dst_content.as_ref());

        fs::create_dir_all(dst_storage.parent().unwrap())?;
        fs::rename(&src_storage, &dst_storage)?;
        util::clean_dirs_up(src_storage.parent().unwrap())?;

        Ok(())
    }

    /// Deletes staged content files.
    fn rm_staged_files(&self, inventory: &Inventory, paths: &[&InventoryPath]) -> Result<()> {
        let object_root = PathBuf::from(&inventory.storage_path);

        for path in paths.iter() {
            let full_path = object_root.join(path.as_ref());
            info!("Deleting staged file: {}", full_path.to_string_lossy());
            util::remove_file_ignore_not_found(&full_path)?;
            util::clean_dirs_up(full_path.parent().unwrap())?;
        }

        Ok(())
    }

    /// Deletes any staged files that are not referenced in the manifest
    fn rm_orphaned_files(&self, inventory: &Inventory) -> Result<()> {
        let object_root = PathBuf::from(&inventory.storage_path);

        let content_dir = paths::head_content_path(&object_root, inventory);

        if content_dir.exists() {
            for file in WalkDir::new(&content_dir) {
                let file = file?;
                if file.path().is_file() {
                    let relative = pathdiff::diff_paths(file.path(), &object_root)
                        .unwrap()
                        .to_string_lossy()
                        .to_string();
                    let content_path = util::convert_backslash_to_forward(&relative);
                    if !inventory
                        .contains_content_path(&InventoryPath::try_from(content_path.as_ref())?)
                    {
                        info!("Deleting orphaned file: {}", file.path().to_string_lossy());
                        util::remove_file_ignore_not_found(file.path())?;
                        util::clean_dirs_up(file.path().parent().unwrap())?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Serializes the inventory to the object's staging directory. If `finalize` is true,
    /// then the inventory file will additionally be copied into the version directory.
    fn stage_inventory(
        &self,
        inventory: &Inventory,
        finalize: bool,
        pretty_print: bool,
    ) -> Result<()> {
        let object_root = PathBuf::from(&inventory.storage_path);
        let inventory_path = paths::inventory_path(&object_root);
        let sidecar_path = paths::sidecar_path(&object_root, inventory.digest_algorithm);

        let mut inv_writer = inventory
            .digest_algorithm
            .writer(File::create(&inventory_path)?)?;

        if pretty_print {
            serde_json::to_writer_pretty(&mut inv_writer, &inventory)?;
        } else {
            serde_json::to_writer(&mut inv_writer, &inventory)?;
        }

        let digest = inv_writer.finalize_hex();

        let mut sidecar_file = File::create(&sidecar_path)?;
        writeln!(&mut sidecar_file, "{}  {}", digest, INVENTORY_FILE)?;

        if finalize {
            let version_path = paths::version_path(&object_root, inventory.head);
            fs::create_dir_all(&version_path)?;
            self.copy_inventory_files(&inventory, &object_root, &version_path)?;
        }

        Ok(())
    }
}

/// Iterates over ever object in an OCFL repository by walking the file tree.
struct InventoryIter {
    root: PathBuf,
    dir_iters: Vec<ReadDir>,
    current: RefCell<Option<ReadDir>>,
    id_matcher: Option<Box<dyn Fn(&str) -> bool>>,
    closed: Arc<AtomicBool>,
}

impl InventoryIter {
    /// Creates a new iterator that only returns objects that match the given object ID.
    fn new_id_matching<P: AsRef<Path>>(
        root: P,
        object_id: &str,
        closed: Arc<AtomicBool>,
    ) -> Result<Self> {
        let o = object_id.to_string();
        InventoryIter::new(root, Some(Box::new(move |id| id == o)), closed)
    }

    /// Creates a new iterator that only returns objects with IDs that match the specified glob
    /// pattern.
    fn new_glob_matching<P: AsRef<Path>>(
        root: P,
        glob: &str,
        closed: Arc<AtomicBool>,
    ) -> Result<Self> {
        let matcher = GlobBuilder::new(glob)
            .backslash_escape(true)
            .build()?
            .compile_matcher();
        InventoryIter::new(root, Some(Box::new(move |id| matcher.is_match(id))), closed)
    }

    /// Creates a new iterator that returns all objects if no `id_matcher` is provided, or only
    /// the objects the `id_matcher` returns `true` for if one is provided.
    fn new<P: AsRef<Path>>(
        root: P,
        id_matcher: Option<Box<dyn Fn(&str) -> bool>>,
        closed: Arc<AtomicBool>,
    ) -> Result<Self> {
        Ok(InventoryIter {
            dir_iters: vec![fs::read_dir(&root)?],
            root: root.as_ref().to_path_buf(),
            current: RefCell::new(None),
            id_matcher,
            closed,
        })
    }

    fn create_if_matches<P: AsRef<Path>>(&self, object_root: P) -> Option<Inventory> {
        let inventory_path = paths::inventory_path(&object_root);

        if self.id_matcher.is_some() {
            if let Some(object_id) = self.extract_object_id(&inventory_path) {
                if self.id_matcher.as_ref().unwrap().deref()(&object_id) {
                    return parse_inventory_optional(object_root, &self.root);
                }
            }
        } else {
            return parse_inventory_optional(object_root, &self.root);
        }

        None
    }

    fn extract_object_id<P: AsRef<Path>>(&self, path: P) -> Option<String> {
        let mut matches: Vec<String> = vec![];

        let result = Searcher::new().search_path(
            &*OBJECT_ID_MATCHER,
            &path,
            UTF8(|_, line| {
                let mut captures = OBJECT_ID_MATCHER.new_captures()?;
                OBJECT_ID_MATCHER.captures(line.as_bytes(), &mut captures)?;
                matches.push(line[captures.get(1).unwrap()].to_string());
                Ok(true)
            }),
        );

        if let Err(e) = result {
            error!(
                "Failed to locate object ID in inventory at {}: {:#}",
                path.as_ref().to_string_lossy(),
                e
            );
            None
        } else {
            match matches.get(0) {
                Some(id) => Some(id.to_string()),
                None => {
                    error!(
                        "Failed to locate object ID in inventory at {}",
                        path.as_ref().to_string_lossy()
                    );
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
                Some(Err(e)) => error!("{:#}", e),
                Some(Ok(entry)) => match entry.file_type() {
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
                                    Err(e) => error!("{:#}", e),
                                }
                            }
                            Err(e) => error!("{:#}", e),
                            _ => unreachable!(),
                        }
                    }
                    _ => (),
                },
            }
        }
    }
}

/// Returns true if the path contains an OCFL object root marker file
fn is_object_root<P: AsRef<Path>>(path: P) -> Result<bool> {
    for entry in fs::read_dir(path)? {
        let entry_path = entry?.path();
        if entry_path.is_file() && entry_path.file_name().unwrap_or_default() == OBJECT_NAMASTE_FILE
        {
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
    let (inventory_path, mutable_head) = resolve_inventory_path(&object_root);
    // TODO should validate hash
    let mut inventory = match parse_inventory_file(&inventory_path) {
        Ok(inventory) => inventory,
        Err(e) => {
            return Err(RocflError::General(format!(
                "Failed to parse inventory at {}: {}",
                inventory_path.to_string_lossy(),
                e
            )))
        }
    };

    let relative = match pathdiff::diff_paths(&object_root, &storage_root) {
        Some(relative) => relative.to_string_lossy().to_string(),
        None => object_root.as_ref().to_string_lossy().to_string(),
    };

    inventory.object_root = util::convert_backslash_to_forward(&relative).to_string();
    inventory.storage_path =
        util::convert_forwardslash_to_back(&object_root.as_ref().to_string_lossy()).into();
    inventory.mutable_head = mutable_head;
    Ok(inventory)
}

fn parse_inventory_file<P: AsRef<Path>>(inventory_file: P) -> Result<Inventory> {
    let bytes = file_to_bytes(inventory_file)?;
    let inventory: Inventory = serde_json::from_slice(&bytes)?;
    inventory.validate()?;
    Ok(inventory)
}

fn resolve_inventory_path<P: AsRef<Path>>(object_root: P) -> (PathBuf, bool) {
    let mutable_head_inv = object_root.as_ref().join(MUTABLE_HEAD_INVENTORY_FILE);
    if mutable_head_inv.exists() {
        info!(
            "Found mutable HEAD at {}",
            mutable_head_inv.to_string_lossy()
        );
        return (mutable_head_inv, true);
    }
    (paths::inventory_path(object_root), false)
}

fn check_extensions(storage_root: impl AsRef<Path>) {
    let extensions_dir = paths::extensions_path(storage_root);

    if !extensions_dir.exists() {
        return;
    }

    match fs::read_dir(&extensions_dir) {
        Ok(entries) => {
            for entry in entries {
                match entry {
                    Ok(entry) => {
                        let name = entry.file_name().to_string_lossy().to_string();
                        if !SUPPORTED_EXTENSIONS.contains(&name.as_ref()) {
                            warn!(
                                "Storage root extension {} is not supported at this time",
                                name
                            );
                        }
                    }
                    Err(e) => error!("Failed to list storage root extensions: {}", e),
                }
            }
        }
        Err(e) => error!("Failed to list storage root extensions: {}", e),
    }
}

fn load_storage_layout<P: AsRef<Path>>(storage_root: P) -> Option<StorageLayout> {
    let layout = parse_layout(&storage_root);

    match layout {
        Some(layout) => {
            let config_bytes = read_layout_config(&storage_root, &layout);
            let storage_layout = StorageLayout::new(layout.extension, config_bytes.as_deref());

            match storage_layout {
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
            }
        }
        None => None,
    }
}

/// Parses the `ocfl_layout.json` file if it exists
fn parse_layout<P: AsRef<Path>>(storage_root: P) -> Option<OcflLayout> {
    let layout_file = paths::ocfl_layout_path(&storage_root);
    if layout_file.exists() {
        match parse_layout_file(&layout_file) {
            Ok(layout) => Some(layout),
            Err(e) => {
                error!(
                    "Failed to parse OCFL layout file at {}: {:#}",
                    layout_file.to_string_lossy(),
                    e
                );
                None
            }
        }
    } else {
        info!(
            "The OCFL repository at {} does not contain an ocfl_layout.json file.",
            canonical_str(storage_root)
        );
        None
    }
}

fn parse_layout_file<P: AsRef<Path>>(layout_file: P) -> Result<OcflLayout> {
    let bytes = file_to_bytes(layout_file)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn read_layout_config<P: AsRef<Path>>(storage_root: P, layout: &OcflLayout) -> Option<Vec<u8>> {
    let mut config_file = paths::extensions_path(storage_root);
    config_file.push(layout.extension.to_string());
    config_file.push(EXTENSIONS_CONFIG_FILE);

    if config_file.exists() {
        return match file_to_bytes(&config_file) {
            Ok(bytes) => Some(bytes),
            Err(e) => {
                error!(
                    "Failed to parse OCFL storage layout extension config at {}: {:#}",
                    config_file.to_string_lossy(),
                    e
                );
                None
            }
        };
    }

    info!(
        "Storage layout configuration not found at {}",
        config_file.to_string_lossy()
    );
    None
}

fn init_new_repo(root: impl AsRef<Path>, layout: Option<&StorageLayout>) -> Result<()> {
    let root = root.as_ref().to_path_buf();

    if root.exists() {
        if !root.is_dir() {
            return Err(RocflError::IllegalState(format!(
                "Cannot create new repository. Storage root {} is not a directory",
                canonical_str(root)
            )));
        }

        if fs::read_dir(&root)?.next().is_some() {
            return Err(RocflError::IllegalState(format!(
                "Cannot create new repository. Storage root {} must be empty",
                canonical_str(root)
            )));
        }
    }

    info!("Initializing OCFL storage root at {}", canonical_str(&root));

    fs::create_dir_all(&root)?;

    writeln!(
        File::create(paths::root_namaste_path(&root))?,
        "{}",
        OCFL_VERSION
    )?;

    write!(
        File::create(paths::ocfl_spec_path(&root))?,
        "{}",
        specs::OCFL_1_0_SPEC
    )?;

    if let Some(layout) = layout {
        write_layout_config(&root, layout)?;
    }

    Ok(())
}

fn write_layout_config(root: impl AsRef<Path>, layout: &StorageLayout) -> Result<()> {
    let extension_name = layout.extension_name().to_string();

    let ocfl_layout = OcflLayout {
        extension: layout.extension_name(),
        description: format!("See specification document {}.md", extension_name),
    };

    serde_json::to_writer_pretty(
        File::create(paths::ocfl_layout_path(root.as_ref()))?,
        &ocfl_layout,
    )?;

    let mut layout_ext_dir = paths::extensions_path(root.as_ref());
    layout_ext_dir.push(&extension_name);
    fs::create_dir_all(&layout_ext_dir)?;

    File::create(layout_ext_dir.join(EXTENSIONS_CONFIG_FILE))?.write_all(&layout.serialize()?)?;

    let extension_spec = match layout.extension_name() {
        LayoutExtensionName::FlatDirectLayout => specs::EXT_0002_SPEC,
        LayoutExtensionName::HashedNTupleObjectIdLayout => specs::EXT_0003_SPEC,
        LayoutExtensionName::HashedNTupleLayout => specs::EXT_0004_SPEC,
        LayoutExtensionName::FlatOmitPrefixLayout => specs::EXT_0006_SPEC,
    };

    write!(
        File::create(root.as_ref().join(format!("{}.md", extension_name)))?,
        "{}",
        extension_spec
    )?;

    Ok(())
}

fn file_to_bytes(file: impl AsRef<Path>) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(&file)?.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn canonical_str(path: impl AsRef<Path>) -> String {
    match fs::canonicalize(path.as_ref()) {
        Ok(path) => path.to_string_lossy().into(),
        Err(_) => path.as_ref().to_string_lossy().into(),
    }
}

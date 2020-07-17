use std::cell::RefCell;
use std::io::{Read};
use std::fs::{File, ReadDir};
use std::path::{Path, PathBuf};
use grep::searcher::Searcher;
use anyhow::{anyhow, Result, Context};
use grep::searcher::sinks::UTF8;
use grep::matcher::{Matcher, Captures};
use crate::{OcflRepo, OBJECT_MARKER, OBJECT_ID_MATCHER, Inventory, OcflObjectVersion, VersionId, ROOT_INVENTORY_FILE, MUTABLE_HEAD_INVENTORY_FILE, VersionDetails};
use globset::{GlobMatcher, Glob};

pub struct FsOcflRepo {
    pub root: PathBuf
}

impl FsOcflRepo {
    pub fn new<P: AsRef<Path>>(root: P) -> Result<FsOcflRepo> {
        let root = root.as_ref().to_path_buf();

        if !root.exists() {
            return Err(anyhow!("Path {} does not exist", root.to_string_lossy()));
        } else if !root.is_dir() {
            return Err(anyhow!("Path {} is not a directory", root.to_string_lossy()))
        }

        // TODO verify is an OCFL repository
        // TODO load storage layout

        Ok(FsOcflRepo {
            root
        })
    }
}

impl OcflRepo for FsOcflRepo {

    fn list_objects(&self, filter_glob: Option<&str>) -> Result<Box<dyn Iterator<Item=Result<OcflObjectVersion>>>> {
        Ok(Box::new(FsObjectVersionIter::new(None, FsInventoryIter::new(&self.root, None, filter_glob)?)))
    }

    fn get_object(&self, object_id: &str, version: Option<VersionId>) -> Result<Option<OcflObjectVersion>> {
        let mut iter = FsObjectVersionIter::new(version,
                                                FsInventoryIter::new(&self.root,
                                                                     Some(object_id.to_string()),
                                                                     None)?);
        loop {
            match iter.next() {
                Some(Ok(object)) => return Ok(Some(object)),
                // TODO should print error?
                Some(Err(_)) => (),
                None => return Ok(None)
            };
        }
    }

    fn list_object_versions(&self, object_id: &str) -> Result<Option<Vec<VersionDetails>>> {
        let mut iter = FsInventoryIter::new(&self.root, Some(object_id.to_string()), None)?;

        loop {
            match iter.next() {
                Some(Ok(_inventory)) => {
                    return Ok(Some(vec![]))
                },
                // TODO should print error?
                Some(Err(_)) => (),
                None => return Ok(None)
            }
        }
    }

    fn list_file_versions(&self, object_id: &str, path: &str) -> Result<Option<Vec<VersionDetails>>> {
        Ok(Some(vec![]))
    }

}

struct FsObjectVersionIter {
    version: Option<VersionId>,
    iter: FsInventoryIter,
}

impl FsObjectVersionIter {
    fn new(version: Option<VersionId>, iter: FsInventoryIter) -> Self {
        Self {
            version,
            iter,
        }
    }
}

impl Iterator for FsObjectVersionIter {
    type Item = Result<OcflObjectVersion>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(inventory)) => {
                Some(OcflObjectVersion::from_inventory(inventory, self.version.as_ref()))
            }
        }
    }
}

struct FsInventoryIter {
    dir_iters: Vec<ReadDir>,
    current: RefCell<Option<ReadDir>>,
    object_id: Option<String>,
    object_id_glob: Option<GlobMatcher>,
}

impl FsInventoryIter {

    fn new<P: AsRef<Path>>(root: P, object_id: Option<String>, object_id_glob: Option<&str>) -> Result<Self> {
        Ok(FsInventoryIter {
            dir_iters: vec![std::fs::read_dir(&root)?],
            current: RefCell::new(None),
            object_id,
            object_id_glob: match object_id_glob {
                Some(glob) => Some(Glob::new(glob)?.compile_matcher()),
                None => None
            },
        })
    }

    fn is_matching(&self) -> bool {
        self.object_id.is_some() || self.object_id_glob.is_some()
    }

    fn is_match(&self, object_id: &str) -> bool {
        if self.object_id.is_some() {
            return self.object_id.as_ref().unwrap().eq(object_id);
        } else if self.object_id_glob.is_some() {
            return self.object_id_glob.as_ref().unwrap().is_match(object_id);
        }
        false
    }

    fn create_if_matches<P: AsRef<Path>>(&self, object_root: P) -> Result<Option<Inventory>>{
        let inventory_path = object_root.as_ref().join(ROOT_INVENTORY_FILE);

        if self.is_matching() {
            match self.extract_object_id(&inventory_path) {
                Ok(object_id) => {
                    if self.is_match(&object_id) {
                        return self.create_object_version(&object_root);
                    }
                },
                Err(e) => return Err(e)
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
        }))?;

        match matches.get(0) {
            Some(id) => Ok(id.to_string()),
            None => Err(anyhow!("Failed to locate object ID in inventory at {}",
                        path.as_ref().to_str().unwrap_or_default()))
        }
    }

}

impl Iterator for FsInventoryIter {
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

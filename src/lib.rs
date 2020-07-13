pub mod ocfl {
    use std::collections::HashMap;

    use anyhow::{Result};
    use chrono::{Local, DateTime};
    use serde::Deserialize;
    use thiserror::Error;

    const OBJECT_MARKER: &str = "0=ocfl_object_1.0";

    const OBJECT_ID_PATTERN: &str = r#""id"\s*:\s*"([^"]+)""#;

    pub trait OcflRepo {

        fn list_objects(&self) -> Result<Box<dyn Iterator<Item=Result<OcflObject>>>>;

        fn get_object(&self, object_id: &str) -> Result<Option<OcflObject>>;

    }

    pub mod fs {
        use std::cell::RefCell;
        use std::io::Read;
        use std::fs::{File, ReadDir};
        use std::path::{Path, PathBuf};
        use grep::searcher::Searcher;
        use grep::regex::{RegexMatcher};
        use anyhow::{anyhow, Result};

        use crate::ocfl::{OcflObject, OcflRepo, OBJECT_MARKER, OBJECT_ID_PATTERN, Inventory};
        use grep::searcher::sinks::UTF8;
        use grep::matcher::{Matcher, Captures};

        pub struct FsOcflRepo {
            pub root: PathBuf
        }

        impl FsOcflRepo {
            pub fn new<P: AsRef<Path>>(root: P) -> FsOcflRepo {
                // TODO verify is an OCFL repository
                // TODO load storage layout
                return FsOcflRepo {
                    root: root.as_ref().to_path_buf()
                }
            }
        }

        impl OcflRepo for FsOcflRepo {

            fn list_objects<'a>(&self) -> Result<Box<dyn Iterator<Item=Result<OcflObject>>>> {
                Ok(Box::new(FsObjectIdIter::new(&self.root, None)?))
            }

            fn get_object<'a>(&self, object_id: &str) -> Result<Option<OcflObject>> {
                let mut iter = FsObjectIdIter::new(&self.root, Some(object_id.to_string()))?;
                loop {
                    match iter.next() {
                        Some(Ok(object)) => return Ok(Some(object)),
                        // TODO should print error?
                        Some(Err(_)) => (),
                        None => return Ok(None)
                    };
                }
            }

        }

        struct FsObjectIdIter {
            dir_iters: Vec<ReadDir>,
            current: RefCell<Option<ReadDir>>,
            object_id: Option<String>,
            id_matcher: RegexMatcher,
        }

        impl FsObjectIdIter {

            // TODO support glob matching instead of exact matching
            fn new<P: AsRef<Path>>(root: P, object_id: Option<String>) -> Result<FsObjectIdIter> {
                Ok(FsObjectIdIter {
                    dir_iters: vec![std::fs::read_dir(&root)?],
                    current: RefCell::new(None),
                    object_id,
                    id_matcher: RegexMatcher::new(OBJECT_ID_PATTERN)?,
                })
            }

            fn extract_object_id<P: AsRef<Path>>(&self, path: P) -> Result<String> {
                let mut matches: Vec<String> = vec![];
                Searcher::new().search_path(&self.id_matcher, &path, UTF8(|_, line| {
                    let mut captures = self.id_matcher.new_captures()?;
                    self.id_matcher.captures(line.as_bytes(), &mut captures)?;
                    matches.push(line[captures.get(1).unwrap()].to_string());
                    Ok(true)
                }))?;

                match matches.get(0) {
                    Some(id) => Ok(id.to_string()),
                    None => Err(anyhow!("Failed to locate object ID in inventory at {}",
                        path.as_ref().to_str().unwrap_or_default()))
                }
            }

            fn parse_inventory<P: AsRef<Path>>(&self, path: P) -> Option<Result<OcflObject>> {
                match read_inventory(&path) {
                    Ok(object) => Some(Ok(object)),
                    Err(e) => Some(Err(
                        e.context(format!("Failed to parse inventory at {}",
                                          path.as_ref().to_str().unwrap_or_default()))))
                }
            }

        }

        impl Iterator for FsObjectIdIter {
            type Item = Result<OcflObject>;

            fn next(&mut self) -> Option<Result<OcflObject>> {
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
                                            let inventory_path = path.join("inventory.json");

                                            if !self.object_id.is_none() {
                                                match self.extract_object_id(&inventory_path) {
                                                    Ok(object_id) => {
                                                        // TODO compare id with glob search pattern https://crates.io/crates/globset
                                                        if self.object_id.as_ref().unwrap().eq(&object_id) {
                                                            return self.parse_inventory(&inventory_path);
                                                        }
                                                    },
                                                    Err(e) => return Some(Err(e))
                                                }
                                            } else {
                                                return self.parse_inventory(&inventory_path);
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

        fn read_inventory<'a, P: AsRef<Path>>(path: P) -> Result<OcflObject> {
            let mut bytes = Vec::new();
            File::open(&path)?.read_to_end(&mut bytes)?;
            let inventory: Inventory = serde_json::from_slice(&bytes)?;
            let root = String::from(path.as_ref().parent()
                .unwrap_or_else(|| Path::new(""))
                .to_str().unwrap_or_default());
            inventory.validate()?;
            let object = OcflObject::from(root.as_str(), inventory)?;
            Ok(object)
        }

    }

    #[derive(Deserialize, Debug)]
    #[serde(rename_all = "camelCase")]
    struct Inventory {
        id: String,
        #[serde(rename = "type")]
        type_declaration: String,
        digest_algorithm: String,
        head: String,
        content_directory: Option<String>,
        manifest: HashMap<String, Vec<String>>,
        versions: HashMap<String, Version>,
        fixity: Option<HashMap<String, HashMap<String, Vec<String>>>>,
    }

    #[derive(Deserialize, Debug)]
    struct Version {
        created: DateTime<Local>,
        state: HashMap<String, Vec<String>>,
        message: Option<String>,
        user: Option<User>
    }

    #[derive(Deserialize, Debug)]
    struct User {
        name: Option<String>,
        address: Option<String>
    }

    impl Inventory {

        // TODO fill in more validations
        // TODO have a shallow and a deep validation
        pub fn validate(&self) -> Result<(), RocError> {
            if !self.versions.contains_key(&self.head) {
                return Err(RocError::Validation {
                    object_id: self.id.clone(),
                    message: format!("HEAD version {} was not found", self.head),
                })
            }
            Ok(())
        }

    }

    // TODO we usually only care about one version -- make a view of an object a specific state
    pub struct OcflObject {
        pub id: String,
        pub root: String,
        // TODO create object for version id rep
        pub head: String,
        versions: HashMap<String, OcflVersion>,

        // TODO add missing fields
    }

    impl OcflObject {

        fn from(root: &str, inventory: Inventory) -> Result<Self, RocError> {
            let mut manifest = HashMap::new();

            for (digest, paths) in inventory.manifest {
                match paths.first() {
                    Some(path) => {
                        manifest.insert(digest, path.to_owned());
                    },
                    None => return Err(RocError::Validation {
                        object_id: inventory.id,
                        message: format!("No manifest entries found {}", digest)
                    })
                }
            }

            let mut versions = HashMap::new();

            for (id, version) in inventory.versions {
                versions.insert(id, OcflVersion::from(&inventory.id, version, &manifest)?);
            }

            Ok(Self {
                id: inventory.id,
                root: root.to_string(),
                head: inventory.head,
                versions
            })
        }

        pub fn head_version(&self) -> &OcflVersion {
            self.versions.get(&self.head).expect("Head version not found")
        }

    }

    pub struct OcflVersion {
        pub created: DateTime<Local>,
        state: HashMap<String, ManifestEntry>,
        // TODO add missing fields
    }

    impl OcflVersion {

        fn from(object_id: &str, version: Version, manifest: &HashMap<String, String>) -> Result<Self, RocError> {
            let mut state = HashMap::new();

            for (digest, paths) in version.state {
                for path in paths {
                    match manifest.get(&digest) {
                        Some(content_path) => {
                            state.insert(path, ManifestEntry{
                                digest: digest.clone(),
                                content_path: content_path.clone(),
                            });
                        },
                        None => return Err(RocError::Validation {
                            object_id: object_id.to_string(),
                            message: format!("No manifest entries found {}", digest)
                        })
                    }
                }
            }

            Ok(Self {
                created: version.created,
                state
            })
        }

    }

    struct ManifestEntry {
        // TODO ideally these wouldn't be owned...
        digest: String,
        content_path: String,
    }

    #[derive(Error, Debug)]
    pub enum RocError {
        #[error("Object {object_id} failed validation: {message}")]
        Validation {
            object_id: String,
            message: String,
        },
    }

}
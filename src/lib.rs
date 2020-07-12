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

        use crate::ocfl::{OcflObject, OcflRepo, OBJECT_MARKER, OBJECT_ID_PATTERN};
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

            fn list_objects(&self) -> Result<Box<dyn Iterator<Item=Result<OcflObject>>>> {
                Ok(Box::new(FsObjectIdIter::new(&self.root, None)?))
            }

            fn get_object(&self, object_id: &str) -> Result<Option<OcflObject>> {
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

                                            match self.extract_object_id(&inventory_path) {
                                                Ok(object_id) => {
                                                    if self.object_id.is_none()
                                                        || self.object_id.as_ref().unwrap().eq(&object_id) {
                                                        // TODO compare id with glob search pattern https://crates.io/crates/globset
                                                        return match read_inventory(&inventory_path) {
                                                            Ok(object) => Some(Ok(object)),
                                                            Err(e) => Some(Err(
                                                                e.context(format!("Failed to parse inventory at {}",
                                                                                  inventory_path.to_str().unwrap_or_default()))))
                                                        }
                                                    }
                                                },
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

        fn read_inventory<P: AsRef<Path>>(path: P) -> Result<OcflObject> {
            let mut bytes = Vec::new();
            File::open(&path)?.read_to_end(&mut bytes)?;
            let mut object: OcflObject = serde_json::from_slice(&bytes)?;
            object.root = String::from(path.as_ref().parent()
                .unwrap_or_else(|| Path::new(""))
                .to_str().unwrap_or_default());
            object.validate()?;
            Ok(object)
        }

    }

    #[derive(Deserialize, Debug)]
    #[serde(rename_all = "camelCase")]
    pub struct OcflObject {
        pub id: String,
        #[serde(rename = "type")]
        pub type_declaration: String,
        pub digest_algorithm: String,
        pub head: String,
        pub content_directory: Option<String>,
        pub manifest: HashMap<String, Vec<String>>,
        pub versions: HashMap<String, Version>,
        pub fixity: Option<HashMap<String, HashMap<String, Vec<String>>>>,

        #[serde(skip_serializing, skip_deserializing)]
        pub root: String,
    }

    #[derive(Deserialize, Debug)]
    pub struct Version {
        pub created: DateTime<Local>,
        pub state: HashMap<String, Vec<String>>,
        pub message: Option<String>,
        pub user: Option<User>
    }

    #[derive(Deserialize, Debug)]
    pub struct User {
        pub name: Option<String>,
        pub address: Option<String>
    }

    impl OcflObject {

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

        pub fn head_version(&self) -> &Version {
            // Should be safe to call unwrap provided that validate() was called after creation
            self.versions.get(&self.head).unwrap()
        }

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
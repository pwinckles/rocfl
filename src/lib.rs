pub mod ocfl {
    use std::collections::HashMap;

    use anyhow::Result;
    use chrono::{Local, DateTime};
    use serde::Deserialize;
    use thiserror::Error;

    const OBJECT_MARKER: &str = "0=ocfl_object_1.0";

    pub trait OcflRepo {
        fn list_objects(&self) -> Result<Box<dyn Iterator<Item=Result<OcflObject>>>>;
    }

    pub mod fs {
        use std::cell::RefCell;
        use std::io::Read;
        use std::fs::{File, ReadDir};
        use std::path::{Path, PathBuf};

        use anyhow::{Result};

        use crate::ocfl::{OcflObject, OcflRepo, OBJECT_MARKER};

        pub struct FsOcflRepo {
            pub root: PathBuf
        }

        impl FsOcflRepo {
            pub fn new<P: AsRef<Path>>(root: P) -> FsOcflRepo {
                return FsOcflRepo {
                    root: root.as_ref().to_path_buf()
                }
            }
        }

        impl OcflRepo for FsOcflRepo {
            fn list_objects(&self) -> Result<Box<dyn Iterator<Item=Result<OcflObject>>>> {
                Ok(Box::new(FsObjectIdIter::new(&self.root)?))
            }
        }

        struct FsObjectIdIter {
            dir_iters: Vec<ReadDir>,
            current: RefCell<Option<ReadDir>>,
        }

        impl FsObjectIdIter {
            fn new<P: AsRef<Path>>(root: P) -> Result<FsObjectIdIter> {
                Ok(FsObjectIdIter {
                    dir_iters: vec![std::fs::read_dir(&root)?],
                    current: RefCell::new(None),
                })
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
                            continue
                        },
                        Some(entry) => {
                            if entry.is_err() {
                                return Some(Err(entry.err().unwrap().into()))
                            }

                            let entry = entry.unwrap();

                            match entry.file_type() {
                                Ok(ftype) => {
                                    if ftype.is_dir() {
                                        let path = entry.path();
                                        match is_object_root(&path) {
                                            Ok(is_root) => {
                                                if is_root {
                                                    // TODO extract id without parsing json
                                                    // TODO compare id with glob search pattern https://crates.io/crates/globset
                                                    return match read_inventory(path.join("inventory.json")) {
                                                        Ok(object) => Some(Ok(object)),
                                                        Err(e) => Some(Err(
                                                            e.context(format!("Failed to parse inventory at {}",
                                                                              path.to_str().unwrap_or_default()))))
                                                    }
                                                } else {
                                                    self.dir_iters.push(self.current.replace(None).unwrap());
                                                    match std::fs::read_dir(&path) {
                                                        Ok(next) => self.current.replace(Some(next)),
                                                        Err(e) => return Some(Err(e.into()))
                                                    }
                                                };
                                            },
                                            Err(e) => return Some(Err(e.into()))
                                        }
                                    }
                                },
                                Err(e) => return Some(Err(e.into()))
                            }

                            continue
                        }
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
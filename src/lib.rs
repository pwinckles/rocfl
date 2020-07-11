pub mod ocfl {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::Read;
    use std::path::Path;

    use anyhow::Result;
    use chrono::{Local, DateTime};
    use serde::Deserialize;

    pub trait OcflRepo {
        fn list_objects(&self) -> Result<Box<dyn Iterator<Item=Result<Inventory>>>>;
    }

    pub mod fs {
        use std::cell::RefCell;
        use std::fs::ReadDir;
        use std::path::{Path, PathBuf};

        use anyhow::{Result};

        use crate::ocfl::{Inventory, OcflRepo};

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
            fn list_objects(&self) -> Result<Box<dyn Iterator<Item=Result<Inventory>>>> {
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

            fn is_object_root<P: AsRef<Path>>(&self, path: P) -> Result<bool> {
                for entry in std::fs::read_dir(path)? {
                    let entry_path = entry?.path();
                    if entry_path.is_file()
                        && entry_path.file_name().unwrap_or_default() == "0=ocfl_object_1.0" {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }

        impl Iterator for FsObjectIdIter {
            type Item = Result<Inventory>;

            fn next(&mut self) -> Option<Result<Inventory>> {
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
                                        match self.is_object_root(&path) {
                                            Ok(is_root) => {
                                                if is_root {
                                                    // TODO extract id without parsing json
                                                    // TODO compare id with glob search pattern https://crates.io/crates/globset
                                                    return match super::read_inventory(path.join("inventory.json")) {
                                                        Ok(inventory) => Some(Ok(inventory)),
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

    }

    pub fn read_inventory<P: AsRef<Path>>(path: P) -> Result<Inventory> {
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    #[derive(Deserialize, Debug)]
    #[serde(rename_all = "camelCase")]
    pub struct Inventory {
        pub id: String,
        #[serde(rename = "type")]
        pub type_declaration: String,
        pub digest_algorithm: String,
        pub head: String,
        pub content_directory: Option<String>,
        pub manifest: HashMap<String, Vec<String>>,
        pub versions: HashMap<String, Version>,
        pub fixity: Option<HashMap<String, HashMap<String, Vec<String>>>>
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

}
pub mod ocfl {
    use std::collections::{HashMap, BTreeMap};
    use anyhow::{Result};
    use chrono::{Local, DateTime};
    use serde::Deserialize;
    use thiserror::Error;
    use std::convert::TryFrom;
    use lazy_static::lazy_static;
    use regex::Regex;
    use grep::regex::{RegexMatcher};
    use core::fmt;
    use serde::export::Formatter;
    use std::cmp::Ordering;
    use std::hash::{Hash, Hasher};

    const OBJECT_MARKER: &str = "0=ocfl_object_1.0";

    lazy_static! {
        static ref VERSION_REGEX: Regex = Regex::new(r#"^v\d+$"#).unwrap();
        static ref OBJECT_ID_MATCHER: RegexMatcher = RegexMatcher::new(r#""id"\s*:\s*"([^"]+)""#).unwrap();
    }

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
        use anyhow::{anyhow, Result};
        use grep::searcher::sinks::UTF8;
        use grep::matcher::{Matcher, Captures};
        use crate::ocfl::{OcflObject, OcflRepo, OBJECT_MARKER, OBJECT_ID_MATCHER, Inventory};

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
        }

        impl FsObjectIdIter {

            // TODO support glob matching instead of exact matching
            fn new<P: AsRef<Path>>(root: P, object_id: Option<String>) -> Result<FsObjectIdIter> {
                Ok(FsObjectIdIter {
                    dir_iters: vec![std::fs::read_dir(&root)?],
                    current: RefCell::new(None),
                    object_id,
                })
            }

            // TODO this can move outside this type
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
    #[serde(try_from = "&str")]
    pub struct VersionId {
        pub version_num: u32,
        pub version_str: String,
    }

    impl TryFrom<&str> for VersionId {
        type Error = RocError;

        fn try_from(version: &str) -> Result<Self, Self::Error> {
            if !VERSION_REGEX.is_match(version) {
                return Err(RocError::IllegalArgument(format!("Invalid version {}", version)));
            }

           match version[1..].parse::<u32>() {
               Ok(num) => {
                   if num < 1 {
                       return Err(RocError::IllegalArgument(format!("Invalid version {}", version)));
                   }

                   Ok(Self {
                       version_num: num,
                       version_str: version.to_string(),
                   })
               },
               Err(_) => return Err(RocError::IllegalArgument(format!("Invalid version {}", version)))
           }
        }
    }

    impl TryFrom<u32> for VersionId {
        type Error = RocError;

        fn try_from(version: u32) -> Result<Self, Self::Error> {
            if version < 1 {
                return Err(RocError::IllegalArgument(format!("Invalid version number {}", version)));
            }

            Ok(Self {
                version_num: version,
                version_str: format!("v{}", version),
            })
        }
    }

    impl fmt::Display for VersionId {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.version_str)
        }
    }

    impl PartialEq for VersionId {
        fn eq(&self, other: &Self) -> bool {
            self.version_num == other.version_num
        }
    }

    impl Eq for VersionId {}

    impl Hash for VersionId {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.version_num.hash(state)
        }
    }

    impl PartialOrd for VersionId {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for VersionId {
        fn cmp(&self, other: &Self) -> Ordering {
            self.version_num.cmp(&other.version_num)
        }
    }

    #[derive(Deserialize, Debug)]
    #[serde(rename_all = "camelCase")]
    struct Inventory {
        id: String,
        #[serde(rename = "type")]
        type_declaration: String,
        digest_algorithm: String,
        head: VersionId,
        content_directory: Option<String>,
        manifest: HashMap<String, Vec<String>>,
        versions: BTreeMap<VersionId, Version>,
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

    pub struct OcflObjectVersion {
        pub id: String,
        pub version: String,
        pub root: String,
        pub created: DateTime<Local>,
        pub state: HashMap<String, FileDetails>,
        // TODO more fields
    }

    // impl OcflObjectVersion {
    //
    //     fn from(version: &str, root: &str, inventory: Inventory) -> Result<Self, RocError> {
    //         let mut manifest = HashMap::new();
    //
    //         for (digest, paths) in inventory.manifest {
    //             match paths.first() {
    //                 Some(path) => {
    //                     manifest.insert(digest, path.to_owned());
    //                 },
    //                 None => return Err(RocError::Validation {
    //                     object_id: inventory.id,
    //                     message: format!("No manifest entries found {}", digest)
    //                 })
    //             }
    //         }
    //
    //         let mut versions = HashMap::new();
    //
    //         for (id, version) in inventory.versions {
    //             versions.insert(id, OcflVersion::from(&inventory.id, version, &manifest)?);
    //         }
    //
    //
    //         match inventory.versions.get(version) {
    //             Some(version) => {
    //                 for (digest, paths) in &version.state {
    //                     for path in paths {
    //
    //                     }
    //                 }
    //             },
    //             None => return Err(RocError::NotFound {
    //                 message: format!("Object {} version {}", inventory.id, version)
    //             })
    //         }
    //
    //
    //         Ok(Self {
    //             id: inventory.id,
    //             version: version.to_string(),
    //             root: root.to_string(),
    //         })
    //     }
    //
    // }

    // TODO move
    // fn create_last_update_index(versions: &HashMap<String, Version>) {
    //
    // }

    pub struct FileDetails {
        pub digest: String,
        pub content_path: String,
        pub last_update_version: String,
        pub last_update: DateTime<Local>,
    }


    pub struct OcflObject {
        pub id: String,
        pub root: String,
        pub head: VersionId,
        versions: BTreeMap<VersionId, OcflVersion>,
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

            let mut versions = BTreeMap::new();

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
        #[error("Not found: {0}")]
        NotFound(String),
        #[error("Illegal argument: {0}")]
        IllegalArgument(String)
    }

}
pub mod ocfl {
    use std::collections::{HashMap};
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
    use std::path::Path;

    const OBJECT_MARKER: &str = "0=ocfl_object_1.0";
    const INVENTORY_FILE: &str = "inventory.json";

    lazy_static! {
        static ref VERSION_REGEX: Regex = Regex::new(r#"^v\d+$"#).unwrap();
        static ref OBJECT_ID_MATCHER: RegexMatcher = RegexMatcher::new(r#""id"\s*:\s*"([^"]+)""#).unwrap();
    }

    pub trait OcflRepo {

        fn list_objects(&self) -> Result<Box<dyn Iterator<Item=Result<OcflObjectVersion>>>>;

        fn get_object(&self, object_id: &str, version: Option<VersionId>) -> Result<Option<OcflObjectVersion>>;

    }

    pub mod fs {
        use std::cell::RefCell;
        use std::io::Read;
        use std::fs::{File, ReadDir};
        use std::path::{Path, PathBuf};
        use grep::searcher::Searcher;
        use anyhow::{anyhow, Result, Context};
        use grep::searcher::sinks::UTF8;
        use grep::matcher::{Matcher, Captures};
        use crate::ocfl::{OcflRepo, OBJECT_MARKER, OBJECT_ID_MATCHER, Inventory, OcflObjectVersion, VersionId, INVENTORY_FILE};

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

            fn list_objects(&self) -> Result<Box<dyn Iterator<Item=Result<OcflObjectVersion>>>> {
                Ok(Box::new(FsObjectIdIter::new(&self.root, None, None)?))
            }

            fn get_object(&self, object_id: &str, version: Option<VersionId>) -> Result<Option<OcflObjectVersion>> {
                let mut iter = FsObjectIdIter::new(&self.root, Some(object_id.to_string()), version)?;
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
            version: Option<VersionId>,
        }

        impl FsObjectIdIter {

            // TODO support glob matching instead of exact matching
            fn new<P: AsRef<Path>>(root: P, object_id: Option<String>, version: Option<VersionId>) -> Result<FsObjectIdIter> {
                Ok(FsObjectIdIter {
                    dir_iters: vec![std::fs::read_dir(&root)?],
                    current: RefCell::new(None),
                    object_id,
                    version,
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

            fn create_object_version<P: AsRef<Path>>(&self, path: P) -> Option<Result<OcflObjectVersion>> {
                match create_object_version(&self.version, &path) {
                    Ok(object) => Some(Ok(object)),
                    Err(e) => Some(Err(e))
                }
            }

        }

        impl Iterator for FsObjectIdIter {
            type Item = Result<OcflObjectVersion>;

            fn next(&mut self) -> Option<Result<OcflObjectVersion>> {
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
                                            let inventory_path = path.join(INVENTORY_FILE);

                                            if !self.object_id.is_none() {
                                                match self.extract_object_id(&inventory_path) {
                                                    Ok(object_id) => {
                                                        // TODO compare id with glob search pattern https://crates.io/crates/globset
                                                        if self.object_id.as_ref().unwrap().eq(&object_id) {
                                                            return self.create_object_version(&path);
                                                        }
                                                    },
                                                    Err(e) => return Some(Err(e))
                                                }
                                            } else {
                                                return self.create_object_version(&path);
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

        fn create_object_version<P: AsRef<Path>>(version: &Option<VersionId>, object_root: P) -> Result<OcflObjectVersion> {
            // TODO support mutable head
            let inventory_path = object_root.as_ref().join(INVENTORY_FILE);
            let inventory = parse_inventory(&inventory_path)
                .with_context(|| format!("Failed to parse inventory at {}",
                                         inventory_path.to_str().unwrap_or_default()))?;
            let head = inventory.head.clone();
            let v = version.as_ref().unwrap_or_else(|| &head);
            OcflObjectVersion::new(object_root, v, &inventory)
        }

        fn parse_inventory<P: AsRef<Path>>(path: P) -> Result<Inventory> {
            let mut bytes = Vec::new();
            File::open(&path)?.read_to_end(&mut bytes)?;
            let inventory: Inventory = serde_json::from_slice(&bytes)?;
            inventory.validate()?;
            Ok(inventory)
        }

    }

    #[derive(Deserialize, Debug)]
    #[serde(try_from = "&str")]
    pub struct VersionId {
        pub version_num: u32,
        pub version_str: String,
    }

    impl VersionId {

        // TODO breaks 0-padding
        fn previous(&self) -> Result<VersionId, RocError> {
            VersionId::try_from(self.version_num - 1)
        }

        // TODO breaks 0-padding
        fn next(&self) -> Result<VersionId, RocError> {
            VersionId::try_from(self.version_num + 1)
        }

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

    impl Clone for VersionId {
        fn clone(&self) -> Self {
            Self {
                version_num: self.version_num.clone(),
                version_str: self.version_str.clone(),
            }
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
        versions: HashMap<VersionId, Version>,
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
                return Err(RocError::CorruptObject {
                    object_id: self.id.clone(),
                    message: format!("HEAD version {} was not found", self.head),
                })
            }
            Ok(())
        }

    }

    pub struct OcflObjectVersion {
        pub id: String,
        pub version: VersionId,
        pub root: String,
        pub created: DateTime<Local>,
        pub state: HashMap<String, FileDetails>,
        // TODO more fields
    }

    pub struct FileDetails {
        pub digest: String,
        pub content_path: String,
        pub storage_path: String,
        // TODO see about making this a reference
        pub last_update: VersionDetails,
    }

    pub struct VersionDetails {
        pub version: VersionId,
        pub created: DateTime<Local>,
    }

    impl OcflObjectVersion {

        fn new<P: AsRef<Path>>(root: P, version: &VersionId, inventory: &Inventory) -> Result<Self> {
            let state = construct_state(&root, &version, inventory)?;

            Ok(Self {
                id: inventory.id.clone(),
                version: version.clone(),
                root: root.as_ref().to_str().unwrap_or_default().to_string(),
                created: ensure_version(version, inventory)?.created.clone(),
                state
            })
        }

    }

    fn construct_state<P: AsRef<Path>>(object_root: P, target: &VersionId, inventory: &Inventory) -> Result<HashMap<String, FileDetails>> {
        let mut state = HashMap::new();

        let target_version = ensure_version(target, inventory)?;
        let mut target_path_map = invert_path_map(&target_version.state);

        let mut current_version_id = (*target).clone();
        let mut current = target_version;

        while !target_path_map.is_empty() {
            let mut found: Vec<String> = vec![];

            if current_version_id.version_num == 1 {
                for (target_path, target_digest) in target_path_map.into_iter() {
                    let content_path = lookup_content_path(&target_digest, inventory)?.to_string();
                    state.insert(target_path, FileDetails {
                        storage_path: object_root.as_ref().join(&content_path).to_str().unwrap_or_default().to_string(),
                        content_path,
                        digest: target_digest,
                        last_update: VersionDetails {
                            version: current_version_id.clone(),
                            created: current.created.clone()
                        }
                    });
                }

                break;
            }

            let previous_version_id = current_version_id.previous()?;
            let previous = ensure_version(&previous_version_id, inventory)?;
            let mut previous_path_map = invert_path_map(&previous.state);

            for (target_path, target_digest) in target_path_map.iter() {
                let entry = previous_path_map.remove_entry(target_path);

                if entry.is_none() || entry.unwrap().1 != *target_digest {
                    found.push(target_path.clone());
                    let content_path = lookup_content_path(&target_digest, inventory)?.to_string();
                    state.insert(target_path.clone(), FileDetails {
                        digest: target_digest.clone(),
                        storage_path: object_root.as_ref().join(&content_path).to_str().unwrap_or_default().to_string(),
                        content_path,
                        last_update: VersionDetails {
                            version: current_version_id.clone(),
                            created: current.created.clone()
                        }
                    });
                }
            }

            current_version_id = previous_version_id;
            current = previous;

            for path in found {
                target_path_map.remove(&path);
            }
        }

        Ok(state)
    }

    fn ensure_version<'a, 'b>(version: &'b VersionId, inventory: &'a Inventory) -> Result<&'a Version> {
        match inventory.versions.get(version) {
            Some(v) => Ok(v),
            None => Err(RocError::NotFound(format!("Object {} version {}", inventory.id, version)).into())
        }
    }

    fn invert_path_map(map: &HashMap<String, Vec<String>>) -> HashMap<String, String> {
        let mut inverted = HashMap::new();

        for (digest, paths) in map {
            for path in paths {
                inverted.insert(path.clone(), digest.clone());
            }
        }

        inverted
    }

    fn lookup_content_path<'a>(digest: &'a str, inventory: &'a Inventory) -> Result<&'a str> {
        match inventory.manifest.get(digest) {
            Some(paths) => {
                match paths.first() {
                    Some(path) => Ok(path.as_str()),
                    None => Err(RocError::CorruptObject {
                        object_id: inventory.id.clone(),
                        message: format!("Digest {} is not mapped to any content paths", digest)
                    }.into())
                }
            },
            None => Err(RocError::CorruptObject {
                object_id: inventory.id.clone(),
                message: format!("Digest {} not found in manifest", digest)
            }.into())
        }
    }

    #[derive(Error, Debug)]
    pub enum RocError {
        #[error("Object {object_id} is corrupt: {message}")]
        CorruptObject {
            object_id: String,
            message: String,
        },
        #[error("Not found: {0}")]
        NotFound(String),
        #[error("Illegal argument: {0}")]
        IllegalArgument(String)
    }

}
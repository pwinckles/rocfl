use std::collections::{hash_map, HashMap, HashSet};
use std::fmt;
use std::fmt::Formatter;
use std::rc::Rc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{MapAccess, Visitor};

use crate::ocfl::digest::HexDigest;
use crate::ocfl::InventoryPath;

/// A bidirectional map that maps a file id, `HexDigest` to a set of paths, `InventoryPath`,
/// and a path to its file id. An id may have many paths, but a path may only have one id.
#[derive(Debug, Clone)]
pub struct PathBiMap {
    id_to_paths: HashMap<Rc<HexDigest>, HashSet<Rc<InventoryPath>>>,
    path_to_id: HashMap<Rc<InventoryPath>, Rc<HexDigest>>,
}

impl PathBiMap {
    pub fn new() -> Self {
        Self {
            id_to_paths: HashMap::new(),
            path_to_id: HashMap::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            id_to_paths: HashMap::with_capacity(capacity),
            path_to_id: HashMap::with_capacity(capacity),
        }
    }

    /// Inserts a new id to path mapping. If the path already has a mapping, then the existing
    /// mapping is removed.
    pub fn insert(&mut self, id: HexDigest, path: InventoryPath) {
        let id_ref = Rc::new(id);
        let path_ref = Rc::new(path);

        self.insert_rc(id_ref, path_ref);
    }

    /// Same as `insert`, but it accepts Rc values
    pub fn insert_rc(&mut self, id_ref: Rc<HexDigest>, path_ref: Rc<InventoryPath>) {
        if self.path_to_id.contains_key(&path_ref) {
            self.remove_path(&path_ref);
        }

        let entry = self.id_to_paths.entry(id_ref);
        let id_ref = entry.key().clone();

        entry.or_insert_with(HashSet::new)
            .insert(path_ref.clone());

        self.path_to_id.insert(path_ref, id_ref);
    }

    /// Inserts all of the path mappings for an id. This is used for deserialization.
    fn insert_multiple(&mut self, id: HexDigest, paths: Vec<InventoryPath>) {
        if paths.is_empty() {
            return
        }

        let id_ref = Rc::new(id);

        let set = self.id_to_paths.entry(id_ref.clone())
            .or_insert_with(HashSet::new);

        for path in paths {
            let path_ref = Rc::new(path);
            set.insert(path_ref.clone());
            self.path_to_id.insert(path_ref, id_ref.clone());
        }
    }

    /// Gets all of the paths associated with an id
    pub fn get_paths(&self, id: &HexDigest) -> Option<&HashSet<Rc<InventoryPath>>> {
        self.id_to_paths.get(id)
    }

    /// Gets the id associated with a path
    pub fn get_id(&self, path: &InventoryPath) -> Option<&Rc<HexDigest>> {
        self.path_to_id.get(path)
    }

    // Gets the underlying Rc value of the specified id if it exists
    pub fn get_id_rc(&self, id: &HexDigest) -> Option<&Rc<HexDigest>> {
        match self.id_to_paths.get_key_value(id) {
            Some((id, _)) => Some(id),
            None => None
        }
    }

    // Gets the underlying Rc value of the specified path if it exists
    pub fn get_path_rc(&self, path: &InventoryPath) -> Option<&Rc<InventoryPath>> {
        match self.path_to_id.get_key_value(path) {
            Some((path, _)) => Some(path),
            None => None
        }
    }

    /// True, if a mapping exists for the path
    pub fn contains_path(&self, path: &InventoryPath) -> bool {
        self.path_to_id.contains_key(path)
    }

    /// True, if a mapping exists for the id
    pub fn contains_id(&self, id: &HexDigest) -> bool {
        self.id_to_paths.contains_key(id)
    }

    pub fn is_empty(&self) -> bool {
        self.id_to_paths.is_empty()
    }

    /// Removes a path mapping
    pub fn remove_path(&mut self, path: &InventoryPath)
        -> Option<(Rc<InventoryPath>, Rc<HexDigest>)> {
        if let Some((path, id)) = self.path_to_id.remove_entry(path) {
            let mut remove = false;
            if let Some(paths) = self.id_to_paths.get_mut(&id) {
                paths.remove(path.as_ref());
                remove = paths.is_empty();
            }
            if remove {
                self.id_to_paths.remove(&id);
            }
            Some((path, id))
        } else {
            None
        }
    }

    /// Return an iterator that iterates over references to all path-id pairs
    pub fn iter(&self) -> Iter {
        Iter {
            iter: self.path_to_id.iter()
        }
    }

    /// Returns the number of path-id pairs in the map
    pub fn len(&self) -> usize {
        self.path_to_id.len()
    }
}

impl Default for PathBiMap {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoIterator for PathBiMap {
    type Item = (Rc<InventoryPath>, Rc<HexDigest>);
    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            iter: self.path_to_id.into_iter()
        }
    }
}

pub struct IntoIter {
    iter: hash_map::IntoIter<Rc<InventoryPath>, Rc<HexDigest>>,
}

impl Iterator for IntoIter {
    type Item = (Rc<InventoryPath>, Rc<HexDigest>);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub struct Iter<'a> {
    iter: hash_map::Iter<'a, Rc<InventoryPath>, Rc<HexDigest>>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a Rc<InventoryPath>, &'a Rc<HexDigest>);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

struct PathBiMapVisitor {

}

impl<'a> Visitor<'a> for PathBiMapVisitor {
    type Value = PathBiMap;

    fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
        formatter.write_str("a map of digests to paths")
    }

    fn visit_map<M: MapAccess<'a>>(self, mut access: M) -> Result<Self::Value, M::Error> {
        let mut map = PathBiMap::with_capacity(access.size_hint().unwrap_or(0));

        while let Some((key, value)) = access.next_entry()? {
            map.insert_multiple(key, value);
        }

        Ok(map)
    }
}

impl<'a> Deserialize<'a> for PathBiMap {
    fn deserialize<D: Deserializer<'a>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(PathBiMapVisitor{})
    }
}

impl Serialize for PathBiMap {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.id_to_paths.iter())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::convert::TryInto;
    use std::hash::Hash;
    use std::rc::Rc;

    use crate::ocfl::bimap::PathBiMap;
    use crate::ocfl::digest::HexDigest;
    use crate::ocfl::InventoryPath;

    #[test]
    fn insert_retrieve_remove() {
        let mut map = PathBiMap::new();
        map.insert("abcd".into(), path("foo/bar"));
        map.insert("efgh".into(), path("foo/baz"));
        map.insert("abcd".into(), path("2"));

        assert_eq!(&set(vec![path_rc("foo/bar"), path_rc("2")]),
                   map.get_paths(&"abcd".into()).unwrap());

        assert_eq!(&set(vec![path_rc("foo/baz")]),
                   map.get_paths(&"efgh".into()).unwrap());

        assert_eq!(&hex_rc("abcd"), map.get_id(&path("2")).unwrap());
        assert_eq!(&hex_rc("efgh"), map.get_id(&path("foo/baz")).unwrap());
        assert_eq!(&hex_rc("abcd"), map.get_id(&path("foo/bar")).unwrap());

        assert_eq!(None, map.get_id(&path("bogus")));
        assert_eq!(None, map.get_paths(&"bogus".into()));

        assert!(map.contains_id(&"abcd".into()));
        assert!(map.contains_id(&"efgh".into()));
        assert!(map.contains_path(&path("foo/bar")));
        assert!(map.contains_path(&path("foo/baz")));
        assert!(map.contains_path(&path("2")));

        assert!(!map.contains_id(&"bogus".into()));
        assert!(!map.contains_path(&path("bogus")));

        map.remove_path(&path("foo/baz"));

        assert!(!map.contains_id(&"efgh".into()));
        assert!(!map.contains_path(&path("foo/baz")));

        map.remove_path(&path("foo/bar"));

        assert_eq!(&set(vec![path_rc("2")]),
                   map.get_paths(&"abcd".into()).unwrap());
    }

    #[test]
    fn insert_existing_path() {
        let mut map = PathBiMap::new();
        map.insert("abcd".into(), path("foo/bar"));
        map.insert("123".into(), path("foo/bar"));

        assert!(!map.contains_id(&"abcd".into()));
        assert_eq!(&hex_rc("123"), map.get_id(&path("foo/bar")).unwrap());
    }

    #[test]
    fn serialize() {
        let mut map = PathBiMap::new();
        map.insert("abcd".into(), path("foo/bar"));
        map.insert("efgh".into(), path("foo/baz"));
        map.insert("abcd".into(), path("2"));

        let json = serde_json::to_string(&map).unwrap();

        if !(json.eq(r#"{"abcd":["foo/bar","2"],"efgh":["foo/baz"]}"#)
            || json.eq(r#"{"abcd":["2","foo/bar"],"efgh":["foo/baz"]}"#)
            || json.eq(r#"{"efgh":["foo/baz"],"abcd":["foo/bar","2"]}"#)
            || json.eq(r#"{"efgh":["foo/baz"],"abcd":["2","foo/bar"]}"#)) {
            panic!("Unexpected JSON: {}", json);
        }

        let value: PathBiMap = serde_json::from_str(&json).unwrap();

        assert_eq!(map.path_to_id, value.path_to_id);
        assert_eq!(map.id_to_paths, value.id_to_paths);
    }

    #[test]
    fn serialize_empty() {
        let map = PathBiMap::new();

        let json = serde_json::to_string(&map).unwrap();

        assert_eq!("{}", json);

        let value: PathBiMap = serde_json::from_str(&json).unwrap();

        assert_eq!(map.path_to_id, value.path_to_id);
        assert_eq!(map.id_to_paths, value.id_to_paths);
    }

    fn set<T: Eq + Hash>(vec: Vec<T>) -> HashSet<T> {
        vec.into_iter().collect()
    }

    fn path(p: &str) -> InventoryPath {
        p.try_into().unwrap()
    }

    fn path_rc(p: &str) -> Rc<InventoryPath> {
        Rc::new(path(p))
    }

    fn hex_rc(d: &str) -> Rc<HexDigest> {
        Rc::new(HexDigest::from(d))
    }
}
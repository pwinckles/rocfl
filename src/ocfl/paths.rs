use std::path::{Path, PathBuf};

use crate::ocfl::consts::*;
use crate::ocfl::inventory::Inventory;
use crate::ocfl::{DigestAlgorithm, SpecVersion, VersionNum};

/// Returns the path to `inventory.json` within the specified directory
pub fn inventory_path<P>(dir: P) -> PathBuf
where
    P: AsRef<Path>,
{
    dir.as_ref().join(INVENTORY_FILE)
}

/// Returns the path to `inventory.json.ALGO` within the specified directory
pub fn sidecar_path<P>(dir: P, algorithm: DigestAlgorithm) -> PathBuf
where
    P: AsRef<Path>,
{
    dir.as_ref().join(sidecar_name(algorithm))
}

/// Returns the name of an inventory sidecar for the specified algorithm
pub fn sidecar_name(algorithm: DigestAlgorithm) -> String {
    format!("{}.{}", INVENTORY_FILE, algorithm)
}

/// Returns the path to an object's namaste file
pub fn object_namaste_path<P>(dir: P, version: SpecVersion) -> PathBuf
where
    P: AsRef<Path>,
{
    dir.as_ref().join(version.object_namaste().filename)
}

/// Returns the path to the version directory within the object root
pub fn version_path<P>(object_root: P, version_num: VersionNum) -> PathBuf
where
    P: AsRef<Path>,
{
    object_root.as_ref().join(version_num.to_string())
}

/// Returns the a version's content directory
pub fn content_path<P>(object_root: P, version_num: VersionNum, inventory: &Inventory) -> PathBuf
where
    P: AsRef<Path>,
{
    let mut version_dir = version_path(object_root, version_num);
    version_dir.push(inventory.defaulted_content_dir());
    version_dir
}

/// Returns the path to the head version's content directory
pub fn head_content_path<P>(object_root: P, inventory: &Inventory) -> PathBuf
where
    P: AsRef<Path>,
{
    content_path(object_root, inventory.head, inventory)
}

/// Returns the path to the `extensions` directory within the specified directory
pub fn extensions_path<P>(dir: P) -> PathBuf
where
    P: AsRef<Path>,
{
    dir.as_ref().join(EXTENSIONS_DIR)
}

/// Returns the path to the root of the staging extension
pub fn staging_extension_path<P>(storage_root: P) -> PathBuf
where
    P: AsRef<Path>,
{
    let mut extensions = extensions_path(storage_root);
    extensions.push(ROCFL_STAGING_EXTENSION);
    extensions
}

/// Returns the path to the root of the staging extension
pub fn locks_extension_path<P>(storage_root: P) -> PathBuf
where
    P: AsRef<Path>,
{
    let mut extensions = extensions_path(storage_root);
    extensions.push(ROCFL_LOCKS_EXTENSION);
    extensions
}

/// Returns the path to the `ocfl_layout.json`
pub fn ocfl_layout_path<P>(storage_root: P) -> PathBuf
where
    P: AsRef<Path>,
{
    storage_root.as_ref().join(OCFL_LAYOUT_FILE)
}

/// Returns the path to the OCFL root namaste file
pub fn root_namaste_path<P>(storage_root: P, version: SpecVersion) -> PathBuf
where
    P: AsRef<Path>,
{
    storage_root.as_ref().join(version.root_namaste().filename)
}

/// Returns the path to the OCFL spec file
pub fn ocfl_spec_path<P>(storage_root: P, version: SpecVersion) -> PathBuf
where
    P: AsRef<Path>,
{
    storage_root.as_ref().join(version.spec_filename())
}

/// Joins two string path parts, inserting at `/` if needed
pub fn join(part1: &str, part2: &str) -> String {
    let mut joined = match part1.ends_with('/') {
        true => part1[..part1.len() - 1].to_string(),
        false => part1.to_string(),
    };

    if !part2.is_empty() {
        if (!joined.is_empty() || part1 == "/") && !part2.starts_with('/') {
            joined.push('/');
        }
        joined.push_str(part2);
    }

    joined
}

/// Joins two string path parts, inserting at `/` if needed, and appends a `/` to the end,
/// if there is not already one
pub fn join_with_trailing_slash(part1: &str, part2: &str) -> String {
    let mut joined = join(part1, part2);

    if !joined.is_empty() && !joined.ends_with('/') {
        joined.push('/');
    }

    joined
}

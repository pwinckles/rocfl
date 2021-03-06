use std::path::{Path, PathBuf};

use crate::ocfl::consts::{
    EXTENSIONS_DIR, INVENTORY_FILE, OBJECT_NAMASTE_FILE, OCFL_LAYOUT_FILE, OCFL_SPEC_FILE,
    REPO_NAMASTE_FILE, ROCFL_LOCKS_EXTENSION, ROCFL_STAGING_EXTENSION,
};
use crate::ocfl::inventory::Inventory;
use crate::ocfl::{DigestAlgorithm, VersionNum};

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
    let sidecar_name = format!("{}.{}", INVENTORY_FILE, algorithm.to_string());
    dir.as_ref().join(sidecar_name)
}

/// Returns the path to an object's namaste file
pub fn object_namaste_path<P>(dir: P) -> PathBuf
where
    P: AsRef<Path>,
{
    dir.as_ref().join(OBJECT_NAMASTE_FILE)
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
pub fn root_namaste_path<P>(storage_root: P) -> PathBuf
where
    P: AsRef<Path>,
{
    storage_root.as_ref().join(REPO_NAMASTE_FILE)
}

/// Returns the path to the OCFL spec file
pub fn ocfl_spec_path<P>(storage_root: P) -> PathBuf
where
    P: AsRef<Path>,
{
    storage_root.as_ref().join(OCFL_SPEC_FILE)
}

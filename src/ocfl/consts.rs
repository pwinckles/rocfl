use std::collections::HashSet;

use const_format::concatcp;
use once_cell::sync::Lazy;

const NAMASTE_FILE_PREFIX: &str = "0=";
const ROOT_NAMASTE_PREFIX: &str = "ocfl_";
const OBJECT_NAMASTE_PREFIX: &str = "ocfl_object_";
pub const ROOT_NAMASTE_FILE_PREFIX: &str = concatcp!(NAMASTE_FILE_PREFIX, ROOT_NAMASTE_PREFIX);
pub const OBJECT_NAMASTE_FILE_PREFIX: &str = concatcp!(NAMASTE_FILE_PREFIX, OBJECT_NAMASTE_PREFIX);

// 1.0
pub const ROOT_NAMASTE_FILE_1_0: &str = concatcp!(ROOT_NAMASTE_FILE_PREFIX, "1.0");
pub const OBJECT_NAMASTE_FILE_1_0: &str = concatcp!(OBJECT_NAMASTE_FILE_PREFIX, "1.0");
pub const ROOT_NAMASTE_CONTENT_1_0: &str = concatcp!(ROOT_NAMASTE_PREFIX, "1.0\n");
pub const OBJECT_NAMASTE_CONTENT_1_0: &str = concatcp!(OBJECT_NAMASTE_PREFIX, "1.0\n");
pub const INVENTORY_TYPE_1_0: &str = "https://ocfl.io/1.0/spec/#inventory";
pub const OCFL_SPEC_FILE_1_0: &str = "ocfl_1.0.txt";

// 1.1
pub const ROOT_NAMASTE_FILE_1_1: &str = concatcp!(ROOT_NAMASTE_FILE_PREFIX, "1.1");
pub const OBJECT_NAMASTE_FILE_1_1: &str = concatcp!(OBJECT_NAMASTE_FILE_PREFIX, "1.1");
pub const ROOT_NAMASTE_CONTENT_1_1: &str = concatcp!(ROOT_NAMASTE_PREFIX, "1.1\n");
pub const OBJECT_NAMASTE_CONTENT_1_1: &str = concatcp!(OBJECT_NAMASTE_PREFIX, "1.1\n");
pub const INVENTORY_TYPE_1_1: &str = "https://ocfl.io/1.1/spec/#inventory";
pub const OCFL_SPEC_FILE_1_1: &str = "ocfl_1.1.txt";

pub const INVENTORY_FILE: &str = "inventory.json";
pub const INVENTORY_SIDECAR_PREFIX: &str = "inventory.json.";
pub const OCFL_LAYOUT_FILE: &str = "ocfl_layout.json";
pub const EXTENSIONS_DIR: &str = "extensions";
pub const LOGS_DIR: &str = "logs";
pub const EXTENSIONS_CONFIG_FILE: &str = "config.json";

pub const DEFAULT_CONTENT_DIR: &str = "content";

pub const MUTABLE_HEAD_EXT_DIR: &str = "extensions/0005-mutable-head";
pub const MUTABLE_HEAD_INVENTORY_FILE: &str = "extensions/0005-mutable-head/head/inventory.json";

pub const FLAT_DIRECT_LAYOUT_EXTENSION: &str = "0002-flat-direct-storage-layout";
pub const HASHED_NTUPLE_OBJECT_ID_LAYOUT_EXTENSION: &str =
    "0003-hash-and-id-n-tuple-storage-layout";
pub const HASHED_NTUPLE_LAYOUT_EXTENSION: &str = "0004-hashed-n-tuple-storage-layout";
pub const MUTABLE_HEAD_EXTENSION: &str = "0005-mutable-head";
pub const FLAT_OMIT_PREFIX_LAYOUT_EXTENSION: &str = "0006-flat-omit-prefix-storage-layout";
pub const NTUPLE_OMIT_PREFIX_LAYOUT_EXTENSION: &str = "0007-n-tuple-omit-prefix-storage-layout";
pub const ROCFL_STAGING_EXTENSION: &str = "rocfl-staging";
pub const ROCFL_LOCKS_EXTENSION: &str = "rocfl-locks";

pub static SUPPORTED_EXTENSIONS: Lazy<HashSet<&str>> = Lazy::new(|| {
    let mut set = HashSet::with_capacity(8);
    set.insert(FLAT_DIRECT_LAYOUT_EXTENSION);
    set.insert(HASHED_NTUPLE_OBJECT_ID_LAYOUT_EXTENSION);
    set.insert(HASHED_NTUPLE_LAYOUT_EXTENSION);
    set.insert(MUTABLE_HEAD_EXTENSION);
    set.insert(FLAT_OMIT_PREFIX_LAYOUT_EXTENSION);
    set.insert(NTUPLE_OMIT_PREFIX_LAYOUT_EXTENSION);
    set.insert(ROCFL_STAGING_EXTENSION);
    set.insert(ROCFL_LOCKS_EXTENSION);
    set
});

pub const REPO_NAMASTE_FILE: &str = "0=ocfl_1.0";
pub const OBJECT_NAMASTE_FILE: &str = "0=ocfl_object_1.0";
pub const INVENTORY_FILE: &str = "inventory.json";
pub const OCFL_LAYOUT_FILE: &str = "ocfl_layout.json";
pub const OCFL_SPEC_FILE: &str = "ocfl_1.0.txt";
pub const EXTENSIONS_DIR: &str = "extensions";
pub const EXTENSIONS_CONFIG_FILE: &str = "config.json";
pub const OCFL_VERSION: &str = "ocfl_1.0";
pub const OCFL_OBJECT_VERSION: &str = "ocfl_object_1.0";
pub const INVENTORY_TYPE: &str = "https://ocfl.io/1.0/spec/#inventory";

pub const DEFAULT_CONTENT_DIR: &str = "content";

pub const MUTABLE_HEAD_EXT_DIR: &str = "extensions/0005-mutable-head";
pub const MUTABLE_HEAD_INVENTORY_FILE: &str = "extensions/0005-mutable-head/head/inventory.json";

pub const FLAT_DIRECT_LAYOUT_EXTENSION: &str = "0002-flat-direct-storage-layout";
pub const HASHED_NTUPLE_OBJECT_ID_LAYOUT_EXTENSION: &str = "0003-hash-and-id-n-tuple-storage-layout";
pub const HASHED_NTUPLE_LAYOUT_EXTENSION: &str = "0004-hashed-n-tuple-storage-layout";
pub const MUTABLE_HEAD_EXTENSION: &str = "0005-mutable-head";
pub const ROCFL_STAGING_EXTENSION: &str = "rocfl-staging";

pub const SUPPORTED_EXTENSIONS: &[&str] = &[
    FLAT_DIRECT_LAYOUT_EXTENSION,
    HASHED_NTUPLE_OBJECT_ID_LAYOUT_EXTENSION,
    HASHED_NTUPLE_LAYOUT_EXTENSION,
    MUTABLE_HEAD_EXTENSION,
    ROCFL_STAGING_EXTENSION,
];

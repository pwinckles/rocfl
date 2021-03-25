//! This library is a storage agnostic abstraction over [OCFL repositories](https://ocfl.io/).
//! It is **not** thread-safe.
//!
//! Create a new `OcflRepo` as follows:
//!
//! ```rust
//! use rocfl::ocfl::OcflRepo;
//!
//! let repo = OcflRepo::fs_repo("path/to/ocfl/storage/root");
//! ```

pub use self::digest::DigestAlgorithm;
pub use self::error::{Result, RocflError};
pub use self::repo::OcflRepo;
pub use self::store::layout::{LayoutExtensionName, StorageLayout};
pub use self::types::*;

mod bimap;
mod consts;
mod digest;
mod error;
mod inventory;
mod repo;
mod specs;
mod store;
mod types;
mod util;

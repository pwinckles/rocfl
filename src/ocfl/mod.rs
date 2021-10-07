//! This library is a storage agnostic abstraction over [OCFL repositories](https://ocfl.io/).
//!
//! Create a new `OcflRepo` as follows:
//!
//! ```rust
//! use rocfl::ocfl::OcflRepo;
//!
//! let repo = OcflRepo::fs_repo("path/to/ocfl/storage/root", None);
//! ```

pub use self::digest::DigestAlgorithm;
pub use self::error::{Result, RocflError};
pub use self::repo::OcflRepo;
pub use self::store::layout::{LayoutExtensionName, StorageLayout};
pub use self::types::*;
pub use self::validate::{
    ErrorCode, IncrementalValidator, IncrementalValidatorImpl, ObjectValidationResult,
    ProblemLocation, ValidationError, ValidationResult, ValidationWarning, WarnCode,
};

mod bimap;
mod consts;
mod digest;
mod error;
mod inventory;
mod lock;
mod paths;
mod repo;
mod serde;
mod specs;
mod store;
mod types;
mod util;
mod validate;

use core::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::{error, io};

#[cfg(feature = "s3")]
use rusoto_core::region::ParseRegionError;
#[cfg(feature = "s3")]
use rusoto_core::RusotoError;
use thiserror::Error;

use crate::ocfl::{LogicalPath, VersionNum};

pub type Result<T, E = RocflError> = core::result::Result<T, E>;

/// Application errors
#[derive(Error)]
pub enum RocflError {
    #[error("Object {object_id} is corrupt: {message}")]
    CorruptObject { object_id: String, message: String },

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Illegal argument: {0}")]
    IllegalArgument(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Illegal state: {0}")]
    IllegalState(String),

    #[error("Failed to acquire a lock for object {0}. If you think the lock is held in error, manually delete {1}")]
    LockAcquire(String, String),

    #[error("{0}")]
    General(String),

    #[error("{0}")]
    CopyMoveError(MultiError),

    #[error("The OCFL repository is closed")]
    Closed,

    #[error("{0}")]
    Io(io::Error),

    #[error("{0}")]
    Wrapped(Box<dyn error::Error>),
}

pub struct MultiError(pub Vec<String>);

impl Display for MultiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut iter = self.0.iter().peekable();

        while let Some(next) = iter.next() {
            write!(f, "{}", next)?;

            if iter.peek().is_some() {
                writeln!(f)?;
            }
        }

        Ok(())
    }
}

/// Constructs a `RocflError::NotFound` error
pub fn not_found(object_id: &str, version_num: Option<VersionNum>) -> RocflError {
    match version_num {
        Some(version) => RocflError::NotFound(format!("Object {} version {}", object_id, version)),
        None => RocflError::NotFound(format!("Object {}", object_id)),
    }
}

/// Constructs a `RocflError::NotFound` error for paths
pub fn not_found_path(
    object_id: &str,
    version_num: VersionNum,
    logical_path: &LogicalPath,
) -> RocflError {
    RocflError::NotFound(format!(
        "Object {} version {} path {}",
        object_id, version_num, logical_path
    ))
}

impl Debug for RocflError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl From<io::Error> for RocflError {
    fn from(e: io::Error) -> Self {
        RocflError::Io(e)
    }
}

impl From<globset::Error> for RocflError {
    fn from(e: globset::Error) -> Self {
        RocflError::Wrapped(Box::new(e))
    }
}

impl From<serde_json::Error> for RocflError {
    fn from(e: serde_json::Error) -> Self {
        RocflError::Wrapped(Box::new(e))
    }
}

impl From<toml::de::Error> for RocflError {
    fn from(e: toml::de::Error) -> Self {
        RocflError::Wrapped(Box::new(e))
    }
}

impl From<walkdir::Error> for RocflError {
    fn from(e: walkdir::Error) -> Self {
        RocflError::Wrapped(Box::new(e))
    }
}

impl From<ctrlc::Error> for RocflError {
    fn from(e: ctrlc::Error) -> Self {
        RocflError::Wrapped(Box::new(e))
    }
}

#[cfg(feature = "s3")]
impl From<ParseRegionError> for RocflError {
    fn from(e: ParseRegionError) -> Self {
        RocflError::Wrapped(Box::new(e))
    }
}

#[cfg(feature = "s3")]
impl<T: error::Error + 'static> From<RusotoError<T>> for RocflError {
    fn from(e: RusotoError<T>) -> Self {
        RocflError::Wrapped(Box::new(e))
    }
}

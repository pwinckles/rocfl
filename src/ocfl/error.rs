use core::fmt;
use std::{error, io};
use std::fmt::{Debug, Display, Formatter};

#[cfg(feature = "s3")]
use rusoto_core::region::ParseRegionError;
#[cfg(feature = "s3")]
use rusoto_core::RusotoError;
use thiserror::Error;

pub type Result<T, E = RocflError> = core::result::Result<T, E>;

/// Application errors
#[derive(Error)]
pub enum RocflError {
    #[error("Object {object_id} is corrupt: {message}")]
    CorruptObject {
        object_id: String,
        message: String,
    },

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Illegal argument: {0}")]
    IllegalArgument(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Illegal state: {0}")]
    IllegalState(String),

    #[error("{0}")]
    General(String),

    #[error("{0}")]
    Io(io::Error),

    #[error("{0}")]
    Wrapped(Box<dyn error::Error>),
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
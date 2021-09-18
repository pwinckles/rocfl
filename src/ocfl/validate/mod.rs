use std::cell::RefCell;

use strum_macros::Display as EnumDisplay;

use crate::ocfl::error::{Result, RocflError};
use crate::ocfl::inventory::Inventory;
use crate::ocfl::DigestAlgorithm;

mod serde;

#[derive(Debug)]
pub enum ParseResult {
    Ok(ValidationResult, Inventory),
    Error(ValidationResult),
}

#[derive(Debug)]
pub struct ValidationResult {
    errors: RefCell<Vec<ValidationError>>,
    warnings: RefCell<Vec<ValidationWarning>>,
}

#[derive(Debug)]
pub struct ValidationError {
    code: ErrorCode,
    text: String,
}

// TODO move
impl ValidationError {
    fn new(code: ErrorCode, text: String) -> Self {
        Self { code, text }
    }
}

#[derive(Debug)]
pub struct ValidationWarning {
    code: WarnCode,
    text: String,
}

// TODO move
impl ValidationWarning {
    fn new(code: WarnCode, text: String) -> Self {
        Self { code, text }
    }
}

#[allow(dead_code)]
#[derive(Debug, EnumDisplay, Copy, Clone, PartialEq)]
pub enum ErrorCode {
    E001,
    E002,
    E003,
    E004,
    E005,
    E006,
    E007,
    E008,
    E009,
    E010,
    E011,
    E012,
    E013,
    E014,
    E015,
    E016,
    E017,
    E018,
    E019,
    E020,
    E021,
    E022,
    E023,
    E024,
    E025,
    E026,
    E027,
    E028,
    E029,
    E030,
    E031,
    E032,
    E033,
    E034,
    E035,
    E036,
    E037,
    E038,
    E039,
    E040,
    E041,
    E042,
    E043,
    E044,
    E045,
    E046,
    E047,
    E048,
    E049,
    E050,
    E051,
    E052,
    E053,
    E054,
    E055,
    E056,
    E057,
    E058,
    E059,
    E060,
    E061,
    E062,
    E063,
    E064,
    E066,
    E067,
    E068,
    E069,
    E070,
    E071,
    E072,
    E073,
    E074,
    E075,
    E076,
    E077,
    E078,
    E079,
    E080,
    E081,
    E082,
    E083,
    E084,
    E085,
    E086,
    E087,
    E088,
    E089,
    E090,
    E091,
    E092,
    E093,
    E094,
    E095,
    E096,
    E097,
    E098,
    E099,
    E100,
    E101,
    E102,
}

#[allow(dead_code)]
#[derive(Debug, EnumDisplay, Copy, Clone, PartialEq)]
pub enum WarnCode {
    W001,
    W002,
    W003,
    W004,
    W005,
    W006,
    W007,
    W008,
    W009,
    W010,
    W011,
    W012,
    W013,
    W014,
    W015,
}

// TODO move
impl ValidationResult {
    pub fn new() -> Self {
        Self {
            errors: RefCell::new(Vec::new()),
            warnings: RefCell::new(Vec::new()),
        }
    }

    pub fn error(&self, code: ErrorCode, message: String) {
        self.errors
            .borrow_mut()
            .push(ValidationError::new(code, message));
    }

    pub fn warn(&self, code: WarnCode, message: String) {
        self.warnings
            .borrow_mut()
            .push(ValidationWarning::new(code, message));
    }

    pub fn has_errors(&self) -> bool {
        self.errors.borrow().len() > 0
    }
}

pub fn validate_object_id(object_id: &str) -> Result<()> {
    if object_id.is_empty() {
        return Err(RocflError::InvalidValue(
            "Object IDs may not be blank".to_string(),
        ));
    }
    Ok(())
}

pub fn validate_digest_algorithm(digest_algorithm: DigestAlgorithm) -> Result<()> {
    if digest_algorithm != DigestAlgorithm::Sha512 && digest_algorithm != DigestAlgorithm::Sha256 {
        return Err(RocflError::InvalidValue(format!(
            "The inventory digest algorithm must be sha512 or sha256. Found: {}",
            digest_algorithm.to_string()
        )));
    }
    Ok(())
}

pub fn validate_content_dir(content_dir: &str) -> Result<()> {
    if content_dir.eq(".") || content_dir.eq("..") || content_dir.contains('/') {
        return Err(RocflError::InvalidValue(format!(
            "The content directory cannot equal '.' or '..' and cannot contain a '/'. Found: {}",
            content_dir
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::ocfl::RocflError;
}

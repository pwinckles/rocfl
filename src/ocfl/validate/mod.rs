use std::borrow::Cow;
use std::cell::RefCell;
use std::str::FromStr;

use strum_macros::Display as EnumDisplay;

use crate::ocfl::consts::{
    INVENTORY_FILE, INVENTORY_SIDECAR_PREFIX, INVENTORY_TYPE, OBJECT_NAMASTE_CONTENTS_1_0,
    OBJECT_NAMASTE_FILE,
};
use crate::ocfl::digest::{MultiDigestWriter, HexDigest};
use crate::ocfl::error::{Result, RocflError};
use crate::ocfl::inventory::Inventory;
use crate::ocfl::validate::store::{Listing, Storage};
use crate::ocfl::{paths, DigestAlgorithm, VersionNum};

mod serde;
mod store;

// TODO
pub struct Validator<S: Storage> {
    storage: S,
}

// TODO move
#[derive(Debug)]
enum ParseResult {
    Ok(ParseValidationResult, Inventory),
    Error(ParseValidationResult),
}

#[derive(Debug)]
struct ParseValidationResult {
    errors: RefCell<Vec<ValidationError>>,
    warnings: RefCell<Vec<ValidationWarning>>,
}

#[derive(Debug)]
pub struct ValidationResult {
    pub object_id: Option<String>,
    pub errors: Vec<ValidationError>,
    pub warnings: Vec<ValidationWarning>,
}

// TODO move

impl ValidationResult {
    pub fn new() -> Self {
        Self {
            object_id: None,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    pub fn with_id(object_id: &str) -> Self {
        Self {
            object_id: Some(object_id.to_string()),
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    fn add_parse_result(&mut self, version: &str, result: ParseValidationResult) {
        self.errors
            .extend(result.errors.take().into_iter().map(|mut e| {
                e.version_num = Some(version.to_string());
                e
            }));
        self.warnings
            .extend(result.warnings.take().into_iter().map(|mut w| {
                w.version_num = Some(version.to_string());
                w
            }));
    }

    pub fn error(&mut self, code: ErrorCode, message: String) {
        self.errors.push(ValidationError::new(code, message));
    }

    pub fn warn(&mut self, code: WarnCode, message: String) {
        self.warnings.push(ValidationWarning::new(code, message));
    }

    pub fn error_version(&mut self, version_num: String, code: ErrorCode, message: String) {
        self.errors
            .push(ValidationError::with_version(version_num, code, message));
    }

    pub fn warn_version(&mut self, version_num: String, code: WarnCode, message: String) {
        self.warnings
            .push(ValidationWarning::with_version(version_num, code, message));
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }
}

#[derive(Debug)]
pub struct ValidationError {
    pub version_num: Option<String>,
    pub code: ErrorCode,
    pub text: String,
}

// TODO move
impl ValidationError {
    pub fn new(code: ErrorCode, text: String) -> Self {
        Self {
            version_num: None,
            code,
            text,
        }
    }

    pub fn with_version(version_num: String, code: ErrorCode, text: String) -> Self {
        Self {
            version_num: Some(version_num),
            code,
            text,
        }
    }
}

#[derive(Debug)]
pub struct ValidationWarning {
    pub version_num: Option<String>,
    pub code: WarnCode,
    pub text: String,
}

// TODO move
impl ValidationWarning {
    pub fn new(code: WarnCode, text: String) -> Self {
        Self {
            version_num: None,
            code,
            text,
        }
    }

    pub fn with_version(version_num: String, code: WarnCode, text: String) -> Self {
        Self {
            version_num: Some(version_num),
            code,
            text,
        }
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

// TODO
impl<S: Storage> Validator<S> {
    pub fn validate_object(
        &self,
        object_id: &str,
        object_root: &str,
        fixity_check: bool,
    ) -> Result<ValidationResult> {
        let mut result = ValidationResult::with_id(object_id);

        // TODO error handling
        let root_listing = self.storage.list(object_root, false)?;

        if root_listing.contains(&Listing::File(Cow::Borrowed(OBJECT_NAMASTE_FILE))) {
            // TODO this should also determine what the version is
            self.validate_object_namaste(object_root, &mut result);
        } else {
            result.error(
                ErrorCode::E003,
                "Object version declaration does not exist".to_string(),
            );
        }

        let mut algorithms = Vec::new();

        for entry in &root_listing {
            if let Listing::File(filename) = entry {
                if let Some(algorithm) = filename.strip_prefix(INVENTORY_SIDECAR_PREFIX) {
                    if let Ok(algorithm) = DigestAlgorithm::from_str(algorithm) {
                        algorithms.push(algorithm);
                    }
                }
            }
        }

        let (inventory, digest) = self.validate_inventory(
            &paths::join(object_root, INVENTORY_FILE),
            None,
            &algorithms,
            &mut result,
        );

        // TODO validate sidecar

        // TODO id validation
        // TODO E083 when root with expected id
        // TODO E037 when comparing to root https://github.com/OCFL/spec/issues/542

        // TODO validate root inventory
        // TODO validate sidecar
        // TODO validate root contents

        // TODO don't forget to compare contentDirectory

        todo!()
    }

    pub fn validate_repo(&self, fixity_check: bool) {
        todo!()
    }

    // TODO this should resolve the OCFL object version
    fn validate_object_namaste(&self, object_root: &str, result: &mut ValidationResult) {
        // TODO only valid for 1.0
        let path = paths::join(object_root, OBJECT_NAMASTE_FILE);
        let mut bytes: Vec<u8> = Vec::new();
        if self.storage.read(&path, &mut bytes).is_err() {
            match String::from_utf8(bytes) {
                Ok(contents) => {
                    // TODO only valid for 1.0
                    if contents != OBJECT_NAMASTE_CONTENTS_1_0 {
                        result.error(
                            ErrorCode::E007,
                            format!(
                                "Object version declaration is invalid. Expected: {}; Found: {}",
                                OBJECT_NAMASTE_CONTENTS_1_0, contents
                            ),
                        );
                    }
                }
                Err(_) => {
                    result.error(
                        ErrorCode::E007,
                        "Object version declaration contains invalid UTF-8 content".to_string(),
                    );
                }
            }
        } else {
            result.error(
                ErrorCode::E003,
                "Object version declaration does not exist".to_string(),
            );
        }
    }

    fn validate_inventory(
        &self,
        inventory_path: &str,
        version: Option<VersionNum>,
        algorithms: &[DigestAlgorithm],
        result: &mut ValidationResult,
    ) -> (Option<Inventory>, Option<HexDigest>) {
        let mut inventory = None;
        let mut digest = None;

        let mut writer = MultiDigestWriter::new(algorithms, Vec::new());

        fn version_str(version: Option<VersionNum>) -> String {
            match version {
                Some(version) => version.to_string(),
                None => "root".to_string(),
            }
        }

        if self.storage.read(inventory_path, &mut writer).is_ok() {
            match serde_json::from_slice::<ParseResult>(writer.inner()) {
                Ok(parse_result) => match parse_result {
                    ParseResult::Ok(parse_result, inv) => {
                        // TODO this is only valid for 1.0
                        if inv.type_declaration != INVENTORY_TYPE {
                            parse_result.error(
                                ErrorCode::E038,
                                format!(
                                    "Inventory field 'type' must equal '{}'. Found: {}",
                                    INVENTORY_TYPE, inv.type_declaration
                                ),
                            );
                        }

                        if let Some(version) = version {
                            if inv.head != version {
                                // TODO suspect code
                                parse_result.error(
                                    ErrorCode::E040,
                                    format!(
                                        "Inventory field 'head' must equal '{}'. Found: {}",
                                        version, inv.head
                                    ),
                                );
                            }
                        }

                        let has_errors = parse_result.has_errors();

                        result.add_parse_result(&version_str(version), parse_result);

                        digest = writer.finalize_hex().remove(&inv.digest_algorithm);
                        if !has_errors {
                            inventory = Some(inv);
                        }
                    }
                    ParseResult::Error(parse_result) => {
                        result.add_parse_result(&version_str(version), parse_result)
                    }
                },
                Err(_) => {
                    if let Some(version) = version {
                        result.error(
                            ErrorCode::E033,
                            format!("Version {} inventory could not be parsed", version),
                        );
                    } else {
                        result.error(
                            ErrorCode::E033,
                            "Root inventory could not be parsed".to_string(),
                        );
                    }
                }
            }
        } else if version.is_none() {
            result.error(ErrorCode::E063, "Root inventory does not exist".to_string());
        }

        (inventory, digest)
    }
}

// TODO move
impl ParseValidationResult {
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

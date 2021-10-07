use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::slice::Iter;
use std::str::FromStr;

use once_cell::sync::Lazy;
use regex::Regex;
use strum_macros::Display as EnumDisplay;

use crate::ocfl::consts::{
    EXTENSIONS_DIR, INVENTORY_FILE, INVENTORY_SIDECAR_PREFIX, INVENTORY_TYPE, LOGS_DIR,
    OBJECT_NAMASTE_CONTENTS_1_0, OBJECT_NAMASTE_FILE, OCFL_OBJECT_VERSION, OCFL_VERSION,
    REPO_NAMASTE_CONTENTS_1_0, REPO_NAMASTE_FILE, SUPPORTED_EXTENSIONS,
};
use crate::ocfl::digest::{HexDigest, MultiDigestWriter};
use crate::ocfl::error::{Result, RocflError};
use crate::ocfl::inventory::Inventory;
use crate::ocfl::validate::store::{Listing, Storage};
use crate::ocfl::{
    paths, util, ContentPath, ContentPathVersion, DigestAlgorithm, InventoryPath, PrettyPrintSet,
    VersionNum,
};

mod serde;
pub mod store;

static SIDECAR_SPLIT: Lazy<Regex> = Lazy::new(|| Regex::new(r#"[\t ]+"#).unwrap());
static EMPTY_PATHS: Vec<ContentPath> = vec![];

/// If `object_id` is empty, then an `InvalidValue` error is returned. This does not enforce that
/// the id is a URI.
pub fn validate_object_id(object_id: &str) -> Result<()> {
    if object_id.is_empty() {
        return Err(RocflError::InvalidValue(
            "Object IDs may not be blank".to_string(),
        ));
    }
    Ok(())
}

/// If `digest_algorithm` is not `sha256` or `sha512`, then an `InvalidValue` error is returned.
pub fn validate_digest_algorithm(digest_algorithm: DigestAlgorithm) -> Result<()> {
    if digest_algorithm != DigestAlgorithm::Sha512 && digest_algorithm != DigestAlgorithm::Sha256 {
        return Err(RocflError::InvalidValue(format!(
            "The inventory digest algorithm must be sha512 or sha256. Found: {}",
            digest_algorithm
        )));
    }
    Ok(())
}

/// If `content_dir` contains `.`, `..`, or `/`, then an `InvalidValue` error is returned.
pub fn validate_content_dir(content_dir: &str) -> Result<()> {
    if content_dir.eq(".") || content_dir.eq("..") || content_dir.contains('/') {
        return Err(RocflError::InvalidValue(format!(
            "The content directory cannot equal '.' or '..' and cannot contain a '/'. Found: {}",
            content_dir
        )));
    }
    Ok(())
}

/// OCFL validation codes for errors: https://ocfl.io/1.0/spec/validation-codes.html
#[allow(dead_code)]
#[derive(Debug, EnumDisplay, Copy, Clone, Eq, PartialEq)]
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

/// OCFL validation codes for warnings: https://ocfl.io/1.0/spec/validation-codes.html
#[allow(dead_code)]
#[derive(Debug, EnumDisplay, Copy, Clone, Eq, PartialEq)]
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
pub trait ValidationResult {
    fn has_errors(&self) -> bool;

    fn has_warnings(&self) -> bool;

    fn errors(&self) -> &[ValidationError];

    fn warnings(&self) -> &[ValidationWarning];

    fn error(&mut self, location: ProblemLocation, code: ErrorCode, message: String);

    fn warn(&mut self, location: ProblemLocation, code: WarnCode, message: String);
}

/// The the results of validating the structure of an OCFL repository
#[derive(Debug)]
pub struct StorageValidationResult {
    /// Any errors identified in the storage hierarchy
    errors: Vec<ValidationError>,
    /// Any warning identified in the storage hierarchy
    warnings: Vec<ValidationWarning>,
}

/// The the results of validating an OCFL object
#[derive(Debug)]
pub struct ObjectValidationResult {
    /// The id of the object, if known
    pub object_id: Option<String>,
    /// The path to the object's root directory, relative the repository root
    pub storage_path: String,
    /// Any errors identified in the object
    errors: Vec<ValidationError>,
    /// Any warning identified in the object
    warnings: Vec<ValidationWarning>,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum ProblemLocation {
    StorageRoot,
    StorageHierarchy,
    ObjectRoot,
    ObjectVersion(VersionNum),
}

#[derive(Debug, Eq, PartialEq)]
pub struct ValidationError {
    /// Indicates where the problem occurred
    pub location: ProblemLocation,
    /// The validation code the error maps to
    pub code: ErrorCode,
    /// A specific description of the problem
    pub text: String,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ValidationWarning {
    /// Indicates where the problem occurred
    pub location: ProblemLocation,
    /// The validation code the warning maps to
    pub code: WarnCode,
    /// A specific description of the problem
    pub text: String,
}

/// A validator that's able to validate OCFL objects and repositories against the OCFL spec
pub struct Validator<S: Storage> {
    /// Storage abstraction used to access files in any backend
    storage: S,
}

pub trait IncrementalValidator {
    fn storage_root_result(&self) -> &StorageValidationResult;
    // TODO
}

// TODO
pub struct IncrementalValidatorImpl<'a, S: Storage> {
    storage_root_result: StorageValidationResult,
    validator: &'a Validator<S>,
    storage: &'a S,
    fixity_check: bool,
}

/// The result of deserializing an inventory
#[derive(Debug)]
enum ParseResult {
    /// Indicates the inventory was successfully deserialized and no errors were detected. The
    /// `ParseValidationResult` may still contain warnings.
    Ok(ParseValidationResult, Inventory),
    /// Indicates the inventory either could not be deserialized or errors were detected during
    /// validation. The `ParseValidationResult` will contain at least one error.
    Error(ParseValidationResult),
}

/// Errors or warnings identified while deserializing an inventory
#[derive(Debug)]
struct ParseValidationResult {
    object_id: RefCell<Option<String>>,
    errors: RefCell<Vec<ValidationError>>,
    warnings: RefCell<Vec<ValidationWarning>>,
}

struct ContentPaths {
    path_map: HashMap<VersionNum, Vec<ContentPath>>,
}

struct ContentPathsIter<'a> {
    current_version: VersionNum,
    current_iter: Iter<'a, ContentPath>,
    path_map: &'a HashMap<VersionNum, Vec<ContentPath>>,
}

impl StorageValidationResult {
    pub fn new() -> Self {
        Self {
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }
}

impl ValidationResult for StorageValidationResult {
    /// True if any errors were identified
    fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// True if any warnings were identified
    fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    fn errors(&self) -> &[ValidationError] {
        &self.errors
    }

    fn warnings(&self) -> &[ValidationWarning] {
        &self.warnings
    }

    fn error(&mut self, location: ProblemLocation, code: ErrorCode, message: String) {
        self.errors
            .push(ValidationError::new(location, code, message));
    }

    fn warn(&mut self, location: ProblemLocation, code: WarnCode, message: String) {
        self.warnings
            .push(ValidationWarning::new(location, code, message));
    }
}

impl Default for StorageValidationResult {
    fn default() -> Self {
        StorageValidationResult::new()
    }
}

impl ObjectValidationResult {
    pub fn new(object_id: Option<&str>, storage_path: String) -> Self {
        Self {
            object_id: object_id.map(String::from),
            storage_path,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    fn object_id(&mut self, object_id: &str) {
        if self.object_id.is_none() {
            self.object_id = Some(object_id.to_string());
        }
    }

    fn add_parse_result(&mut self, version_num: Option<VersionNum>, result: ParseValidationResult) {
        self.errors
            .extend(result.errors.take().into_iter().map(|mut e| {
                e.location = version_num.into();
                e
            }));
        self.warnings
            .extend(result.warnings.take().into_iter().map(|mut w| {
                w.location = version_num.into();
                w
            }));
    }
}

impl ValidationResult for ObjectValidationResult {
    /// True if any errors were identified
    fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// True if any warnings were identified
    fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    fn errors(&self) -> &[ValidationError] {
        &self.errors
    }

    fn warnings(&self) -> &[ValidationWarning] {
        &self.warnings
    }

    fn error(&mut self, location: ProblemLocation, code: ErrorCode, message: String) {
        self.errors
            .push(ValidationError::new(location, code, message));
    }

    fn warn(&mut self, location: ProblemLocation, code: WarnCode, message: String) {
        self.warnings
            .push(ValidationWarning::new(location, code, message));
    }
}

impl From<Option<VersionNum>> for ProblemLocation {
    fn from(version_num: Option<VersionNum>) -> Self {
        version_num.map_or_else(
            || ProblemLocation::ObjectRoot,
            ProblemLocation::ObjectVersion,
        )
    }
}

impl From<VersionNum> for ProblemLocation {
    fn from(version_num: VersionNum) -> Self {
        ProblemLocation::ObjectVersion(version_num)
    }
}

impl ValidationError {
    pub fn new(location: ProblemLocation, code: ErrorCode, text: String) -> Self {
        Self {
            location,
            code,
            text,
        }
    }
}

impl ValidationWarning {
    pub fn new(location: ProblemLocation, code: WarnCode, text: String) -> Self {
        Self {
            location,
            code,
            text,
        }
    }
}

// TODO need to hookup ctrl+c

impl<S: Storage> Validator<S> {
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    /// Validates an object at a specific location relative the repository root. if `fixity_check`
    /// is false, then the digests of the object's content files will not be validated.
    pub fn validate_object(
        &self,
        object_id: Option<&str>,
        object_root: &str,
        fixity_check: bool,
    ) -> Result<ObjectValidationResult> {
        let mut result = ObjectValidationResult::new(
            object_id,
            util::convert_backslash_to_forward(object_root).to_string(),
        );

        // TODO path does not exist

        // TODO error handling ?

        let root_files = self.storage.list(object_root, false)?;

        self.validate_object_namaste(object_root, &root_files, &mut result);

        let (inventory, sidecar_file, digest) = self.validate_inventory_and_sidecar(
            object_id,
            None,
            object_root,
            &root_files,
            &mut result,
        )?;

        // If the root inventory is not valid, then we don't have a fixed point to use to validate
        // anything else in the object.
        if !result.has_errors() {
            self.validate_object_root_contents(
                object_root,
                &root_files,
                &inventory,
                &sidecar_file,
                &mut result,
            )?;

            if let (Some(inventory), Some(digest)) = (inventory, digest) {
                let mut inventories = HashMap::new();

                let content_files =
                    self.find_all_content_files(object_root, &inventory, &mut result)?;
                self.validate_manifest(
                    &inventory,
                    &content_files,
                    &inventory,
                    &inventories,
                    &mut result,
                );

                for (num, _) in inventory.versions.iter().rev() {
                    let version_dir = paths::join(object_root, &num.to_string());
                    if *num == inventory.head {
                        self.validate_head_version(&version_dir, &inventory, &digest, &mut result)?;
                    } else {
                        let inv = self.validate_version(
                            *num,
                            &version_dir,
                            &inventory,
                            &inventories,
                            &content_files,
                            &mut result,
                        )?;
                        if let Some(inv) = inv {
                            inventories.entry(inv.digest_algorithm).or_insert(inv);
                        }
                    }
                }

                if fixity_check {
                    self.fixity_check(
                        object_root,
                        &content_files,
                        &inventory,
                        &inventories,
                        &mut result,
                    )?;
                }
            }
        }

        Ok(result)
    }

    /// Validates the structure of an OCFL repository as well as all of the objects in the repository
    /// When `fixity_check` is `false`, then the digests of object content files are not validated.
    ///
    /// The storage root is validated immediately, and an incremental validator is returned that
    /// is used to lazily validate the rest of the repository.
    pub fn validate_repo(&self, fixity_check: bool) -> Result<IncrementalValidatorImpl<S>> {
        let mut root_result = StorageValidationResult::new();
        let files = self.storage.list("", false)?;

        self.validate_root_namaste(&files, &mut root_result);

        if files.contains(&Listing::dir(EXTENSIONS_DIR)) {
            let ext_files = self.storage.list(EXTENSIONS_DIR, false)?;
            self.validate_extension_contents(&ext_files, &mut root_result)?;
        }

        // TODO E070 ocfl_layout.json contents

        // TODO W015 should either be direct children or hierarchy; not both

        // TODO E072 no files
        // TODO E073 no emtpy dirs in hierarchy
        // TODO E090 no links

        // TODO E037 object id unique
        // TODO E081 object ocfl versions must be same or earlier

        Ok(IncrementalValidatorImpl::new(
            root_result,
            self,
            &self.storage,
            fixity_check,
        ))
    }

    // TODO this should resolve the OCFL object version
    fn validate_root_namaste(&self, files: &[Listing], result: &mut StorageValidationResult) {
        if files.contains(&Listing::file(REPO_NAMASTE_FILE)) {
            // TODO only valid for 1.0
            let mut bytes: Vec<u8> = Vec::new();
            if self.storage.read(REPO_NAMASTE_FILE, &mut bytes).is_ok() {
                match String::from_utf8(bytes) {
                    Ok(contents) => {
                        // TODO only valid for 1.0
                        if contents != REPO_NAMASTE_CONTENTS_1_0 {
                            result.error(
                                ProblemLocation::StorageRoot,
                                ErrorCode::E080,
                                format!(
                                    "Root version declaration is invalid. Expected: {}; Found: {}",
                                    OCFL_VERSION, contents
                                ),
                            );
                        }
                    }
                    Err(_) => {
                        result.error(
                            ProblemLocation::StorageRoot,
                            ErrorCode::E080,
                            "Root version declaration contains invalid UTF-8 content".to_string(),
                        );
                    }
                }
            } else {
                result.error(
                    ProblemLocation::StorageRoot,
                    ErrorCode::E069,
                    "Root version declaration does not exist".to_string(),
                );
            }
        } else {
            result.error(
                ProblemLocation::StorageRoot,
                ErrorCode::E069,
                "Root version declaration does not exist".to_string(),
            );
        }
    }

    // TODO this should resolve the OCFL object version
    fn validate_object_namaste(
        &self,
        object_root: &str,
        files: &[Listing],
        result: &mut ObjectValidationResult,
    ) {
        if files.contains(&Listing::file(OBJECT_NAMASTE_FILE)) {
            // TODO only valid for 1.0
            let path = paths::join(object_root, OBJECT_NAMASTE_FILE);
            let mut bytes: Vec<u8> = Vec::new();
            if self.storage.read(&path, &mut bytes).is_ok() {
                match String::from_utf8(bytes) {
                    Ok(contents) => {
                        // TODO only valid for 1.0
                        if contents != OBJECT_NAMASTE_CONTENTS_1_0 {
                            result.error(
                                ProblemLocation::ObjectRoot,
                                ErrorCode::E007,
                                format!(
                                    "Object version declaration is invalid. Expected: {}; Found: {}",
                                    OCFL_OBJECT_VERSION, contents
                                ),
                            );
                        }
                    }
                    Err(_) => {
                        result.error(
                            ProblemLocation::ObjectRoot,
                            ErrorCode::E007,
                            "Object version declaration contains invalid UTF-8 content".to_string(),
                        );
                    }
                }
            } else {
                result.error(
                    ProblemLocation::ObjectRoot,
                    ErrorCode::E003,
                    "Object version declaration does not exist".to_string(),
                );
            }
        } else {
            result.error(
                ProblemLocation::ObjectRoot,
                ErrorCode::E003,
                "Object version declaration does not exist".to_string(),
            );
        }
    }

    fn validate_inventory_and_sidecar(
        &self,
        object_id: Option<&str>,
        version_num: Option<VersionNum>,
        path: &str,
        files: &[Listing],
        result: &mut ObjectValidationResult,
    ) -> Result<(Option<Inventory>, Option<String>, Option<HexDigest>)> {
        let mut inventory = None;
        let mut sidecar_file = None;
        let mut digest = None;

        if files.contains(&Listing::file(INVENTORY_FILE)) {
            let mut algorithms = Vec::new();

            for entry in files {
                if let Listing::File(filename) = entry {
                    if let Some(algorithm) = filename.strip_prefix(INVENTORY_SIDECAR_PREFIX) {
                        if let Ok(algorithm) = DigestAlgorithm::from_str(algorithm) {
                            algorithms.push(algorithm);
                        }
                    }
                }
            }

            let (inv, inv_digest) = self.validate_inventory(
                &paths::join(path, INVENTORY_FILE),
                version_num,
                &algorithms,
                result,
            )?;
            inventory = inv;
            digest = inv_digest;

            if version_num.is_none() {
                if let (Some(inventory), Some(object_id)) = (&inventory, object_id) {
                    if object_id != inventory.id {
                        result.error(
                            ProblemLocation::ObjectRoot,
                            ErrorCode::E083,
                            format!(
                                "Inventory 'id' must equal '{}'. Found: {}",
                                object_id, inventory.id
                            ),
                        );
                    }
                }
            }

            let algorithm = match &inventory {
                Some(inventory) => Some(inventory.digest_algorithm),
                None => {
                    if algorithms.len() == 1 {
                        Some(algorithms[0])
                    } else {
                        None
                    }
                }
            };

            if let Some(algorithm) = algorithm {
                let sidecar = paths::sidecar_name(algorithm);
                if files.contains(&Listing::file(&sidecar)) {
                    if let Some(digest) = &digest {
                        self.validate_sidecar(
                            &paths::join(path, &sidecar),
                            version_num,
                            digest,
                            result,
                        )?;
                    }
                } else {
                    result.error(
                        version_num.into(),
                        ErrorCode::E058,
                        format!("Inventory sidecar {} does not exist", sidecar),
                    );
                }
                sidecar_file = Some(sidecar);
            }
        } else {
            result.error(
                version_num.into(),
                ErrorCode::E063,
                "Inventory does not exist".to_string(),
            );
        }

        Ok((inventory, sidecar_file, digest))
    }

    fn validate_inventory(
        &self,
        inventory_path: &str,
        version: Option<VersionNum>,
        algorithms: &[DigestAlgorithm],
        result: &mut ObjectValidationResult,
    ) -> Result<(Option<Inventory>, Option<HexDigest>)> {
        let mut inventory = None;
        let mut digest = None;

        let mut writer = MultiDigestWriter::new(algorithms, Vec::new());

        self.storage.read(inventory_path, &mut writer)?;

        match serde::parse(writer.inner()) {
            ParseResult::Ok(parse_result, inv) => {
                result.object_id(&inv.id);

                // TODO this is only valid for 1.0
                if inv.type_declaration != INVENTORY_TYPE {
                    parse_result.error(
                        ErrorCode::E038,
                        format!(
                            "Inventory 'type' must equal '{}'. Found: {}",
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
                                "Inventory 'head' must equal '{}'. Found: {}",
                                version, inv.head
                            ),
                        );
                    }
                }

                let has_errors = parse_result.has_errors();

                result.add_parse_result(version, parse_result);

                digest = writer.finalize_hex().remove(&inv.digest_algorithm);
                if !has_errors {
                    inventory = Some(inv);
                }
            }
            ParseResult::Error(mut parse_result) => {
                result.object_id =
                    std::mem::replace(&mut parse_result.object_id, RefCell::new(None)).take();
                result.add_parse_result(version, parse_result)
            }
        }

        Ok((inventory, digest))
    }

    fn validate_sidecar(
        &self,
        sidecar_path: &str,
        version: Option<VersionNum>,
        digest: &HexDigest,
        result: &mut ObjectValidationResult,
    ) -> Result<()> {
        let mut bytes = Vec::new();
        self.storage.read(sidecar_path, &mut bytes)?;
        match String::from_utf8(bytes) {
            Ok(contents) => {
                let parts: Vec<&str> = SIDECAR_SPLIT.split(&contents).collect();
                if parts.len() != 2 || parts[1].trim_end() != INVENTORY_FILE {
                    result.error(
                        version.into(),
                        ErrorCode::E061,
                        "Inventory sidecar is invalid".to_string(),
                    )
                } else {
                    let expected_digest = HexDigest::from(parts[0]);
                    if expected_digest != *digest {
                        result.error(
                            version.into(),
                            ErrorCode::E060,
                            format!(
                                "Inventory does not match expected digest. Expected: {}; Found: {}",
                                expected_digest, digest
                            ),
                        );
                    }
                }
            }
            Err(_) => result.error(
                version.into(),
                ErrorCode::E061,
                "Inventory sidecar is invalid".to_string(),
            ),
        }

        Ok(())
    }

    fn validate_object_root_contents(
        &self,
        object_root: &str,
        files: &[Listing],
        inventory: &Option<Inventory>,
        sidecar_file: &Option<String>,
        result: &mut ObjectValidationResult,
    ) -> Result<()> {
        let mut expected_files = Vec::with_capacity(5);

        expected_files.push(Listing::file(OBJECT_NAMASTE_FILE));
        expected_files.push(Listing::file(INVENTORY_FILE));
        expected_files.push(Listing::dir(LOGS_DIR));
        expected_files.push(Listing::dir(EXTENSIONS_DIR));

        if let Some(sidecar_file) = &sidecar_file {
            expected_files.push(Listing::file(sidecar_file))
        }

        let mut expected_versions = match &inventory {
            Some(inventory) => {
                let mut expected_versions = HashSet::with_capacity(inventory.versions.len());
                inventory.versions.keys().for_each(|v| {
                    expected_versions.insert(Listing::dir_owned(v.to_string()));
                });
                Some(expected_versions)
            }
            None => None,
        };

        for entry in files {
            let found = match &mut expected_versions {
                Some(expected_versions) => expected_versions.remove(entry),
                None => false,
            };

            if !found && !expected_files.contains(entry) {
                if let Listing::Other(path) = entry {
                    result.error(
                        ProblemLocation::ObjectRoot,
                        ErrorCode::E090,
                        format!("Object root contains an illegal file: {}", path),
                    );
                } else {
                    result.error(
                        ProblemLocation::ObjectRoot,
                        ErrorCode::E001,
                        format!("Unexpected file in object root: {}", entry.path()),
                    );
                }
            }
        }

        if let Some(expected_versions) = expected_versions {
            expected_versions.iter().for_each(|v| {
                result.error(
                    ProblemLocation::ObjectRoot,
                    ErrorCode::E010,
                    format!(
                        "Object root does not contain version directory '{}'",
                        v.path()
                    ),
                );
            });
        }

        if files.contains(&Listing::dir(EXTENSIONS_DIR)) {
            let extensions = paths::join(object_root, EXTENSIONS_DIR);
            let ext_files = self.storage.list(&extensions, false)?;
            self.validate_extension_contents(&ext_files, result)?;
        }

        Ok(())
    }

    fn validate_extension_contents<V: ValidationResult>(
        &self,
        ext_files: &[Listing],
        result: &mut V,
    ) -> Result<()> {
        for file in ext_files {
            match file {
                Listing::Directory(path) => {
                    if !SUPPORTED_EXTENSIONS.contains(path.as_ref()) {
                        result.warn(
                            ProblemLocation::ObjectRoot,
                            WarnCode::W013,
                            format!(
                                "Object extensions directory contains unknown extension: {}",
                                path
                            ),
                        );
                    }
                }
                Listing::File(path) | Listing::Other(path) => {
                    result.error(
                        ProblemLocation::ObjectRoot,
                        ErrorCode::E067,
                        format!(
                            "Object extensions directory contains an illegal file: {}",
                            path
                        ),
                    );
                }
            }
        }

        Ok(())
    }

    fn find_all_content_files(
        &self,
        object_root: &str,
        root_inventory: &Inventory,
        result: &mut ObjectValidationResult,
    ) -> Result<ContentPaths> {
        let mut content_paths = ContentPaths::new();

        for version in root_inventory.versions.keys() {
            let prefix = paths::join(&version.to_string(), root_inventory.defaulted_content_dir());
            let content_root = paths::join(object_root, &prefix);

            let paths = self.storage.list(&content_root, true)?;

            for path in &paths {
                let full_path = paths::join(&prefix, path.path());

                match path {
                    Listing::File(_) => {
                        // TODO error handling
                        content_paths.add_path(ContentPath::try_from(full_path)?);
                    }
                    Listing::Directory(_) => {
                        result.error(
                            ProblemLocation::from(*version),
                            ErrorCode::E024,
                            format!(
                                "An empty directory exists within the content directory: {}",
                                full_path
                            ),
                        );
                    }
                    Listing::Other(_) => {
                        result.error(
                            ProblemLocation::from(*version),
                            ErrorCode::E090,
                            format!("Content directory contains an illegal file: {}", full_path),
                        );
                    }
                }
            }
        }

        Ok(content_paths)
    }

    fn validate_manifest(
        &self,
        inventory: &Inventory,
        content_files: &ContentPaths,
        root_inventory: &Inventory,
        inventories: &HashMap<DigestAlgorithm, Inventory>,
        result: &mut ObjectValidationResult,
    ) {
        let mut manifest_paths = inventory.manifest_paths();
        let mut fixity_paths = inventory.fixity_paths();
        let comparing_inventory = if root_inventory.digest_algorithm == inventory.digest_algorithm {
            Some(root_inventory)
        } else {
            // this will be the case for the first inventory after an algorithm change
            inventories.get(&inventory.digest_algorithm)
        };
        let context_version = if inventory.head == root_inventory.head {
            None
        } else {
            Some(inventory.head)
        };

        for content_file in content_files.iter(inventory.head) {
            fixity_paths.remove(content_file.as_str());
            if manifest_paths.remove(content_file) {
                if let Some(comparing_inventory) = comparing_inventory {
                    if let Some(expected) =
                        comparing_inventory.digest_for_content_path(content_file)
                    {
                        let digest = inventory.digest_for_content_path(content_file).unwrap();
                        if expected != digest {
                            result.error(
                                context_version.into(),
                                ErrorCode::E092,
                                format!(
                                    "Inventory manifest entry for content path '{}' differs from later versions. Expected: {}; Found: {}",
                                    content_file, expected, digest
                                ),
                            );
                        }
                    }
                }
            } else {
                result.error(
                    context_version.into(),
                    ErrorCode::E023,
                    format!(
                        "A content file exists that is not referenced in the manifest: {}",
                        content_file
                    ),
                );
            }
        }

        for path in manifest_paths {
            result.error(
                context_version.into(),
                ErrorCode::E092,
                format!(
                    "Inventory manifest references a file that does not exist in a content directory: {}",
                    path
                ),
            );
        }

        for path in fixity_paths {
            result.error(
                context_version.into(),
                ErrorCode::E093,
                format!(
                    "Inventory fixity references a file that does not exist in a content directory: {}",
                    path
                ),
            );
        }
    }

    fn validate_head_version(
        &self,
        version_dir: &str,
        inventory: &Inventory,
        root_digest: &HexDigest,
        result: &mut ObjectValidationResult,
    ) -> Result<()> {
        let files = self.storage.list(version_dir, false)?;

        if files.contains(&Listing::file(INVENTORY_FILE)) {
            let inventory_path = paths::join(version_dir, INVENTORY_FILE);
            let mut digester = inventory.digest_algorithm.writer(Vec::new()).unwrap();
            self.storage.read(&inventory_path, &mut digester)?;

            let digest = digester.finalize_hex();
            if digest != *root_digest {
                result.error(
                    inventory.head.into(),
                    ErrorCode::E064,
                    "Inventory file must be identical to the root inventory".to_string(),
                );
            }

            let sidecar_name = paths::sidecar_name(inventory.digest_algorithm);

            if files.contains(&Listing::file(&sidecar_name)) {
                let sidecar_path = paths::join(version_dir, &sidecar_name);
                self.validate_sidecar(&sidecar_path, Some(inventory.head), &digest, result)?;
            } else {
                result.error(
                    inventory.head.into(),
                    ErrorCode::E058,
                    format!("Inventory sidecar {} does not exist", sidecar_name),
                );
            }
        } else {
            result.warn(
                inventory.head.into(),
                WarnCode::W010,
                "Inventory file does not exist".to_string(),
            );
        }

        self.validate_version_contents(
            version_dir,
            &files,
            inventory.head,
            inventory.defaulted_content_dir(),
            inventory.digest_algorithm,
            result,
        )?;

        Ok(())
    }

    fn validate_version(
        &self,
        version_num: VersionNum,
        version_dir: &str,
        root_inventory: &Inventory,
        inventories: &HashMap<DigestAlgorithm, Inventory>,
        content_files: &ContentPaths,
        result: &mut ObjectValidationResult,
    ) -> Result<Option<Inventory>> {
        let mut inventory_opt = None;
        let files = self.storage.list(version_dir, false)?;

        let mut digest_algorithm = root_inventory.digest_algorithm;

        if files.contains(&Listing::file(INVENTORY_FILE)) {
            let (inventory, _, _) = self.validate_inventory_and_sidecar(
                Some(&root_inventory.id),
                Some(version_num),
                version_dir,
                &files,
                result,
            )?;

            if let Some(inventory) = inventory {
                digest_algorithm = inventory.digest_algorithm;

                if inventory.id != root_inventory.id {
                    result.error(
                        version_num.into(),
                        ErrorCode::E037,
                        format!(
                            "Inventory 'id' is inconsistent. Expected: {}; Found: {}",
                            root_inventory.id, inventory.id
                        ),
                    );
                }
                if inventory.defaulted_content_dir() != root_inventory.defaulted_content_dir() {
                    result.error(
                        version_num.into(),
                        ErrorCode::E019,
                        format!(
                            "Inventory 'contentDirectory' is inconsistent. Expected: {}; Found: {}",
                            root_inventory.defaulted_content_dir(),
                            inventory.defaulted_content_dir()
                        ),
                    );
                }
                if inventory.head.to_string() != version_num.to_string() {
                    result.error(
                        version_num.into(),
                        // TODO suspect code
                        ErrorCode::E040,
                        format!(
                            "Inventory 'head' must equal '{}'. Found: {}",
                            version_num, inventory.head
                        ),
                    );
                }

                self.validate_version_consistent(
                    version_num,
                    root_inventory,
                    &inventory,
                    inventories,
                    result,
                );

                self.validate_manifest(
                    &inventory,
                    content_files,
                    root_inventory,
                    inventories,
                    result,
                );

                inventory_opt = Some(inventory);
            }
        } else {
            result.warn(
                version_num.into(),
                WarnCode::W010,
                "Inventory file does not exist".to_string(),
            );
        }

        self.validate_version_contents(
            version_dir,
            &files,
            version_num,
            root_inventory.defaulted_content_dir(),
            digest_algorithm,
            result,
        )?;

        Ok(inventory_opt)
    }

    fn validate_version_consistent(
        &self,
        version_num: VersionNum,
        root_inventory: &Inventory,
        other_inventory: &Inventory,
        inventories: &HashMap<DigestAlgorithm, Inventory>,
        result: &mut ObjectValidationResult,
    ) {
        let mut current_num = version_num;
        let comparing_inventory =
            if root_inventory.digest_algorithm == other_inventory.digest_algorithm {
                Some(root_inventory)
            } else {
                inventories.get(&other_inventory.digest_algorithm)
            };

        loop {
            let root_version = root_inventory.get_version(current_num).unwrap();
            let other_version = other_inventory.get_version(current_num).unwrap();

            if let Some(comparing_inventory) = comparing_inventory {
                self.validate_state_consistent(
                    version_num,
                    current_num,
                    comparing_inventory,
                    other_inventory,
                    true,
                    result,
                );
            } else {
                self.validate_state_consistent(
                    version_num,
                    current_num,
                    root_inventory,
                    other_inventory,
                    false,
                    result,
                );
            }

            if root_version.message != other_version.message {
                result.warn(
                    version_num.into(),
                    WarnCode::W011,
                    format!(
                        "Inventory version {} 'message' is inconsistent with the root inventory",
                        current_num
                    ),
                );
            }

            if root_version.created != other_version.created {
                result.warn(
                    version_num.into(),
                    WarnCode::W011,
                    format!(
                        "Inventory version {} 'created' is inconsistent with the root inventory",
                        current_num
                    ),
                );
            }

            if root_version.user != other_version.user {
                result.warn(
                    version_num.into(),
                    WarnCode::W011,
                    format!(
                        "Inventory version {} 'user' is inconsistent with the root inventory",
                        current_num
                    ),
                );
            }

            if current_num == VersionNum::v1() {
                break;
            } else {
                current_num = current_num.previous().unwrap();
            }
        }
    }

    fn validate_state_consistent(
        &self,
        version_num: VersionNum,
        current_version: VersionNum,
        comparing_inventory: &Inventory,
        inventory: &Inventory,
        compare_digests: bool,
        result: &mut ObjectValidationResult,
    ) {
        let comparing_version = comparing_inventory.get_version(current_version).unwrap();
        let version = inventory.get_version(current_version).unwrap();

        let mut paths = version.logical_paths();

        for (comparing_path, comparing_digest) in comparing_version.state_iter() {
            paths.remove(comparing_path);
            match version.lookup_digest(comparing_path) {
                None => {
                    result.error(
                        version_num.into(),
                        ErrorCode::E066,
                        format!(
                            "Inventory version {} state is missing a path that exists in later inventories: {}",
                            current_version, comparing_path
                        ),
                    );
                }
                Some(digest) => {
                    if compare_digests {
                        if comparing_digest != digest {
                            result.error(
                                version_num.into(),
                                ErrorCode::E066,
                                format!(
                                    "In inventory version {}, path '{}' does not match the digest in later inventories. Expected: {}; Found: {}",
                                    current_version, comparing_path, comparing_digest, digest
                                ),
                            );
                        }
                    } else {
                        let comparing_content_paths =
                            comparing_inventory.content_paths(comparing_digest).unwrap();
                        let content_paths = inventory.content_paths(digest).unwrap();

                        if comparing_content_paths.len() == 1 {
                            if comparing_content_paths != content_paths {
                                result.error(
                                    version_num.into(),
                                    ErrorCode::E066,
                                    format!(
                                        "In inventory version {}, path '{}' maps to different content paths than it does in later inventories. Expected: {}; Found: {}",
                                        current_version, comparing_path, PrettyPrintSet(comparing_content_paths), PrettyPrintSet(content_paths)
                                    ),
                                );
                            }
                        } else {
                            let mut filtered_paths = HashSet::new();

                            for content_path in comparing_content_paths {
                                if let ContentPathVersion::VersionNum(num) = content_path.version {
                                    if num <= current_version {
                                        filtered_paths.insert(content_path.clone());
                                    }
                                }
                            }

                            if filtered_paths != *content_paths {
                                result.error(
                                    version_num.into(),
                                    ErrorCode::E066,
                                    format!(
                                        "In inventory version {}, path '{}' maps to different content paths than it does in later inventories. Expected: {}; Found: {}",
                                        current_version, comparing_path, PrettyPrintSet(&filtered_paths), PrettyPrintSet(content_paths)
                                    ),
                                );
                            }
                        }
                    }
                }
            }
        }

        for path in paths {
            result.error(
                version_num.into(),
                ErrorCode::E066,
                format!(
                    "Inventory version {} state contains a path not in later inventories: {}",
                    current_version, path
                ),
            );
        }
    }

    fn validate_version_contents(
        &self,
        version_dir: &str,
        files: &[Listing],
        version_num: VersionNum,
        content_dir: &str,
        digest_algorithm: DigestAlgorithm,
        result: &mut ObjectValidationResult,
    ) -> Result<()> {
        if files.contains(&Listing::dir(content_dir))
            && self
                .storage
                .list(&paths::join(version_dir, content_dir), false)?
                .is_empty()
        {
            result.warn(
                version_num.into(),
                WarnCode::W003,
                "Content directory exists but is empty".to_string(),
            );
        }

        let ignore = [
            Listing::file(INVENTORY_FILE),
            Listing::file_owned(paths::sidecar_name(digest_algorithm)),
            Listing::dir(content_dir),
        ];

        for file in files {
            if ignore.contains(file) {
                continue;
            }

            match file {
                Listing::File(name) => {
                    result.error(
                        version_num.into(),
                        ErrorCode::E015,
                        format!("Version directory contains unexpected file: {}", name),
                    );
                }
                Listing::Directory(name) => {
                    result.warn(
                        version_num.into(),
                        WarnCode::W002,
                        format!("Version directory contains unexpected directory: {}", name),
                    );
                }
                Listing::Other(name) => {
                    result.error(
                        version_num.into(),
                        ErrorCode::E090,
                        format!("Version directory contains an illegal file: {}", name),
                    );
                }
            }
        }

        Ok(())
    }

    fn fixity_check(
        &self,
        object_root: &str,
        content_files: &ContentPaths,
        root_inventory: &Inventory,
        inventories: &HashMap<DigestAlgorithm, Inventory>,
        result: &mut ObjectValidationResult,
    ) -> Result<()> {
        let root_algorithm = root_inventory.digest_algorithm;
        let mut fixity = root_inventory.invert_fixity();

        for path in content_files.iter(root_inventory.head) {
            if let Some(digest) = root_inventory.digest_for_content_path(path) {
                let mut expectations = HashMap::new();
                expectations.insert(root_algorithm, digest);

                if let Some(fixity) = &mut fixity {
                    if let Some(fixity_expectations) = fixity.get(path) {
                        for (algorithm, alt_digest) in fixity_expectations {
                            expectations.insert(*algorithm, alt_digest);
                        }
                    }
                }
                for (algorithm, inventory) in inventories {
                    if let Some(alt_digest) = inventory.digest_for_content_path(path) {
                        expectations.insert(*algorithm, alt_digest);
                    }
                }

                let algorithms: Vec<DigestAlgorithm> = expectations.keys().copied().collect();
                let mut digester = MultiDigestWriter::new(&algorithms, std::io::sink());

                let full_path = paths::join(object_root, path.as_str());

                self.storage.read(&full_path, &mut digester)?;

                for (algorithm, actual) in digester.finalize_hex() {
                    let expected = expectations.get(&algorithm).unwrap();
                    if actual != ***expected {
                        // TODO technically, one of these digests could be in the fixity block...
                        let code = if algorithm == DigestAlgorithm::Sha512
                            || algorithm == DigestAlgorithm::Sha256
                        {
                            ErrorCode::E092
                        } else {
                            ErrorCode::E093
                        };

                        result.error(
                            ProblemLocation::ObjectRoot,
                            code,
                            format!(
                                "Content file {} failed {} fixity check. Expected: {}; Found: {}",
                                path, algorithm, expected, actual
                            ),
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

impl<'a, S: Storage> IncrementalValidatorImpl<'a, S> {
    fn new(
        storage_root_result: StorageValidationResult,
        validator: &'a Validator<S>,
        storage: &'a S,
        fixity_check: bool,
    ) -> Self {
        Self {
            storage_root_result,
            validator,
            storage,
            fixity_check,
        }
    }

    // TODO
}

impl<'a, S: Storage> IncrementalValidator for IncrementalValidatorImpl<'a, S> {
    fn storage_root_result(&self) -> &StorageValidationResult {
        &self.storage_root_result
    }
}

impl ParseValidationResult {
    pub fn new() -> Self {
        Self {
            object_id: RefCell::new(None),
            errors: RefCell::new(Vec::new()),
            warnings: RefCell::new(Vec::new()),
        }
    }

    pub fn error(&self, code: ErrorCode, message: String) {
        self.errors.borrow_mut().push(ValidationError::new(
            ProblemLocation::ObjectRoot,
            code,
            message,
        ));
    }

    pub fn warn(&self, code: WarnCode, message: String) {
        self.warnings.borrow_mut().push(ValidationWarning::new(
            ProblemLocation::ObjectRoot,
            code,
            message,
        ));
    }

    pub fn object_id(&self, object_id: &str) {
        self.object_id.replace(Some(object_id.to_string()));
    }

    pub fn has_errors(&self) -> bool {
        self.errors.borrow().len() > 0
    }
}

impl Default for ParseValidationResult {
    fn default() -> Self {
        Self::new()
    }
}

impl ContentPaths {
    fn new() -> Self {
        Self {
            path_map: HashMap::new(),
        }
    }

    fn add_path(&mut self, path: ContentPath) {
        if let ContentPathVersion::VersionNum(num) = path.version {
            self.path_map.entry(num).or_insert_with(Vec::new).push(path);
        }
    }

    /// Iterates over all of the content files that _should_ appear in the manifest of an inventory
    /// at the specified `version_num`. Paths from later versions are not included.
    fn iter(&self, version_num: VersionNum) -> ContentPathsIter {
        ContentPathsIter {
            current_version: version_num,
            current_iter: self
                .path_map
                .get(&version_num)
                .unwrap_or(&EMPTY_PATHS)
                .iter(),
            path_map: &self.path_map,
        }
    }
}

impl<'a> Iterator for ContentPathsIter<'a> {
    type Item = &'a ContentPath;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current_iter.next() {
            Some(next) => Some(next),
            None => {
                while self.current_version != VersionNum::v1() {
                    self.current_version = self.current_version.previous().unwrap();
                    match self.path_map.get(&self.current_version) {
                        Some(paths) => {
                            self.current_iter = paths.iter();
                        }
                        None => continue,
                    }
                    return self.next();
                }
                None
            }
        }
    }
}

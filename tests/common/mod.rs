#![allow(dead_code)]

use std::convert::TryFrom;
use std::rc::Rc;
use std::str::FromStr;

use assert_fs::fixture::ChildPath;
use assert_fs::prelude::*;
use assert_fs::TempDir;
use rocfl::ocfl::{
    ContentPath, ErrorCode, LogicalPath, ObjectValidationResult, ProblemLocation,
    StorageValidationResult, ValidationError, ValidationResult, ValidationWarning, VersionNum,
    WarnCode,
};

pub fn create_dirs(temp: &TempDir, path: &str) -> ChildPath {
    let child = resolve_child(temp, path);
    child.create_dir_all().unwrap();
    child
}

pub fn create_file(temp: &TempDir, path: &str, content: &str) -> ChildPath {
    let child = resolve_child(temp, path);
    child.write_str(content).unwrap();
    child
}

pub fn resolve_child(temp: &TempDir, path: &str) -> ChildPath {
    let mut child: Option<ChildPath> = None;
    for part in path.split('/') {
        child = match child {
            Some(child) => Some(child.child(part)),
            None => Some(temp.child(part)),
        };
    }
    child.unwrap()
}

pub fn lpath(path: &str) -> LogicalPath {
    LogicalPath::try_from(path).unwrap()
}

pub fn lpath_rc(path: &str) -> Rc<LogicalPath> {
    Rc::new(LogicalPath::try_from(path).unwrap())
}

pub fn cpath(path: &str) -> ContentPath {
    ContentPath::try_from(path).unwrap()
}

pub fn cpath_rc(path: &str) -> Rc<ContentPath> {
    Rc::new(ContentPath::try_from(path).unwrap())
}

pub fn version_error(num: &str, code: ErrorCode, text: &str) -> ValidationError {
    ValidationError::new(
        VersionNum::from_str(num).unwrap().into(),
        code,
        text.to_string(),
    )
}

pub fn root_error(code: ErrorCode, text: &str) -> ValidationError {
    ValidationError::new(ProblemLocation::ObjectRoot, code, text.to_string())
}

pub fn version_warning(num: &str, code: WarnCode, text: &str) -> ValidationWarning {
    ValidationWarning::new(
        VersionNum::from_str(num).unwrap().into(),
        code,
        text.to_string(),
    )
}

pub fn root_warning(code: WarnCode, text: &str) -> ValidationWarning {
    ValidationWarning::new(ProblemLocation::ObjectRoot, code, text.to_string())
}

pub fn has_errors(result: &ObjectValidationResult, expected_errors: &[ValidationError]) {
    for expected in expected_errors {
        assert!(
            result.errors().contains(expected),
            "Expected errors to contain {:?}. Found: {:?}",
            expected,
            result.errors()
        );
    }
    assert_eq!(
        expected_errors.len(),
        result.errors().len(),
        "Expected {} errors; found {}: {:?}",
        expected_errors.len(),
        result.errors().len(),
        result.errors()
    )
}

pub fn has_warnings(result: &ObjectValidationResult, expected_warnings: &[ValidationWarning]) {
    for expected in expected_warnings {
        assert!(
            result.warnings().contains(expected),
            "Expected warnings to contain {:?}. Found: {:?}",
            expected,
            result.warnings()
        );
    }
    assert_eq!(
        expected_warnings.len(),
        result.warnings().len(),
        "Expected {} warnings; found {}: {:?}",
        expected_warnings.len(),
        result.warnings().len(),
        result.warnings()
    )
}

pub fn has_errors_storage(result: &StorageValidationResult, expected_errors: &[ValidationError]) {
    for expected in expected_errors {
        assert!(
            result.errors().contains(expected),
            "Expected errors to contain {:?}. Found: {:?}",
            expected,
            result.errors()
        );
    }
    assert_eq!(
        expected_errors.len(),
        result.errors().len(),
        "Expected {} errors; found {}: {:?}",
        expected_errors.len(),
        result.errors().len(),
        result.errors()
    )
}

pub fn has_warnings_storage(
    result: &StorageValidationResult,
    expected_warnings: &[ValidationWarning],
) {
    for expected in expected_warnings {
        assert!(
            result.warnings().contains(expected),
            "Expected warnings to contain {:?}. Found: {:?}",
            expected,
            result.warnings()
        );
    }
    assert_eq!(
        expected_warnings.len(),
        result.warnings().len(),
        "Expected {} warnings; found {}: {:?}",
        expected_warnings.len(),
        result.warnings().len(),
        result.warnings()
    )
}

pub fn error_count(expected: usize, result: &ObjectValidationResult) {
    assert_eq!(
        expected,
        result.errors().len(),
        "Expected {} errors; found {}: {:?}",
        expected,
        result.errors().len(),
        result.errors()
    )
}

pub fn warning_count(expected: usize, result: &ObjectValidationResult) {
    assert_eq!(
        expected,
        result.warnings().len(),
        "Expected {} warnings; found {}: {:?}",
        expected,
        result.warnings().len(),
        result.warnings()
    )
}

pub fn no_warnings(result: &ObjectValidationResult) {
    assert!(
        result.warnings().is_empty(),
        "Expected no warnings; found: {:?}",
        result.warnings()
    )
}

pub fn no_errors(result: &ObjectValidationResult) {
    assert!(
        result.errors().is_empty(),
        "Expected no errors; found: {:?}",
        result.errors()
    )
}

pub fn no_warnings_storage(result: &StorageValidationResult) {
    assert!(
        result.warnings().is_empty(),
        "Expected no warnings; found: {:?}",
        result.warnings()
    )
}

pub fn no_errors_storage(result: &StorageValidationResult) {
    assert!(
        result.errors().is_empty(),
        "Expected no errors; found: {:?}",
        result.errors()
    )
}

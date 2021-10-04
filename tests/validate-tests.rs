use std::path::{Path, PathBuf};

use rocfl::ocfl::{
    ErrorCode, OcflRepo, ValidationError, ValidationResult, ValidationWarning, WarnCode,
};

const ROOT: &str = "root";

#[test]
fn extra_dir_in_root() {
    let repo = new_repo(official_bad_root());
    let result = repo
        .validate_object_at("E001_extra_dir_in_root", true)
        .unwrap();

    has_root_error(
        ErrorCode::E001,
        "Unexpected file in object root: extra_dir",
        &result,
    );
    has_root_warning(
        WarnCode::W007,
        "Inventory version 'v1' is missing recommended key 'message'",
        &result,
    );
    has_root_warning(
        WarnCode::W007,
        "Inventory version 'v1' is missing recommended key 'user'",
        &result,
    );
    error_count(1, &result);
    warning_count(2, &result);
}

fn error_count(count: usize, result: &ValidationResult) {
    assert_eq!(
        count,
        result.errors.len(),
        "Expected {} errors; found {}: {:?}",
        count,
        result.errors.len(),
        result.errors
    )
}

fn has_root_error(code: ErrorCode, message: &str, result: &ValidationResult) {
    assert!(
        result.errors.contains(&ValidationError::with_version(
            ROOT.to_string(),
            code,
            message.to_string()
        )),
        "Expected errors to contain code={}; msg='{}'. Found: {:?}",
        code,
        message,
        result.errors
    );
}

fn warning_count(count: usize, result: &ValidationResult) {
    assert_eq!(
        count,
        result.warnings.len(),
        "Expected {} warnings; found {}: {:?}",
        count,
        result.warnings.len(),
        result.warnings
    )
}

fn has_root_warning(code: WarnCode, message: &str, result: &ValidationResult) {
    assert!(
        result.warnings.contains(&ValidationWarning::with_version(
            ROOT.to_string(),
            code,
            message.to_string()
        )),
        "Expected warnings to contain code={}; msg='{}'. Found: {:?}",
        code,
        message,
        result.warnings
    );
}

fn new_repo(root: impl AsRef<Path>) -> OcflRepo {
    OcflRepo::fs_repo(root, None).unwrap()
}

fn official_bad_root() -> PathBuf {
    let mut path = validate_repo_root();
    path.push("official-1.0");
    path.push("bad");
    path
}

fn validate_repo_root() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("resources");
    path.push("test");
    path.push("validate");
    path
}

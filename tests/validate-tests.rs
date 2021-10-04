use std::path::{Path, PathBuf};

use rocfl::ocfl::{
    ErrorCode, OcflRepo, ValidationError, ValidationResult, ValidationWarning, WarnCode,
};

const ROOT: &str = "root";

#[test]
fn extra_dir_in_root() {
    let result = official_bad_test("E001_extra_dir_in_root");

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

#[test]
fn extra_file_in_root() {
    let result = official_bad_test("E001_extra_file_in_root");

    has_root_error(
        ErrorCode::E001,
        "Unexpected file in object root: extra_file",
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

#[test]
fn invalid_version_format() {
    let result = official_bad_test("E001_invalid_version_format");

    has_root_error(
        ErrorCode::E011,
        "Inventory 'head' must be a valid version number. Found: 1",
        &result,
    );
    has_root_error(
        ErrorCode::E046,
        "Inventory 'versions' contains an invalid version number. Found: 1",
        &result,
    );
    has_root_error(
        ErrorCode::E001,
        "Unexpected file in object root: 1",
        &result,
    );
    has_root_error(
        ErrorCode::E008,
        "Inventory does not contain any valid versions",
        &result,
    );
    has_root_error(
        ErrorCode::E099,
        "Inventory manifest key 'ffc150e7944b5cf5ddb899b2f48efffbd490f97632fc258434aefc4afb92aef2e3441ddcceae11404e5805e1b6c804083c9398c28f061c9ba42dd4bac53d5a2e' contains a content path containing an illegal path part. Found: 1/content/my_content/dracula.txt",
        &result,
    );
    has_root_error(
        ErrorCode::E099,
        "Inventory manifest key '69f54f2e9f4568f7df4a4c3b07e4cbda4ba3bba7913c5218add6dea891817a80ce829b877d7a84ce47f93cbad8aa522bf7dd8eda2778e16bdf3c47cf49ee3bdf' contains a content path containing an illegal path part. Found: 1/content/my_content/poe.txt",
        &result,
    );
    error_count(6, &result);
    warning_count(0, &result);
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
        result.errors.contains(&ValidationError::with_context(
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
        result.warnings.contains(&ValidationWarning::with_context(
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

fn official_bad_test(name: &str) -> ValidationResult {
    let repo = new_repo(official_bad_root());
    repo.validate_object_at(name, true).unwrap()
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

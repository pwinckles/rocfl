use std::path::{Path, PathBuf};

use rocfl::ocfl::{
    ErrorCode, OcflRepo, ValidationError, ValidationResult, ValidationWarning, WarnCode,
};

const ROOT: &str = "root";

#[test]
fn extra_dir_in_root() {
    let result = official_bad_test("E001_extra_dir_in_root");

    has_errors(
        &result,
        &vec![root_error(
            ErrorCode::E001,
            "Unexpected file in object root: extra_dir",
        )],
    );
    has_warnings(
        &result,
        &vec![
            root_warning(
                WarnCode::W007,
                "Inventory version 'v1' is missing recommended key 'message'",
            ),
            root_warning(
                WarnCode::W007,
                "Inventory version 'v1' is missing recommended key 'user'",
            ),
        ],
    );
}

#[test]
fn extra_file_in_root() {
    let result = official_bad_test("E001_extra_file_in_root");

    has_errors(
        &result,
        &vec![root_error(
            ErrorCode::E001,
            "Unexpected file in object root: extra_file",
        )],
    );
    has_warnings(
        &result,
        &vec![
            root_warning(
                WarnCode::W007,
                "Inventory version 'v1' is missing recommended key 'message'",
            ),
            root_warning(
                WarnCode::W007,
                "Inventory version 'v1' is missing recommended key 'user'",
            ),
        ],
    );
}

#[test]
fn invalid_version_format() {
    let result = official_bad_test("E001_invalid_version_format");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E011,
            "Inventory 'head' must be a valid version number. Found: 1",
        ),
        root_error(
            ErrorCode::E046,
            "Inventory 'versions' contains an invalid version number. Found: 1",
        ),
        root_error(
            ErrorCode::E008,
            "Inventory does not contain any valid versions",
        ),
        root_error(
            ErrorCode::E099,
            "Inventory manifest key 'ffc150e7944b5cf5ddb899b2f48efffbd490f97632fc258434aefc4afb92aef2e3441ddcceae11404e5805e1b6c804083c9398c28f061c9ba42dd4bac53d5a2e' contains a path containing an illegal path part. Found: 1/content/my_content/dracula.txt",
        ),
        root_error(
            ErrorCode::E099,
            "Inventory manifest key '69f54f2e9f4568f7df4a4c3b07e4cbda4ba3bba7913c5218add6dea891817a80ce829b877d7a84ce47f93cbad8aa522bf7dd8eda2778e16bdf3c47cf49ee3bdf' contains a path containing an illegal path part. Found: 1/content/my_content/poe.txt",
        )
    ]);
    no_warnings(&result);
}

#[test]
fn v2_file_in_root() {
    let result = official_bad_test("E001_v2_file_in_root");

    has_errors(
        &result,
        &vec![root_error(
            ErrorCode::E001,
            "Unexpected file in object root: v2",
        )],
    );
    no_warnings(&result);
}

#[test]
fn empty_object() {
    let result = official_bad_test("E003_E063_empty");

    has_errors(
        &result,
        &vec![
            root_error(ErrorCode::E003, "Object version declaration does not exist"),
            root_error(ErrorCode::E063, "Inventory does not exist"),
        ],
    );
    no_warnings(&result);
}

#[test]
fn no_decl() {
    let result = official_bad_test("E003_no_decl");

    has_errors(
        &result,
        &vec![root_error(
            ErrorCode::E003,
            "Object version declaration does not exist",
        )],
    );
    no_warnings(&result);
}

#[test]
fn bad_declaration_contents() {
    let result = official_bad_test("E007_bad_declaration_contents");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E007,
            "Object version declaration is invalid. Expected: ocfl_object_1.0; Found: This is not the right content!\n",
        ),
    ]);
    has_warnings(
        &result,
        &vec![
            root_warning(
                WarnCode::W007,
                "Inventory version 'v1' is missing recommended key 'message'",
            ),
            root_warning(
                WarnCode::W007,
                "Inventory version 'v1' is missing recommended key 'user'",
            ),
        ],
    );
}

#[test]
fn missing_versions() {
    let result = official_bad_test("E010_missing_versions");

    has_errors(
        &result,
        &vec![root_error(
            ErrorCode::E010,
            "Object root does not contain version directory 'v3'",
        )],
    );
    has_warnings(
        &result,
        &vec![version_warning(
            "v3",
            WarnCode::W010,
            "Inventory file does not exist",
        )],
    );
}

#[test]
fn skipped_versions() {
    let result = official_bad_test("E010_skipped_versions");

    has_errors(
        &result,
        &vec![
            root_error(
                ErrorCode::E010,
                "Inventory 'versions' is missing version 'v2'",
            ),
            root_error(
                ErrorCode::E010,
                "Inventory 'versions' is missing version 'v3'",
            ),
            root_error(
                ErrorCode::E010,
                "Inventory 'versions' is missing version 'v6'",
            ),
        ],
    );
    no_warnings(&result);
}

#[test]
fn invalid_padded_head_version() {
    let result = official_bad_test("E011_E013_invalid_padded_head_version");

    has_errors(
        &result,
        &vec![root_error(
            ErrorCode::E013,
            "Inventory 'versions' contains inconsistently padded version numbers",
        )],
    );
    no_warnings(&result);
}

#[test]
fn content_not_in_content_dir() {
    let result = official_bad_test("E015_content_not_in_content_dir");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a version content directory: v3/a_file.txt",
        ),
        root_error(
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a version content directory: v1/a_file.txt",
        ),
        root_error(
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a version content directory: v2/a_file.txt",
        ),
        version_error(
            "v3",
            ErrorCode::E015,
            "Version directory contains unexpected file: a_file.txt",
        ),
        version_error(
            "v2",
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a version content directory: v1/a_file.txt",
        ),
        version_error(
            "v2",
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a version content directory: v2/a_file.txt",
        ),
        version_error(
            "v2",
            ErrorCode::E015,
            "Version directory contains unexpected file: a_file.txt",
        ),
        version_error(
            "v1",
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a version content directory: v1/a_file.txt",
        ),
        version_error(
            "v1",
            ErrorCode::E015,
            "Version directory contains unexpected file: a_file.txt",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn invalid_content_dir() {
    let result = official_bad_test("E017_invalid_content_dir");

    has_errors(
        &result,
        &vec![root_error(
            ErrorCode::E017,
            "Inventory 'contentDirectory' cannot contain '/'. Found: content/dir",
        )],
    );
    no_warnings(&result);
}

#[test]
fn inconsistent_content_dir() {
    let result = official_bad_test("E019_inconsistent_content_dir");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a version content directory: v1/content-dir/test.txt",
        ),
        version_error(
            "v1",
            ErrorCode::E019,
            "Inventory 'contentDirectory' is inconsistent. Expected: content; Found: content-dir",
        ),
        version_error(
            "v1",
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a version content directory: v1/content-dir/test.txt",
        ),
    ]);
    has_warnings(
        &result,
        &vec![version_warning(
            "v1",
            WarnCode::W002,
            "Version directory contains unexpected directory: content-dir",
        )],
    );
}

#[test]
fn extra_file() {
    let result = official_bad_test("E023_extra_file");

    has_errors(
        &result,
        &vec![root_error(
            ErrorCode::E023,
            "A content file exists that is not referenced in the manifest: v1/content/file2.txt",
        )],
    );
    has_warnings(
        &result,
        &vec![root_warning(
            WarnCode::W009,
            "Inventory version v1 user 'address' should be a URI. Found: somewhere",
        )],
    );
}

#[test]
fn missing_file() {
    let result = official_bad_test("E023_missing_file");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a content directory: v1/content/file2.txt",
        ),
    ]);
    has_warnings(
        &result,
        &vec![root_warning(
            WarnCode::W009,
            "Inventory version v1 user 'address' should be a URI. Found: somewhere",
        )],
    );
}

fn version_error(num: &str, code: ErrorCode, text: &str) -> ValidationError {
    ValidationError::with_context(num.to_string(), code, text.to_string())
}

fn root_error(code: ErrorCode, text: &str) -> ValidationError {
    ValidationError::with_context(ROOT.to_string(), code, text.to_string())
}

fn version_warning(num: &str, code: WarnCode, text: &str) -> ValidationWarning {
    ValidationWarning::with_context(num.to_string(), code, text.to_string())
}

fn root_warning(code: WarnCode, text: &str) -> ValidationWarning {
    ValidationWarning::with_context(ROOT.to_string(), code, text.to_string())
}

fn has_errors(result: &ValidationResult, expected_errors: &[ValidationError]) {
    for expected in expected_errors {
        assert!(
            result.errors.contains(expected),
            "Expected errors to contain {:?}. Found: {:?}",
            expected,
            result.errors
        );
    }
    assert_eq!(
        expected_errors.len(),
        result.errors.len(),
        "Expected {} errors; found {}: {:?}",
        expected_errors.len(),
        result.errors.len(),
        result.errors
    )
}

fn has_warnings(result: &ValidationResult, expected_warnings: &[ValidationWarning]) {
    for expected in expected_warnings {
        assert!(
            result.warnings.contains(expected),
            "Expected warnings to contain {:?}. Found: {:?}",
            expected,
            result.warnings
        );
    }
    assert_eq!(
        expected_warnings.len(),
        result.warnings.len(),
        "Expected {} warnings; found {}: {:?}",
        expected_warnings.len(),
        result.warnings.len(),
        result.warnings
    )
}

fn error_count(result: &ValidationResult) {
    assert!(
        result.errors.is_empty(),
        "Expected no errors; found: {:?}",
        result.errors
    )
}

fn no_warnings(result: &ValidationResult) {
    assert!(
        result.warnings.is_empty(),
        "Expected no warnings; found: {:?}",
        result.warnings
    )
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

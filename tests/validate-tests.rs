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
            "Inventory manifest references a file that does not exist in a content directory: v3/a_file.txt",
        ),
        root_error(
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a content directory: v1/a_file.txt",
        ),
        root_error(
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a content directory: v2/a_file.txt",
        ),
        version_error(
            "v3",
            ErrorCode::E015,
            "Version directory contains unexpected file: a_file.txt",
        ),
        version_error(
            "v2",
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a content directory: v1/a_file.txt",
        ),
        version_error(
            "v2",
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a content directory: v2/a_file.txt",
        ),
        version_error(
            "v2",
            ErrorCode::E015,
            "Version directory contains unexpected file: a_file.txt",
        ),
        version_error(
            "v1",
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a content directory: v1/a_file.txt",
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
            "Inventory manifest references a file that does not exist in a content directory: v1/content-dir/test.txt",
        ),
        version_error(
            "v1",
            ErrorCode::E019,
            "Inventory 'contentDirectory' is inconsistent. Expected: content; Found: content-dir",
        ),
        version_error(
            "v1",
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a content directory: v1/content-dir/test.txt",
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

#[test]
fn old_manifest_missing_entries() {
    let result = official_bad_test("E023_old_manifest_missing_entries");

    has_errors(&result, &vec![
        version_error(
            "v2",
            ErrorCode::E023,
            "A content file exists that is not referenced in the manifest: v1/content/file-3.txt",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn wrong_digest_algorithm() {
    let result = official_bad_test("E025_wrong_digest_algorithm");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E025,
            "Inventory 'digestAlgorithm' must be 'sha512' or 'sha256. Found: md5",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn no_head() {
    let result = official_bad_test("E036_no_head");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E036,
            "Inventory is missing required key 'head'",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn no_id() {
    let result = official_bad_test("E036_no_id");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E036,
            "Inventory is missing required key 'id'",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn inconsistent_id() {
    let result = official_bad_test("E037_inconsistent_id");

    has_errors(&result, &vec![
        version_error(
            "v1",
            ErrorCode::E037,
            "Inventory 'id' is inconsistent. Expected: urn:example-2; Found: urn:example-two",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn head_not_most_recent() {
    let result = official_bad_test("E040_head_not_most_recent");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E040,
            "Inventory 'head' references 'v1' but 'v2' was expected",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn wrong_head_doesnt_exist() {
    let result = official_bad_test("E040_wrong_head_doesnt_exist");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E040,
            "Inventory 'head' references 'v2' but 'v1' was expected",
        ),
        root_error(
            ErrorCode::E010,
            "Inventory 'versions' is missing version 'v2'",
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
fn wrong_head_format() {
    let result = official_bad_test("E040_wrong_head_format");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E040,
            "Inventory 'head' must be a string",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn wrong_version_in_version_dir() {
    let result = official_bad_test("E040_wrong_version_in_version_dir");

    has_errors(&result, &vec![
        version_error(
            "v2",
            ErrorCode::E040,
            "Inventory 'head' must equal 'v2'. Found: v3",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn no_manifest() {
    let result = official_bad_test("E041_no_manifest");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E041,
            "Inventory is missing required key 'manifest'",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn root_no_most_recent() {
    let result = official_bad_test("E046_root_not_most_recent");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E001,
            "Unexpected file in object root: v2",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn created_no_timezone() {
    let result = official_bad_test("E049_created_no_timezone");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E049,
            "Inventory version v1 'created' must be an RFC3339 formatted date. Found: 2019-01-01T02:03:04",
        ),
        // TODO it is a little unfortunate how much this cascades
        root_error(
            ErrorCode::E010,
            "Inventory 'versions' is missing version 'v1'",
        ),
        root_error(
            ErrorCode::E008,
            "Inventory does not contain any valid versions",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn created_not_to_seconds() {
    let result = official_bad_test("E049_created_not_to_seconds");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E049,
            "Inventory version v1 'created' must be an RFC3339 formatted date. Found: 2019-01-01T01:02Z",
        ),
        // TODO it is a little unfortunate how much this cascades
        root_error(
            ErrorCode::E010,
            "Inventory 'versions' is missing version 'v1'",
        ),
        root_error(
            ErrorCode::E008,
            "Inventory does not contain any valid versions",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn bad_version_block_values() {
    let result = official_bad_test("E049_E050_E054_bad_version_block_values");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E049,
            "Inventory version v1 'created' must be a string",
        ),
    ]);
    no_warnings(&result);
}

// TODO this is _not_ a 1.0 requirement
// #[test]
fn file_in_manifest_not_used() {
    let result = official_bad_test("E050_file_in_manifest_not_used");

    has_errors(&result, &vec![]);
    no_warnings(&result);
}

#[test]
fn manifest_digest_wrong_case() {
    let result = official_bad_test("E050_manifest_digest_wrong_case");

    // TODO this is supposed to be a case-sensitive match
    has_errors(&result, &vec![
        root_error(
            ErrorCode::E050,
            "Inventory version v1 'created' must be a string",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn invalid_logical_paths() {
    let result = official_bad_test("E053_E052_invalid_logical_paths");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E053,
            "Inventory version v1 state key '07e41ccb166d21a5327d5a2ae1bb48192b8470e1357266c9d119c294cb1e95978569472c9de64fb6d93cbd4dd0aed0bf1e7c47fd1920de17b038a08a85eb4fa1' contains a path with a leading/trailing '/'. Found: /file-1.txt",
        ),
        root_error(
            ErrorCode::E052,
            "Inventory version v1 state key '9fef2458ee1a9277925614272adfe60872f4c1bf02eecce7276166957d1ab30f65cf5c8065a294bf1b13e3c3589ba936a3b5db911572e30dfcb200ef71ad33d5' contains a path containing an illegal path part. Found: ../../file-2.txt",
        ),
        root_error(
            ErrorCode::E053,
            "Inventory version v1 state key 'b3b26d26c9d8cfbb884b50e798f93ac6bef275a018547b1560af3e6d38f2723785731d3ca6338682fa7ac9acb506b3c594a125ce9d3d60cd14498304cc864cf2' contains a path with a leading/trailing '/'. Found: //file-3.txt",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn no_sidecar() {
    let result = official_bad_test("E058_no_sidecar");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E058,
            "Inventory sidecar inventory.json.sha512 does not exist",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn root_inventory_digest_mismatch() {
    let result = official_bad_test("E060_E064_root_inventory_digest_mismatch");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E060,
            "Inventory does not match expected digest. Expected: cb7a451c595050e0e50d979b79bce86e28728b8557a3cf4ea430114278b5411c7bad6a7ecc1f4d0250e94f9d8add3b648194d75a74c0cb14c4439f427829569e; Found: 5bf08b6519f6692cc83f3d275de1f02414a41972d069ac167c5cf34468fad82ae621c67e1ff58a8ef15d5f58a193aa1f037f588372bdfc33ae6c38a2b349d846",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn version_inventory_digest_mismatch() {
    let result = official_bad_test("E060_version_inventory_digest_mismatch");

    has_errors(&result, &vec![
        version_error(
            "v1",
            ErrorCode::E060,
            "Inventory does not match expected digest. Expected: cb7a451c595050e0e50d979b79bce86e28728b8557a3cf4ea430114278b5411c7bad6a7ecc1f4d0250e94f9d8add3b648194d75a74c0cb14c4439f427829569e; Found: 5bf08b6519f6692cc83f3d275de1f02414a41972d069ac167c5cf34468fad82ae621c67e1ff58a8ef15d5f58a193aa1f037f588372bdfc33ae6c38a2b349d846",
        ),
    ]);
    has_warnings(&result, &vec![
        version_warning(
            "v1",
            WarnCode::W011,
            "Inventory version v1 'message' is inconsistent with the root inventory",
        )
    ]);
}

#[test]
fn invalid_sidecar() {
    let result = official_bad_test("E061_invalid_sidecar");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E061,
            "Inventory sidecar is invalid",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn no_inv() {
    let result = official_bad_test("E063_no_inv");

    has_errors(&result, &vec![
        root_error(
            ErrorCode::E063,
            "Inventory does not exist",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn different_root_and_latest_inventories() {
    let result = official_bad_test("E064_different_root_and_latest_inventories");

    has_errors(&result, &vec![
        version_error(
            "v1",
            ErrorCode::E064,
            "Inventory file must be identical to the root inventory",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn algorithm_change_state_mismatch() {
    let result = official_bad_test("E066_algorithm_change_state_mismatch");

    has_errors(&result, &vec![
        version_error(
            "v1",
            ErrorCode::E066,
            "Inventory file must be identical to the root inventory",
        ),
    ]);
    no_warnings(&result);
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

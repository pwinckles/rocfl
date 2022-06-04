use std::path::{Path, PathBuf};

use common::*;
use rocfl::ocfl::{
    ErrorCode, ObjectValidationResult, OcflRepo, ProblemLocation, ValidationError,
    ValidationResult, ValidationWarning, WarnCode,
};

mod common;

#[test]
fn extra_dir_in_root() {
    let result = official_error_test("E001_extra_dir_in_root");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E001,
            "Unexpected file in object root: extra_dir",
        )],
    );
    has_warnings(
        &result,
        &[
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
    let result = official_error_test("E001_extra_file_in_root");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E001,
            "Unexpected file in object root: extra_file",
        )],
    );
    has_warnings(
        &result,
        &[
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
    let result = official_error_test("E001_invalid_version_format");

    has_errors(&result, &[
        root_error(
            ErrorCode::E104,
            "Inventory 'head' must be a valid version number. Found: 1",
        ),
        root_error(
            ErrorCode::E104,
            "Inventory 'versions' contains an invalid version number. Found: 1",
        ),
        root_error(
            ErrorCode::E008,
            "Inventory does not contain any valid versions",
        ),
        root_error(
            ErrorCode::E099,
            "Inventory manifest key 'ffc150e7944b5cf5ddb899b2f48efffbd490f97632fc258434aefc4afb92aef2e3441ddcceae11404e5805e1b6c804083c9398c28f061c9ba42dd4bac53d5a2e' \
            contains a path containing an illegal path part. Found: 1/content/my_content/dracula.txt",
        ),
        root_error(
            ErrorCode::E099,
            "Inventory manifest key '69f54f2e9f4568f7df4a4c3b07e4cbda4ba3bba7913c5218add6dea891817a80ce829b877d7a84ce47f93cbad8aa522bf7dd8eda2778e16bdf3c47cf49ee3bdf' \
            contains a path containing an illegal path part. Found: 1/content/my_content/poe.txt",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn v2_file_in_root() {
    let result = official_error_test("E001_v2_file_in_root");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E001,
            "Unexpected file in object root: v2",
        )],
    );
    no_warnings(&result);
}

#[test]
fn empty_object() {
    let result = official_error_test("E003_E063_empty");

    has_errors(
        &result,
        &[
            root_error(ErrorCode::E003, "Object version declaration does not exist"),
            root_error(ErrorCode::E063, "Inventory does not exist"),
        ],
    );
    no_warnings(&result);
}

#[test]
fn no_decl() {
    let result = official_error_test("E003_no_decl");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E003,
            "Object version declaration does not exist",
        )],
    );
    no_warnings(&result);
}

#[test]
fn bad_declaration_contents() {
    let result = official_error_test("E007_bad_declaration_contents");

    has_errors(&result, &[
        root_error(
            ErrorCode::E007,
            "Object version declaration is invalid. Expected: ocfl_object_1.0; Found: This is not the right content!",
        ),
    ]);
    has_warnings(
        &result,
        &[
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
    let result = official_error_test("E010_missing_versions");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E010,
            "Object root does not contain version directory 'v3'",
        )],
    );
    has_warnings(
        &result,
        &[version_warning(
            "v3",
            WarnCode::W010,
            "Inventory file does not exist",
        )],
    );
}

#[test]
fn skipped_versions() {
    let result = official_error_test("E010_skipped_versions");

    has_errors(
        &result,
        &[
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
    let result = official_error_test("E011_E013_invalid_padded_head_version");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E013,
            "Inventory 'versions' contains inconsistently padded version numbers",
        )],
    );
    has_warnings(
        &result,
        &[root_warning(
            WarnCode::W001,
            "Contains zero-padded version numbers",
        )],
    )
}

#[test]
fn content_not_in_content_dir() {
    let result = official_error_test("E015_content_not_in_content_dir");

    has_errors(&result, &[
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
    let result = official_error_test("E017_invalid_content_dir");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E017,
            "Inventory 'contentDirectory' cannot contain '/'. Found: content/dir",
        )],
    );
    no_warnings(&result);
}

#[test]
fn inconsistent_content_dir() {
    let result = official_error_test("E019_inconsistent_content_dir");

    has_errors(&result, &[
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
        &[version_warning(
            "v1",
            WarnCode::W002,
            "Version directory contains unexpected directory: content-dir",
        )],
    );
}

#[test]
fn extra_file() {
    let result = official_error_test("E023_extra_file");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E023,
            "A content file exists that is not referenced in the manifest: v1/content/file2.txt",
        )],
    );
    has_warnings(
        &result,
        &[root_warning(
            WarnCode::W009,
            "Inventory version v1 user 'address' should be a URI. Found: somewhere",
        )],
    );
}

#[test]
fn missing_file() {
    let result = official_error_test("E023_missing_file");

    has_errors(&result, &[
        root_error(
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a content directory: v1/content/file2.txt",
        ),
    ]);
    has_warnings(
        &result,
        &[root_warning(
            WarnCode::W009,
            "Inventory version v1 user 'address' should be a URI. Found: somewhere",
        )],
    );
}

#[test]
fn old_manifest_missing_entries() {
    let result = official_error_test("E023_old_manifest_missing_entries");

    has_errors(
        &result,
        &[version_error(
            "v2",
            ErrorCode::E023,
            "A content file exists that is not referenced in the manifest: v1/content/file-3.txt",
        )],
    );
    no_warnings(&result);
}

#[test]
fn wrong_digest_algorithm() {
    let result = official_error_test("E025_wrong_digest_algorithm");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E025,
            "Inventory 'digestAlgorithm' must be 'sha512' or 'sha256. Found: md5",
        )],
    );
    no_warnings(&result);
}

#[test]
fn no_head() {
    let result = official_error_test("E036_no_head");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E036,
            "Inventory is missing required key 'head'",
        )],
    );
    no_warnings(&result);
}

#[test]
fn no_id() {
    let result = official_error_test("E036_no_id");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E036,
            "Inventory is missing required key 'id'",
        )],
    );
    no_warnings(&result);
}

#[test]
fn inconsistent_id() {
    let result = official_error_test("E037_inconsistent_id");

    has_errors(
        &result,
        &[version_error(
            "v1",
            ErrorCode::E110,
            "Inventory 'id' is inconsistent. Expected: urn:example-2; Found: urn:example-two",
        )],
    );
    no_warnings(&result);
}

#[test]
fn head_not_most_recent() {
    let result = official_error_test("E040_head_not_most_recent");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E040,
            "Inventory 'head' references 'v1' but 'v2' was expected",
        )],
    );
    no_warnings(&result);
}

#[test]
fn wrong_head_doesnt_exist() {
    let result = official_error_test("E040_wrong_head_doesnt_exist");

    has_errors(
        &result,
        &[
            root_error(
                ErrorCode::E040,
                "Inventory 'head' references 'v2' but 'v1' was expected",
            ),
            root_error(
                ErrorCode::E010,
                "Inventory 'versions' is missing version 'v2'",
            ),
        ],
    );
    has_warnings(
        &result,
        &[
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
    let result = official_error_test("E040_wrong_head_format");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E040,
            "Inventory 'head' must be a string",
        )],
    );
    no_warnings(&result);
}

#[test]
fn wrong_version_in_version_dir() {
    let result = official_error_test("E040_wrong_version_in_version_dir");

    has_errors(
        &result,
        &[version_error(
            "v2",
            ErrorCode::E040,
            "Inventory 'head' must equal 'v2'. Found: v3",
        )],
    );
    no_warnings(&result);
}

#[test]
fn no_manifest() {
    let result = official_error_test("E041_no_manifest");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E041,
            "Inventory is missing required key 'manifest'",
        )],
    );
    has_warnings(
        &result,
        &[
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
fn root_no_most_recent() {
    let result = official_error_test("E046_root_not_most_recent");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E001,
            "Unexpected file in object root: v2",
        )],
    );
    no_warnings(&result);
}

#[test]
fn created_no_timezone() {
    let result = official_error_test("E049_created_no_timezone");

    has_errors(&result, &[
        root_error(
            ErrorCode::E049,
            "Inventory version v1 'created' must be an RFC3339 formatted date. Found: 2019-01-01T02:03:04",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn created_not_to_seconds() {
    let result = official_error_test("E049_created_not_to_seconds");

    has_errors(&result, &[
        root_error(
            ErrorCode::E049,
            "Inventory version v1 'created' must be an RFC3339 formatted date. Found: 2019-01-01T01:02Z",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn bad_version_block_values() {
    let result = official_error_test("E049_E050_E054_bad_version_block_values");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E049,
            "Inventory version v1 'created' must be a string",
        )],
    );
    no_warnings(&result);
}

#[test]
fn file_in_manifest_not_used() {
    let result = official_error_test("E050_file_in_manifest_not_used");

    has_errors(&result, &[
        root_error(
            ErrorCode::E107,
            "Inventory manifest contains a digest that is not referenced in any version. \
            Found: dfe9a0bbfdaab7173036571a1d9e34e2465b1e3a52e8b707bbf6dea9239a9a55b0fc9e511fc24882d7f493cd950a9dbef1de13e08a007909b21cd5ba54dc4888",
        ),
    ]);
    has_warnings(
        &result,
        &[root_warning(
            WarnCode::W009,
            "Inventory version v1 user 'address' should be a URI. Found: somewhere",
        )],
    );
}

#[test]
fn manifest_digest_wrong_case() {
    let result = official_error_test("E050_manifest_digest_wrong_case");

    has_errors(
        &result,
        &[
            root_error(
                ErrorCode::E050,
                "Inventory version v1 state contains a digest that is not present in the manifest. \
            Found: 24F950AAC7B9EA9B3CB728228A0C82B67C39E96B4B344798870D5DAEE93E3AE5931BAAE8C7CACFEA4B629452C38026A81D138BC7AAD1AF3EF7BFD5EC646D6C28",
            ),
            root_error(
                ErrorCode::E107,
                "Inventory manifest contains a digest that is not referenced in any version. \
                Found: 24f950aac7b9ea9b3cb728228a0c82b67c39e96b4b344798870d5daee93e3ae5931baae8c7cacfea4b629452c38026a81d138bc7aad1af3ef7bfd5ec646d6c28",
            ),
    ]);
    no_warnings(&result);
}

#[test]
fn invalid_logical_paths() {
    let result = official_error_test("E053_E052_invalid_logical_paths");

    has_errors(&result, &[
        root_error(
            ErrorCode::E053,
            "In inventory version v1, state key '07e41ccb166d21a5327d5a2ae1bb48192b8470e1357266c9d119c294cb1e95978569472c9de64fb6d93cbd4dd0aed0bf1e7c47fd1920de17b038a08a85eb4fa1' \
            contains a path with a leading/trailing '/'. Found: /file-1.txt",
        ),
        root_error(
            ErrorCode::E052,
            "In inventory version v1, state key '9fef2458ee1a9277925614272adfe60872f4c1bf02eecce7276166957d1ab30f65cf5c8065a294bf1b13e3c3589ba936a3b5db911572e30dfcb200ef71ad33d5' \
            contains a path containing an illegal path part. Found: ../../file-2.txt",
        ),
        root_error(
            ErrorCode::E053,
            "In inventory version v1, state key 'b3b26d26c9d8cfbb884b50e798f93ac6bef275a018547b1560af3e6d38f2723785731d3ca6338682fa7ac9acb506b3c594a125ce9d3d60cd14498304cc864cf2' \
            contains a path with a leading/trailing '/'. Found: //file-3.txt",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn no_sidecar() {
    let result = official_error_test("E058_no_sidecar");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E058,
            "Inventory sidecar inventory.json.sha512 does not exist",
        )],
    );
    no_warnings(&result);
}

#[test]
fn root_inventory_digest_mismatch() {
    let result = official_error_test("E060_E064_root_inventory_digest_mismatch");

    has_errors(&result, &[
        root_error(
            ErrorCode::E060,
            "Inventory does not match expected digest. Expected: cb7a451c595050e0e50d979b79bce86e28728b8557a3cf4ea430114278b5411c7bad6a7ecc1f4d0250e94f9d8add3b648194d75a74c0cb14c4439f427829569e; \
            Found: 5bf08b6519f6692cc83f3d275de1f02414a41972d069ac167c5cf34468fad82ae621c67e1ff58a8ef15d5f58a193aa1f037f588372bdfc33ae6c38a2b349d846",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn version_inventory_digest_mismatch() {
    let result = official_error_test("E060_version_inventory_digest_mismatch");

    has_errors(&result, &[
        version_error(
            "v1",
            ErrorCode::E060,
            "Inventory does not match expected digest. Expected: cb7a451c595050e0e50d979b79bce86e28728b8557a3cf4ea430114278b5411c7bad6a7ecc1f4d0250e94f9d8add3b648194d75a74c0cb14c4439f427829569e; \
            Found: 5bf08b6519f6692cc83f3d275de1f02414a41972d069ac167c5cf34468fad82ae621c67e1ff58a8ef15d5f58a193aa1f037f588372bdfc33ae6c38a2b349d846",
        ),
    ]);
    has_warnings(
        &result,
        &[version_warning(
            "v1",
            WarnCode::W011,
            "Inventory version v1 'message' is inconsistent with the root inventory",
        )],
    );
}

#[test]
fn invalid_sidecar() {
    let result = official_error_test("E061_invalid_sidecar");

    has_errors(
        &result,
        &[root_error(ErrorCode::E061, "Inventory sidecar is invalid")],
    );
    no_warnings(&result);
}

#[test]
fn no_inv() {
    let result = official_error_test("E063_no_inv");

    has_errors(
        &result,
        &[root_error(ErrorCode::E063, "Inventory does not exist")],
    );
    no_warnings(&result);
}

#[test]
fn different_root_and_latest_inventories() {
    let result = official_error_test("E064_different_root_and_latest_inventories");

    has_errors(
        &result,
        &[version_error(
            "v1",
            ErrorCode::E064,
            "Inventory file must be identical to the root inventory",
        )],
    );
    no_warnings(&result);
}

#[test]
fn algorithm_change_state_mismatch() {
    let result = official_error_test("E066_algorithm_change_state_mismatch");

    has_errors(&result, &[
        version_error(
            "v1",
            ErrorCode::E066,
            "In inventory version v1, path 'file-3.txt' maps to different content paths than it \
            does in later inventories. Expected: [v1/content/file-2.txt]; Found: [v1/content/file-3.txt]",
        ),
        version_error(
            "v1",
            ErrorCode::E066,
            "Inventory version v1 state is missing a path that exists in later inventories: changed",
        ),
        version_error(
            "v1",
            ErrorCode::E066,
            "In inventory version v1, path 'file-2.txt' maps to different content paths than it \
            does in later inventories. Expected: [v1/content/file-3.txt]; Found: [v1/content/file-2.txt]",
        ),
        version_error(
            "v1",
            ErrorCode::E066,
            "Inventory version v1 state contains a path not in later inventories: file-1.txt",
        ),
    ]);
    has_warnings(
        &result,
        &[root_warning(
            WarnCode::W004,
            "Inventory 'digestAlgorithm' should be 'sha512'. Found: sha256",
        )],
    )
}

#[test]
fn old_manifest_digest_incorrect() {
    let result = official_error_test("E066_E092_old_manifest_digest_incorrect");

    has_errors(&result, &[
        version_error(
            "v1",
            ErrorCode::E066,
            "In inventory version v1, path 'file-1.txt' does not match the digest in later inventories. \
            Expected: 07e41ccb166d21a5327d5a2ae1bb48192b8470e1357266c9d119c294cb1e95978569472c9de64fb6d93cbd4dd0aed0bf1e7c47fd1920de17b038a08a85eb4fa1; Found: 17e41ccb166d21a5327d5a2ae1bb48192b8470e1357266c9d119c294cb1e95978569472c9de64fb6d93cbd4dd0aed0bf1e7c47fd1920de17b038a08a85eb4fa1",
        ),
        version_error(
            "v1",
            ErrorCode::E092,
            "Inventory manifest entry for content path 'v1/content/file-1.txt' differs from later versions. \
            Expected: 07e41ccb166d21a5327d5a2ae1bb48192b8470e1357266c9d119c294cb1e95978569472c9de64fb6d93cbd4dd0aed0bf1e7c47fd1920de17b038a08a85eb4fa1; Found: 17e41ccb166d21a5327d5a2ae1bb48192b8470e1357266c9d119c294cb1e95978569472c9de64fb6d93cbd4dd0aed0bf1e7c47fd1920de17b038a08a85eb4fa1",
        ),
        root_error(
            ErrorCode::E092,
            "Content file v1/content/file-1.txt failed sha512 fixity check. Expected: \
            17e41ccb166d21a5327d5a2ae1bb48192b8470e1357266c9d119c294cb1e95978569472c9de64fb6d93cbd4dd0aed0bf1e7c47fd1920de17b038a08a85eb4fa1; \
            Found: 07e41ccb166d21a5327d5a2ae1bb48192b8470e1357266c9d119c294cb1e95978569472c9de64fb6d93cbd4dd0aed0bf1e7c47fd1920de17b038a08a85eb4fa1",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn inconsistent_version_state() {
    let result = official_error_test("E066_inconsistent_version_state");

    has_errors(
        &result,
        &[
            version_error(
                "v1",
                ErrorCode::E066,
                "Inventory version v1 state contains a path not in later inventories: 2.txt",
            ),
            version_error(
                "v1",
                ErrorCode::E066,
                "Inventory version v1 state contains a path not in later inventories: 1.txt",
            ),
            version_error(
                "v1",
                ErrorCode::E066,
                "Inventory version v1 state contains a path not in later inventories: 3.txt",
            ),
        ],
    );
    no_warnings(&result);
}

#[test]
fn file_in_extensions_dir() {
    let result = official_error_test("E067_file_in_extensions_dir");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E067,
            "Extensions directory contains an illegal file: extra_file",
        )],
    );
    has_warnings(
        &result,
        &[
            root_warning(
                WarnCode::W007,
                "Inventory version 'v1' is missing recommended key 'message'",
            ),
            root_warning(
                WarnCode::W007,
                "Inventory version 'v1' is missing recommended key 'user'",
            ),
            root_warning(
                WarnCode::W013,
                "Extensions directory contains unknown extension: unregistered",
            ),
        ],
    );
}

#[test]
fn algorithm_change_incorrect_digest() {
    let result = official_error_test("E092_algorithm_change_incorrect_digest");

    has_errors(&result, &[
        root_error(
            ErrorCode::E092,
            "Content file v1/content/file-3.txt failed sha512 fixity check. Expected: \
            13b26d26c9d8cfbb884b50e798f93ac6bef275a018547b1560af3e6d38f2723785731d3ca6338682fa7ac9acb506b3c594a125ce9d3d60cd14498304cc864cf2; \
            Found: b3b26d26c9d8cfbb884b50e798f93ac6bef275a018547b1560af3e6d38f2723785731d3ca6338682fa7ac9acb506b3c594a125ce9d3d60cd14498304cc864cf2",
        ),
        root_error(
            ErrorCode::E092,
            "Content file v1/content/file-1.txt failed sha512 fixity check. Expected: \
            17e41ccb166d21a5327d5a2ae1bb48192b8470e1357266c9d119c294cb1e95978569472c9de64fb6d93cbd4dd0aed0bf1e7c47fd1920de17b038a08a85eb4fa1; \
            Found: 07e41ccb166d21a5327d5a2ae1bb48192b8470e1357266c9d119c294cb1e95978569472c9de64fb6d93cbd4dd0aed0bf1e7c47fd1920de17b038a08a85eb4fa1",
        ),
        root_error(
            ErrorCode::E092,
            "Content file v1/content/file-2.txt failed sha512 fixity check. \
            Expected: 1fef2458ee1a9277925614272adfe60872f4c1bf02eecce7276166957d1ab30f65cf5c8065a294bf1b13e3c3589ba936a3b5db911572e30dfcb200ef71ad33d5; \
            Found: 9fef2458ee1a9277925614272adfe60872f4c1bf02eecce7276166957d1ab30f65cf5c8065a294bf1b13e3c3589ba936a3b5db911572e30dfcb200ef71ad33d5",
        ),
    ]);
    has_warnings(
        &result,
        &[root_warning(
            WarnCode::W004,
            "Inventory 'digestAlgorithm' should be 'sha512'. Found: sha256",
        )],
    );
}

#[test]
fn content_file_digest_mismatch() {
    let result = official_error_test("E092_content_file_digest_mismatch");

    has_errors(&result, &[
        root_error(
            ErrorCode::E092,
            "Content file v1/content/test.txt failed sha512 fixity check. Expected: \
            24f950aac7b9ea9b3cb728228a0c82b67c39e96b4b344798870d5daee93e3ae5931baae8c7cacfea4b629452c38026a81d138bc7aad1af3ef7bfd5ec646d6c28; \
            Found: 1277a792c8196a2504007a40f31ed93bf826e71f16273d8503f7d3e46503d00b8d8cda0a59d6a33b9c1aebc84ea6a79f7062ee080f4a9587055a7b6fb92f5fa8",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn content_path_does_not_exist() {
    let result = official_error_test("E092_E093_content_path_does_not_exist");

    has_errors(&result, &[
        root_error(
            ErrorCode::E092,
            "Inventory manifest references a file that does not exist in a content directory: v1/content/bonus.txt",
        ),
        root_error(
            ErrorCode::E093,
            "Inventory fixity references a file that does not exist in a content directory: v1/content/bonus.txt",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn fixity_digest_mismatch() {
    let result = official_error_test("E093_fixity_digest_mismatch");

    has_errors(&result, &[
        root_error(
            ErrorCode::E093,
            "Content file v1/content/test.txt failed md5 fixity check. Expected: 9eacfb9289073dd9c9a8c4cdf820ac71; \
            Found: eb1a3227cdc3fedbaec2fe38bf6c044a",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn conflicting_logical_paths() {
    let result = official_error_test("E095_conflicting_logical_paths");

    has_errors(&result, &[
        root_error(
            ErrorCode::E095,
            "In inventory version v1, state contains a path, 'sub-path/a_file.txt', that conflicts \
            with another path, 'sub-path'",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn non_unique_logical_paths() {
    let result = official_error_test("E095_non_unique_logical_paths");

    has_errors(
        &result,
        &[
            root_error(
                ErrorCode::E095,
                "In inventory version v1, state contains duplicate path 'file-1.txt'",
            ),
            root_error(
                ErrorCode::E095,
                "In inventory version v1, state contains duplicate path 'file-3.txt'",
            ),
        ],
    );
    no_warnings(&result);
}

#[test]
fn manifest_duplicate_digests() {
    let result = official_error_test("E096_manifest_duplicate_digests");

    has_errors(&result, &[
        root_error(
            ErrorCode::E101,
            "Inventory manifest contains duplicate path 'v1/content/test.txt'",
        ),
        root_error(
            ErrorCode::E096,
            "Inventory manifest contains a duplicate key '24F950AAC7B9EA9B3CB728228A0C82B67C39E96B4B344798870D5DAEE93E3AE5931BAAE8C7CACFEA4B629452C38026A81D138BC7AAD1AF3EF7BFD5EC646D6C28'",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn fixity_duplicate_digests() {
    let result = official_error_test("E097_fixity_duplicate_digests");

    has_errors(&result, &[
        root_error(
            ErrorCode::E101,
            "Inventory fixity block 'md5' contains duplicate path 'v1/content/test.txt'",
        ),
        root_error(
            ErrorCode::E097,
            "Inventory fixity block 'md5' contains duplicate digest 'eb1a3227cdc3fedbaec2fe38bf6c044a'",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn fixity_invalid_content_paths() {
    let result = official_error_test("E100_E099_fixity_invalid_content_paths");

    has_errors(&result, &[
        root_error(
            ErrorCode::E099,
            "Inventory fixity block 'md5' contains a path containing an illegal path part. Found: v1/content/../content/file-1.txt",
        ),
        root_error(
            ErrorCode::E100,
            "Inventory fixity block 'md5' contains a path with a leading/trailing '/'. Found: /v1/content/file-3.txt",
        ),
        root_error(
            ErrorCode::E099,
            "Inventory fixity block 'md5' contains a path containing an illegal path part. Found: v1/content//file-2.txt",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn manifest_invalid_content_paths() {
    let result = official_error_test("E100_E099_manifest_invalid_content_paths");

    has_errors(&result, &[
        root_error(
            ErrorCode::E100,
            "Inventory manifest key 'b3b26d26c9d8cfbb884b50e798f93ac6bef275a018547b1560af3e6d38f2723785731d3ca6338682fa7ac9acb506b3c594a125ce9d3d60cd14498304cc864cf2' \
            contains a path with a leading/trailing '/'. Found: /v1/content/file-3.txt",
        ),
        root_error(
            ErrorCode::E099,
            "Inventory manifest key '07e41ccb166d21a5327d5a2ae1bb48192b8470e1357266c9d119c294cb1e95978569472c9de64fb6d93cbd4dd0aed0bf1e7c47fd1920de17b038a08a85eb4fa1' \
            contains a path containing an illegal path part. Found: v1/content/../content/file-1.txt",
        ),
        root_error(
            ErrorCode::E099,
            "Inventory manifest key '9fef2458ee1a9277925614272adfe60872f4c1bf02eecce7276166957d1ab30f65cf5c8065a294bf1b13e3c3589ba936a3b5db911572e30dfcb200ef71ad33d5' \
            contains a path containing an illegal path part. Found: v1/content//file-2.txt",
        ),
    ]);
    no_warnings(&result);
}

#[test]
fn non_unique_content_paths() {
    let result = official_error_test("E101_non_unique_content_paths");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E101,
            "Inventory manifest contains duplicate path 'v1/content/test.txt'",
        )],
    );
    no_warnings(&result);
}

#[test]
fn monotonically_increasing_versions() {
    let result = official_error_test("E103_inconsistent_ocfl_versions");

    has_errors(
        &result,
        &[version_error(
            "v2",
            ErrorCode::E103,
            "Inventory 'type' must be 'https://ocfl.io/1.0/spec/#inventory' or earlier. Found: https://ocfl.io/1.1/spec/#inventory",
        )],
    );
    no_warnings(&result);
}

#[test]
fn multiple_object_version_declarations() {
    let result = official_error_test("E003_multiple_decl");

    has_errors(
        &result,
        &[root_error(
            ErrorCode::E003,
            "Multiple object version declarations found",
        )],
    );
    no_warnings(&result);
}

#[test]
fn zero_padded_versions() {
    let result = official_warn_test("W001_W004_W005_zero_padded_versions");

    no_errors(&result);
    has_warnings(
        &result,
        &[
            root_warning(
                WarnCode::W005,
                "Inventory 'id' should be a URI. Found: bb123cd4567",
            ),
            root_warning(
                WarnCode::W004,
                "Inventory 'digestAlgorithm' should be 'sha512'. Found: sha256",
            ),
            root_warning(WarnCode::W001, "Contains zero-padded version numbers"),
            version_warning(
                "v0003",
                WarnCode::W005,
                "Inventory 'id' should be a URI. Found: bb123cd4567",
            ),
            version_warning(
                "v0003",
                WarnCode::W004,
                "Inventory 'digestAlgorithm' should be 'sha512'. Found: sha256",
            ),
            version_warning(
                "v0003",
                WarnCode::W001,
                "Contains zero-padded version numbers",
            ),
            version_warning(
                "v0002",
                WarnCode::W005,
                "Inventory 'id' should be a URI. Found: bb123cd4567",
            ),
            version_warning(
                "v0002",
                WarnCode::W004,
                "Inventory 'digestAlgorithm' should be 'sha512'. Found: sha256",
            ),
            version_warning(
                "v0002",
                WarnCode::W001,
                "Contains zero-padded version numbers",
            ),
            version_warning(
                "v0001",
                WarnCode::W005,
                "Inventory 'id' should be a URI. Found: bb123cd4567",
            ),
            version_warning(
                "v0001",
                WarnCode::W004,
                "Inventory 'digestAlgorithm' should be 'sha512'. Found: sha256",
            ),
            version_warning(
                "v0001",
                WarnCode::W001,
                "Contains zero-padded version numbers",
            ),
        ],
    );
}

#[test]
fn zero_padded_versions_2() {
    let result = official_warn_test("W001_zero_padded_versions");

    no_errors(&result);
    has_warnings(
        &result,
        &[
            root_warning(WarnCode::W001, "Contains zero-padded version numbers"),
            version_warning(
                "v002",
                WarnCode::W001,
                "Contains zero-padded version numbers",
            ),
            version_warning(
                "v001",
                WarnCode::W001,
                "Contains zero-padded version numbers",
            ),
        ],
    );
}

#[test]
fn extra_dir_in_version_dir() {
    let result = official_warn_test("W002_extra_dir_in_version_dir");

    no_errors(&result);
    has_warnings(
        &result,
        &[version_warning(
            "v1",
            WarnCode::W002,
            "Version directory contains unexpected directory: extra_dir",
        )],
    );
}

#[test]
fn uses_sha256() {
    let result = official_warn_test("W004_uses_sha256");

    no_errors(&result);
    has_warnings(
        &result,
        &[root_warning(
            WarnCode::W004,
            "Inventory 'digestAlgorithm' should be 'sha512'. Found: sha256",
        )],
    );
}

#[test]
fn versions_diff_digests() {
    let result = official_warn_test("W004_versions_diff_digests");

    no_errors(&result);
    has_warnings(
        &result,
        &[version_warning(
            "v1",
            WarnCode::W004,
            "Inventory 'digestAlgorithm' should be 'sha512'. Found: sha256",
        )],
    );
}

#[test]
fn id_not_uri() {
    let result = official_warn_test("W005_id_not_uri");

    no_errors(&result);
    has_warnings(
        &result,
        &[root_warning(
            WarnCode::W005,
            "Inventory 'id' should be a URI. Found: not_a_uri",
        )],
    );
}

#[test]
fn no_message_or_user() {
    let result = official_warn_test("W007_no_message_or_user");

    no_errors(&result);
    has_warnings(
        &result,
        &[
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
fn user_no_address() {
    let result = official_warn_test("W008_user_no_address");

    no_errors(&result);
    has_warnings(
        &result,
        &[root_warning(
            WarnCode::W008,
            "Inventory version 'v1' is missing recommended key 'address'",
        )],
    );
}

#[test]
fn user_address_not_uri() {
    let result = official_warn_test("W009_user_address_not_uri");

    no_errors(&result);
    has_warnings(
        &result,
        &[root_warning(
            WarnCode::W009,
            "Inventory version v1 user 'address' should be a URI. \
        Found: 1 Wonky Way, Wibblesville, WW",
        )],
    );
}

#[test]
fn no_version_inventory() {
    let result = official_warn_test("W010_no_version_inventory");

    no_errors(&result);
    has_warnings(
        &result,
        &[version_warning(
            "v1",
            WarnCode::W010,
            "Inventory file does not exist",
        )],
    );
}

#[test]
fn version_inv_diff_metadata() {
    let result = official_warn_test("W011_version_inv_diff_metadata");

    no_errors(&result);
    has_warnings(
        &result,
        &[
            version_warning(
                "v1",
                WarnCode::W011,
                "Inventory version v1 'message' is inconsistent with the root inventory",
            ),
            version_warning(
                "v1",
                WarnCode::W011,
                "Inventory version v1 'created' is inconsistent with the root inventory",
            ),
            version_warning(
                "v1",
                WarnCode::W011,
                "Inventory version v1 'user' is inconsistent with the root inventory",
            ),
        ],
    );
}

#[test]
fn unregistered_extension() {
    let result = official_warn_test("W013_unregistered_extension");

    no_errors(&result);
    has_warnings(
        &result,
        &[root_warning(
            WarnCode::W013,
            "Extensions directory contains unknown extension: unregistered",
        )],
    );
}

#[test]
fn official_valid() {
    let names = [
        "minimal_content_dir_called_stuff",
        "minimal_mixed_digests",
        "minimal_no_content",
        "minimal_one_version_one_file",
        "minimal_uppercase_digests",
        "ocfl_object_all_fixity_digests",
        "spec-ex-full",
        "updates_all_actions",
        "updates_three_versions_one_file",
        "ocfl_version_change",
    ];

    for name in names {
        let result = official_valid_test(name);
        assert!(
            !result.has_errors(),
            "{} should have no errors; found: {:?}",
            name,
            result.errors()
        );
        assert!(
            !result.has_warnings(),
            "{} should have no warnings; found: {:?}",
            name,
            result.warnings()
        );
    }
}

#[test]
#[should_panic(expected = "Not found: Object at path bogus")]
fn validate_object_does_not_exist() {
    official_warn_test("bogus");
}

#[test]
fn validate_valid_repo() {
    let repo = new_repo(&repo_test_path("valid"));
    let mut validator = repo.validate_repo(true).unwrap();

    no_errors_storage(validator.storage_root_result());
    no_warnings_storage(validator.storage_root_result());

    for result in &mut validator {
        let result = result.unwrap();
        no_errors(&result);
        no_warnings(&result);
    }

    no_errors_storage(validator.storage_hierarchy_result());
    no_warnings_storage(validator.storage_hierarchy_result());
}

#[test]
fn validate_invalid_repo() {
    let repo = new_repo(&repo_test_path("invalid"));
    let mut validator = repo.validate_repo(true).unwrap();

    has_errors_storage(
        &validator.storage_root_result(),
        &[
            ValidationError::new(
                ProblemLocation::StorageRoot,
                ErrorCode::E069,
                "Root version declaration does not exist".to_string(),
            ),
            ValidationError::new(
                ProblemLocation::StorageRoot,
                ErrorCode::E112,
                "Extensions directory contains an illegal file: file.txt".to_string(),
            ),
        ],
    );
    has_warnings_storage(
        &validator.storage_root_result(),
        &[ValidationWarning::new(
            ProblemLocation::StorageRoot,
            WarnCode::W016,
            "Extensions directory contains unknown extension: bogus-ext".to_string(),
        )],
    );

    for result in &mut validator {
        let result = result.unwrap();
        match result.object_id.as_ref().unwrap().as_ref() {
            "urn:example:rocfl:obj-2" => {
                error_count(2, &result);
                warning_count(0, &result);
            }
            "urn:example:rocfl:obj-1" => {
                no_errors(&result);
                no_warnings(&result);
            }
            id => {
                panic!("Unexpected object: {}", id)
            }
        }
    }

    has_errors_storage(&validator.storage_hierarchy_result(), &[
        ValidationError::new(ProblemLocation::StorageHierarchy, ErrorCode::E072,
                             "Found a file in the storage hierarchy: b01/0ba/world.txt".to_string()),
        ValidationError::new(ProblemLocation::StorageHierarchy, ErrorCode::E072,
                             "Found a file in the storage hierarchy: \
                             b99/7a6/7ea/b997a67eacd839691ff9d6e490c5654e14a1783d460e4a4ef8d027547ddbf9e2/v1/content/dir/sub/file3.txt".to_string()),
        ValidationError::new(ProblemLocation::StorageHierarchy, ErrorCode::E072,
                             "Found a file in the storage hierarchy: \
                             b99/7a6/7ea/b997a67eacd839691ff9d6e490c5654e14a1783d460e4a4ef8d027547ddbf9e2/v1/content/dir/file2.txt".to_string()),
        ValidationError::new(ProblemLocation::StorageHierarchy, ErrorCode::E072,
                             "Found a file in the storage hierarchy: \
                             b99/7a6/7ea/b997a67eacd839691ff9d6e490c5654e14a1783d460e4a4ef8d027547ddbf9e2/v1/content/file1.txt".to_string()),
        ValidationError::new(ProblemLocation::StorageHierarchy, ErrorCode::E072,
                             "Found a file in the storage hierarchy: \
                             b99/7a6/7ea/b997a67eacd839691ff9d6e490c5654e14a1783d460e4a4ef8d027547ddbf9e2/v1/inventory.json".to_string()),
        ValidationError::new(ProblemLocation::StorageHierarchy, ErrorCode::E072,
                             "Found a file in the storage hierarchy: \
                             b99/7a6/7ea/b997a67eacd839691ff9d6e490c5654e14a1783d460e4a4ef8d027547ddbf9e2/v1/inventory.json.sha512".to_string()),
        ValidationError::new(ProblemLocation::StorageHierarchy, ErrorCode::E072,
                             "Found a file in the storage hierarchy: \
                             b99/7a6/7ea/b997a67eacd839691ff9d6e490c5654e14a1783d460e4a4ef8d027547ddbf9e2/inventory.json".to_string()),
        ValidationError::new(ProblemLocation::StorageHierarchy, ErrorCode::E072,
                             "Found a file in the storage hierarchy: \
                             b99/7a6/7ea/b997a67eacd839691ff9d6e490c5654e14a1783d460e4a4ef8d027547ddbf9e2/inventory.json.sha512".to_string()),
    ]);
    no_warnings_storage(validator.storage_hierarchy_result());
}

#[test]
fn multiple_root_version_declarations() {
    let repo = new_repo(&repo_test_path("multiple-root-decls"));
    let mut validator = repo.validate_repo(true).unwrap();

    has_errors_storage(
        &validator.storage_root_result(),
        &[ValidationError::new(
            ProblemLocation::StorageRoot,
            ErrorCode::E076,
            "Multiple root version declarations found".to_string(),
        )],
    );
    no_warnings_storage(validator.storage_root_result());

    for result in &mut validator {
        let result = result.unwrap();
        no_errors(&result);
        no_warnings(&result);
    }

    no_errors_storage(validator.storage_hierarchy_result());
    no_warnings_storage(validator.storage_hierarchy_result());
}

fn official_valid_test(name: &str) -> ObjectValidationResult {
    let repo = new_repo(official_valid_root());
    repo.validate_object_at(name, true).unwrap()
}

fn official_error_test(name: &str) -> ObjectValidationResult {
    let repo = new_repo(official_error_root());
    repo.validate_object_at(name, true).unwrap()
}

fn official_warn_test(name: &str) -> ObjectValidationResult {
    let repo = new_repo(official_warn_root());
    repo.validate_object_at(name, true).unwrap()
}

fn repo_test_path(name: &str) -> PathBuf {
    let mut path = validate_repo_root();
    path.push("custom");
    path.push("repos");
    path.push(name);
    path
}

fn new_repo(root: impl AsRef<Path>) -> OcflRepo {
    OcflRepo::fs_repo(root, None).unwrap()
}

fn official_valid_root() -> PathBuf {
    let mut path = validate_repo_root();
    path.push("official-1.0");
    path.push("valid");
    path
}

fn official_error_root() -> PathBuf {
    let mut path = validate_repo_root();
    path.push("official-1.0");
    path.push("error");
    path
}

fn official_warn_root() -> PathBuf {
    let mut path = validate_repo_root();
    path.push("official-1.0");
    path.push("warn");
    path
}

fn validate_repo_root() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("resources");
    path.push("test");
    path.push("validate");
    path
}

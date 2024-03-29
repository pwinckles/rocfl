// TODO fix this so the tests can be run in parallel
//! These tests **MUST** be run sequentially with `cargo test -- --test-threads=1` because of
//! https://github.com/hyperium/hyper/issues/2112
//!
//! The following env variables must be set for the tests to run:
//! - AWS_ACCESS_KEY_ID
//! - AWS_SECRET_ACCESS_KEY
//! - OCFL_TEST_S3_BUCKET
#![cfg(feature = "s3")]

use std::panic::UnwindSafe;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::{env, fs, panic};

use assert_fs::prelude::*;
use assert_fs::TempDir;
use common::*;
use fs_extra::dir::CopyOptions;
use rand::Rng;
use rocfl::ocfl::{
    CommitMeta, DigestAlgorithm, ErrorCode, FileDetails, LayoutExtensionName, OcflRepo,
    ProblemLocation, RocflError, SpecVersion, StorageLayout, ValidationError, VersionNum,
    VersionRef, WarnCode,
};
use rusoto_core::Region;
use rusoto_s3::{
    DeleteObjectRequest, GetObjectRequest, HeadObjectRequest, ListObjectsV2Request,
    PutObjectRequest, S3Client, S3,
};
use tokio::io::AsyncReadExt;

mod common;

const BUCKET_VAR: &str = "OCFL_TEST_S3_BUCKET";
const ACCESS_VAR: &str = "AWS_ACCESS_KEY_ID";
const ACCESS_HASH: &str = "20cdc6f24747a49e6d295082e4bcaa81612a31e27d5916540429720bc0a43226";
const REGION: Region = Region::UsEast2;

const DEFAULT_LAYOUT: &str = r#"{
  "extensionName": "0004-hashed-n-tuple-storage-layout",
  "digestAlgorithm": "sha512",
  "tupleSize": 5,
  "numberOfTuples": 2,
  "shortObjectRoot": true
}"#;

#[test]
fn create_new_repo_empty_dir() {
    skip_or_run_s3_test(
        "create_new_repo_empty_dir",
        |s3_client: S3Client, prefix: String, staging: TempDir, temp: TempDir| {
            let repo = default_repo(&prefix, staging.path());

            assert_file(&s3_client, &prefix, "0=ocfl_1.0", "ocfl_1.0\n");
            assert_file(
                &s3_client,
                &prefix,
                "ocfl_1.0.txt",
                &read_spec("ocfl_1.0.txt"),
            );
            assert_storage_layout(
                &s3_client,
                &prefix,
                "0004-hashed-n-tuple-storage-layout",
                DEFAULT_LAYOUT,
            );

            let object_id = "s3-object";

            repo.create_object(
                object_id,
                Some(SpecVersion::Ocfl1_0),
                DigestAlgorithm::Sha256,
                "content",
                0,
            )
            .unwrap();
            repo.copy_files_external(
                object_id,
                &[create_file(&temp, "test.txt", "testing").path()],
                "/",
                false,
            )
            .unwrap();
            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            assert_file_exists(
                &s3_client,
                &prefix,
                "a7aba/e5855/9c91bb9cca7697aca8789730e82ad82e1c1a63736e52dafc99ba4b7e3896276d5266ca\
                5947374b59d15735e38d6e5b8d131268509bf601bdad8d4c/0=ocfl_object_1.0",
            );
        },
    );
}

#[test]
#[should_panic(expected = "Cannot create new repository. Storage root must be empty")]
fn fail_create_new_repo_when_repo_already_exists() {
    panic_or_run_s3_test(
        "fail_create_new_repo_when_repo_already_exists",
        "Cannot create new repository. Storage root must be empty",
        |_s3_client: S3Client, prefix: String, staging: TempDir, _temp: TempDir| {
            let _ = default_repo(&prefix, staging.path());
            let _ = default_repo(&prefix, staging.path());
        },
    );
}

#[test]
fn create_new_object() {
    skip_or_run_s3_test(
        "create_new_object",
        |s3_client: S3Client, prefix: String, staging: TempDir, temp: TempDir| {
            let repo = default_repo(&prefix, staging.path());
            let object_id = "s3-object";

            repo.create_object(
                object_id,
                Some(SpecVersion::Ocfl1_0),
                DigestAlgorithm::Sha256,
                "content",
                0,
            )
            .unwrap();
            repo.copy_files_external(
                object_id,
                &[create_file(&temp, "test.txt", "testing").path()],
                "/",
                false,
            )
            .unwrap();
            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            let object = repo.get_object(object_id, VersionRef::Head).unwrap();

            assert_eq!(1, object.state.len());

            assert_file_details(
                &s3_client,
                object.state.get(&lpath("test.txt")).unwrap(),
                &object.object_root,
                "v1/content/test.txt",
                "cf80cd8aed482d5d1527d7dc72fceff84e6326592848447d2dc0b0e87dfc9a90",
            );
        },
    );
}

#[test]
#[should_panic(expected = "Cannot create object s3-object because it already exists")]
fn fail_create_new_object_when_already_exists() {
    panic_or_run_s3_test(
        "fail_create_new_object_when_already_exists",
        "Cannot create object s3-object because it already exists",
        |_s3_client: S3Client, prefix: String, staging: TempDir, temp: TempDir| {
            let repo = default_repo(&prefix, staging.path());
            let object_id = "s3-object";

            repo.create_object(
                object_id,
                Some(SpecVersion::Ocfl1_0),
                DigestAlgorithm::Sha256,
                "content",
                0,
            )
            .unwrap();
            repo.copy_files_external(
                object_id,
                &[create_file(&temp, "test.txt", "testing").path()],
                "/",
                false,
            )
            .unwrap();
            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            repo.create_object(
                object_id,
                Some(SpecVersion::Ocfl1_0),
                DigestAlgorithm::Sha256,
                "content",
                0,
            )
            .unwrap();
        },
    );
}

#[test]
fn create_and_update_object() {
    skip_or_run_s3_test(
        "create_and_update_object",
        |s3_client: S3Client, prefix: String, staging: TempDir, temp: TempDir| {
            let repo = default_repo(&prefix, staging.path());
            let object_id = "s3-object";

            repo.create_object(
                object_id,
                Some(SpecVersion::Ocfl1_0),
                DigestAlgorithm::Sha256,
                "content",
                0,
            )
            .unwrap();

            create_dirs(&temp, "a/b/c");
            create_dirs(&temp, "a/d/e");
            create_dirs(&temp, "a/f");

            create_file(&temp, "a/file1.txt", "File One");
            create_file(&temp, "a/b/file2.txt", "File Two");
            create_file(&temp, "a/b/file3.txt", "File Three");
            create_file(&temp, "a/b/c/file4.txt", "File Four");
            create_file(&temp, "a/d/e/file5.txt", "File Five");
            create_file(&temp, "a/f/file6.txt", "File Six");

            repo.move_files_external(object_id, &[temp.child("a").path()], "/")
                .unwrap();

            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            repo.remove_files(object_id, &["a/b/file3.txt", "a/b/c/file4.txt"], false)
                .unwrap();

            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            repo.copy_files_internal(
                object_id,
                VersionNum::v1().into(),
                &["a/b/file3.txt"],
                "/",
                false,
            )
            .unwrap();
            repo.copy_files_internal(
                object_id,
                VersionNum::v1().into(),
                &["a/file1.txt"],
                "something/file1.txt",
                false,
            )
            .unwrap();

            create_dirs(&temp, "something");

            repo.copy_files_external(
                object_id,
                &[create_file(&temp, "something/new.txt", "NEW").path()],
                "something/new.txt",
                true,
            )
            .unwrap();

            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            repo.copy_files_external(
                object_id,
                &[create_file(&temp, "file6.txt", "UPDATED!").path()],
                "a/f/file6.txt",
                true,
            )
            .unwrap();

            repo.move_files_internal(object_id, &["a/d/e/file5.txt"], "a/file5.txt")
                .unwrap();

            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            let object = repo.get_object(object_id, VersionRef::Head).unwrap();

            assert_eq!(7, object.state.len());

            assert_file_details(
                &s3_client,
                object.state.get(&lpath("file3.txt")).unwrap(),
                &object.object_root,
                "v1/content/a/b/file3.txt",
                "e18fad97c1b6512b1588a1fa2b7f9a0e549df9cfc538ce6943b4f0f4ae78322c",
            );
            assert_file_details(
                &s3_client,
                object.state.get(&lpath("a/file1.txt")).unwrap(),
                &object.object_root,
                "v1/content/a/file1.txt",
                "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
            );
            assert_file_details(
                &s3_client,
                object.state.get(&lpath("a/file5.txt")).unwrap(),
                &object.object_root,
                "v1/content/a/d/e/file5.txt",
                "4ccdbf78d368aed12d806efaf67fbce3300bca8e62a6f32716af2f447de1821e",
            );
            assert_file_details(
                &s3_client,
                object.state.get(&lpath("a/b/file2.txt")).unwrap(),
                &object.object_root,
                "v1/content/a/b/file2.txt",
                "b47592b10bc3e5c8ca8703d0862df10a6e409f43478804f93a08dd1844ae81b6",
            );
            assert_file_details(
                &s3_client,
                object.state.get(&lpath("a/f/file6.txt")).unwrap(),
                &object.object_root,
                "v4/content/a/f/file6.txt",
                "df21fb2fb83c1c64015a00e7677ccceb8da5377cba716611570230fb91d32bc9",
            );
            assert_file_details(
                &s3_client,
                object.state.get(&lpath("something/file1.txt")).unwrap(),
                &object.object_root,
                "v1/content/a/file1.txt",
                "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
            );
            assert_file_details(
                &s3_client,
                object.state.get(&lpath("something/new.txt")).unwrap(),
                &object.object_root,
                "v3/content/something/new.txt",
                "a253ff09c5a8678e1fd1962b2c329245e139e45f9cc6ced4e5d7ad42c4108fc0",
            );
        },
    );
}

#[test]
fn validate_valid_object() {
    skip_or_run_s3_test(
        "validate_valid_object",
        |_s3_client: S3Client, prefix: String, staging: TempDir, temp: TempDir| {
            let repo = default_repo(&prefix, staging.path());
            let object_id = "urn:example:rocfl:s3-object";
            let commit_meta = CommitMeta::new()
                .with_message(Some("commit".to_string()))
                .with_user(
                    Some("Peter Winckles".to_string()),
                    Some("mailto:me@example.com".to_string()),
                )
                .unwrap();

            repo.create_object(
                object_id,
                Some(SpecVersion::Ocfl1_0),
                DigestAlgorithm::Sha512,
                "content",
                0,
            )
            .unwrap();

            create_dirs(&temp, "a/b/c");
            create_dirs(&temp, "a/d/e");
            create_dirs(&temp, "a/f");

            create_file(&temp, "a/file1.txt", "File One");
            create_file(&temp, "a/b/file2.txt", "File Two");
            create_file(&temp, "a/b/file3.txt", "File Three");
            create_file(&temp, "a/b/c/file4.txt", "File Four");
            create_file(&temp, "a/d/e/file5.txt", "File Five");
            create_file(&temp, "a/f/file6.txt", "File Six");

            repo.move_files_external(object_id, &[temp.child("a").path()], "/")
                .unwrap();

            repo.commit(object_id, commit_meta.clone(), None, false)
                .unwrap();

            repo.remove_files(object_id, &["a/b/file3.txt", "a/b/c/file4.txt"], false)
                .unwrap();

            repo.commit(object_id, commit_meta.clone(), None, false)
                .unwrap();

            repo.copy_files_internal(
                object_id,
                VersionNum::v1().into(),
                &["a/b/file3.txt"],
                "/",
                false,
            )
            .unwrap();
            repo.copy_files_internal(
                object_id,
                VersionNum::v1().into(),
                &["a/file1.txt"],
                "something/file1.txt",
                false,
            )
            .unwrap();

            create_dirs(&temp, "something");

            repo.copy_files_external(
                object_id,
                &[create_file(&temp, "something/new.txt", "NEW").path()],
                "something/new.txt",
                true,
            )
            .unwrap();

            repo.commit(object_id, commit_meta.clone(), None, false)
                .unwrap();

            repo.copy_files_external(
                object_id,
                &[create_file(&temp, "file6.txt", "UPDATED!").path()],
                "a/f/file6.txt",
                true,
            )
            .unwrap();

            repo.move_files_internal(object_id, &["a/d/e/file5.txt"], "a/file5.txt")
                .unwrap();

            repo.commit(object_id, commit_meta, None, false).unwrap();

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
        },
    );
}

#[test]
fn validate_invalid_object() {
    skip_or_run_s3_test(
        "validate_invalid_object",
        |s3_client: S3Client, prefix: String, staging: TempDir, temp: TempDir| {
            let repo = default_repo(&prefix, staging.path());
            let object_id = "urn:example:rocfl:s3-object";
            let object_id_2 = "urn:example:rocfl:s3-object-2";

            let commit_meta = CommitMeta::new()
                .with_message(Some("commit".to_string()))
                .with_user(
                    Some("Peter Winckles".to_string()),
                    Some("mailto:me@example.com".to_string()),
                )
                .unwrap();

            repo.create_object(
                object_id,
                Some(SpecVersion::Ocfl1_0),
                DigestAlgorithm::Sha256,
                "content",
                0,
            )
            .unwrap();

            create_dirs(&temp, "a/b/c");
            create_dirs(&temp, "a/d/e");
            create_dirs(&temp, "a/f");

            create_file(&temp, "a/file1.txt", "File One");
            create_file(&temp, "a/b/file2.txt", "File Two");
            create_file(&temp, "a/b/file3.txt", "File Three");
            create_file(&temp, "a/b/c/file4.txt", "File Four");
            create_file(&temp, "a/d/e/file5.txt", "File Five");
            create_file(&temp, "a/f/file6.txt", "File Six");

            repo.move_files_external(object_id, &[temp.child("a").path()], "/")
                .unwrap();

            repo.commit(object_id, commit_meta.clone(), None, false)
                .unwrap();

            repo.create_object(
                object_id_2,
                Some(SpecVersion::Ocfl1_0),
                DigestAlgorithm::Sha512,
                "content",
                0,
            )
            .unwrap();
            repo.copy_files_external(
                object_id_2,
                &[create_file(&temp, "test.txt", "testing").path()],
                "/",
                false,
            )
            .unwrap();

            repo.commit(object_id_2, commit_meta, None, false).unwrap();

            let details = repo
                .get_object_details(object_id, VersionRef::Head)
                .unwrap();

            write_file(&s3_client, &format!("{}/0=ocfl_1.0", prefix), "garbage");
            write_file(&s3_client, &format!("{}/abc/random.txt", prefix), "garbage");
            write_file(
                &s3_client,
                &format!("{}/file.txt", &details.object_root),
                "garbage",
            );
            write_file(
                &s3_client,
                &format!("{}/v1/content/file.txt", &details.object_root),
                "garbage",
            );

            let mut validator = repo.validate_repo(true).unwrap();

            has_errors_storage(
                validator.storage_root_result(),
                &[ValidationError::new(
                    ProblemLocation::StorageRoot,
                    ErrorCode::E080,
                    "Root version declaration is invalid. Expected: ocfl_1.0; Found: garbage"
                        .to_string(),
                )],
            );
            no_warnings_storage(validator.storage_root_result());

            for result in &mut validator {
                let result = result.unwrap();
                match result.object_id.as_ref().unwrap().as_ref() {
                    "urn:example:rocfl:s3-object" => {
                        has_errors(&result, &[
                            root_error(ErrorCode::E001, "Unexpected file in object root: file.txt"),
                            root_error(ErrorCode::E023, "A content file exists that is not referenced in the manifest: v1/content/file.txt")
                        ]);
                        has_warnings(
                            &result,
                            &[root_warning(
                                WarnCode::W004,
                                "Inventory 'digestAlgorithm' should be 'sha512'. Found: sha256",
                            )],
                        );
                    }
                    "urn:example:rocfl:s3-object-2" => {
                        no_errors(&result);
                        no_warnings(&result);
                    }
                    id => panic!("Unexpected object: {}", id),
                }
            }

            has_errors_storage(
                validator.storage_hierarchy_result(),
                &[ValidationError::new(
                    ProblemLocation::StorageHierarchy,
                    ErrorCode::E072,
                    "Found a file in the storage hierarchy: abc/random.txt".to_string(),
                )],
            );
            no_warnings_storage(validator.storage_hierarchy_result());
        },
    );
}

#[test]
fn purge_object() {
    skip_or_run_s3_test(
        "purge_object",
        |_s3_client: S3Client, prefix: String, staging: TempDir, temp: TempDir| {
            let repo = default_repo(&prefix, staging.path());
            let object_id = "s3-object-purge";

            repo.create_object(
                object_id,
                Some(SpecVersion::Ocfl1_0),
                DigestAlgorithm::Sha256,
                "content",
                0,
            )
            .unwrap();
            repo.copy_files_external(
                object_id,
                &[create_file(&temp, "test.txt", "testing").path()],
                "/",
                false,
            )
            .unwrap();
            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            let _ = repo.get_object(object_id, VersionRef::Head).unwrap();

            repo.purge_object(object_id).unwrap();

            match repo.get_object(object_id, VersionRef::Head) {
                Err(RocflError::NotFound(_)) => (),
                _ => panic!("Expected {} to not be found.", object_id),
            }
        },
    );
}

#[test]
fn purge_object_when_not_exists() {
    skip_or_run_s3_test(
        "purge_object_when_not_exists",
        |_s3_client: S3Client, prefix: String, staging: TempDir, _temp: TempDir| {
            let repo = default_repo(&prefix, staging.path());
            let object_id = "s3-object-purge";
            repo.purge_object(object_id).unwrap();
        },
    );
}

#[test]
#[should_panic(
    expected = "Cannot create version v2 in object out-of-sync because the current version is at v2"
)]
fn fail_commit_when_out_of_sync() {
    panic_or_run_s3_test(
        "fail_commit_when_out_of_sync",
        "Cannot create version v2 in object out-of-sync because the current version is at v2",
        |_s3_client: S3Client, prefix: String, staging: TempDir, temp: TempDir| {
            let repo = default_repo(&prefix, staging.path());
            let object_id = "out-of-sync";
            let id_hash = "46acfc156ff00023c6ff7c5cfc923eaf43123f63dd558579e90293f0eba1e574";

            repo.create_object(
                object_id,
                Some(SpecVersion::Ocfl1_0),
                DigestAlgorithm::Sha256,
                "content",
                0,
            )
            .unwrap();
            repo.move_files_external(
                object_id,
                &[create_file(&temp, "test.txt", "testing").path()],
                "/",
            )
            .unwrap();
            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            repo.move_files_external(
                object_id,
                &[create_file(&temp, "test2.txt", "testing 2").path()],
                "/",
            )
            .unwrap();

            let staged = repo.get_staged_object(object_id).unwrap();
            let staged_root = PathBuf::from(&staged.object_root);

            let mut options = CopyOptions::new();
            options.copy_inside = true;

            fs_extra::dir::copy(&staged_root, temp.path(), &options).unwrap();

            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            fs_extra::dir::copy(temp.child(id_hash).path(), &staged_root, &options).unwrap();

            repo.move_files_external(
                object_id,
                &[create_file(&temp, "b-file.txt", "another").path()],
                "/",
            )
            .unwrap();

            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();
        },
    );
}

fn panic_or_run_s3_test(
    name: &str,
    message: &str,
    test: impl FnOnce(S3Client, String, TempDir, TempDir) + UnwindSafe,
) {
    if should_ignore_test() {
        println!("Skipping test {}", name);
        panic!("{}", message);
    }

    run_s3_test(name, test)
}

fn skip_or_run_s3_test(
    name: &str,
    test: impl FnOnce(S3Client, String, TempDir, TempDir) + UnwindSafe,
) {
    if should_ignore_test() {
        println!("Skipping test {}", name);
        return;
    }

    run_s3_test(name, test)
}

/// Runs the test if the environment is configured to run S3 tests, and removes all resources
/// created during the test run, regardless of the test's outcome.
fn run_s3_test(name: &str, test: impl FnOnce(S3Client, String, TempDir, TempDir) + UnwindSafe) {
    // let _ = env_logger::builder().is_test(true).filter_level(LevelFilter::Info).try_init();

    let staging = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();
    let prefix = s3_prefix();

    let result = panic::catch_unwind(|| test(S3Client::new(REGION), prefix.clone(), staging, temp));

    if let Err(e) = panic::catch_unwind(|| delete_all(&S3Client::new(REGION), &prefix)) {
        let s = e
            .downcast()
            .unwrap_or_else(|e| Box::new(format!("{:?}", e)));
        eprintln!("Failed to cleanup test {}: {}", name, s);
    }

    if let Err(e) = result {
        let s = e
            .downcast()
            .unwrap_or_else(|e| Box::new(format!("{:?}", e)));
        panic!("Test {} failed: {}", name, s);
    }
}

/// These tests rely on the following env variables to be set:
///
/// - AWS_ACCESS_KEY_ID
/// - AWS_SECRET_ACCESS_KEY
/// - OCFL_TEST_S3_BUCKET
fn should_ignore_test() -> bool {
    let has_creds = match env::var(ACCESS_VAR) {
        Ok(key) => DigestAlgorithm::Sha256
            .hash_hex(&mut key.as_bytes())
            .unwrap()
            .as_ref()
            .eq(ACCESS_HASH),
        Err(_e) => false,
    };

    !has_creds || env::var(BUCKET_VAR).is_err()
}

fn assert_file_details(
    s3_client: &S3Client,
    actual: &FileDetails,
    object_root: &str,
    content_path: &str,
    digest: &str,
) {
    assert_eq!(cpath_rc(content_path), actual.content_path);
    assert_eq!(
        format!("{}/{}", object_root, content_path),
        actual.storage_path
    );
    if digest.len() == 64 {
        assert_eq!(
            digest,
            file_digest(s3_client, &actual.storage_path, DigestAlgorithm::Sha256).as_str()
        )
    } else {
        assert_eq!(
            digest,
            file_digest(s3_client, &actual.storage_path, DigestAlgorithm::Sha512).as_str()
        )
    }
    assert_eq!(Rc::new(digest.into()), actual.digest);
}

fn assert_file_exists(s3_client: &S3Client, root: &str, path: &str) {
    let key = format!("{}/{}", root, path);
    let _ = tokio_test::block_on(s3_client.head_object(HeadObjectRequest {
        bucket: bucket(),
        key: key.clone(),
        ..Default::default()
    }))
    .unwrap_or_else(|_| panic!("Expected {} to exist", key));
}

fn assert_file(s3_client: &S3Client, root: &str, path: &str, content: &str) {
    let key = format!("{}/{}", root, path);
    let actual_content = get_content_with_key(s3_client, &key);
    assert_eq!(content, actual_content);
}

fn assert_file_contains(s3_client: &S3Client, root: &str, path: &str, content: &str) {
    let key = format!("{}/{}", root, path);
    let actual_content = get_content_with_key(s3_client, &key);
    assert!(
        actual_content.contains(content),
        "Expected {} to contain {}. Found: {}",
        key,
        content,
        actual_content
    );
}

fn get_content_with_key(s3_client: &S3Client, key: &str) -> String {
    tokio_test::block_on(async move {
        let response = s3_client
            .get_object(GetObjectRequest {
                bucket: bucket(),
                key: key.to_string(),
                ..Default::default()
            })
            .await
            .unwrap_or_else(|_| panic!("Expected {} to exist", key));

        let mut reader = response.body.unwrap().into_async_read();
        let mut buf = [0; 8192];
        let mut content = Vec::new();
        loop {
            let read = reader.read(&mut buf).await.unwrap();
            if read == 0 {
                break;
            }
            content.extend_from_slice(&buf[..read]);
        }

        String::from_utf8(content).unwrap()
    })
}

fn write_file(s3_client: &S3Client, key: &str, contents: &str) {
    tokio_test::block_on(async move {
        let _ = s3_client
            .put_object(PutObjectRequest {
                bucket: bucket(),
                key: key.to_string(),
                body: Some(contents.to_string().into_bytes().into()),
                ..Default::default()
            })
            .await
            .unwrap_or_else(|_| panic!("Expected put {} to succeed", key));
    })
}

fn file_digest(s3_client: &S3Client, key: &str, algorithm: DigestAlgorithm) -> String {
    let content = get_content_with_key(s3_client, key);
    algorithm
        .hash_hex(&mut content.as_bytes())
        .unwrap()
        .to_string()
}

fn delete_all(s3_client: &S3Client, root: &str) {
    tokio_test::block_on(async move {
        let list = s3_client
            .list_objects_v2(ListObjectsV2Request {
                bucket: bucket(),
                prefix: Some(format!("{}/", root)),
                ..Default::default()
            })
            .await
            .unwrap();

        for object in list.contents.unwrap() {
            s3_client
                .delete_object(DeleteObjectRequest {
                    bucket: bucket(),
                    key: object.key.unwrap(),
                    ..Default::default()
                })
                .await
                .unwrap();
        }
    });
}

fn assert_storage_layout(s3_client: &S3Client, root: &str, layout_name: &str, config: &str) {
    assert_file_contains(
        s3_client,
        root,
        "ocfl_layout.json",
        &format!("\"extension\": \"{}\"", layout_name),
    );

    let layout_spec = format!("{}.md", layout_name);
    assert_file(s3_client, root, &layout_spec, &read_spec(&layout_spec));

    assert_file(
        s3_client,
        root,
        &format!("extensions/{}/config.json", layout_name),
        config,
    );
}

fn default_repo(prefix: &str, staging: impl AsRef<Path>) -> OcflRepo {
    init_repo(
        prefix,
        staging,
        Some(
            StorageLayout::new(
                LayoutExtensionName::HashedNTupleLayout,
                Some(DEFAULT_LAYOUT.as_bytes()),
            )
            .unwrap(),
        ),
    )
}

fn init_repo(prefix: &str, staging: impl AsRef<Path>, layout: Option<StorageLayout>) -> OcflRepo {
    OcflRepo::init_s3_repo(
        REGION,
        &bucket(),
        Some(prefix),
        None,
        staging,
        SpecVersion::Ocfl1_0,
        layout,
    )
    .unwrap()
}

fn s3_prefix() -> String {
    let mut rng = rand::thread_rng();
    let random: u32 = rng.gen();
    format!("rocfl-{}", random)
}

fn bucket() -> String {
    env::var(BUCKET_VAR).unwrap()
}

fn read_spec(name: &str) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("resources");
    path.push("main");
    path.push("specs");
    path.push(name);
    fs::read_to_string(path).unwrap()
}

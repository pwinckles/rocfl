//! These tests **MUST** be run sequentially with `cargo test -- --test-threads=1` because of
//! https://github.com/hyperium/hyper/issues/2112
#![cfg(feature = "s3")]

use std::convert::TryFrom;
use std::panic::UnwindSafe;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::{env, fs, panic};

use assert_fs::fixture::ChildPath;
use assert_fs::prelude::*;
use assert_fs::TempDir;
use fs_extra::dir::CopyOptions;
use rand::Rng;
use rocfl::ocfl::{
    CommitMeta, DigestAlgorithm, FileDetails, InventoryPath, LayoutExtensionName, OcflRepo,
    RocflError, StorageLayout, VersionNum,
};
use rusoto_core::Region;
use rusoto_s3::{
    DeleteObjectRequest, GetObjectRequest, HeadObjectRequest, ListObjectsV2Request, S3Client, S3,
};
use tokio::io::AsyncReadExt;

const BUCKET_VAR: &str = "OCFL_TEST_S3_BUCKET";
const ACCESS_VAR: &str = "AWS_ACCESS_KEY_ID";
const ACCESS_HASH: &str = "20cdc6f24747a49e6d295082e4bcaa81612a31e27d5916540429720bc0a43226";
const REGION: Region = Region::UsEast2;

#[test]
fn create_new_repo_empty_dir() {
    run_s3_test(
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
                r#"{
  "extensionName": "0004-hashed-n-tuple-storage-layout",
  "digestAlgorithm": "sha256",
  "tupleSize": 3,
  "numberOfTuples": 3,
  "shortObjectRoot": false
}"#,
            );

            let object_id = "s3-object";

            repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
                .unwrap();
            repo.copy_files_external(
                object_id,
                &vec![create_file(&temp, "test.txt", "testing").path()],
                "/",
                false,
            )
            .unwrap();
            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            assert_file_exists(
                &s3_client,
                &prefix,
                "eb0/07b/776/eb007b776561e27481743c3a4d40568fee20eae5\
        949b99c2235946004246bc60/0=ocfl_object_1.0",
            );
        },
    );
}

#[test]
#[should_panic(expected = "Cannot create new repository. Storage root must be empty")]
fn fail_create_new_repo_when_repo_already_exists() {
    run_s3_test(
        "fail_create_new_repo_when_repo_already_exists",
        |_s3_client: S3Client, prefix: String, staging: TempDir, _temp: TempDir| {
            let _ = default_repo(&prefix, staging.path());
            let _ = default_repo(&prefix, staging.path());
        },
    );
}

#[test]
fn create_new_object() {
    run_s3_test(
        "create_new_object",
        |s3_client: S3Client, prefix: String, staging: TempDir, temp: TempDir| {
            let repo = default_repo(&prefix, staging.path());
            let object_id = "s3-object";

            repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
                .unwrap();
            repo.copy_files_external(
                object_id,
                &vec![create_file(&temp, "test.txt", "testing").path()],
                "/",
                false,
            )
            .unwrap();
            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            let object = repo.get_object(object_id, None).unwrap();

            assert_eq!(1, object.state.len());

            assert_file_details(
                &s3_client,
                object.state.get(&path("test.txt")).unwrap(),
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
    run_s3_test(
        "fail_create_new_object_when_already_exists",
        |_s3_client: S3Client, prefix: String, staging: TempDir, temp: TempDir| {
            let repo = default_repo(&prefix, staging.path());
            let object_id = "s3-object";

            repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
                .unwrap();
            repo.copy_files_external(
                object_id,
                &vec![create_file(&temp, "test.txt", "testing").path()],
                "/",
                false,
            )
            .unwrap();
            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
                .unwrap();
        },
    );
}

#[test]
fn create_and_update_object() {
    run_s3_test(
        "create_and_update_object",
        |s3_client: S3Client, prefix: String, staging: TempDir, temp: TempDir| {
            let repo = default_repo(&prefix, staging.path());
            let object_id = "s3-object";

            repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
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

            repo.move_files_external(object_id, &vec![temp.child("a").path()], "/")
                .unwrap();

            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            repo.remove_files(object_id, &vec!["a/b/file3.txt", "a/b/c/file4.txt"], false)
                .unwrap();

            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            repo.copy_files_internal(
                object_id,
                Some(VersionNum::new(1)),
                &vec!["a/b/file3.txt"],
                "/",
                false,
            )
            .unwrap();
            repo.copy_files_internal(
                object_id,
                Some(VersionNum::new(1)),
                &vec!["a/file1.txt"],
                "something/file1.txt",
                false,
            )
            .unwrap();

            create_dirs(&temp, "something");

            repo.copy_files_external(
                object_id,
                &vec![create_file(&temp, "something/new.txt", "NEW").path()],
                "something/new.txt",
                true,
            )
            .unwrap();

            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            repo.copy_files_external(
                object_id,
                &vec![create_file(&temp, "file6.txt", "UPDATED!").path()],
                "a/f/file6.txt",
                true,
            )
            .unwrap();

            repo.move_files_internal(object_id, &vec!["a/d/e/file5.txt"], "a/file5.txt")
                .unwrap();

            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            let object = repo.get_object(object_id, None).unwrap();

            assert_eq!(7, object.state.len());

            assert_file_details(
                &s3_client,
                object.state.get(&path("file3.txt")).unwrap(),
                &object.object_root,
                "v1/content/a/b/file3.txt",
                "e18fad97c1b6512b1588a1fa2b7f9a0e549df9cfc538ce6943b4f0f4ae78322c",
            );
            assert_file_details(
                &s3_client,
                object.state.get(&path("a/file1.txt")).unwrap(),
                &object.object_root,
                "v1/content/a/file1.txt",
                "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
            );
            assert_file_details(
                &s3_client,
                object.state.get(&path("a/file5.txt")).unwrap(),
                &object.object_root,
                "v1/content/a/d/e/file5.txt",
                "4ccdbf78d368aed12d806efaf67fbce3300bca8e62a6f32716af2f447de1821e",
            );
            assert_file_details(
                &s3_client,
                object.state.get(&path("a/b/file2.txt")).unwrap(),
                &object.object_root,
                "v1/content/a/b/file2.txt",
                "b47592b10bc3e5c8ca8703d0862df10a6e409f43478804f93a08dd1844ae81b6",
            );
            assert_file_details(
                &s3_client,
                object.state.get(&path("a/f/file6.txt")).unwrap(),
                &object.object_root,
                "v4/content/a/f/file6.txt",
                "df21fb2fb83c1c64015a00e7677ccceb8da5377cba716611570230fb91d32bc9",
            );
            assert_file_details(
                &s3_client,
                object.state.get(&path("something/file1.txt")).unwrap(),
                &object.object_root,
                "v1/content/a/file1.txt",
                "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
            );
            assert_file_details(
                &s3_client,
                object.state.get(&path("something/new.txt")).unwrap(),
                &object.object_root,
                "v3/content/something/new.txt",
                "a253ff09c5a8678e1fd1962b2c329245e139e45f9cc6ced4e5d7ad42c4108fc0",
            );
        },
    );
}

#[test]
fn purge_object() {
    run_s3_test(
        "purge_object",
        |_s3_client: S3Client, prefix: String, staging: TempDir, temp: TempDir| {
            let repo = default_repo(&prefix, staging.path());
            let object_id = "s3-object-purge";

            repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
                .unwrap();
            repo.copy_files_external(
                object_id,
                &vec![create_file(&temp, "test.txt", "testing").path()],
                "/",
                false,
            )
            .unwrap();
            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            let _ = repo.get_object(object_id, None).unwrap();

            repo.purge_object(object_id).unwrap();

            match repo.get_object(object_id, None) {
                Err(RocflError::NotFound(_)) => (),
                _ => panic!("Expected {} to not be found.", object_id),
            }
        },
    );
}

#[test]
fn purge_object_when_not_exists() {
    run_s3_test(
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
    expected = "Cannot create version v2 in object out-of-sync because the HEAD is at v2"
)]
fn fail_commit_when_out_of_sync() {
    run_s3_test(
        "fail_commit_when_out_of_sync",
        |_s3_client: S3Client, prefix: String, staging: TempDir, temp: TempDir| {
            let repo = default_repo(&prefix, staging.path());
            let object_id = "out-of-sync";
            let id_hash = "46acfc156ff00023c6ff7c5cfc923eaf43123f63dd558579e90293f0eba1e574";

            repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
                .unwrap();
            repo.move_files_external(
                object_id,
                &vec![create_file(&temp, "test.txt", "testing").path()],
                "/",
            )
            .unwrap();
            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();

            repo.move_files_external(
                object_id,
                &vec![create_file(&temp, "test2.txt", "testing 2").path()],
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
                &vec![create_file(&temp, "b-file.txt", "another").path()],
                "/",
            )
            .unwrap();

            repo.commit(object_id, CommitMeta::new(), None, false)
                .unwrap();
        },
    );
}

/// Runs the test if the environment is configured to run S3 tests, and removes all resources
/// created during the test run, regardless of the test's outcome.
fn run_s3_test(name: &str, test: impl FnOnce(S3Client, String, TempDir, TempDir) + UnwindSafe) {
    if should_ignore_test() {
        println!("Skipping test {}", name);
        return;
    }

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
    assert_eq!(path_rc(content_path), actual.content_path);
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
    .expect(&format!("Expected {} to exist", key));
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
            .expect(&format!("Expected {} to exist", key));

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
        Some(StorageLayout::new(LayoutExtensionName::HashedNTupleLayout, None).unwrap()),
    )
}

fn init_repo(prefix: &str, staging: impl AsRef<Path>, layout: Option<StorageLayout>) -> OcflRepo {
    OcflRepo::init_s3_repo(REGION, &bucket(), Some(prefix), staging, layout).unwrap()
}

fn s3_prefix() -> String {
    let mut rng = rand::thread_rng();
    let random: u32 = rng.gen();
    format!("rocfl-{}", random)
}

fn bucket() -> String {
    env::var(BUCKET_VAR).unwrap()
}

fn create_dirs(temp: &TempDir, path: &str) -> ChildPath {
    let child = resolve_child(temp, path);
    child.create_dir_all().unwrap();
    child
}

fn create_file(temp: &TempDir, path: &str, content: &str) -> ChildPath {
    let child = resolve_child(temp, path);
    child.write_str(content).unwrap();
    child
}

fn resolve_child(temp: &TempDir, path: &str) -> ChildPath {
    let mut child: Option<ChildPath> = None;
    for part in path.split('/') {
        child = match child {
            Some(child) => Some(child.child(part)),
            None => Some(temp.child(part)),
        };
    }
    child.unwrap()
}

fn read_spec(name: &str) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("resources");
    path.push("main");
    path.push("specs");
    path.push(name);
    fs::read_to_string(path).unwrap()
}

fn path(path: &str) -> InventoryPath {
    InventoryPath::try_from(path).unwrap()
}

fn path_rc(path: &str) -> Rc<InventoryPath> {
    Rc::new(InventoryPath::try_from(path).unwrap())
}

use std::convert::TryInto;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use assert_fs::prelude::*;
use assert_fs::TempDir;
use chrono::{DateTime, Local, TimeZone};
use common::*;
use fs_extra::dir::CopyOptions;
use maplit::hashmap;
use rocfl::ocfl::{
    CommitMeta, Diff, DigestAlgorithm, FileDetails, InventoryPath, LayoutExtensionName,
    ObjectVersion, ObjectVersionDetails, OcflRepo, Result, RocflError, StorageLayout,
    VersionDetails, VersionNum,
};

mod common;

#[test]
fn list_all_objects() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None)?;

    let mut objects: Vec<ObjectVersionDetails> = repo.list_objects(None)?.collect();

    sort_obj_details(&mut objects);

    assert_eq!(3, objects.len());

    assert_eq!(
        objects.remove(0),
        ObjectVersionDetails {
            id: "o1".to_string(),
            object_root: repo_root
                .join("235")
                .join("2da")
                .join("728")
                .join("2352da7280f1decc3acf1ba84eb945c9fc2b7b541094e1d0992dbffd1b6664cc")
                .to_string_lossy()
                .to_string(),
            digest_algorithm: DigestAlgorithm::Sha512,
            version_details: VersionDetails {
                version_num: VersionNum::new(1),
                created: DateTime::parse_from_rfc3339("2019-08-05T15:57:53Z")
                    .unwrap()
                    .into(),
                user_name: Some("Peter".to_string()),
                user_address: Some("peter@example.com".to_string()),
                message: Some("commit message".to_string())
            }
        }
    );

    assert_eq!(
        objects.remove(0),
        ObjectVersionDetails {
            id: "o2".to_string(),
            object_root: repo_root
                .join("925")
                .join("0b9")
                .join("912")
                .join("9250b9912ee91d6b46e23299459ecd6eb8154451d62558a3a0a708a77926ad04")
                .to_string_lossy()
                .to_string(),
            digest_algorithm: DigestAlgorithm::Sha512,
            version_details: o2_v3_details()
        }
    );

    assert_eq!(
        objects.remove(0),
        ObjectVersionDetails {
            id: "o3".to_string(),
            object_root: repo_root
                .join("de2")
                .join("d91")
                .join("dc0")
                .join("de2d91dc0a2580414e9a70f7dfc76af727b69cac0838f2cbe0a88d12642efcbf")
                .to_string_lossy()
                .to_string(),
            digest_algorithm: DigestAlgorithm::Sha512,
            version_details: VersionDetails {
                version_num: VersionNum::new(2),
                created: DateTime::parse_from_rfc3339("2019-08-05T15:57:53Z")
                    .unwrap()
                    .into(),
                user_name: Some("Peter".to_string()),
                user_address: Some("peter@example.com".to_string()),
                message: Some("2".to_string())
            }
        }
    );

    Ok(())
}

#[test]
fn list_single_object_from_glob() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None)?;

    let mut objects: Vec<ObjectVersionDetails> = repo.list_objects(Some("*1"))?.collect();

    assert_eq!(1, objects.len());

    assert_eq!(
        objects.remove(0),
        ObjectVersionDetails {
            id: "o1".to_string(),
            object_root: repo_root
                .join("235")
                .join("2da")
                .join("728")
                .join("2352da7280f1decc3acf1ba84eb945c9fc2b7b541094e1d0992dbffd1b6664cc")
                .to_string_lossy()
                .to_string(),
            digest_algorithm: DigestAlgorithm::Sha512,
            version_details: VersionDetails {
                version_num: VersionNum::new(1),
                created: DateTime::parse_from_rfc3339("2019-08-05T15:57:53Z")
                    .unwrap()
                    .into(),
                user_name: Some("Peter".to_string()),
                user_address: Some("peter@example.com".to_string()),
                message: Some("commit message".to_string())
            }
        }
    );

    Ok(())
}

#[test]
fn list_empty_repo() -> Result<()> {
    let repo_root = create_repo_root("empty");
    let repo = OcflRepo::fs_repo(&repo_root, None)?;

    let objects: Vec<ObjectVersionDetails> = repo.list_objects(None)?.collect();

    assert_eq!(0, objects.len());

    Ok(())
}

#[test]
fn list_repo_with_invalid_objects() -> Result<()> {
    let repo_root = create_repo_root("invalid");
    let repo = OcflRepo::fs_repo(&repo_root, None)?;

    let object_root = repo_root
        .join("925")
        .join("0b9")
        .join("912")
        .join("9250b9912ee91d6b46e23299459ecd6eb8154451d62558a3a0a708a77926ad04");

    let iter = repo.list_objects(None)?;

    for object in iter {
        assert_eq!(
            object,
            ObjectVersionDetails {
                id: "o2".to_string(),
                object_root: object_root.to_string_lossy().to_string(),
                digest_algorithm: DigestAlgorithm::Sha512,
                version_details: o2_v3_details()
            }
        );
    }

    Ok(())
}

#[test]
fn get_object_when_exists() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None)?;

    let object = repo.get_object("o2", None)?;

    let object_root = repo_root
        .join("925")
        .join("0b9")
        .join("912")
        .join("9250b9912ee91d6b46e23299459ecd6eb8154451d62558a3a0a708a77926ad04");

    assert_eq!(
        object,
        ObjectVersion {
            id: "o2".to_string(),
            object_root: object_root.to_string_lossy().to_string(),
            digest_algorithm: DigestAlgorithm::Sha512,
            version_details: o2_v3_details(),
            state: hashmap! {
                path_rc("dir1/file3") => FileDetails {
                    digest: Rc::new("6e027f3dc89e0bfd97e4c2ec6919a8fb793bdc7b5c513bea618f174beec32a66d2\
                    fc0ce19439751e2f01ae49f78c56dcfc7b49c167a751c823d09da8419a4331".into()),
                    digest_algorithm: DigestAlgorithm::Sha512,
                    content_path: path_rc("v3/content/dir1/file3"),
                    storage_path: object_root.join("v3").join("content").join("dir1").join("file3")
                        .to_string_lossy().to_string(),
                    last_update: Rc::new(o2_v3_details())
                },
                path_rc("dir1/dir2/file2") => FileDetails {
                    digest: Rc::new("4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b30266802\
                    6095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6".into()),
                    digest_algorithm: DigestAlgorithm::Sha512,
                    content_path: path_rc("v1/content/dir1/dir2/file2"),
                    storage_path: object_root.join("v1").join("content").join("dir1").join("dir2").join("file2")
                        .to_string_lossy().to_string(),
                    last_update: Rc::new(o2_v1_details())
                }
            }
        }
    );

    Ok(())
}

#[test]
fn get_object_version_when_exists() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None)?;

    let object = repo.get_object("o2", Some(VersionNum::new(2)))?;

    let object_root = repo_root
        .join("925")
        .join("0b9")
        .join("912")
        .join("9250b9912ee91d6b46e23299459ecd6eb8154451d62558a3a0a708a77926ad04");

    assert_eq!(
        object,
        ObjectVersion {
            id: "o2".to_string(),
            object_root: object_root.to_string_lossy().to_string(),
            digest_algorithm: DigestAlgorithm::Sha512,
            version_details: o2_v2_details(),
            state: hashmap! {
                path_rc("dir1/file3") => FileDetails {
                    digest: Rc::new("7b866cfcfe06bf2bcaea7086f2a059854afe8de12a6e21e4286bec4828d3da36bd\
                    ef28599be8c9be49da3e45ede3ddbc049f99ee197e5244c33e294748b1a986".into()),
                    digest_algorithm: DigestAlgorithm::Sha512,
                    content_path: path_rc("v2/content/dir1/file3"),
                    storage_path: object_root.join("v2").join("content").join("dir1").join("file3")
                        .to_string_lossy().to_string(),
                    last_update: Rc::new(o2_v2_details())
                },
                path_rc("dir1/dir2/file2") => FileDetails {
                    digest: Rc::new("4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b30266802\
                    6095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6".into()),
                    digest_algorithm: DigestAlgorithm::Sha512,
                    content_path: path_rc("v1/content/dir1/dir2/file2"),
                    storage_path: object_root.join("v1").join("content").join("dir1").join("dir2").join("file2")
                        .to_string_lossy().to_string(),
                    last_update: Rc::new(o2_v1_details())
                },
                path_rc("dir3/file1") => FileDetails {
                    digest: Rc::new("96a26e7629b55187f9ba3edc4acc940495d582093b8a88cb1f0303cf3399fe6b1f\
                    5283d76dfd561fc401a0cdf878c5aad9f2d6e7e2d9ceee678757bb5d95c39e".into()),
                    digest_algorithm: DigestAlgorithm::Sha512,
                    content_path: path_rc("v1/content/file1"),
                    storage_path: object_root.join("v1").join("content").join("file1")
                        .to_string_lossy().to_string(),
                    last_update: Rc::new(o2_v2_details())
                }
            }
        }
    );

    Ok(())
}

#[test]
fn get_object_with_mutable_head() -> Result<()> {
    let repo_root = create_repo_root("mutable");
    let repo = OcflRepo::fs_repo(&repo_root, None)?;

    let object = repo.get_object("o1", None)?;
    let object_root = PathBuf::from(&object.object_root);

    assert_file_details(
        object.state.get(&path("dir1/file3")).unwrap(),
        &object_root,
        "extensions/0005-mutable-head/head/content/r1/dir1/file3",
        "b10ff867df18165a0e100d99cd3d27f845f7ef9ad84eeb627a53aabaea04805940c3693154b8a32541a31887dd\
        a9fb1e667e93307473b1c581021714768bd032",
    );
    assert_file_details(
        object.state.get(&path("dir1/file4")).unwrap(),
        &object_root,
        "extensions/0005-mutable-head/head/content/r1/dir1/file3",
        "b10ff867df18165a0e100d99cd3d27f845f7ef9ad84eeb627a53aabaea04805940c3693154b8a32541a31887dd\
        a9fb1e667e93307473b1c581021714768bd032",
    );
    assert_file_details(
        object.state.get(&path("file1")).unwrap(),
        &object_root,
        "v1/content/file1",
        "96a26e7629b55187f9ba3edc4acc940495d582093b8a88cb1f0303cf3399fe6b1f5283d76dfd561fc401a0cdf8\
        78c5aad9f2d6e7e2d9ceee678757bb5d95c39e",
    );
    assert_file_details(
        object.state.get(&path("file2")).unwrap(),
        &object_root,
        "v1/content/file2",
        "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe\
        1f276d18bf0de254992a78573ad6574e7ae1f6",
    );

    Ok(())
}

#[test]
#[should_panic(expected = "Not found: Object o4")]
fn error_when_object_not_exists() {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None).unwrap();
    repo.get_object("o4", None).unwrap();
}

#[test]
fn get_object_when_exists_using_layout() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects-with-layout");
    let repo = OcflRepo::fs_repo(&repo_root, None)?;

    let object = repo.get_object("o2", None)?;

    let object_root = repo_root
        .join("925")
        .join("0b9")
        .join("912")
        .join("9250b9912ee91d6b46e23299459ecd6eb8154451d62558a3a0a708a77926ad04");

    assert_eq!(
        object,
        ObjectVersion {
            id: "o2".to_string(),
            object_root: object_root.to_string_lossy().to_string(),
            digest_algorithm: DigestAlgorithm::Sha512,
            version_details: o2_v3_details(),
            state: hashmap! {
                path_rc("dir1/file3") => FileDetails {
                    digest: Rc::new("6e027f3dc89e0bfd97e4c2ec6919a8fb793bdc7b5c513bea618f174beec32a66d2\
                    fc0ce19439751e2f01ae49f78c56dcfc7b49c167a751c823d09da8419a4331".into()),
                    digest_algorithm: DigestAlgorithm::Sha512,
                    content_path: path_rc("v3/content/dir1/file3"),
                    storage_path: object_root.join("v3").join("content").join("dir1").join("file3")
                        .to_string_lossy().to_string(),
                    last_update: Rc::new(o2_v3_details())
                },
                path_rc("dir1/dir2/file2") => FileDetails {
                    digest: Rc::new("4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b30266802\
                    6095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6".into()),
                    digest_algorithm: DigestAlgorithm::Sha512,
                    content_path: path_rc("v1/content/dir1/dir2/file2"),
                    storage_path: object_root.join("v1").join("content").join("dir1").join("dir2").join("file2")
                        .to_string_lossy().to_string(),
                    last_update: Rc::new(o2_v1_details())
                }
            }
        }
    );

    Ok(())
}

#[test]
#[should_panic(expected = "Not found: Object o4")]
fn error_when_object_not_exists_with_layout() {
    let repo_root = create_repo_root("multiple-objects-with-layout");
    let repo = OcflRepo::fs_repo(&repo_root, None).unwrap();
    repo.get_object("o4", None).unwrap();
}

#[test]
#[should_panic(expected = "Not found: Object o2 version v4")]
fn error_when_version_not_exists() {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None).unwrap();
    repo.get_object("o2", Some(VersionNum::new(4))).unwrap();
}

#[test]
#[should_panic(expected = "Not found: Object o3")]
fn error_when_get_invalid_object() {
    let repo_root = create_repo_root("invalid");
    let repo = OcflRepo::fs_repo(&repo_root, None).unwrap();
    repo.get_object("o3", None).unwrap();
}

#[test]
fn list_versions_when_multiple() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None)?;

    let mut versions = repo.list_object_versions("o2")?;

    assert_eq!(3, versions.len());

    assert_eq!(versions.remove(0), o2_v1_details());
    assert_eq!(versions.remove(0), o2_v2_details());
    assert_eq!(versions.remove(0), o2_v3_details());

    Ok(())
}

#[test]
fn list_file_versions_when_multiple() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None)?;

    let mut versions = repo.list_file_versions("o2", &"dir3/file1".try_into()?)?;

    assert_eq!(2, versions.len());

    assert_eq!(versions.remove(0), o2_v2_details());
    assert_eq!(versions.remove(0), o2_v3_details());

    Ok(())
}

#[test]
#[should_panic(expected = "Not found: Object o5")]
fn list_versions_not_exists() {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None).unwrap();
    repo.list_object_versions("o5").unwrap();
}

#[test]
#[should_panic(expected = "Not found: Path bogus.txt not found in object o2")]
fn list_file_versions_not_exists() {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None).unwrap();
    repo.list_file_versions("o2", &"bogus.txt".try_into().unwrap())
        .unwrap();
}

#[test]
fn diff_when_left_and_right_specified() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None)?;

    let mut diff = repo.diff("o2", Some(VersionNum::new(1)), VersionNum::new(3))?;

    sort_diffs(&mut diff);

    assert_eq!(2, diff.len());

    assert_eq!(diff.remove(0), Diff::Added(path_rc("dir1/file3")));
    assert_eq!(diff.remove(0), Diff::Deleted(path_rc("file1")));

    Ok(())
}

#[test]
fn diff_with_previous_when_left_not_specified() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None)?;

    let mut diff = repo.diff("o2", None, VersionNum::new(3))?;

    sort_diffs(&mut diff);

    assert_eq!(2, diff.len());

    assert_eq!(diff.remove(0), Diff::Modified(path_rc("dir1/file3")));
    assert_eq!(diff.remove(0), Diff::Deleted(path_rc("dir3/file1")));

    Ok(())
}

#[test]
fn diff_first_version_all_adds() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None)?;

    let mut diff = repo.diff("o2", None, VersionNum::new(1))?;

    sort_diffs(&mut diff);

    assert_eq!(2, diff.len());

    assert_eq!(diff.remove(0), Diff::Added(path_rc("dir1/dir2/file2")));
    assert_eq!(diff.remove(0), Diff::Added(path_rc("file1")));

    Ok(())
}

#[test]
fn diff_same_version_no_diff() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None)?;

    let diff = repo.diff("o2", Some(VersionNum::new(2)), VersionNum::new(2))?;

    assert_eq!(0, diff.len());

    Ok(())
}

#[test]
#[should_panic(expected = "Not found: Object o6")]
fn diff_object_not_exists() {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None).unwrap();
    repo.diff("o6", None, VersionNum::new(2)).unwrap();
}

#[test]
#[should_panic(expected = "Not found: Object o1 version v2")]
fn diff_version_not_exists() {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None).unwrap();
    repo.diff("o1", None, VersionNum::new(2)).unwrap();
}

#[test]
fn get_object_file_when_exists() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None)?;

    let id = "o2";
    let version = VersionNum::new(2);
    let mut out: Vec<u8> = Vec::new();

    repo.get_object_file(id, &"dir1/file3".try_into()?, Some(version), &mut out)?;

    assert_eq!("file 3", String::from_utf8(out).unwrap());

    Ok(())
}

#[test]
#[should_panic(expected = "Path dir1/bogus not found in object o2 version v2")]
fn fail_get_object_file_when_does_not_exist() {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root, None).unwrap();

    let id = "o2";
    let version = VersionNum::new(2);
    let mut out: Vec<u8> = Vec::new();

    repo.get_object_file(
        id,
        &"dir1/bogus".try_into().unwrap(),
        Some(version),
        &mut out,
    )
    .unwrap();
}

#[test]
fn create_new_repo_empty_dir() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = OcflRepo::init_fs_repo(
        root.path(),
        None,
        Some(StorageLayout::new(
            LayoutExtensionName::HashedNTupleLayout,
            None,
        )?),
    )?;

    assert_storage_root(&root);
    assert_layout_extension(
        &root,
        "0004-hashed-n-tuple-storage-layout",
        r#"{
  "extensionName": "0004-hashed-n-tuple-storage-layout",
  "digestAlgorithm": "sha256",
  "tupleSize": 3,
  "numberOfTuples": 3,
  "shortObjectRoot": false
}"#,
    );

    let object_id = "foobar";
    create_simple_object(object_id, &repo, &temp);

    root.child("c3a")
        .child("b8f")
        .child("f13")
        .child("c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2")
        .assert(predicates::path::is_dir());

    Ok(())
}

#[test]
fn create_new_flat_repo_empty_dir() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = OcflRepo::init_fs_repo(
        root.path(),
        None,
        Some(StorageLayout::new(
            LayoutExtensionName::FlatDirectLayout,
            None,
        )?),
    )?;

    assert_storage_root(&root);
    assert_layout_extension(
        &root,
        "0002-flat-direct-storage-layout",
        r#"{
  "extensionName": "0002-flat-direct-storage-layout"
}"#,
    );

    let object_id = "foobar";
    create_simple_object(object_id, &repo, &temp);

    root.child(object_id).assert(predicates::path::is_dir());

    Ok(())
}

#[test]
fn create_new_hash_id_repo_empty_dir() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = OcflRepo::init_fs_repo(
        root.path(),
        None,
        Some(StorageLayout::new(
            LayoutExtensionName::HashedNTupleObjectIdLayout,
            None,
        )?),
    )?;

    assert_storage_root(&root);
    assert_layout_extension(
        &root,
        "0003-hash-and-id-n-tuple-storage-layout",
        r#"{
  "extensionName": "0003-hash-and-id-n-tuple-storage-layout",
  "digestAlgorithm": "sha256",
  "tupleSize": 3,
  "numberOfTuples": 3
}"#,
    );

    let object_id = "foobar";
    create_simple_object(object_id, &repo, &temp);

    root.child("c3a")
        .child("b8f")
        .child("f13")
        .child(object_id)
        .assert(predicates::path::is_dir());

    Ok(())
}

#[test]
fn create_new_repo_empty_dir_custom_layout() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let layout = r#"{
  "extensionName": "0004-hashed-n-tuple-storage-layout",
  "digestAlgorithm": "sha512",
  "tupleSize": 5,
  "numberOfTuples": 2,
  "shortObjectRoot": true
}"#;

    let repo = OcflRepo::init_fs_repo(
        root.path(),
        None,
        Some(StorageLayout::new(
            LayoutExtensionName::HashedNTupleLayout,
            Some(layout.as_bytes()),
        )?),
    )?;

    assert_storage_root(&root);
    assert_layout_extension(&root, "0004-hashed-n-tuple-storage-layout", layout);

    let object_id = "foobar";
    create_simple_object(object_id, &repo, &temp);

    root.child("0a502")
        .child("61ebd")
        .child(
            "1a390fed2bf326f2673c145582a6342d523204973d0219337f81616a8069b012587cf5635f6925f1b56\
        c360230c19b273500ee013e030601bf2425",
        )
        .assert(predicates::path::is_dir());

    Ok(())
}

#[test]
#[should_panic(expected = "must be empty")]
fn fail_new_repo_creation_when_non_empty_root() {
    let root = TempDir::new().unwrap();

    root.child("file").write_str("contents").unwrap();

    let _repo = OcflRepo::init_fs_repo(
        root.path(),
        None,
        Some(StorageLayout::new(LayoutExtensionName::HashedNTupleLayout, None).unwrap()),
    )
    .unwrap();
}

#[test]
fn copy_files_into_new_object() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "foobar";

    assert_staged_obj_count(&repo, 0);
    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)?;
    assert_staged_obj_count(&repo, 1);

    let staged: Vec<ObjectVersionDetails> = repo.list_staged_objects(None)?.collect();
    assert_eq!(object_id, staged.first().unwrap().id);

    create_file(&temp, "test.txt", "testing");
    repo.copy_files_external(
        object_id,
        &vec![temp.child("test.txt").path()],
        "test.txt",
        false,
    )?;

    create_dirs(&temp, "nested/dir");
    create_file(&temp, "nested/1.txt", "File 1");
    create_file(&temp, "nested/dir/2.txt", "File 2");
    create_file(&temp, "nested/dir/3.txt", "File 3");

    repo.copy_files_external(object_id, &vec![temp.path()], "another", true)?;

    let staged_obj = repo.get_staged_object(object_id)?;
    let obj_root = PathBuf::from(&staged_obj.object_root);

    assert_eq!(5, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("test.txt")).unwrap(),
        &obj_root,
        "v1/content/test.txt",
        "521b9ccefbcd14d179e7a1bb877752870a6d620938b28a66a107eac6e6805b9d0989f45b57\
                        30508041aa5e710847d439ea74cd312c9355f1f2dae08d40e41d50",
    );
    assert_file_details(
        staged_obj.state.get(&path("another/test.txt")).unwrap(),
        &obj_root,
        "v1/content/another/test.txt",
        "521b9ccefbcd14d179e7a1bb877752870a6d620938b28a66a107eac6e6805b9d0989f45b57\
                        30508041aa5e710847d439ea74cd312c9355f1f2dae08d40e41d50",
    );
    assert_file_details(
        staged_obj.state.get(&path("another/nested/1.txt")).unwrap(),
        &obj_root,
        "v1/content/another/nested/1.txt",
        "9c614ba0d58c976d0b39f8f5536eb8af89fae745cbe3783ac2ca3e3055bb0b1e3687417a1d\
                        1104288d2883a4368d3dacb9931460c6e523117ff3eaa28810481a",
    );
    assert_file_details(
        staged_obj
            .state
            .get(&path("another/nested/dir/2.txt"))
            .unwrap(),
        &obj_root,
        "v1/content/another/nested/dir/2.txt",
        "70ffe50550ae07cd0fc154cc1cd3a47b71499b5f67921b52219750441791981fb36476cd47\
                        8440601bc26da16b28c8a2be4478b36091f2615ac94a575581902c",
    );
    assert_file_details(
        staged_obj
            .state
            .get(&path("another/nested/dir/3.txt"))
            .unwrap(),
        &obj_root,
        "v1/content/another/nested/dir/3.txt",
        "79c994f97612eb4ee6a3cb1fbbb45278da184ea73bfb483274bb783f0bce6a7bf8dd8cb0d4\
                        fc0eb2b065ebd28b2959b59d9a489929edf9ea7db4dcda8a09a76f",
    );

    assert_obj_count(&repo, 0);

    commit(object_id, &repo);

    assert_staged_obj_count(&repo, 0);
    assert_obj_count(&repo, 1);

    let obj = repo.get_object(object_id, None)?;

    let obj_root = PathBuf::from(&obj.object_root);

    assert_eq!(5, obj.state.len());

    let deduped_path = assert_deduped_path(
        &obj_root,
        obj.state.get(&path("test.txt")).unwrap(),
        &["v1/content/another/test.txt", "v1/content/test.txt"],
    );

    assert_file_details(
        obj.state.get(&path("test.txt")).unwrap(),
        &obj_root,
        (*deduped_path).as_ref(),
        "521b9ccefbcd14d179e7a1bb877752870a6d620938b28a66a107eac6e6805b9d0989f45b57\
                        30508041aa5e710847d439ea74cd312c9355f1f2dae08d40e41d50",
    );
    assert_file_details(
        obj.state.get(&path("another/test.txt")).unwrap(),
        &obj_root,
        (*deduped_path).as_ref(),
        "521b9ccefbcd14d179e7a1bb877752870a6d620938b28a66a107eac6e6805b9d0989f45b57\
                        30508041aa5e710847d439ea74cd312c9355f1f2dae08d40e41d50",
    );
    assert_file_details(
        obj.state.get(&path("another/nested/1.txt")).unwrap(),
        &obj_root,
        "v1/content/another/nested/1.txt",
        "9c614ba0d58c976d0b39f8f5536eb8af89fae745cbe3783ac2ca3e3055bb0b1e3687417a1d\
                        1104288d2883a4368d3dacb9931460c6e523117ff3eaa28810481a",
    );
    assert_file_details(
        obj.state.get(&path("another/nested/dir/2.txt")).unwrap(),
        &obj_root,
        "v1/content/another/nested/dir/2.txt",
        "70ffe50550ae07cd0fc154cc1cd3a47b71499b5f67921b52219750441791981fb36476cd47\
                        8440601bc26da16b28c8a2be4478b36091f2615ac94a575581902c",
    );
    assert_file_details(
        obj.state.get(&path("another/nested/dir/3.txt")).unwrap(),
        &obj_root,
        "v1/content/another/nested/dir/3.txt",
        "79c994f97612eb4ee6a3cb1fbbb45278da184ea73bfb483274bb783f0bce6a7bf8dd8cb0d4\
                        fc0eb2b065ebd28b2959b59d9a489929edf9ea7db4dcda8a09a76f",
    );

    Ok(())
}

#[test]
fn copy_files_into_existing_object() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "existing object";

    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)?;

    create_file(&temp, "test.txt", "testing");
    repo.copy_files_external(
        object_id,
        &vec![temp.child("test.txt").path()],
        "test.txt",
        false,
    )?;

    commit(object_id, &repo);

    assert_staged_obj_count(&repo, 0);
    assert_obj_count(&repo, 1);

    create_dirs(&temp, "nested/dir");
    create_file(&temp, "nested/1.txt", "File 1");
    create_file(&temp, "nested/dir/2.txt", "File 2");
    create_file(&temp, "nested/dir/3.txt", "File 3");

    repo.copy_files_external(
        object_id,
        &vec![resolve_child(&temp, "nested/dir").path()],
        "another",
        true,
    )?;

    let staged_obj = repo.get_staged_object(object_id)?;
    let staged_root = PathBuf::from(&staged_obj.object_root);
    let object_root = PathBuf::from(&repo.get_object_details(object_id, None)?.object_root);

    assert_eq!(3, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("test.txt")).unwrap(),
        &object_root,
        "v1/content/test.txt",
        "521b9ccefbcd14d179e7a1bb877752870a6d620938b28a66a107eac6e6805b9d0989f45b57\
                        30508041aa5e710847d439ea74cd312c9355f1f2dae08d40e41d50",
    );
    assert_file_details(
        staged_obj.state.get(&path("another/2.txt")).unwrap(),
        &staged_root,
        "v2/content/another/2.txt",
        "70ffe50550ae07cd0fc154cc1cd3a47b71499b5f67921b52219750441791981fb36476cd47\
                        8440601bc26da16b28c8a2be4478b36091f2615ac94a575581902c",
    );
    assert_file_details(
        staged_obj.state.get(&path("another/3.txt")).unwrap(),
        &staged_root,
        "v2/content/another/3.txt",
        "79c994f97612eb4ee6a3cb1fbbb45278da184ea73bfb483274bb783f0bce6a7bf8dd8cb0d4\
                        fc0eb2b065ebd28b2959b59d9a489929edf9ea7db4dcda8a09a76f",
    );

    commit(object_id, &repo);

    let obj = repo.get_object(object_id, None)?;

    assert_eq!(3, obj.state.len());

    assert_file_details(
        obj.state.get(&path("test.txt")).unwrap(),
        &object_root,
        "v1/content/test.txt",
        "521b9ccefbcd14d179e7a1bb877752870a6d620938b28a66a107eac6e6805b9d0989f45b57\
                        30508041aa5e710847d439ea74cd312c9355f1f2dae08d40e41d50",
    );
    assert_file_details(
        obj.state.get(&path("another/2.txt")).unwrap(),
        &object_root,
        "v2/content/another/2.txt",
        "70ffe50550ae07cd0fc154cc1cd3a47b71499b5f67921b52219750441791981fb36476cd47\
                        8440601bc26da16b28c8a2be4478b36091f2615ac94a575581902c",
    );
    assert_file_details(
        obj.state.get(&path("another/3.txt")).unwrap(),
        &object_root,
        "v2/content/another/3.txt",
        "79c994f97612eb4ee6a3cb1fbbb45278da184ea73bfb483274bb783f0bce6a7bf8dd8cb0d4\
                        fc0eb2b065ebd28b2959b59d9a489929edf9ea7db4dcda8a09a76f",
    );

    Ok(())
}

#[test]
fn copied_files_should_dedup_on_commit() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "dedup";

    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)?;

    create_file(&temp, "test.txt", "testing");
    repo.copy_files_external(
        object_id,
        &vec![temp.child("test.txt").path()],
        "test.txt",
        false,
    )?;

    commit(object_id, &repo);

    repo.copy_files_external(
        object_id,
        &vec![temp.child("test.txt").path()],
        "/dir/file.txt",
        false,
    )?;
    repo.copy_files_external(
        object_id,
        &vec![temp.child("test.txt").path()],
        "another/copy/here/surprise.txt",
        false,
    )?;

    commit(object_id, &repo);

    let obj = repo.get_object(object_id, None)?;
    let object_root = PathBuf::from(&obj.object_root);

    assert_eq!(3, obj.state.len());

    assert_file_details(
        obj.state.get(&path("test.txt")).unwrap(),
        &object_root,
        "v1/content/test.txt",
        "521b9ccefbcd14d179e7a1bb877752870a6d620938b28a66a107eac6e6805b9d0989f45b57\
                        30508041aa5e710847d439ea74cd312c9355f1f2dae08d40e41d50",
    );
    assert_file_details(
        obj.state.get(&path("dir/file.txt")).unwrap(),
        &object_root,
        "v1/content/test.txt",
        "521b9ccefbcd14d179e7a1bb877752870a6d620938b28a66a107eac6e6805b9d0989f45b57\
                        30508041aa5e710847d439ea74cd312c9355f1f2dae08d40e41d50",
    );
    assert_file_details(
        obj.state
            .get(&path("another/copy/here/surprise.txt"))
            .unwrap(),
        &object_root,
        "v1/content/test.txt",
        "521b9ccefbcd14d179e7a1bb877752870a6d620938b28a66a107eac6e6805b9d0989f45b57\
                        30508041aa5e710847d439ea74cd312c9355f1f2dae08d40e41d50",
    );

    Ok(())
}

#[test]
#[should_panic(
    expected = "Conflicting logical path test.txt/is/not/a/directory/test.txt: The path part test.txt is an existing logical file"
)]
fn copy_should_reject_conflicting_files() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "conflicting";

    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)
        .unwrap();

    let test_file = create_file(&temp, "test.txt", "testing");
    repo.copy_files_external(object_id, &vec![test_file.path()], "test.txt", false)
        .unwrap();

    repo.copy_files_external(
        object_id,
        &vec![test_file.path()],
        "test.txt/is/not/a/directory/test.txt",
        false,
    )
    .unwrap();
}

#[test]
#[should_panic(
    expected = "Conflicting logical path dir: This path is already in use as a directory"
)]
fn copy_should_reject_conflicting_dirs() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "conflicting";

    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)
        .unwrap();

    let test_file = create_file(&temp, "test.txt", "testing");
    repo.copy_files_external(
        object_id,
        &vec![test_file.path()],
        "dir/sub/test.txt",
        false,
    )
    .unwrap();

    let test_file_2 = create_file(&temp, "dir", "conflict");
    repo.copy_files_external(object_id, &vec![test_file_2.path()], "/", false)
        .unwrap();
}

#[test]
fn copy_to_dir_when_dst_ends_in_slash() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "conflicting";

    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)?;

    let test_file = create_file(&temp, "test.txt", "testing");
    repo.copy_files_external(object_id, &vec![test_file.path()], "dir/", false)?;

    let staged_obj = repo.get_staged_object(object_id)?;
    let staged_root = PathBuf::from(&staged_obj.object_root);

    assert_eq!(1, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("dir/test.txt")).unwrap(),
        &staged_root,
        "v1/content/dir/test.txt",
        "521b9ccefbcd14d179e7a1bb877752870a6d620938b28a66a107eac6e6805b9d0989f45b57\
                        30508041aa5e710847d439ea74cd312c9355f1f2dae08d40e41d50",
    );

    Ok(())
}

#[test]
fn copy_into_dir_when_dest_is_existing_dir() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "existing dir";

    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)?;

    let test_file = create_file(&temp, "test.txt", "testing");
    repo.copy_files_external(
        object_id,
        &vec![test_file.path()],
        "a/dir/here/test.txt",
        false,
    )?;

    let test_file_2 = create_file(&temp, "different.txt", "different");
    repo.copy_files_external(object_id, &vec![test_file_2.path()], "a/dir", false)?;

    let staged_obj = repo.get_staged_object(object_id)?;
    let staged_root = PathBuf::from(&staged_obj.object_root);

    assert_eq!(2, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("a/dir/here/test.txt")).unwrap(),
        &staged_root,
        "v1/content/a/dir/here/test.txt",
        "521b9ccefbcd14d179e7a1bb877752870a6d620938b28a66a107eac6e6805b9d0989f45b57\
                        30508041aa5e710847d439ea74cd312c9355f1f2dae08d40e41d50",
    );
    assert_file_details(
        staged_obj.state.get(&path("a/dir/different.txt")).unwrap(),
        &staged_root,
        "v1/content/a/dir/different.txt",
        "49d5b8799558e22d3890d03b56a6c7a46faa1a7d216c2df22507396242ab3540e2317b87088\
        2b2384d707254333a8439fd3ca191e93293f745786ff78ef069f8",
    );

    Ok(())
}

#[test]
#[should_panic(expected = "Not found: Object does-not-exist")]
fn fail_copy_when_target_obj_does_not_exist() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    create_file(&temp, "test.txt", "testing");

    repo.copy_files_external(
        "does-not-exist",
        &vec![temp.child("test.txt").path()],
        "test.txt",
        false,
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "test.txt: Does not exist")]
fn fail_copy_when_src_does_not_exist() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "partial success";

    let repo = default_repo(root.path());

    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)
        .unwrap();

    repo.copy_files_external(
        object_id,
        &vec![temp.child("test.txt").path()],
        "test.txt",
        false,
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "recursion is not enabled")]
fn fail_copy_when_src_dir_and_recursion_not_enabled() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "missing";

    let repo = default_repo(root.path());

    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)
        .unwrap();

    create_dirs(&temp, "sub");
    create_file(&temp, "sub/test.txt", "testing");

    repo.copy_files_external(object_id, &vec![temp.child("sub").path()], "dst", false)
        .unwrap();

    let staged_obj = repo.get_staged_object(object_id).unwrap();
    assert_eq!(0, staged_obj.state.len());
}

#[test]
#[should_panic(
    expected = "Illegal argument: Paths may not contain '.', '..', or '' parts. Found: some/../../dir"
)]
fn copy_should_reject_bad_dst() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "internal bad dst";

    create_example_object(object_id, &repo, &temp);

    repo.copy_files_external(
        object_id,
        &vec![create_file(&temp, "test.txt", "test").path()],
        "some/../../dir",
        false,
    )
    .unwrap();
}

#[test]
fn copy_should_partially_succeed_when_multiple_src_and_some_fail() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "missing";

    let repo = default_repo(root.path());

    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)
        .unwrap();

    create_file(&temp, "test.txt", "testing");

    let result = repo.copy_files_external(
        object_id,
        &vec![temp.child("bogus").path(), temp.child("test.txt").path()],
        "dst",
        false,
    );

    match result {
        Err(RocflError::CopyMoveError(e)) => {
            assert_eq!(1, e.0.len());
            assert!(e.0.get(0).unwrap().contains("bogus: Does not exist"));
        }
        _ => panic!("Expected copy to return an error"),
    }

    let staged_obj = repo.get_staged_object(object_id).unwrap();
    let staged_root = PathBuf::from(&staged_obj.object_root);

    assert_eq!(1, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("dst/test.txt")).unwrap(),
        &staged_root,
        "v1/content/dst/test.txt",
        "521b9ccefbcd14d179e7a1bb877752870a6d620938b28a66a107eac6e6805b9d0989f45b57\
                        30508041aa5e710847d439ea74cd312c9355f1f2dae08d40e41d50",
    );
}

#[test]
fn copy_multiple_sources() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "missing";

    let repo = default_repo(root.path());

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)?;

    create_dirs(&temp, "a/b/c");
    create_dirs(&temp, "a/d/e");
    create_dirs(&temp, "a/f");
    create_file(&temp, "a/file1.txt", "File One");
    create_file(&temp, "a/b/file2.txt", "File Two");
    create_file(&temp, "a/b/file3.txt", "File Three");
    create_file(&temp, "a/b/c/file4.txt", "File Four");
    create_file(&temp, "a/d/e/file5.txt", "File Five");
    create_file(&temp, "a/f/file6.txt", "File Six");

    repo.copy_files_external(
        object_id,
        &vec![
            resolve_child(&temp, "a/b").path(),
            resolve_child(&temp, "a/d").path(),
            resolve_child(&temp, "a/file1.txt").path(),
        ],
        "dst",
        true,
    )?;

    let staged_obj = repo.get_staged_object(object_id)?;
    let staged_root = PathBuf::from(&staged_obj.object_root);

    assert_eq!(5, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("dst/file1.txt")).unwrap(),
        &staged_root,
        "v1/content/dst/file1.txt",
        "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
    );
    assert_file_details(
        staged_obj.state.get(&path("dst/b/file2.txt")).unwrap(),
        &staged_root,
        "v1/content/dst/b/file2.txt",
        "b47592b10bc3e5c8ca8703d0862df10a6e409f43478804f93a08dd1844ae81b6",
    );
    assert_file_details(
        staged_obj.state.get(&path("dst/b/file3.txt")).unwrap(),
        &staged_root,
        "v1/content/dst/b/file3.txt",
        "e18fad97c1b6512b1588a1fa2b7f9a0e549df9cfc538ce6943b4f0f4ae78322c",
    );
    assert_file_details(
        staged_obj.state.get(&path("dst/b/c/file4.txt")).unwrap(),
        &staged_root,
        "v1/content/dst/b/c/file4.txt",
        "1971cbe108f98338aab3960c4537cc0c820dbc244d0ff4b99e32909a49b35267",
    );
    assert_file_details(
        staged_obj.state.get(&path("dst/d/e/file5.txt")).unwrap(),
        &staged_root,
        "v1/content/dst/d/e/file5.txt",
        "4ccdbf78d368aed12d806efaf67fbce3300bca8e62a6f32716af2f447de1821e",
    );

    Ok(())
}

#[test]
fn create_object_with_non_standard_config() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "non-standard";

    assert_staged_obj_count(&repo, 0);
    repo.create_object(object_id, DigestAlgorithm::Sha256, "content-dir", 5)
        .unwrap();
    assert_staged_obj_count(&repo, 1);

    create_file(&temp, "test.txt", "testing");

    repo.copy_files_external(
        object_id,
        &vec![temp.child("test.txt").path()],
        "test.txt",
        false,
    )
    .unwrap();

    let object = repo.get_staged_object(object_id).unwrap();

    assert_eq!(DigestAlgorithm::Sha256, object.digest_algorithm);
    assert_eq!("v00001", object.version_details.version_num.to_string());
    assert!(object
        .state
        .get(&path("test.txt"))
        .unwrap()
        .content_path
        .as_ref()
        .as_ref()
        .contains("/content-dir/"));
}

#[test]
#[should_panic(expected = "Object IDs may not be blank")]
fn reject_object_creation_with_empty_id() {
    let root = TempDir::new().unwrap();
    let repo = default_repo(root.path());
    repo.create_object(" ", DigestAlgorithm::Sha512, "content", 0)
        .unwrap();
}

#[test]
#[should_panic(expected = "The inventory digest algorithm must be sha512 or sha256. Found: md5")]
fn reject_object_creation_with_invalid_algorithm() {
    let root = TempDir::new().unwrap();
    let repo = default_repo(root.path());
    repo.create_object("id", DigestAlgorithm::Md5, "content", 0)
        .unwrap();
}

#[test]
#[should_panic(
    expected = "The content directory cannot equal '.' or '..' and cannot contain a '/'"
)]
fn reject_object_creation_with_invalid_content_dir_slash() {
    let root = TempDir::new().unwrap();
    let repo = default_repo(root.path());
    repo.create_object("id", DigestAlgorithm::Sha256, "content/dir", 0)
        .unwrap();
}

#[test]
#[should_panic(
    expected = "The content directory cannot equal '.' or '..' and cannot contain a '/'"
)]
fn reject_object_creation_with_invalid_content_dir_dot() {
    let root = TempDir::new().unwrap();
    let repo = default_repo(root.path());
    repo.create_object("id", DigestAlgorithm::Sha256, ".", 0)
        .unwrap();
}

#[test]
#[should_panic(
    expected = "The content directory cannot equal '.' or '..' and cannot contain a '/'"
)]
fn reject_object_creation_with_invalid_content_dir_dot_dot() {
    let root = TempDir::new().unwrap();
    let repo = default_repo(root.path());
    repo.create_object("id", DigestAlgorithm::Sha256, "..", 0)
        .unwrap();
}

#[test]
#[should_panic(expected = "Cannot create object id because it already exists")]
fn reject_object_creation_when_object_already_exists_in_main() {
    let root = TempDir::new().unwrap();
    let repo = default_repo(root.path());

    let object_id = "id";

    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)
        .unwrap();
    commit(object_id, &repo);

    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)
        .unwrap();
}

#[test]
#[should_panic(expected = "Cannot create object id because it already exists")]
fn reject_object_creation_when_object_already_exists_in_staging() {
    let root = TempDir::new().unwrap();
    let repo = default_repo(root.path());

    let object_id = "id";

    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)
        .unwrap();

    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)
        .unwrap();
}

#[test]
#[should_panic(
    expected = "Cannot create object because the repository does not have a defined storage layout, and an object root path was not specified."
)]
fn reject_object_commit_when_no_known_storage_layout() {
    let root = TempDir::new().unwrap();
    let repo = OcflRepo::fs_repo(root.path(), None).unwrap();
    repo.create_object("id", DigestAlgorithm::Sha512, "content", 0)
        .unwrap();
    commit("id", &repo);
}

#[test]
fn object_commit_when_no_known_storage_layout_and_root_specified() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();
    let repo = OcflRepo::fs_repo(root.path(), None).unwrap();

    let object_id = "custom_layout";
    let object_root = "random/path/to/object";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
        .unwrap();

    repo.copy_files_external(
        object_id,
        &vec![create_file(&temp, "test.txt", "testing").path()],
        "test.txt",
        false,
    )
    .unwrap();

    repo.commit(object_id, CommitMeta::new(), Some(object_root), false)
        .unwrap();

    let committed_obj = repo.get_object(object_id, None).unwrap();

    assert_eq!(1, committed_obj.state.len());

    assert_file_details(
        committed_obj.state.get(&path("test.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/test.txt",
        "cf80cd8aed482d5d1527d7dc72fceff84e6326592848447d2dc0b0e87dfc9a90",
    );
}

#[test]
#[should_panic(expected = "Cannot create object object 2 because an object already exists at")]
fn fail_object_commit_when_no_known_storage_layout_and_root_specified_and_obj_already_there() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();
    let repo = OcflRepo::fs_repo(root.path(), None).unwrap();

    let object_id = "custom_layout";
    let object_root = "random/path/to/object";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
        .unwrap();

    repo.copy_files_external(
        object_id,
        &vec![create_file(&temp, "test.txt", "testing").path()],
        "test.txt",
        false,
    )
    .unwrap();

    repo.commit(object_id, CommitMeta::new(), Some(object_root), false)
        .unwrap();

    let committed_obj = repo.get_object(object_id, None).unwrap();

    assert_eq!(1, committed_obj.state.len());

    assert_file_details(
        committed_obj.state.get(&path("test.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/test.txt",
        "cf80cd8aed482d5d1527d7dc72fceff84e6326592848447d2dc0b0e87dfc9a90",
    );

    let object_2_id = "object 2";

    repo.create_object(object_2_id, DigestAlgorithm::Sha256, "content", 0)
        .unwrap();

    repo.copy_files_external(
        object_2_id,
        &vec![resolve_child(&temp, "test.txt").path()],
        "test.txt",
        false,
    )
    .unwrap();

    repo.commit(object_2_id, CommitMeta::new(), Some(object_root), false)
        .unwrap();
}

#[test]
fn internal_copy_single_existing_file() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "InternalCopy";

    let repo = default_repo(root.path());

    create_example_object(object_id, &repo, &temp);

    repo.copy_files_internal(object_id, None, &vec!["a/file1.txt"], "new/blah.txt", false)?;

    let committed_obj = repo.get_object(object_id, None)?;
    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(8, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("new/blah.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/file1.txt",
        "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
    );
    assert_file_details(
        staged_obj.state.get(&path("a/file1.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/file1.txt",
        "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
    );

    commit(object_id, &repo);

    let committed_obj = repo.get_object(object_id, None)?;

    assert_eq!(8, committed_obj.state.len());

    assert_file_details(
        committed_obj.state.get(&path("new/blah.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/file1.txt",
        "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
    );

    Ok(())
}

#[test]
fn internal_copy_multiple_existing_file() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "InternalCopy";

    let repo = default_repo(root.path());

    create_example_object(object_id, &repo, &temp);

    repo.copy_files_internal(
        object_id,
        Some(VersionNum::new(1)),
        &vec!["a/b/*", "a/d/e/file5.txt"],
        "new-dir",
        false,
    )?;

    let committed_obj = repo.get_object(object_id, None)?;
    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(10, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("new-dir/file2.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/b/file2.txt",
        "b47592b10bc3e5c8ca8703d0862df10a6e409f43478804f93a08dd1844ae81b6",
    );
    assert_file_details(
        staged_obj.state.get(&path("new-dir/file3.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/b/file3.txt",
        "e18fad97c1b6512b1588a1fa2b7f9a0e549df9cfc538ce6943b4f0f4ae78322c",
    );
    assert_file_details(
        staged_obj.state.get(&path("new-dir/file5.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/d/e/file5.txt",
        "4ccdbf78d368aed12d806efaf67fbce3300bca8e62a6f32716af2f447de1821e",
    );

    commit(object_id, &repo);

    let committed_obj = repo.get_object(object_id, None)?;

    assert_eq!(10, committed_obj.state.len());

    assert_file_details(
        committed_obj.state.get(&path("new-dir/file2.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/b/file2.txt",
        "b47592b10bc3e5c8ca8703d0862df10a6e409f43478804f93a08dd1844ae81b6",
    );
    assert_file_details(
        committed_obj.state.get(&path("new-dir/file3.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/b/file3.txt",
        "e18fad97c1b6512b1588a1fa2b7f9a0e549df9cfc538ce6943b4f0f4ae78322c",
    );
    assert_file_details(
        committed_obj.state.get(&path("new-dir/file5.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/d/e/file5.txt",
        "4ccdbf78d368aed12d806efaf67fbce3300bca8e62a6f32716af2f447de1821e",
    );

    Ok(())
}

#[test]
fn internal_copy_files_added_in_staged_version() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "InternalCopy staged version";

    let repo = default_repo(root.path());

    create_example_object(object_id, &repo, &temp);

    repo.copy_files_external(
        object_id,
        &vec![create_file(&temp, "just in.txt", "new file").path()],
        "just in.txt",
        true,
    )?;

    repo.copy_files_internal(object_id, None, &vec!["just in.txt"], "just-in.txt", false)?;

    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(9, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("just in.txt")).unwrap(),
        &Path::new(&staged_obj.object_root),
        "v5/content/just in.txt",
        "b37d2cbfd875891e9ed073fcbe61f35a990bee8eecbdd07f9efc51339d5ffd66",
    );
    assert_file_details(
        staged_obj.state.get(&path("just-in.txt")).unwrap(),
        &Path::new(&staged_obj.object_root),
        "v5/content/just-in.txt",
        "b37d2cbfd875891e9ed073fcbe61f35a990bee8eecbdd07f9efc51339d5ffd66",
    );

    commit(object_id, &repo);

    let committed_obj = repo.get_object(object_id, None)?;

    assert_eq!(9, committed_obj.state.len());

    let deduped_path = assert_deduped_path(
        &Path::new(&committed_obj.object_root),
        committed_obj.state.get(&path("just in.txt")).unwrap(),
        &["v5/content/just in.txt", "v5/content/just-in.txt"],
    );

    assert_file_details(
        committed_obj.state.get(&path("just in.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        (*deduped_path).as_ref(),
        "b37d2cbfd875891e9ed073fcbe61f35a990bee8eecbdd07f9efc51339d5ffd66",
    );
    assert_file_details(
        committed_obj.state.get(&path("just-in.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        (*deduped_path).as_ref(),
        "b37d2cbfd875891e9ed073fcbe61f35a990bee8eecbdd07f9efc51339d5ffd66",
    );

    Ok(())
}
#[test]
fn internal_copy_files_with_recursive_glob() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "InternalCopy globs!";

    let repo = default_repo(root.path());

    create_example_object(object_id, &repo, &temp);

    repo.copy_files_internal(
        object_id,
        Some(VersionNum::new(3)),
        &vec!["a/*"],
        "copied",
        true,
    )?;

    let committed_obj = repo.get_object(object_id, None)?;
    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(11, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("copied/file1.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/file1.txt",
        "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
    );
    assert_file_details(
        staged_obj.state.get(&path("copied/b/file2.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/b/file2.txt",
        "b47592b10bc3e5c8ca8703d0862df10a6e409f43478804f93a08dd1844ae81b6",
    );
    assert_file_details(
        staged_obj.state.get(&path("copied/d/e/file5.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/d/e/file5.txt",
        "4ccdbf78d368aed12d806efaf67fbce3300bca8e62a6f32716af2f447de1821e",
    );
    assert_file_details(
        staged_obj.state.get(&path("copied/f/file6.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/f/file6.txt",
        "ac055b59cef48e2c34706677198cd8445ad692689be5169f33f1d93f957581e0",
    );

    commit(object_id, &repo);

    let committed_obj = repo.get_object(object_id, None)?;

    assert_eq!(11, committed_obj.state.len());

    assert_file_details(
        committed_obj.state.get(&path("copied/file1.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/file1.txt",
        "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
    );
    assert_file_details(
        committed_obj
            .state
            .get(&path("copied/b/file2.txt"))
            .unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/b/file2.txt",
        "b47592b10bc3e5c8ca8703d0862df10a6e409f43478804f93a08dd1844ae81b6",
    );
    assert_file_details(
        committed_obj
            .state
            .get(&path("copied/d/e/file5.txt"))
            .unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/d/e/file5.txt",
        "4ccdbf78d368aed12d806efaf67fbce3300bca8e62a6f32716af2f447de1821e",
    );
    assert_file_details(
        committed_obj
            .state
            .get(&path("copied/f/file6.txt"))
            .unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/f/file6.txt",
        "ac055b59cef48e2c34706677198cd8445ad692689be5169f33f1d93f957581e0",
    );

    Ok(())
}

#[test]
#[should_panic(
    expected = "Conflicting logical path file3.txt/file1.txt: The path part file3.txt is an existing logical file"
)]
fn internal_copy_should_reject_conflicting_files() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "internal conflicting";

    create_example_object(object_id, &repo, &temp);

    repo.copy_files_internal(
        object_id,
        None,
        &vec!["a/file1.txt"],
        "file3.txt/file1.txt",
        false,
    )
    .unwrap();
}

#[test]
#[should_panic(
    expected = "Conflicting logical path a/b: This path is already in use as a directory"
)]
fn internal_copy_should_reject_conflicting_dirs() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "internal conflicting";

    create_example_object(object_id, &repo, &temp);

    repo.copy_files_external(
        object_id,
        &vec![create_file(&temp, "b", "b").path()],
        "b",
        true,
    )
    .unwrap();

    repo.copy_files_internal(object_id, None, &vec!["b"], "a", false)
        .unwrap();
}

#[test]
#[should_panic(
    expected = "Illegal argument: Paths may not contain '.', '..', or '' parts. Found: some/../../dir"
)]
fn internal_copy_should_reject_bad_dst() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "internal bad dst";

    create_example_object(object_id, &repo, &temp);

    repo.copy_files_internal(object_id, None, &vec!["file3.txt"], "some/../../dir", false)
        .unwrap();
}

#[test]
fn internal_copy_should_continue_on_partial_success() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "int-copy-partial-success";

    let repo = default_repo(root.path());

    create_example_object(object_id, &repo, &temp);

    let result = repo.copy_files_internal(
        object_id,
        None,
        &vec!["a/file1.txt", "bogus.txt", "a/file5.txt"],
        "new-dir",
        false,
    );

    match result {
        Err(RocflError::CopyMoveError(e)) => {
            assert_eq!(1, e.0.len());
            assert!(e
                .0
                .get(0)
                .unwrap()
                .contains("does not contain any files at bogus.txt"));
        }
        _ => panic!("Expected copy to return an error"),
    }

    let committed_obj = repo.get_object(object_id, None)?;
    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(9, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("new-dir/file1.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/file1.txt",
        "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
    );
    assert_file_details(
        staged_obj.state.get(&path("new-dir/file5.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/d/e/file5.txt",
        "4ccdbf78d368aed12d806efaf67fbce3300bca8e62a6f32716af2f447de1821e",
    );

    assert!(staged_obj.state.get(&path("a/file1.txt")).is_some());
    assert!(staged_obj.state.get(&path("a/file5.txt")).is_some());

    Ok(())
}

#[test]
fn move_files_into_new_object() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "move files";

    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)?;

    create_file(&temp, "test.txt", "testing");
    create_dirs(&temp, "nested/dir");
    create_file(&temp, "nested/1.txt", "File 1");
    create_file(&temp, "nested/dir/2.txt", "File 2");
    create_file(&temp, "nested/dir/3.txt", "File 3");

    repo.move_files_external(
        object_id,
        &vec![
            temp.child("test.txt").path(),
            resolve_child(&temp, "nested").path(),
        ],
        "/",
    )?;

    temp.child("test.txt").assert(predicates::path::missing());
    temp.child("nested").assert(predicates::path::missing());

    let staged_obj = repo.get_staged_object(object_id)?;
    let obj_root = PathBuf::from(&staged_obj.object_root);

    assert_eq!(4, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("test.txt")).unwrap(),
        &obj_root,
        "v1/content/test.txt",
        "521b9ccefbcd14d179e7a1bb877752870a6d620938b28a66a107eac6e6805b9d0989f45b57\
                        30508041aa5e710847d439ea74cd312c9355f1f2dae08d40e41d50",
    );
    assert_file_details(
        staged_obj.state.get(&path("nested/1.txt")).unwrap(),
        &obj_root,
        "v1/content/nested/1.txt",
        "9c614ba0d58c976d0b39f8f5536eb8af89fae745cbe3783ac2ca3e3055bb0b1e3687417a1d\
                        1104288d2883a4368d3dacb9931460c6e523117ff3eaa28810481a",
    );
    assert_file_details(
        staged_obj.state.get(&path("nested/dir/2.txt")).unwrap(),
        &obj_root,
        "v1/content/nested/dir/2.txt",
        "70ffe50550ae07cd0fc154cc1cd3a47b71499b5f67921b52219750441791981fb36476cd47\
                        8440601bc26da16b28c8a2be4478b36091f2615ac94a575581902c",
    );
    assert_file_details(
        staged_obj.state.get(&path("nested/dir/3.txt")).unwrap(),
        &obj_root,
        "v1/content/nested/dir/3.txt",
        "79c994f97612eb4ee6a3cb1fbbb45278da184ea73bfb483274bb783f0bce6a7bf8dd8cb0d4\
                        fc0eb2b065ebd28b2959b59d9a489929edf9ea7db4dcda8a09a76f",
    );

    assert_obj_count(&repo, 0);

    commit(object_id, &repo);

    assert_staged_obj_count(&repo, 0);
    assert_obj_count(&repo, 1);

    let obj = repo.get_object(object_id, None)?;
    let obj_root = PathBuf::from(&obj.object_root);

    assert_eq!(4, obj.state.len());

    assert_file_details(
        obj.state.get(&path("test.txt")).unwrap(),
        &obj_root,
        "v1/content/test.txt",
        "521b9ccefbcd14d179e7a1bb877752870a6d620938b28a66a107eac6e6805b9d0989f45b57\
                        30508041aa5e710847d439ea74cd312c9355f1f2dae08d40e41d50",
    );
    assert_file_details(
        obj.state.get(&path("nested/1.txt")).unwrap(),
        &obj_root,
        "v1/content/nested/1.txt",
        "9c614ba0d58c976d0b39f8f5536eb8af89fae745cbe3783ac2ca3e3055bb0b1e3687417a1d\
                        1104288d2883a4368d3dacb9931460c6e523117ff3eaa28810481a",
    );
    assert_file_details(
        obj.state.get(&path("nested/dir/2.txt")).unwrap(),
        &obj_root,
        "v1/content/nested/dir/2.txt",
        "70ffe50550ae07cd0fc154cc1cd3a47b71499b5f67921b52219750441791981fb36476cd47\
                        8440601bc26da16b28c8a2be4478b36091f2615ac94a575581902c",
    );
    assert_file_details(
        obj.state.get(&path("nested/dir/3.txt")).unwrap(),
        &obj_root,
        "v1/content/nested/dir/3.txt",
        "79c994f97612eb4ee6a3cb1fbbb45278da184ea73bfb483274bb783f0bce6a7bf8dd8cb0d4\
                        fc0eb2b065ebd28b2959b59d9a489929edf9ea7db4dcda8a09a76f",
    );

    Ok(())
}

#[test]
fn move_files_into_existing_object() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "move existing object";

    create_example_object(object_id, &repo, &temp);

    create_dirs(&temp, "nested/dir");
    create_file(&temp, "nested/1.txt", "File 1");
    create_file(&temp, "nested/dir/2.txt", "File 2");
    create_file(&temp, "nested/dir/3.txt", "File 3");

    repo.move_files_external(
        object_id,
        &vec![resolve_child(&temp, "nested/dir").path()],
        "another",
    )?;

    resolve_child(&temp, "nested/1.txt").assert(predicates::path::exists());
    resolve_child(&temp, "nested/dir").assert(predicates::path::missing());

    let staged_obj = repo.get_staged_object(object_id)?;
    let staged_root = PathBuf::from(&staged_obj.object_root);

    assert_eq!(9, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("another/2.txt")).unwrap(),
        &staged_root,
        "v5/content/another/2.txt",
        "a87974a0f8d71939d4ef8db398cf8487a0cf5aef5842cf3dad733d07db9044d8",
    );
    assert_file_details(
        staged_obj.state.get(&path("another/3.txt")).unwrap(),
        &staged_root,
        "v5/content/another/3.txt",
        "d9c924093b541d5f76801cd8d7d0c74799fd52c221f51816b801ebb3385b0329",
    );

    commit(object_id, &repo);

    let obj = repo.get_object(object_id, None)?;
    let object_root = PathBuf::from(&obj.object_root);

    assert_eq!(9, obj.state.len());

    assert_file_details(
        obj.state.get(&path("another/2.txt")).unwrap(),
        &object_root,
        "v5/content/another/2.txt",
        "a87974a0f8d71939d4ef8db398cf8487a0cf5aef5842cf3dad733d07db9044d8",
    );
    assert_file_details(
        obj.state.get(&path("another/3.txt")).unwrap(),
        &object_root,
        "v5/content/another/3.txt",
        "d9c924093b541d5f76801cd8d7d0c74799fd52c221f51816b801ebb3385b0329",
    );

    Ok(())
}

#[test]
fn move_files_should_dedup_on_commit() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "move dedup";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)?;

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "test.txt", "testing").path()],
        "test.txt",
    )?;

    commit(object_id, &repo);

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "test.txt", "testing").path()],
        "/dir/file.txt",
    )?;
    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "test.txt", "testing").path()],
        "another/copy/here/surprise.txt",
    )?;

    let staged_obj = repo.get_staged_object(object_id)?;
    let staged_root = PathBuf::from(&staged_obj.object_root);

    assert_file_details(
        staged_obj.state.get(&path("dir/file.txt")).unwrap(),
        &staged_root,
        "v2/content/dir/file.txt",
        "cf80cd8aed482d5d1527d7dc72fceff84e6326592848447d2dc0b0e87dfc9a90",
    );
    assert_file_details(
        staged_obj
            .state
            .get(&path("another/copy/here/surprise.txt"))
            .unwrap(),
        &staged_root,
        "v2/content/another/copy/here/surprise.txt",
        "cf80cd8aed482d5d1527d7dc72fceff84e6326592848447d2dc0b0e87dfc9a90",
    );

    commit(object_id, &repo);

    let obj = repo.get_object(object_id, None)?;
    let object_root = PathBuf::from(&obj.object_root);

    assert_eq!(3, obj.state.len());

    assert_file_details(
        obj.state.get(&path("test.txt")).unwrap(),
        &object_root,
        "v1/content/test.txt",
        "cf80cd8aed482d5d1527d7dc72fceff84e6326592848447d2dc0b0e87dfc9a90",
    );
    assert_file_details(
        obj.state.get(&path("dir/file.txt")).unwrap(),
        &object_root,
        "v1/content/test.txt",
        "cf80cd8aed482d5d1527d7dc72fceff84e6326592848447d2dc0b0e87dfc9a90",
    );
    assert_file_details(
        obj.state
            .get(&path("another/copy/here/surprise.txt"))
            .unwrap(),
        &object_root,
        "v1/content/test.txt",
        "cf80cd8aed482d5d1527d7dc72fceff84e6326592848447d2dc0b0e87dfc9a90",
    );

    Ok(())
}

#[test]
#[should_panic(
    expected = "Conflicting logical path test.txt/is/not/a/directory/test.txt: The path part test.txt is an existing logical file"
)]
fn move_should_reject_conflicting_files() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "conflicting-move";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
        .unwrap();

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "test.txt", "testing").path()],
        "test.txt",
    )
    .unwrap();

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "test.txt", "testing").path()],
        "test.txt/is/not/a/directory/test.txt",
    )
    .unwrap();
}

#[test]
#[should_panic(
    expected = "Conflicting logical path dir: This path is already in use as a directory"
)]
fn move_should_reject_conflicting_dirs() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "conflicting-move-dirs";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
        .unwrap();

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "test.txt", "testing").path()],
        "dir/sub/test.txt",
    )
    .unwrap();

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "dir", "conflict").path()],
        "/",
    )
    .unwrap();
}

#[test]
#[should_panic(
    expected = "Illegal argument: Paths may not contain '.', '..', or '' parts. Found: some/../../dir"
)]
fn move_should_reject_bad_dst() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "move bad dst";

    create_example_object(object_id, &repo, &temp);

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "test.txt", "testing").path()],
        "some/../../dir",
    )
    .unwrap();
}

#[test]
fn move_into_dir_when_dst_ends_with_slash() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "move inside";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)?;

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "test.txt", "testing").path()],
        "dir/",
    )?;

    let staged_obj = repo.get_staged_object(object_id)?;
    let staged_root = PathBuf::from(&staged_obj.object_root);

    assert_eq!(1, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("dir/test.txt")).unwrap(),
        &staged_root,
        "v1/content/dir/test.txt",
        "cf80cd8aed482d5d1527d7dc72fceff84e6326592848447d2dc0b0e87dfc9a90",
    );

    Ok(())
}

#[test]
fn move_into_dir_when_dest_is_existing_dir() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "existing dir";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)?;

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "test.txt", "testing").path()],
        "a/dir/here/test.txt",
    )?;

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "different.txt", "different").path()],
        "a/dir",
    )?;

    let staged_obj = repo.get_staged_object(object_id)?;
    let staged_root = PathBuf::from(&staged_obj.object_root);

    assert_eq!(2, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("a/dir/here/test.txt")).unwrap(),
        &staged_root,
        "v1/content/a/dir/here/test.txt",
        "cf80cd8aed482d5d1527d7dc72fceff84e6326592848447d2dc0b0e87dfc9a90",
    );
    assert_file_details(
        staged_obj.state.get(&path("a/dir/different.txt")).unwrap(),
        &staged_root,
        "v1/content/a/dir/different.txt",
        "9d6f965ac832e40a5df6c06afe983e3b449c07b843ff51ce76204de05c690d11",
    );

    Ok(())
}

#[test]
#[should_panic(expected = "Not found: Object does-not-exist")]
fn fail_move_when_target_obj_does_not_exist() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    repo.move_files_external(
        "does-not-exist",
        &vec![create_file(&temp, "test.txt", "testing").path()],
        "test.txt",
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "test.txt: Does not exist")]
fn fail_move_when_src_does_not_exist() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "partial success";

    let repo = default_repo(root.path());

    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)
        .unwrap();

    repo.move_files_external(object_id, &vec![temp.child("test.txt").path()], "test.txt")
        .unwrap();
}

#[test]
fn move_should_partially_succeed_when_multiple_src_and_some_fail() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "missing";

    let repo = default_repo(root.path());

    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)
        .unwrap();

    create_file(&temp, "test.txt", "testing");

    let result = repo.move_files_external(
        object_id,
        &vec![temp.child("bogus").path(), temp.child("test.txt").path()],
        "dst",
    );

    match result {
        Err(RocflError::CopyMoveError(e)) => {
            assert_eq!(1, e.0.len());
            assert!(e.0.get(0).unwrap().contains("bogus: Does not exist"));
        }
        _ => panic!("Expected copy to return an error"),
    }

    let staged_obj = repo.get_staged_object(object_id).unwrap();
    let staged_root = PathBuf::from(&staged_obj.object_root);

    assert_eq!(1, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("dst/test.txt")).unwrap(),
        &staged_root,
        "v1/content/dst/test.txt",
        "521b9ccefbcd14d179e7a1bb877752870a6d620938b28a66a107eac6e6805b9d0989f45b57\
                        30508041aa5e710847d439ea74cd312c9355f1f2dae08d40e41d50",
    );
}

#[test]
fn fail_copy_when_conflicting_src() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "conflicting source";

    let repo = default_repo(root.path());

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
        .unwrap();

    create_file(&temp, "a/test", "1");
    create_file(&temp, "b/test/testing.txt", "2");

    match repo.copy_files_external(
        object_id,
        &vec![
            resolve_child(&temp, "a/test").path(),
            resolve_child(&temp, "b/test").path(),
        ],
        "/",
        true,
    ) {
        Err(e) => {
            assert!(e.to_string().ends_with(
                "Illegal state: Conflicting logical path \
            test/testing.txt: The path part test is an existing logical file"
            ));
        }
        Ok(_) => panic!("Should have failed"),
    }

    let object = repo.get_staged_object(object_id).unwrap();

    assert_eq!(1, object.state.len());

    assert_file_details(
        object.state.get(&path("test")).unwrap(),
        &object.object_root,
        "v1/content/test",
        "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
    );
}

#[test]
fn internal_move_single_existing_file() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "InternalMove";

    let repo = default_repo(root.path());

    create_example_object(object_id, &repo, &temp);

    repo.move_files_internal(object_id, &vec!["a/file1.txt"], "new/blah.txt")?;

    let committed_obj = repo.get_object(object_id, None)?;
    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(7, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("new/blah.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/file1.txt",
        "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
    );

    assert!(staged_obj.state.get(&path("a/file1.txt")).is_none());

    commit(object_id, &repo);

    let committed_obj = repo.get_object(object_id, None)?;

    assert_eq!(7, committed_obj.state.len());

    assert_file_details(
        committed_obj.state.get(&path("new/blah.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/file1.txt",
        "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
    );

    assert!(committed_obj.state.get(&path("a/file1.txt")).is_none());

    Ok(())
}

#[test]
fn internal_move_multiple_existing_file() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "InternalMoveMulti";

    let repo = default_repo(root.path());

    create_example_object(object_id, &repo, &temp);

    repo.move_files_internal(object_id, &vec!["a/*.txt", "a/b"], "new-dir")?;

    let committed_obj = repo.get_object(object_id, None)?;
    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(7, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("new-dir/file1.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/file1.txt",
        "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
    );
    assert_file_details(
        staged_obj.state.get(&path("new-dir/file5.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/d/e/file5.txt",
        "4ccdbf78d368aed12d806efaf67fbce3300bca8e62a6f32716af2f447de1821e",
    );
    assert_file_details(
        staged_obj.state.get(&path("new-dir/b/file2.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/b/file2.txt",
        "b47592b10bc3e5c8ca8703d0862df10a6e409f43478804f93a08dd1844ae81b6",
    );

    assert!(staged_obj.state.get(&path("a/file1.txt")).is_none());
    assert!(staged_obj.state.get(&path("a/file5.txt")).is_none());
    assert!(staged_obj.state.get(&path("a/b/file2.txt")).is_none());

    commit(object_id, &repo);

    let committed_obj = repo.get_object(object_id, None)?;

    assert_eq!(7, committed_obj.state.len());

    assert_file_details(
        committed_obj.state.get(&path("new-dir/file1.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/file1.txt",
        "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
    );
    assert_file_details(
        committed_obj.state.get(&path("new-dir/file5.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/d/e/file5.txt",
        "4ccdbf78d368aed12d806efaf67fbce3300bca8e62a6f32716af2f447de1821e",
    );
    assert_file_details(
        committed_obj
            .state
            .get(&path("new-dir/b/file2.txt"))
            .unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/b/file2.txt",
        "b47592b10bc3e5c8ca8703d0862df10a6e409f43478804f93a08dd1844ae81b6",
    );

    assert!(committed_obj.state.get(&path("a/file1.txt")).is_none());
    assert!(committed_obj.state.get(&path("a/file5.txt")).is_none());
    assert!(committed_obj.state.get(&path("a/b/file2.txt")).is_none());

    Ok(())
}

#[test]
fn internal_move_should_continue_on_partial_success() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "int-move-partial-success";

    let repo = default_repo(root.path());

    create_example_object(object_id, &repo, &temp);

    let result = repo.move_files_internal(
        object_id,
        &vec!["a/file1.txt", "bogus.txt", "a/file5.txt"],
        "new-dir",
    );

    match result {
        Err(RocflError::CopyMoveError(e)) => {
            assert_eq!(1, e.0.len());
            assert!(e
                .0
                .get(0)
                .unwrap()
                .contains("does not contain any files at bogus.txt"));
        }
        _ => panic!("Expected copy to return an error"),
    }

    let committed_obj = repo.get_object(object_id, None)?;
    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(7, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("new-dir/file1.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/file1.txt",
        "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
    );
    assert_file_details(
        staged_obj.state.get(&path("new-dir/file5.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v1/content/a/d/e/file5.txt",
        "4ccdbf78d368aed12d806efaf67fbce3300bca8e62a6f32716af2f447de1821e",
    );

    assert!(staged_obj.state.get(&path("a/file1.txt")).is_none());
    assert!(staged_obj.state.get(&path("a/file5.txt")).is_none());

    Ok(())
}

#[test]
fn internal_move_files_added_in_staged_version() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "InternalMove staged version";

    let repo = default_repo(root.path());

    create_example_object(object_id, &repo, &temp);

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "just in.txt", "new file").path()],
        "just in.txt",
    )
    .unwrap();

    repo.move_files_internal(object_id, &vec!["just in.txt"], "just-in.txt")
        .unwrap();

    let staged_obj = repo.get_staged_object(object_id).unwrap();

    assert_eq!(8, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("just-in.txt")).unwrap(),
        &Path::new(&staged_obj.object_root),
        "v5/content/just-in.txt",
        "b37d2cbfd875891e9ed073fcbe61f35a990bee8eecbdd07f9efc51339d5ffd66",
    );

    assert!(staged_obj.state.get(&path("just in.txt")).is_none());

    commit(object_id, &repo);

    let committed_obj = repo.get_object(object_id, None).unwrap();

    assert_eq!(8, committed_obj.state.len());

    assert_file_details(
        committed_obj.state.get(&path("just-in.txt")).unwrap(),
        &Path::new(&committed_obj.object_root),
        "v5/content/just-in.txt",
        "b37d2cbfd875891e9ed073fcbe61f35a990bee8eecbdd07f9efc51339d5ffd66",
    );

    assert_file_not_exists(&committed_obj, "just in.txt", "v5/content/just in.txt");
}

#[test]
#[should_panic(
    expected = "Conflicting logical path file3.txt/file1.txt: The path part file3.txt is an existing logical file"
)]
fn internal_move_should_reject_conflicting_files() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "internal conflicting";

    create_example_object(object_id, &repo, &temp);

    repo.move_files_internal(object_id, &vec!["a/file1.txt"], "file3.txt/file1.txt")
        .unwrap();
}

#[test]
#[should_panic(
    expected = "Conflicting logical path a/b: This path is already in use as a directory"
)]
fn internal_move_should_reject_conflicting_dirs() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "internal conflicting";

    create_example_object(object_id, &repo, &temp);

    repo.move_files_external(object_id, &vec![create_file(&temp, "b", "b").path()], "b")
        .unwrap();

    repo.move_files_internal(object_id, &vec!["b"], "a")
        .unwrap();
}

#[test]
#[should_panic(
    expected = "Illegal argument: Paths may not contain '.', '..', or '' parts. Found: some/../../dir"
)]
fn internal_move_should_reject_bad_dst() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "internal mv bad dst";

    create_example_object(object_id, &repo, &temp);

    repo.move_files_internal(object_id, &vec!["file1.txt"], "some/../../dir")
        .unwrap();
}

#[test]
fn remove_existing_file() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "remove file";

    create_example_object(object_id, &repo, &temp);

    repo.remove_files(object_id, &vec!["a/file5.txt"], false)?;

    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(6, staged_obj.state.len());
    assert!(staged_obj.state.get(&path("a/file5.txt")).is_none());

    commit(object_id, &repo);

    let committed_obj = repo.get_object(object_id, None)?;

    assert_eq!(6, committed_obj.state.len());
    assert!(committed_obj.state.get(&path("a/file5.txt")).is_none());

    let previous_version = repo.get_object(object_id, Some(VersionNum::new(4)))?;

    assert!(previous_version.state.get(&path("a/file5.txt")).is_some());

    Ok(())
}

#[test]
fn remove_multiple_existing_files() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "remove files";

    create_example_object(object_id, &repo, &temp);

    repo.remove_files(object_id, &vec!["a/file5.txt", "something/new.txt"], false)?;

    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(5, staged_obj.state.len());
    assert!(staged_obj.state.get(&path("a/file5.txt")).is_none());
    assert!(staged_obj.state.get(&path("something/new.txt")).is_none());

    commit(object_id, &repo);

    let committed_obj = repo.get_object(object_id, None)?;

    assert_eq!(5, committed_obj.state.len());
    assert!(committed_obj.state.get(&path("a/file5.txt")).is_none());
    assert!(committed_obj
        .state
        .get(&path("something/new.txt"))
        .is_none());

    let previous_version = repo.get_object(object_id, Some(VersionNum::new(4)))?;

    assert!(previous_version.state.get(&path("a/file5.txt")).is_some());
    assert!(previous_version
        .state
        .get(&path("something/new.txt"))
        .is_some());

    Ok(())
}

#[test]
fn remove_globs() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "remove files";

    create_example_object(object_id, &repo, &temp);

    repo.remove_files(object_id, &vec!["a/*"], false)?;

    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(5, staged_obj.state.len());
    assert!(staged_obj.state.get(&path("a/file5.txt")).is_none());
    assert!(staged_obj.state.get(&path("a/file1.txt")).is_none());
    assert!(staged_obj.state.get(&path("a/f/file6.txt")).is_some());

    commit(object_id, &repo);

    let committed_obj = repo.get_object(object_id, None)?;

    assert_eq!(5, committed_obj.state.len());
    assert!(committed_obj.state.get(&path("a/file5.txt")).is_none());
    assert!(committed_obj.state.get(&path("a/file1.txt")).is_none());

    let previous_version = repo.get_object(object_id, Some(VersionNum::new(4)))?;

    assert!(previous_version.state.get(&path("a/file5.txt")).is_some());
    assert!(previous_version.state.get(&path("a/file1.txt")).is_some());

    Ok(())
}

#[test]
fn remove_recursive() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "remove files";

    create_example_object(object_id, &repo, &temp);

    repo.remove_files(object_id, &vec!["*/*"], true)?;

    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(1, staged_obj.state.len());
    assert!(staged_obj.state.get(&path("file3.txt")).is_some());

    commit(object_id, &repo);

    let committed_obj = repo.get_object(object_id, None)?;

    assert_eq!(1, committed_obj.state.len());
    assert!(committed_obj.state.get(&path("file3.txt")).is_some());

    Ok(())
}

#[test]
fn remove_files_that_do_not_exist_should_do_nothing() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "remove files";

    create_example_object(object_id, &repo, &temp);

    repo.remove_files(object_id, &vec!["bogus", "file3.txt"], true)?;

    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(6, staged_obj.state.len());
    assert!(staged_obj.state.get(&path("file3.txt")).is_none());

    Ok(())
}

#[test]
fn reset_newly_added_files() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "reset";

    create_example_object(object_id, &repo, &temp);

    repo.move_files_external(
        object_id,
        &vec![
            create_file(&temp, "new.txt", "new file").path(),
            create_file(&temp, "new2.txt", "new file2").path(),
        ],
        "/",
    )?;

    let staged_obj = repo.get_staged_object(object_id)?;
    let staged_root = PathBuf::from(&staged_obj.object_root);

    assert_eq!(9, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("new.txt")).unwrap(),
        &staged_root,
        "v5/content/new.txt",
        "b37d2cbfd875891e9ed073fcbe61f35a990bee8eecbdd07f9efc51339d5ffd66",
    );
    assert_file_details(
        staged_obj.state.get(&path("new2.txt")).unwrap(),
        &staged_root,
        "v5/content/new2.txt",
        "104d021d7891c889c85c12e83e35ba1c5327c4415878c69372fe71e8f3992a28",
    );

    repo.reset(object_id, &vec!["new.txt"], false)?;

    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(8, staged_obj.state.len());

    assert!(staged_obj.state.get(&path("new.txt")).is_none());
    assert!(staged_obj.state.get(&path("new2.txt")).is_some());

    commit(object_id, &repo);

    let obj = repo.get_object(object_id, None)?;
    let object_root = PathBuf::from(&obj.object_root);

    assert_eq!(8, obj.state.len());

    assert!(obj.state.get(&path("new.txt")).is_none());
    assert!(!object_root
        .join("v5")
        .join("content")
        .join("new.txt")
        .exists());

    assert_file_details(
        obj.state.get(&path("new2.txt")).unwrap(),
        &object_root,
        "v5/content/new2.txt",
        "104d021d7891c889c85c12e83e35ba1c5327c4415878c69372fe71e8f3992a28",
    );

    Ok(())
}

#[test]
fn reset_copied_file() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "reset dup";

    create_example_object(object_id, &repo, &temp);

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "new.txt", "new file").path()],
        "/",
    )?;

    repo.copy_files_internal(object_id, None, &vec!["new.txt"], "new (copy).txt", false)?;

    let staged_obj = repo.get_staged_object(object_id)?;
    let staged_root = PathBuf::from(&staged_obj.object_root);

    assert_eq!(9, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("new.txt")).unwrap(),
        &staged_root,
        "v5/content/new.txt",
        "b37d2cbfd875891e9ed073fcbe61f35a990bee8eecbdd07f9efc51339d5ffd66",
    );
    assert_file_details(
        staged_obj.state.get(&path("new (copy).txt")).unwrap(),
        &staged_root,
        "v5/content/new (copy).txt",
        "b37d2cbfd875891e9ed073fcbe61f35a990bee8eecbdd07f9efc51339d5ffd66",
    );

    repo.reset(object_id, &vec!["new.txt"], false)?;

    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(8, staged_obj.state.len());

    assert!(staged_obj.state.get(&path("new.txt")).is_none());
    assert!(staged_obj.state.get(&path("new (copy).txt")).is_some());

    commit(object_id, &repo);

    let obj = repo.get_object(object_id, None)?;
    let object_root = PathBuf::from(&obj.object_root);

    assert_eq!(8, obj.state.len());

    assert!(obj.state.get(&path("new.txt")).is_none());

    assert_file_details(
        obj.state.get(&path("new (copy).txt")).unwrap(),
        &object_root,
        "v5/content/new (copy).txt",
        "b37d2cbfd875891e9ed073fcbe61f35a990bee8eecbdd07f9efc51339d5ffd66",
    );

    Ok(())
}

#[test]
fn reset_changes_to_existing_files() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "reset updates";

    create_example_object(object_id, &repo, &temp);

    repo.move_files_external(
        object_id,
        &vec![
            create_file(&temp, "file1.txt", "update").path(),
            create_file(&temp, "file5.txt", "update 2").path(),
        ],
        "a",
    )?;

    let staged_obj = repo.get_staged_object(object_id)?;
    let staged_root = PathBuf::from(&staged_obj.object_root);

    assert_eq!(7, staged_obj.state.len());

    assert_file_details(
        staged_obj.state.get(&path("a/file1.txt")).unwrap(),
        &staged_root,
        "v5/content/a/file1.txt",
        "2937013f2181810606b2a799b05bda2849f3e369a20982a4138f0e0a55984ce4",
    );
    assert_file_details(
        staged_obj.state.get(&path("a/file5.txt")).unwrap(),
        &staged_root,
        "v5/content/a/file5.txt",
        "0c23cc2b5985555eeb46bda05d886e2281c00731bcfc5aca22e00a4d4baa6100",
    );

    repo.reset(object_id, &vec!["a/*"], false)?;

    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(7, staged_obj.state.len());

    let object_root = repo.get_object(object_id, None)?.object_root;

    assert_file_details(
        staged_obj.state.get(&path("a/file1.txt")).unwrap(),
        &Path::new(&object_root),
        "v1/content/a/file1.txt",
        "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
    );
    assert_file_details(
        staged_obj.state.get(&path("a/file5.txt")).unwrap(),
        &Path::new(&object_root),
        "v1/content/a/d/e/file5.txt",
        "4ccdbf78d368aed12d806efaf67fbce3300bca8e62a6f32716af2f447de1821e",
    );

    commit(object_id, &repo);

    let obj = repo.get_object(object_id, None)?;

    assert_eq!(7, obj.state.len());

    assert_file_details(
        obj.state.get(&path("a/file1.txt")).unwrap(),
        &Path::new(&obj.object_root),
        "v1/content/a/file1.txt",
        "7d9fe7396f8f5f9862bfbfff4d98877bf36cf4a44447078c8d887dcc2dab0497",
    );
    assert_file_details(
        obj.state.get(&path("a/file5.txt")).unwrap(),
        &Path::new(&obj.object_root),
        "v1/content/a/d/e/file5.txt",
        "4ccdbf78d368aed12d806efaf67fbce3300bca8e62a6f32716af2f447de1821e",
    );

    Ok(())
}

#[test]
fn reset_removed_file() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "reset";

    create_example_object(object_id, &repo, &temp);

    repo.remove_files(object_id, &vec!["a"], true)?;

    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(3, staged_obj.state.len());

    assert!(staged_obj.state.get(&path("a/file1.txt")).is_none());
    assert!(staged_obj.state.get(&path("a/file5.txt")).is_none());
    assert!(staged_obj.state.get(&path("a/b/file2.txt")).is_none());
    assert!(staged_obj.state.get(&path("a/f/file6.txt")).is_none());

    repo.reset(object_id, &vec!["a/f"], true)?;

    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(4, staged_obj.state.len());

    assert!(staged_obj.state.get(&path("a/file1.txt")).is_none());
    assert!(staged_obj.state.get(&path("a/file5.txt")).is_none());

    let object_root = PathBuf::from(repo.get_object(object_id, None)?.object_root);

    assert_file_details(
        staged_obj.state.get(&path("a/f/file6.txt")).unwrap(),
        &object_root,
        "v4/content/a/f/file6.txt",
        "df21fb2fb83c1c64015a00e7677ccceb8da5377cba716611570230fb91d32bc9",
    );

    commit(object_id, &repo);

    let obj = repo.get_object(object_id, None)?;

    assert_eq!(4, obj.state.len());

    assert!(staged_obj.state.get(&path("a/file1.txt")).is_none());
    assert!(staged_obj.state.get(&path("a/file5.txt")).is_none());

    assert_file_details(
        obj.state.get(&path("a/f/file6.txt")).unwrap(),
        &object_root,
        "v4/content/a/f/file6.txt",
        "df21fb2fb83c1c64015a00e7677ccceb8da5377cba716611570230fb91d32bc9",
    );

    Ok(())
}

#[test]
#[should_panic(expected = "does not have a staged version")]
fn reset_all() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "reset all";

    create_example_object(object_id, &repo, &temp);

    repo.remove_files(object_id, &vec!["*"], true).unwrap();

    let staged_obj = repo.get_staged_object(object_id).unwrap();

    assert_eq!(0, staged_obj.state.len());

    repo.reset_all(object_id).unwrap();

    repo.get_staged_object(object_id).unwrap();
}

#[test]
fn reset_complex_changes_without_conflict() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "reset";

    create_example_object(object_id, &repo, &temp);

    repo.remove_files(object_id, &vec!["a"], true)?;

    repo.move_files_external(object_id, &vec![create_file(&temp, "b", "b").path()], "a/b")?;

    repo.move_files_internal(object_id, &vec!["file3.txt"], "a/file1.txt/file3.txt")?;

    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(4, staged_obj.state.len());

    assert!(staged_obj.state.get(&path("a/file1.txt")).is_none());
    assert!(staged_obj.state.get(&path("a/file5.txt")).is_none());
    assert!(staged_obj.state.get(&path("a/b/file2.txt")).is_none());
    assert!(staged_obj.state.get(&path("a/f/file6.txt")).is_none());

    assert!(staged_obj.state.get(&path("a/b")).is_some());
    assert!(staged_obj
        .state
        .get(&path("a/file1.txt/file3.txt"))
        .is_some());

    repo.reset(object_id, &vec!["*"], true)?;

    let staged_obj = repo.get_staged_object(object_id)?;

    assert_eq!(7, staged_obj.state.len());

    assert!(staged_obj.state.get(&path("a/b")).is_none());
    assert!(staged_obj
        .state
        .get(&path("a/file1.txt/file3.txt"))
        .is_none());

    Ok(())
}

#[test]
#[should_panic(
    expected = "Conflicting logical path a/file1.txt: This path is already in use as a directory"
)]
fn fail_reset_when_conflicted() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "reset conflict";

    create_example_object(object_id, &repo, &temp);

    repo.remove_files(object_id, &vec!["a"], true).unwrap();

    repo.move_files_external(object_id, &vec![create_file(&temp, "b", "b").path()], "a/b")
        .unwrap();

    repo.move_files_internal(object_id, &vec!["file3.txt"], "a/file1.txt/file3.txt")
        .unwrap();

    let staged_obj = repo.get_staged_object(object_id).unwrap();

    assert_eq!(4, staged_obj.state.len());

    assert!(staged_obj.state.get(&path("a/file1.txt")).is_none());
    assert!(staged_obj.state.get(&path("a/file5.txt")).is_none());
    assert!(staged_obj.state.get(&path("a/b/file2.txt")).is_none());
    assert!(staged_obj.state.get(&path("a/f/file6.txt")).is_none());

    assert!(staged_obj.state.get(&path("a/b")).is_some());
    assert!(staged_obj
        .state
        .get(&path("a/file1.txt/file3.txt"))
        .is_some());

    repo.reset(object_id, &vec!["a/file1.txt"], false).unwrap();
}

#[test]
fn reset_should_do_nothing_when_path_does_not_exist() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "reset";

    create_example_object(object_id, &repo, &temp);

    repo.remove_files(object_id, &vec!["a"], true)?;

    let staged_obj = repo.get_staged_object(object_id)?;
    assert_eq!(3, staged_obj.state.len());

    repo.reset(object_id, &vec!["bogus"], true)?;

    let staged_obj = repo.get_staged_object(object_id)?;
    assert_eq!(3, staged_obj.state.len());

    Ok(())
}

#[test]
fn reset_should_do_nothing_if_object_has_no_changes() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "reset";

    create_example_object(object_id, &repo, &temp);

    repo.reset(object_id, &vec!["bogus"], true)?;

    if let Err(RocflError::NotFound(_)) = repo.get_staged_object(object_id) {
        Ok(())
    } else {
        panic!("Expected the staged object to not be found");
    }
}

#[test]
fn reset_should_do_nothing_if_object_does_not_exist() -> Result<()> {
    let root = TempDir::new().unwrap();
    let repo = default_repo(root.path());

    let object_id = "missing";

    repo.reset(object_id, &vec!["bogus"], true)?;

    assert_staged_obj_not_exists(&repo, object_id);

    Ok(())
}

#[test]
fn purge_should_remove_object_from_repo() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "purge me";

    create_example_object(object_id, &repo, &temp);

    repo.purge_object(object_id)?;

    assert_obj_not_exists(&repo, object_id);

    Ok(())
}

#[test]
fn purge_should_remove_object_from_repo_and_staging() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "purge me2";

    create_example_object(object_id, &repo, &temp);

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "blah", "blah").path()],
        "blah",
    )?;

    repo.purge_object(object_id)?;

    assert_obj_not_exists(&repo, object_id);
    assert_staged_obj_not_exists(&repo, object_id);

    Ok(())
}

#[test]
fn purge_should_do_nothing_when_obj_does_not_exist() -> Result<()> {
    let root = TempDir::new().unwrap();
    let repo = default_repo(root.path());

    let object_id = "missing";

    repo.purge_object(object_id)?;

    assert_obj_not_exists(&repo, object_id);
    assert_staged_obj_not_exists(&repo, object_id);

    Ok(())
}

#[test]
fn commit_should_use_custom_meta_when_provided() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "commit meta";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)?;

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "blah", "blah").path()],
        "blah",
    )?;

    let name = "name";
    let address = "address";
    let message = "message";
    let created = Local.ymd(2021, 3, 19).and_hms(6, 1, 30);

    let meta = CommitMeta::new()
        .with_user(Some(name.to_string()), Some(address.to_string()))?
        .with_message(Some(message.to_string()))
        .with_created(Some(created));

    repo.commit(object_id, meta, None, false)?;

    let obj = repo.get_object(object_id, None)?;

    assert_eq!(name, obj.version_details.user_name.unwrap());
    assert_eq!(address, obj.version_details.user_address.unwrap());
    assert_eq!(message, obj.version_details.message.unwrap());
    assert_eq!(created, obj.version_details.created);

    Ok(())
}

#[test]
fn commit_should_use_custom_meta_when_mixture_provided() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "commit meta";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)?;

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "blah", "blah").path()],
        "blah",
    )?;

    let message = "new message";
    let created = Local.ymd(2020, 3, 19).and_hms(6, 1, 30);

    let meta = CommitMeta::new()
        .with_message(Some(message.to_string()))
        .with_created(Some(created));

    repo.commit(object_id, meta, None, false)?;

    let obj = repo.get_object(object_id, None)?;

    assert!(obj.version_details.user_name.is_none());
    assert!(obj.version_details.user_address.is_none());
    assert_eq!(message, obj.version_details.message.unwrap());
    assert_eq!(created, obj.version_details.created);

    Ok(())
}

#[test]
fn commit_should_pretty_print_inventory() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "pretty";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
        .unwrap();

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "blah", "blah").path()],
        "blah",
    )
    .unwrap();

    let timestamp = Local.ymd(2020, 3, 19).and_hms(6, 1, 30);
    let meta = CommitMeta::new().with_created(Some(timestamp));

    repo.commit(object_id, meta, None, true).unwrap();

    let obj = repo.get_object(object_id, None).unwrap();

    let inventory_path = Path::new(&obj.object_root).join("inventory.json");

    let expected_p1 = r#"{
  "id": "pretty",
  "type": "https://ocfl.io/1.0/spec/#inventory",
  "digestAlgorithm": "sha256",
  "head": "v1",
  "contentDirectory": "content",
  "manifest": {
    "8b7df143d91c716ecfa5fc1730022f6b421b05cedee8fd52b1fc65a96030ad52": [
      "v1/content/blah"
    ]
  },
  "versions": {
    "v1": {
      "created": ""#;

    let expected_p2 = r#"",
      "state": {
        "8b7df143d91c716ecfa5fc1730022f6b421b05cedee8fd52b1fc65a96030ad52": [
          "blah"
        ]
      }
    }
  }
}"#;

    assert_eq!(
        format!("{}{}{}", expected_p1, timestamp.to_rfc3339(), expected_p2),
        fs::read_to_string(&inventory_path).unwrap()
    );
}

#[test]
#[should_panic(expected = "User name must be set when user address is set")]
fn commit_should_fail_when_address_and_no_name() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "commit missing name";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
        .unwrap();

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "blah", "blah").path()],
        "blah",
    )
    .unwrap();

    let meta = CommitMeta::new()
        .with_user(None, Some("address".to_string()))
        .unwrap();

    repo.commit(object_id, meta, None, false).unwrap();
}

#[test]
#[should_panic(expected = "No staged changes found for object")]
fn commit_should_fail_when_object_has_no_changes() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "commit missing name";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
        .unwrap();

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "blah", "blah").path()],
        "blah",
    )
    .unwrap();

    commit(object_id, &repo);

    commit(object_id, &repo);
}

#[test]
#[should_panic(expected = "No staged changes found for object")]
fn commit_should_fail_when_object_does_not_exist() {
    let root = TempDir::new().unwrap();
    let repo = default_repo(root.path());

    let object_id = "does not exist";

    commit(object_id, &repo);
}

#[test]
fn commit_should_remove_staged_object() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "commit meta";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)?;

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "blah", "blah").path()],
        "blah",
    )?;

    commit(object_id, &repo);

    let _obj = repo.get_object(object_id, None)?;

    assert_staged_obj_not_exists(&repo, object_id);

    Ok(())
}

#[test]
fn get_staged_object_file_when_exists_in_staged_version() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "get file";

    create_example_object(object_id, &repo, &temp);

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "blah", "blah").path()],
        "blah",
    )?;

    let mut out: Vec<u8> = Vec::new();

    repo.get_staged_object_file(object_id, &path("blah"), &mut out)?;

    assert_eq!("blah", String::from_utf8(out).unwrap());

    Ok(())
}

#[test]
fn get_staged_object_file_when_exists_in_prior_version() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "get file";

    create_example_object(object_id, &repo, &temp);

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "blah", "blah").path()],
        "blah",
    )?;

    let mut out: Vec<u8> = Vec::new();

    repo.get_staged_object_file(object_id, &path("a/file1.txt"), &mut out)?;

    assert_eq!("File One", String::from_utf8(out).unwrap());

    Ok(())
}

#[test]
#[should_panic(expected = "Path a/b/file3.txt not found in object get file version v5")]
fn fail_get_staged_object_file_when_does_not_exist() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "get file";

    create_example_object(object_id, &repo, &temp);

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "blah", "blah").path()],
        "blah",
    )
    .unwrap();

    let mut out: Vec<u8> = Vec::new();

    repo.get_staged_object_file(object_id, &path("a/b/file3.txt"), &mut out)
        .unwrap();
}

#[test]
fn diff_should_detect_simple_rename() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "simple rename";

    create_example_object(object_id, &repo, &temp);

    let mut diff = repo.diff(object_id, None, VersionNum::new(4))?;

    assert_eq!(2, diff.len());

    sort_diffs(&mut diff);

    assert_eq!(
        Diff::Renamed {
            original: vec![path_rc("a/d/e/file5.txt")],
            renamed: vec![path_rc("a/file5.txt")],
        },
        diff.remove(0)
    );
    assert_eq!(Diff::Modified(path_rc("a/f/file6.txt")), diff.remove(0));

    Ok(())
}

#[test]
fn diff_should_detect_multi_origin_rename() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "multi origin rename";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)?;

    let file = create_file(&temp, "file.txt", "some file");

    repo.copy_files_external(object_id, &vec![file.path()], "file-1.txt", false)?;
    repo.copy_files_external(object_id, &vec![file.path()], "file-2.txt", false)?;
    repo.copy_files_external(object_id, &vec![file.path()], "file-3.txt", false)?;

    commit(object_id, &repo);

    repo.move_files_internal(object_id, &vec!["file-1.txt"], "moved.txt")?;
    repo.remove_files(object_id, &vec!["file-2.txt"], false)?;

    commit(object_id, &repo);

    let mut diff = repo.diff(object_id, None, VersionNum::new(2))?;

    assert_eq!(1, diff.len());

    assert_eq!(
        Diff::Renamed {
            original: vec![path_rc("file-1.txt"), path_rc("file-2.txt")],
            renamed: vec![path_rc("moved.txt")],
        },
        diff.remove(0)
    );

    Ok(())
}

#[test]
fn diff_should_detect_multi_dest_rename() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "multi dst rename";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)?;

    let file = create_file(&temp, "file.txt", "some file");

    repo.copy_files_external(object_id, &vec![file.path()], "file-1.txt", false)?;
    repo.copy_files_external(object_id, &vec![file.path()], "file-2.txt", false)?;
    repo.copy_files_external(object_id, &vec![file.path()], "file-3.txt", false)?;

    commit(object_id, &repo);

    repo.move_files_internal(object_id, &vec!["file-1.txt"], "moved.txt")?;
    repo.copy_files_internal(object_id, None, &vec!["file-2.txt"], "moved-2.txt", false)?;

    commit(object_id, &repo);

    let mut diff = repo.diff(object_id, None, VersionNum::new(2))?;

    assert_eq!(1, diff.len());

    assert_eq!(
        Diff::Renamed {
            original: vec![path_rc("file-1.txt")],
            renamed: vec![path_rc("moved-2.txt"), path_rc("moved.txt")],
        },
        diff.remove(0)
    );

    Ok(())
}

#[test]
fn diff_should_detect_multi_src_multi_dest_rename() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "multi multi rename";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)?;

    let file = create_file(&temp, "file.txt", "some file");

    repo.copy_files_external(object_id, &vec![file.path()], "file-1.txt", false)?;
    repo.copy_files_external(object_id, &vec![file.path()], "file-2.txt", false)?;
    repo.copy_files_external(object_id, &vec![file.path()], "file-3.txt", false)?;

    commit(object_id, &repo);

    repo.move_files_internal(object_id, &vec!["file-1.txt"], "moved.txt")?;
    repo.move_files_internal(object_id, &vec!["file-2.txt"], "moved-2.txt")?;

    commit(object_id, &repo);

    let mut diff = repo.diff(object_id, None, VersionNum::new(2))?;

    assert_eq!(1, diff.len());

    assert_eq!(
        Diff::Renamed {
            original: vec![path_rc("file-1.txt"), path_rc("file-2.txt")],
            renamed: vec![path_rc("moved-2.txt"), path_rc("moved.txt")],
        },
        diff.remove(0)
    );

    Ok(())
}

#[test]
fn diff_staged_changes_when_some() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "staged rename";

    create_example_object(object_id, &repo, &temp);

    repo.remove_files(object_id, &vec!["a/file5.txt"], false)?;
    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "new.txt", "new").path()],
        "/",
    )?;
    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "update.txt", "update").path()],
        "a/file1.txt",
    )?;
    repo.move_files_internal(object_id, &vec!["a/f/file6.txt"], "a")?;

    let mut diff = repo.diff_staged(object_id)?;

    sort_diffs(&mut diff);

    assert_eq!(4, diff.len());

    assert_eq!(
        Diff::Renamed {
            original: vec![path_rc("a/f/file6.txt")],
            renamed: vec![path_rc("a/file6.txt")],
        },
        diff.remove(0)
    );
    assert_eq!(Diff::Modified(path_rc("a/file1.txt")), diff.remove(0));
    assert_eq!(Diff::Deleted(path_rc("a/file5.txt")), diff.remove(0));
    assert_eq!(Diff::Added(path_rc("new.txt")), diff.remove(0));

    Ok(())
}

#[test]
fn diff_empty_when_no_staged_changes() -> Result<()> {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "staged rename";

    create_example_object(object_id, &repo, &temp);

    let diff = repo.diff_staged(object_id)?;

    assert_eq!(0, diff.len());

    Ok(())
}

#[test]
fn internal_copy_of_new_file_should_copy_file_on_disk() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "copy overwrite";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
        .unwrap();

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "a-file.txt", "contents").path()],
        "/",
    )
    .unwrap();
    repo.copy_files_internal(object_id, None, &vec!["a-file.txt"], "b-file.txt", false)
        .unwrap();
    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "a-file.txt", "different!").path()],
        "/",
    )
    .unwrap();

    let staged = repo.get_staged_object(object_id).unwrap();
    let staged_root = PathBuf::from(&staged.object_root);

    assert_eq!(2, staged.state.len());

    assert_file_details(
        staged.state.get(&path("a-file.txt")).unwrap(),
        &staged_root,
        "v1/content/a-file.txt",
        "3b6bb43dcbbaa5b3db412a2fd63b1a4c0db38d0a03a65694af8a3e3cc2d78347",
    );
    assert_file_details(
        staged.state.get(&path("b-file.txt")).unwrap(),
        &staged_root,
        "v1/content/b-file.txt",
        "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
    );
}

#[test]
fn internal_move_of_new_file_should_move_file_on_disk() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "move overwrite";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
        .unwrap();

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "a-file.txt", "contents").path()],
        "/",
    )
    .unwrap();
    repo.move_files_internal(object_id, &vec!["a-file.txt"], "b-file.txt")
        .unwrap();
    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "a-file.txt", "different!").path()],
        "/",
    )
    .unwrap();

    let staged = repo.get_staged_object(object_id).unwrap();
    let staged_root = PathBuf::from(&staged.object_root);

    assert_eq!(2, staged.state.len());

    assert_file_details(
        staged.state.get(&path("a-file.txt")).unwrap(),
        &staged_root,
        "v1/content/a-file.txt",
        "3b6bb43dcbbaa5b3db412a2fd63b1a4c0db38d0a03a65694af8a3e3cc2d78347",
    );
    assert_file_details(
        staged.state.get(&path("b-file.txt")).unwrap(),
        &staged_root,
        "v1/content/b-file.txt",
        "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
    );
}

#[test]
fn internal_move_of_new_file_should_move_file_on_disk_and_not_leave_empty_dirs() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "move overwrite";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
        .unwrap();

    create_file(&temp, "dir/a-file.txt", "contents").path();

    repo.move_files_external(object_id, &vec![resolve_child(&temp, "dir").path()], "/")
        .unwrap();
    repo.move_files_internal(object_id, &vec!["dir/a-file.txt"], "b-file.txt")
        .unwrap();
    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "dir", "different!").path()],
        "/",
    )
    .unwrap();

    let staged = repo.get_staged_object(object_id).unwrap();
    let staged_root = PathBuf::from(&staged.object_root);

    assert_eq!(2, staged.state.len());

    assert_file_details(
        staged.state.get(&path("dir")).unwrap(),
        &staged_root,
        "v1/content/dir",
        "3b6bb43dcbbaa5b3db412a2fd63b1a4c0db38d0a03a65694af8a3e3cc2d78347",
    );
    assert_file_details(
        staged.state.get(&path("b-file.txt")).unwrap(),
        &staged_root,
        "v1/content/b-file.txt",
        "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
    );
}

#[test]
fn internal_copy_of_duplicate_file_should_operate_on_staged_version() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "copy dupe overwrite";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
        .unwrap();

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "a-file.txt", "contents").path()],
        "/",
    )
    .unwrap();

    commit(object_id, &repo);

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "a-file-2.txt", "contents").path()],
        "/",
    )
    .unwrap();

    repo.copy_files_internal(object_id, None, &vec!["a-file-2.txt"], "b-file.txt", false)
        .unwrap();
    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "a-file.txt", "different!").path()],
        "/",
    )
    .unwrap();

    let staged = repo.get_staged_object(object_id).unwrap();
    let staged_root = PathBuf::from(&staged.object_root);

    assert_eq!(3, staged.state.len());

    assert_file_details(
        staged.state.get(&path("a-file.txt")).unwrap(),
        &staged_root,
        "v2/content/a-file.txt",
        "3b6bb43dcbbaa5b3db412a2fd63b1a4c0db38d0a03a65694af8a3e3cc2d78347",
    );
    assert_file_details(
        staged.state.get(&path("b-file.txt")).unwrap(),
        &staged_root,
        "v2/content/b-file.txt",
        "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
    );
    assert_file_details(
        staged.state.get(&path("a-file-2.txt")).unwrap(),
        &staged_root,
        "v2/content/a-file-2.txt",
        "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
    );

    commit(object_id, &repo);

    let committed_obj = repo.get_object(object_id, None).unwrap();
    let object_root = PathBuf::from(&committed_obj.object_root);

    assert_file_details(
        committed_obj.state.get(&path("b-file.txt")).unwrap(),
        &object_root,
        "v1/content/a-file.txt",
        "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
    );
    assert_file_details(
        committed_obj.state.get(&path("a-file-2.txt")).unwrap(),
        &object_root,
        "v1/content/a-file.txt",
        "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
    );
}

#[test]
fn fail_commit_when_staged_version_out_of_sync_with_main() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = default_repo(root.path());

    let object_id = "out-of-sync";
    let id_hash = "46acfc156ff00023c6ff7c5cfc923eaf43123f63dd558579e90293f0eba1e574";

    create_example_object(object_id, &repo, &temp);

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "a-file.txt", "contents").path()],
        "/",
    )
    .unwrap();

    let staged = repo.get_staged_object(object_id).unwrap();
    let staged_root = PathBuf::from(&staged.object_root);

    let mut options = CopyOptions::new();
    options.copy_inside = true;

    fs_extra::dir::copy(&staged_root, temp.path(), &options).unwrap();

    commit(object_id, &repo);

    fs_extra::dir::copy(temp.child(id_hash).path(), &staged_root, &options).unwrap();

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "b-file.txt", "another").path()],
        "/",
    )
    .unwrap();

    if let Err(e) = repo.commit(object_id, CommitMeta::new(), None, false) {
        assert_eq!("Illegal state: Cannot create version v5 in object out-of-sync because the current version is at v5",
                   e.to_string());
    } else {
        panic!("Commit should have thrown an error");
    }

    let staged = repo.get_staged_object(object_id).unwrap();

    assert_file_details(
        staged.state.get(&path("a-file.txt")).unwrap(),
        &staged_root,
        "v5/content/a-file.txt",
        "d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8",
    );
    assert_file_details(
        staged.state.get(&path("b-file.txt")).unwrap(),
        &staged_root,
        "v5/content/b-file.txt",
        "ae448ac86c4e8e4dec645729708ef41873ae79c6dff84eff73360989487f08e5",
    );

    let committed_obj = repo.get_object(object_id, None).unwrap();
    assert_eq!(
        VersionNum::new(5),
        committed_obj.version_details.version_num
    );
}

#[test]
#[should_panic(expected = "Cannot stage changes for object because it has an active mutable HEAD.")]
fn do_not_stage_changes_for_objects_with_mutable_heads() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    copy_existing_repo("mutable", &root);

    let repo = OcflRepo::fs_repo(root.child("mutable").path(), None).unwrap();
    let object_id = "o1";

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "test.txt", "testing").path()],
        "/",
    )
    .unwrap();
}

#[test]
fn create_and_update_object_in_repo_with_no_layout() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = OcflRepo::init_fs_repo(root.path(), None, None).unwrap();

    let object_id = "no layout";
    let object_root = "random/path/to/obj";

    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
        .unwrap();

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "test.txt", "testing").path()],
        "test.txt",
    )
    .unwrap();

    repo.commit(object_id, CommitMeta::new(), Some(object_root), false)
        .unwrap();

    let obj = repo.get_object(object_id, None).unwrap();
    let storage_path = PathBuf::from(&obj.object_root);

    assert_eq!(1, obj.state.len());

    assert_file_details(
        obj.state.get(&path("test.txt")).unwrap(),
        &storage_path,
        "v1/content/test.txt",
        "cf80cd8aed482d5d1527d7dc72fceff84e6326592848447d2dc0b0e87dfc9a90",
    );

    if std::path::MAIN_SEPARATOR == '\\' {
        assert!(obj
            .object_root
            .replace("\\", "/")
            .ends_with(&format!("/{}", object_root)));
    } else {
        assert!(obj.object_root.ends_with(&format!("/{}", object_root)));
    }

    repo.move_files_external(
        object_id,
        &vec![create_file(&temp, "test2.txt", "testing2").path()],
        "test2.txt",
    )
    .unwrap();

    repo.commit(object_id, CommitMeta::new(), Some(object_root), false)
        .unwrap();

    let obj = repo.get_object(object_id, None).unwrap();

    assert_eq!(2, obj.state.len());

    assert_file_details(
        obj.state.get(&path("test.txt")).unwrap(),
        &storage_path,
        "v1/content/test.txt",
        "cf80cd8aed482d5d1527d7dc72fceff84e6326592848447d2dc0b0e87dfc9a90",
    );
    assert_file_details(
        obj.state.get(&path("test2.txt")).unwrap(),
        &storage_path,
        "v2/content/test2.txt",
        "431111472993bf4d9b8b347476b79321fea8a337f3c1cb2fedaa185b54185540",
    );
}

#[test]
#[should_panic(expected = "Expected object to exist at")]
fn fail_when_incorrect_object_in_root() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let repo = OcflRepo::init_fs_repo(
        root.path(),
        None,
        Some(StorageLayout::new(LayoutExtensionName::FlatDirectLayout, None).unwrap()),
    )
    .unwrap();

    let object_id_1 = "original";
    let object_id_2 = "different";

    repo.create_object(object_id_1, DigestAlgorithm::Sha256, "content", 0)
        .unwrap();
    repo.move_files_external(
        object_id_1,
        &vec![create_file(&temp, "file1.txt", "one").path()],
        "/",
    )
    .unwrap();
    repo.commit(object_id_1, CommitMeta::new(), None, false)
        .unwrap();

    fs::rename(
        resolve_child(&root, object_id_1).path(),
        resolve_child(&root, object_id_2).path(),
    )
    .unwrap();

    repo.get_object(object_id_2, None).unwrap();
}

// TODO validate all test created inventories after adding validation API

// TODO When version rewrite is implemented it is no longer safe to assume that logical paths
//      were mapped directly to content paths. This means that all move/copy operations must
//      verify that they are not unintentionally overwriting an existing file.

fn assert_staged_obj_count(repo: &OcflRepo, count: usize) {
    assert_eq!(count, repo.list_staged_objects(None).unwrap().count());
}

fn assert_obj_count(repo: &OcflRepo, count: usize) {
    assert_eq!(count, repo.list_objects(None).unwrap().count());
}

fn assert_obj_not_exists(repo: &OcflRepo, object_id: &str) {
    match repo.get_object(object_id, None) {
        Err(RocflError::NotFound(_)) => (),
        _ => panic!("Expected object '{}' to not be found", object_id),
    }
}

fn assert_staged_obj_not_exists(repo: &OcflRepo, object_id: &str) {
    match repo.get_staged_object(object_id) {
        Err(RocflError::NotFound(_)) => (),
        _ => panic!("Expected staged object '{}' to not be found", object_id),
    }
}

fn assert_file_not_exists(obj: &ObjectVersion, logical_path: &str, content_path: &str) {
    assert!(obj.state.get(&path(logical_path)).is_none());
    assert!(!Path::new(&obj.object_root).join(content_path).exists());
}

fn assert_file_details(
    actual: &FileDetails,
    object_root: impl AsRef<Path>,
    content_path: &str,
    digest: &str,
) {
    assert_eq!(path_rc(content_path), actual.content_path);
    assert_eq!(
        join(object_root, &content_path.split('/').collect::<Vec<&str>>())
            .to_string_lossy()
            .to_string(),
        actual.storage_path
    );

    assert!(
        Path::new(&actual.storage_path).is_file(),
        "Expected {} to exist and be a file",
        actual.storage_path
    );
    if digest.len() == 64 {
        assert_eq!(
            digest,
            DigestAlgorithm::Sha256
                .hash_hex(&mut File::open(&actual.storage_path).unwrap())
                .unwrap()
                .as_ref()
        )
    } else {
        assert_eq!(
            digest,
            DigestAlgorithm::Sha512
                .hash_hex(&mut File::open(&actual.storage_path).unwrap())
                .unwrap()
                .as_ref()
        )
    }
    assert_eq!(Rc::new(digest.into()), actual.digest);
}

fn assert_deduped_path(
    object_root: impl AsRef<Path>,
    details: &FileDetails,
    possible_paths: &[&str],
) -> Rc<InventoryPath> {
    assert!(possible_paths.contains(&(*details.content_path).as_ref()));

    let deduped = details.content_path.clone();

    let storage_path = object_root.as_ref().join((*deduped).as_ref());
    assert!(
        storage_path.exists(),
        "Expected '{}' to exist",
        storage_path.to_string_lossy()
    );

    possible_paths
        .iter()
        .filter(|p| (*deduped).as_ref() != **p)
        .for_each(|path| assert!(!object_root.as_ref().join(path).exists()));

    deduped
}

fn assert_storage_root(root: &TempDir) {
    root.child("0=ocfl_1.0")
        .assert(predicates::path::is_file())
        .assert("ocfl_1.0\n");
    root.child("ocfl_1.0.txt")
        .assert(predicates::path::is_file())
        .assert(read_spec("ocfl_1.0.txt"));
}

fn assert_layout_extension(root: &TempDir, layout_name: &str, config: &str) {
    root.child("ocfl_layout.json")
        .assert(predicates::path::is_file())
        .assert(predicates::str::contains(format!(
            "\"extension\": \"{}\"",
            layout_name
        )));

    let layout_spec = format!("{}.md", layout_name);
    root.child(&layout_spec)
        .assert(predicates::path::is_file())
        .assert(read_spec(&layout_spec));

    let extensions = root.child("extensions");
    extensions.assert(predicates::path::is_dir());

    let layout_dir = extensions.child(layout_name);
    layout_dir.assert(predicates::path::is_dir());
    layout_dir
        .child("config.json")
        .assert(predicates::path::is_file())
        .assert(config);
}

fn create_simple_object(object_id: &str, repo: &OcflRepo, temp: &TempDir) {
    repo.create_object(object_id, DigestAlgorithm::Sha512, "content", 0)
        .unwrap();

    temp.child("test.txt").write_str("testing").unwrap();
    repo.copy_files_external(
        object_id,
        &vec![temp.child("test.txt").path()],
        "test.txt",
        false,
    )
    .unwrap();

    commit(object_id, &repo);
}

/// # v1
///
/// - a/file1.txt
/// - a/b/file2.txt
/// - a/b/file3.txt
/// - a/b/c/file4.txt
/// - a/d/e/file5.txt
/// - a/f/file6.txt
///
/// # v2
///
/// - a/file1.txt
/// - a/b/file2.txt
/// - a/d/e/file5.txt
/// - a/f/file6.txt
///
///# v3
///
/// - file3.txt
/// - a/file1.txt
/// - a/b/file2.txt
/// - a/d/e/file5.txt
/// - a/f/file6.txt
/// - something/file1.txt
/// - something/new.txt
///
/// # v4
///
/// - file3.txt
/// - a/file1.txt
/// - a/file5.txt
/// - a/b/file2.txt
/// - a/f/file6.txt (updated)
/// - something/file1.txt
/// - something/new.txt
fn create_example_object(object_id: &str, repo: &OcflRepo, temp: &TempDir) {
    repo.create_object(object_id, DigestAlgorithm::Sha256, "content", 0)
        .unwrap();

    create_dirs(temp, "a/b/c");
    create_dirs(temp, "a/d/e");
    create_dirs(temp, "a/f");

    create_file(temp, "a/file1.txt", "File One");
    create_file(temp, "a/b/file2.txt", "File Two");
    create_file(temp, "a/b/file3.txt", "File Three");
    create_file(temp, "a/b/c/file4.txt", "File Four");
    create_file(temp, "a/d/e/file5.txt", "File Five");
    create_file(temp, "a/f/file6.txt", "File Six");

    repo.move_files_external(object_id, &vec![temp.child("a").path()], "/")
        .unwrap();

    commit(object_id, &repo);

    repo.remove_files(object_id, &vec!["a/b/file3.txt", "a/b/c/file4.txt"], false)
        .unwrap();

    commit(object_id, &repo);

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

    create_dirs(temp, "something");

    repo.copy_files_external(
        object_id,
        &vec![create_file(temp, "something/new.txt", "NEW").path()],
        "something/new.txt",
        true,
    )
    .unwrap();

    commit(object_id, &repo);

    repo.copy_files_external(
        object_id,
        &vec![create_file(temp, "file6.txt", "UPDATED!").path()],
        "a/f/file6.txt",
        true,
    )
    .unwrap();

    repo.move_files_internal(object_id, &vec!["a/d/e/file5.txt"], "a/file5.txt")
        .unwrap();

    commit(object_id, &repo);
}

fn commit(object_id: &str, repo: &OcflRepo) {
    repo.commit(object_id, CommitMeta::new(), None, false)
        .unwrap();
}

fn read_spec(name: &str) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("resources");
    path.push("main");
    path.push("specs");
    path.push(name);
    fs::read_to_string(path).unwrap()
}

fn o2_v1_details() -> VersionDetails {
    VersionDetails {
        version_num: VersionNum::new(1),
        created: DateTime::parse_from_rfc3339("2019-08-05T15:57:53Z")
            .unwrap()
            .into(),
        user_name: Some("Peter".to_string()),
        user_address: Some("peter@example.com".to_string()),
        message: Some("commit message".to_string()),
    }
}

fn o2_v2_details() -> VersionDetails {
    VersionDetails {
        version_num: VersionNum::new(2),
        created: DateTime::parse_from_rfc3339("2019-08-05T16:59:56Z")
            .unwrap()
            .into(),
        user_name: Some("Peter".to_string()),
        user_address: Some("peter@example.com".to_string()),
        message: Some("2".to_string()),
    }
}

fn o2_v3_details() -> VersionDetails {
    VersionDetails {
        version_num: VersionNum::new(3),
        created: DateTime::parse_from_rfc3339("2019-08-07T12:37:43Z")
            .unwrap()
            .into(),
        user_name: Some("Peter".to_string()),
        user_address: Some("peter@example.com".to_string()),
        message: Some("3".to_string()),
    }
}

fn default_repo(root: impl AsRef<Path>) -> OcflRepo {
    OcflRepo::init_fs_repo(
        root,
        None,
        Some(StorageLayout::new(LayoutExtensionName::HashedNTupleLayout, None).unwrap()),
    )
    .unwrap()
}

fn create_repo_root(name: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("resources");
    path.push("test");
    path.push("repos");
    path.push(name);
    path
}

fn copy_existing_repo(name: &str, root: &TempDir) {
    let path = create_repo_root(name);

    let options = CopyOptions::new();

    fs_extra::dir::copy(&path, root.path(), &options).unwrap();
}

fn sort_obj_details(objects: &mut Vec<ObjectVersionDetails>) {
    objects.sort_unstable_by(|a, b| a.id.cmp(&b.id));
}

fn sort_diffs(diffs: &mut Vec<Diff>) {
    diffs.sort_unstable_by(|a, b| a.path().cmp(&b.path()))
}

fn join(base: impl AsRef<Path>, parts: &[impl AsRef<Path>]) -> PathBuf {
    let mut joined = base.as_ref().to_path_buf();

    for part in parts {
        joined = joined.join(part.as_ref());
    }

    joined
}

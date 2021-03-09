use std::convert::{TryFrom, TryInto};
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;

use assert_fs::prelude::*;
use assert_fs::TempDir;
use chrono::DateTime;
use maplit::hashmap;

use rocfl::ocfl::layout::{LayoutExtensionName, StorageLayout};
use rocfl::ocfl::{
    Diff, DigestAlgorithm, FileDetails, InventoryPath, ObjectVersion, ObjectVersionDetails,
    OcflRepo, Result, VersionDetails, VersionNum,
};

#[test]
fn list_all_objects() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root)?;

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
                version_num: VersionNum::try_from(1).unwrap(),
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
                version_num: VersionNum::try_from(2).unwrap(),
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
    let repo = OcflRepo::fs_repo(&repo_root)?;

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
                version_num: VersionNum::try_from(1).unwrap(),
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
    let repo = OcflRepo::fs_repo(&repo_root)?;

    let objects: Vec<ObjectVersionDetails> = repo.list_objects(None)?.collect();

    assert_eq!(0, objects.len());

    Ok(())
}

#[test]
fn list_repo_with_invalid_objects() -> Result<()> {
    let repo_root = create_repo_root("invalid");
    let repo = OcflRepo::fs_repo(&repo_root)?;

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
    let repo = OcflRepo::fs_repo(&repo_root)?;

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
    let repo = OcflRepo::fs_repo(&repo_root)?;

    let object = repo.get_object("o2", Some(VersionNum::try_from(2)?))?;

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
#[should_panic(expected = "Not found: Object o4")]
fn error_when_object_not_exists() {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root).unwrap();
    repo.get_object("o4", None).unwrap();
}

#[test]
fn get_object_when_exists_using_layout() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects-with-layout");
    let repo = OcflRepo::fs_repo(&repo_root)?;

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
    let repo = OcflRepo::fs_repo(&repo_root).unwrap();
    repo.get_object("o4", None).unwrap();
}

#[test]
#[should_panic(expected = "Not found: Object o2 version v4")]
fn error_when_version_not_exists() {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root).unwrap();
    repo.get_object("o2", Some(VersionNum::try_from(4).unwrap()))
        .unwrap();
}

#[test]
#[should_panic(expected = "Not found: Object o3")]
fn error_when_get_invalid_object() {
    let repo_root = create_repo_root("invalid");
    let repo = OcflRepo::fs_repo(&repo_root).unwrap();
    repo.get_object("o3", None).unwrap();
}

#[test]
fn list_versions_when_multiple() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root)?;

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
    let repo = OcflRepo::fs_repo(&repo_root)?;

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
    let repo = OcflRepo::fs_repo(&repo_root).unwrap();
    repo.list_object_versions("o5").unwrap();
}

#[test]
#[should_panic(expected = "Not found: Path bogus.txt not found in object o2")]
fn list_file_versions_not_exists() {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root).unwrap();
    repo.list_file_versions("o2", &"bogus.txt".try_into().unwrap())
        .unwrap();
}

#[test]
fn diff_when_left_and_right_specified() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root)?;

    let mut diff = repo.diff(
        "o2",
        Some(VersionNum::try_from(1).unwrap()),
        VersionNum::try_from(3).unwrap(),
    )?;

    sort_diffs(&mut diff);

    assert_eq!(2, diff.len());

    assert_eq!(diff.remove(0), Diff::Added(path_rc("dir1/file3")));
    assert_eq!(diff.remove(0), Diff::Deleted(path_rc("file1")));

    Ok(())
}

#[test]
fn diff_with_previous_when_left_not_specified() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root)?;

    let mut diff = repo.diff("o2", None, VersionNum::try_from(3).unwrap())?;

    sort_diffs(&mut diff);

    assert_eq!(2, diff.len());

    assert_eq!(diff.remove(0), Diff::Modified(path_rc("dir1/file3")));
    assert_eq!(diff.remove(0), Diff::Deleted(path_rc("dir3/file1")));

    Ok(())
}

#[test]
fn diff_first_version_all_adds() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root)?;

    let mut diff = repo.diff("o2", None, VersionNum::try_from(1).unwrap())?;

    sort_diffs(&mut diff);

    assert_eq!(2, diff.len());

    assert_eq!(diff.remove(0), Diff::Added(path_rc("dir1/dir2/file2")));
    assert_eq!(diff.remove(0), Diff::Added(path_rc("file1")));

    Ok(())
}

#[test]
fn diff_same_version_no_diff() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root)?;

    let diff = repo.diff(
        "o2",
        Some(VersionNum::try_from(2).unwrap()),
        VersionNum::try_from(2).unwrap(),
    )?;

    assert_eq!(0, diff.len());

    Ok(())
}

// TODO rename test

#[test]
#[should_panic(expected = "Not found: Object o6")]
fn diff_object_not_exists() {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root).unwrap();
    repo.diff("o6", None, VersionNum::try_from(2).unwrap())
        .unwrap();
}

#[test]
#[should_panic(expected = "Not found: Object o1 version v2")]
fn diff_version_not_exists() {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root).unwrap();
    repo.diff("o1", None, VersionNum::try_from(2).unwrap())
        .unwrap();
}

#[test]
fn get_object_file_when_exists() -> Result<()> {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root)?;

    let id = "o2";
    let version = VersionNum::try_from(2)?;
    let mut out: Vec<u8> = Vec::new();

    repo.get_object_file(id, &"dir1/file3".try_into()?, Some(version), &mut out)?;

    assert_eq!("file 3", String::from_utf8(out).unwrap());

    Ok(())
}

#[test]
#[should_panic(expected = "Path dir1/bogus not found in object o2 version v2")]
fn fail_get_object_file_when_does_not_exist() {
    let repo_root = create_repo_root("multiple-objects");
    let repo = OcflRepo::fs_repo(&repo_root).unwrap();

    let id = "o2";
    let version = VersionNum::try_from(2).unwrap();
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
        StorageLayout::new(LayoutExtensionName::HashedNTupleLayout, None)?,
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
        StorageLayout::new(LayoutExtensionName::FlatDirectLayout, None)?,
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
        StorageLayout::new(LayoutExtensionName::HashedNTupleObjectIdLayout, None)?,
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
        StorageLayout::new(
            LayoutExtensionName::HashedNTupleLayout,
            Some(layout.as_bytes()),
        )?,
    )?;

    assert_storage_root(&root);
    assert_layout_extension(&root, "0004-hashed-n-tuple-storage-layout", layout);

    let object_id = "foobar";
    create_simple_object(object_id, &repo, &temp);

    root.child("0a502").child("61ebd")
        .child("1a390fed2bf326f2673c145582a6342d523204973d0219337f81616a8069b012587cf5635f6925f1b56c360230c19b273500ee013e030601bf2425")
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
        StorageLayout::new(LayoutExtensionName::HashedNTupleLayout, None).unwrap(),
    )
    .unwrap();
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
    repo.copy_files_external(object_id, &vec![temp.path()], "test.txt", false)
        .unwrap();

    repo.commit(object_id, None, None, None, None).unwrap();
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
        version_num: VersionNum::try_from(1).unwrap(),
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
        version_num: VersionNum::try_from(2).unwrap(),
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
        version_num: VersionNum::try_from(3).unwrap(),
        created: DateTime::parse_from_rfc3339("2019-08-07T12:37:43Z")
            .unwrap()
            .into(),
        user_name: Some("Peter".to_string()),
        user_address: Some("peter@example.com".to_string()),
        message: Some("3".to_string()),
    }
}

fn create_repo_root(name: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("resources");
    path.push("test");
    path.push("repos");
    path.push(name);
    path
}

fn sort_obj_details(objects: &mut Vec<ObjectVersionDetails>) {
    objects.sort_unstable_by(|a, b| a.id.cmp(&b.id));
}

fn sort_diffs(diffs: &mut Vec<Diff>) {
    diffs.sort_unstable_by(|a, b| a.path().cmp(&b.path()))
}

fn path_rc(path: &str) -> Rc<InventoryPath> {
    Rc::new(InventoryPath::try_from(path).unwrap())
}

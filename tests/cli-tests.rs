use std::path::Path;

use assert_cmd::Command;
use assert_fs::TempDir;
use common::*;
use predicates::prelude::*;
use predicates::str::{ContainsPredicate, IsEmptyPredicate};

mod common;

#[test]
fn basic_create_sanity_check() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "obj-1";

    let _ = init(root.path()).assert().success();

    let _ = list(root.path()).assert().success().stdout(empty());

    let _ = new(root.path()).arg(object_id).assert().success();

    let _ = list(root.path()).assert().success().stdout(empty());

    let file1 = "file.txt";

    let _ = copy(root.path())
        .arg(object_id)
        .arg(create_file(&temp, file1, "blah").path())
        .arg("--")
        .arg("/")
        .assert()
        .success();

    let _ = status(root.path())
        .assert()
        .success()
        .stdout(contains_str(object_id));
    let _ = status(root.path())
        .arg(object_id)
        .assert()
        .success()
        .stdout(contains_str(file1));

    let _ = commit(root.path()).arg(object_id).assert().success();

    let _ = list(root.path())
        .assert()
        .success()
        .stdout(contains_str(object_id));
    let _ = list(root.path())
        .arg(object_id)
        .assert()
        .success()
        .stdout(contains_str(file1));

    let _ = status(root.path()).assert().success().stdout(empty());
}

#[test]
fn list_multiple_objects() {
    let root = TempDir::new().unwrap();

    let object_id_1 = "a-obj-1";
    let object_id_2 = "b-obj-2";
    let object_id_3 = "a-obj-3";

    let _ = init(root.path()).assert().success();

    let _ = list(root.path()).assert().success().stdout(empty());

    let _ = new(root.path()).arg(object_id_1).assert().success();
    let _ = new(root.path()).arg(object_id_2).assert().success();
    let _ = new(root.path()).arg(object_id_3).assert().success();

    let _ = commit(root.path()).arg(object_id_1).assert().success();
    let _ = commit(root.path()).arg(object_id_2).assert().success();
    let _ = commit(root.path()).arg(object_id_3).assert().success();

    let _ = list(root.path())
        .assert()
        .success()
        .stdout(contains_str(object_id_1))
        .stdout(contains_str(object_id_2))
        .stdout(contains_str(object_id_3));

    let _ = list(root.path())
        .arg("-o")
        .arg("a-*")
        .assert()
        .success()
        .stdout(contains_str(object_id_1))
        .stdout(contains_str(object_id_2).not())
        .stdout(contains_str(object_id_3));
}

#[test]
fn logical_directory_listing() {
    let root = TempDir::new().unwrap();
    let temp = TempDir::new().unwrap();

    let object_id = "obj-1";

    let _ = init(root.path()).assert().success();

    let _ = new(root.path()).arg(object_id).assert().success();

    let _ = copy(root.path())
        .arg(object_id)
        .arg(create_file(&temp, "file1.txt", "blah").path())
        .arg("--")
        .arg("/")
        .assert()
        .success();

    let _ = commit(root.path()).arg(object_id).assert().success();

    let _ = mv(root.path())
        .arg(object_id)
        .arg(create_file(&temp, "file2.txt", "blahblah").path())
        .arg(create_file(&temp, "file3.txt", "blah").path())
        .arg("--")
        .arg("a")
        .assert()
        .success();

    let _ = mv(root.path())
        .arg(object_id)
        .arg(create_file(&temp, "different.txt", "different").path())
        .arg("--")
        .arg("a/c/")
        .assert()
        .success();

    let _ = copy(root.path())
        .arg(object_id)
        .arg("-i")
        .arg("file1.txt")
        .arg("--")
        .arg("b/")
        .assert()
        .success();

    let _ = commit(root.path()).arg(object_id).assert().success();

    let _ = list(root.path())
        .arg(object_id)
        .assert()
        .success()
        .stdout(contains_str("file1.txt"))
        .stdout(contains_str("b/file1.txt"))
        .stdout(contains_str("a/file2.txt"))
        .stdout(contains_str("a/file3.txt"))
        .stdout(contains_str("a/c/different.txt"));

    let _ = list(root.path())
        .arg("-D")
        .arg(object_id)
        .assert()
        .success()
        .stdout(contains_str("file1.txt"))
        .stdout(contains_str("b/file1.txt").not())
        .stdout(contains_str("a/file2.txt").not())
        .stdout(contains_str("a/file3.txt").not())
        .stdout(contains_str("a/c/different.txt").not())
        .stdout(contains_str("a/"))
        .stdout(contains_str("b/"));

    let _ = list(root.path())
        .arg("-D")
        .arg(object_id)
        .arg("a")
        .assert()
        .success()
        .stdout(contains_str("file1.txt").not())
        .stdout(contains_str("b/file1.txt").not())
        .stdout(contains_str("a/file2.txt"))
        .stdout(contains_str("a/file3.txt"))
        .stdout(contains_str("a/c/different.txt").not())
        .stdout(contains_str("a/c/"));

    let _ = list(root.path())
        .arg("-D")
        .arg(object_id)
        .arg("a/")
        .assert()
        .success()
        .stdout(contains_str("file1.txt").not())
        .stdout(contains_str("b/file1.txt").not())
        .stdout(contains_str("a/file2.txt"))
        .stdout(contains_str("a/file3.txt"))
        .stdout(contains_str("a/c/different.txt").not())
        .stdout(contains_str("a/c/"));

    let _ = list(root.path())
        .arg("-D")
        .arg(object_id)
        .arg("a/*")
        .assert()
        .success()
        .stdout(contains_str("file1.txt").not())
        .stdout(contains_str("b/file1.txt").not())
        .stdout(contains_str("a/file2.txt"))
        .stdout(contains_str("a/file3.txt"))
        .stdout(contains_str("a/c/different.txt").not());

    let _ = list(root.path())
        .arg(object_id)
        .arg("*file*.txt")
        .assert()
        .success()
        .stdout(contains_str("file1.txt"))
        .stdout(contains_str("b/file1.txt"))
        .stdout(contains_str("a/file2.txt"))
        .stdout(contains_str("a/file3.txt"))
        .stdout(contains_str("a/c/different.txt").not());

    let _ = list(root.path())
        .arg("-D")
        .arg(object_id)
        .arg("*file*.txt")
        .assert()
        .success()
        .stdout(contains_str("file1.txt"))
        .stdout(contains_str("b/file1.txt").not())
        .stdout(contains_str("a/file2.txt").not())
        .stdout(contains_str("a/file3.txt").not())
        .stdout(contains_str("a/c/different.txt").not());
}

// TODO backfill more cli sanity tests

fn init(path: impl AsRef<Path>) -> Command {
    rocfl(path, "init")
}

fn new(path: impl AsRef<Path>) -> Command {
    rocfl(path, "new")
}

fn copy(path: impl AsRef<Path>) -> Command {
    rocfl(path, "cp")
}

fn mv(path: impl AsRef<Path>) -> Command {
    rocfl(path, "mv")
}

fn commit(path: impl AsRef<Path>) -> Command {
    rocfl(path, "commit")
}

fn list(path: impl AsRef<Path>) -> Command {
    rocfl(path, "ls")
}

fn status(path: impl AsRef<Path>) -> Command {
    rocfl(path, "status")
}

fn rocfl(path: impl AsRef<Path>, command: &str) -> Command {
    let mut rocfl = Command::cargo_bin("rocfl").unwrap();
    rocfl
        .arg("-S")
        .arg("-r")
        .arg(path.as_ref().to_string_lossy().as_ref())
        .arg(command);
    rocfl
}

fn contains_str(string: &str) -> ContainsPredicate {
    predicates::str::contains(string)
}

fn empty() -> IsEmptyPredicate {
    predicates::str::is_empty()
}

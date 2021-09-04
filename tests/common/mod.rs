#![allow(dead_code)]

use std::convert::TryFrom;
use std::rc::Rc;

use assert_fs::fixture::ChildPath;
use assert_fs::prelude::*;
use assert_fs::TempDir;
use rocfl::ocfl::{ContentPath, LogicalPath};

pub fn create_dirs(temp: &TempDir, path: &str) -> ChildPath {
    let child = resolve_child(temp, path);
    child.create_dir_all().unwrap();
    child
}

pub fn create_file(temp: &TempDir, path: &str, content: &str) -> ChildPath {
    let child = resolve_child(temp, path);
    child.write_str(content).unwrap();
    child
}

pub fn resolve_child(temp: &TempDir, path: &str) -> ChildPath {
    let mut child: Option<ChildPath> = None;
    for part in path.split('/') {
        child = match child {
            Some(child) => Some(child.child(part)),
            None => Some(temp.child(part)),
        };
    }
    child.unwrap()
}

pub fn lpath(path: &str) -> LogicalPath {
    LogicalPath::try_from(path).unwrap()
}

pub fn lpath_rc(path: &str) -> Rc<LogicalPath> {
    Rc::new(LogicalPath::try_from(path).unwrap())
}

pub fn cpath(path: &str) -> ContentPath {
    ContentPath::try_from(path).unwrap()
}

pub fn cpath_rc(path: &str) -> Rc<ContentPath> {
    Rc::new(ContentPath::try_from(path).unwrap())
}

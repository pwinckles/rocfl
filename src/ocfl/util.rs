use std::borrow::Cow;
use std::io::ErrorKind;
use std::path::Path;
use std::{fs, io, path};

use walkdir::WalkDir;

use crate::ocfl::error::Result;

/// Indicates if the system path separator is `\`
pub const BACKSLASH_SEPARATOR: bool = path::MAIN_SEPARATOR == '\\';

/// Walks up the directory hierarchy deleting directories until it finds a non-empty directory.
pub fn clean_dirs_up(start_dir: impl AsRef<Path>) -> Result<()> {
    let mut current = start_dir.as_ref();

    while dir_is_empty(current)? {
        fs::remove_dir(current)?;
        current = current.parent().unwrap();
    }

    Ok(())
}
/// Walks down the directory hierarchy deleting all non-empty directories
pub fn clean_dirs_down(start_dir: impl AsRef<Path>) -> Result<()> {
    let start_dir = start_dir.as_ref();

    for entry in WalkDir::new(start_dir).contents_first(true) {
        let path = entry?;
        if path.file_type().is_dir() && dir_is_empty(path.path())? {
            fs::remove_dir(path.path())?;
        }
    }

    Ok(())
}

/// Identical to `fs::remove_file()` except `NotFound` errors are ignored
pub fn remove_file_ignore_not_found(path: impl AsRef<Path>) -> io::Result<()> {
    if let Err(e) = fs::remove_file(path) {
        if e.kind() != ErrorKind::NotFound {
            return Err(e);
        }
    }
    Ok(())
}

/// Returns true if the specified directory does not contain any files
pub fn dir_is_empty(dir: impl AsRef<Path>) -> Result<bool> {
    Ok(fs::read_dir(dir)?.next().is_none())
}

/// Changes `/` to `\` on Windows
pub fn convert_forwardslash_to_back(path: &str) -> Cow<str> {
    if BACKSLASH_SEPARATOR && path.contains('/') {
        return Cow::Owned(path.replace("/", "\\"));
    }
    path.into()
}

/// Changes `\\` to `/` on Windows
pub fn convert_backslash_to_forward(path: &str) -> Cow<str> {
    if BACKSLASH_SEPARATOR && path.contains('\\') {
        return Cow::Owned(path.replace("\\", "/"));
    }
    path.into()
}

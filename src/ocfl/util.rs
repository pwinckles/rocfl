use std::path::Path;
use std::{fs, path};

use walkdir::WalkDir;

use crate::ocfl::error::Result;
use std::borrow::Cow;

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

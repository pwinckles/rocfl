use std::fs;
use std::path::Path;

use crate::ocfl::error::Result;

/// Walks up the directory hierarchy deleting directories until it finds a non-empty directory.
pub fn clean_dirs_up(start_dir: impl AsRef<Path>) -> Result<()> {
    let mut current = start_dir.as_ref();

    while dir_is_empty(current)? {
        fs::remove_dir(current)?;
        current = current.parent().unwrap();
    }

    Ok(())
}

/// Returns true if the specified directory does not contain any files
pub fn dir_is_empty(dir: impl AsRef<Path>) -> Result<bool> {
    Ok(fs::read_dir(dir)?.next().is_none())
}

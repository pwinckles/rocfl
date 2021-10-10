use std::borrow::Cow;
use std::io::Write;

use crate::ocfl::error::Result;

/// Abstraction over reading files and listing directory contents. `/` _must_ be used as the file
/// path separator.
pub trait Storage {
    /// Reads the file at the specified path and writes its contents to the provided sink.
    fn read<W: Write>(&self, path: &str, sink: &mut W) -> Result<()>;

    /// Lists the contents of the specified directory. If `recursive` is `true`, then all leaf-nodes
    /// are returned. If the directory does not exist, or is empty, then an empty vector is returned.
    /// The returned paths are all relative the directory that was listed.
    fn list(&self, path: &str, recursive: bool) -> Result<Vec<Listing>>;
}

/// Represents filesystem entity
#[derive(Debug, Hash, Eq, PartialEq)]
pub enum Listing<'a> {
    /// A regular file
    File(Cow<'a, str>),
    /// A directory
    Directory(Cow<'a, str>),
    /// Anything that is not a regular file or directory, eg a symbolic link
    Other(Cow<'a, str>),
}

// TODO move to store/fs
pub mod fs {
    use std::borrow::Cow;
    use std::fs;
    use std::fs::File;
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};

    use walkdir::WalkDir;

    use crate::ocfl::error::Result;
    use crate::ocfl::util;
    use crate::ocfl::validate::store::{Listing, Storage};

    pub struct FsStorage {
        storage_root: PathBuf,
    }

    impl FsStorage {
        pub fn new(storage_root: impl AsRef<Path>) -> Self {
            Self {
                storage_root: storage_root.as_ref().to_path_buf(),
            }
        }
    }

    impl Storage for FsStorage {
        /// Reads the file at the specified path and writes its contents to the provided sink.
        fn read<W: Write>(&self, path: &str, sink: &mut W) -> Result<()> {
            io::copy(&mut File::open(self.storage_root.join(path))?, sink)?;
            Ok(())
        }

        /// Lists the contents of the specified directory. If `recursive` is `true`, then all leaf-nodes
        /// are returned. If the directory does not exist, or is empty, then an empty vector is returned.
        /// The returned paths are all relative the directory that was listed.
        fn list(&self, path: &str, recursive: bool) -> Result<Vec<Listing>> {
            let mut listings = Vec::new();
            let root = self.storage_root.join(path);

            if fs::metadata(&root).is_err() {
                return Ok(listings);
            }

            let mut walker = WalkDir::new(&root);

            if !recursive {
                walker = walker.max_depth(1);
            }

            for path in walker {
                let path = path?;

                let relative_path = util::convert_backslash_to_forward(
                    pathdiff::diff_paths(path.path(), &root)
                        .unwrap()
                        .to_string_lossy()
                        .as_ref(),
                )
                .to_string();

                if path.file_type().is_file() {
                    listings.push(Listing::File(Cow::Owned(relative_path)));
                } else if path.file_type().is_dir() {
                    if path.path() != root.as_path()
                        && (!recursive || util::dir_is_empty(path.path())?)
                    {
                        listings.push(Listing::Directory(Cow::Owned(relative_path)));
                    }
                } else {
                    listings.push(Listing::Other(Cow::Owned(relative_path)))
                }
            }

            Ok(listings)
        }
    }
}

impl<'a> Listing<'a> {
    pub fn file(path: &str) -> Listing {
        Listing::File(Cow::Borrowed(path))
    }

    pub fn dir(path: &str) -> Listing {
        Listing::Directory(Cow::Borrowed(path))
    }
    pub fn file_owned(path: String) -> Listing<'a> {
        Listing::File(Cow::Owned(path))
    }

    pub fn dir_owned(path: String) -> Listing<'a> {
        Listing::Directory(Cow::Owned(path))
    }

    pub fn path(&self) -> &str {
        match self {
            Listing::File(path) => path,
            Listing::Directory(path) => path,
            Listing::Other(path) => path,
        }
    }
}

mod s3 {
    // TODO
}

use std::borrow::Cow;
use std::io::Write;

use crate::ocfl::error::Result;

// TODO trait leak problem if I restrict to super
pub trait Storage {
    fn read<W: Write>(&self, path: &str, sink: &mut W) -> Result<()>;

    fn exists(&self, path: &str) -> Result<bool>;

    fn list(&self, path: &str, recursive: bool) -> Result<Vec<Listing>>;
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub enum Listing<'a> {
    File(Cow<'a, str>),
    Directory(Cow<'a, str>),
    Other(Cow<'a, str>),
}

// TODO move to store/fs
pub mod fs {
    use std::borrow::Cow;
    use std::fs;
    use std::fs::File;
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};

    use crate::ocfl::error::Result;
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
        fn read<W: Write>(&self, path: &str, sink: &mut W) -> Result<()> {
            // TODO error handling?
            io::copy(&mut File::open(self.storage_root.join(path))?, sink)?;
            Ok(())
        }

        fn exists(&self, path: &str) -> Result<bool> {
            Ok(self.storage_root.join(path).exists())
        }

        fn list(&self, path: &str, recursive: bool) -> Result<Vec<Listing>> {
            // TODO error handling
            let read_dir = fs::read_dir(self.storage_root.join(path))?;
            let mut listings = Vec::with_capacity(read_dir.size_hint().1.unwrap_or(0));

            for path in read_dir {
                let path = path?;
                let filename = path.file_name().to_string_lossy().into();
                let file_type = path.file_type()?;

                if file_type.is_file() {
                    listings.push(Listing::File(Cow::Owned(filename)));
                } else if file_type.is_dir() {
                    listings.push(Listing::Directory(Cow::Owned(filename)));
                } else {
                    listings.push(Listing::Other(Cow::Owned(filename)))
                }
            }

            // TODO recursive

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

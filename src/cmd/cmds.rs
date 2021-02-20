use std::io;

use log::info;

use crate::cmd::{Cmd, GlobalArgs, println};
use crate::cmd::opts::{Cat, Copy, DigestAlgorithm as OptAlgorithm, New};
use crate::cmd::opts::{Init, Layout, RocflArgs, Storage};
use crate::ocfl::digest::DigestAlgorithm;
use crate::ocfl::error::Result;
use crate::ocfl::layout::{LayoutExtensionName, StorageLayout};
use crate::ocfl::OcflRepo;

impl Cmd for Cat {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        repo.get_object_file(&self.object_id,
                             &self.path,
                             self.version,
                             &mut io::stdout())
    }
}

pub fn init_repo(cmd: &Init, args: &RocflArgs) -> Result<()> {
    match args.target_storage() {
        Storage::FileSystem => {
            let _ = OcflRepo::init_fs_repo(&args.root, create_layout(cmd.layout)?)?;
        }
        // TODO S3
        Storage::S3 => unimplemented!()
    }

    if !args.quiet {
        println("Initialized OCFL repository")?;
    }

    Ok(())
}

fn create_layout(layout: Layout) -> Result<StorageLayout> {
    match layout {
        Layout::FlatDirect => StorageLayout::new(LayoutExtensionName::FlatDirectLayout, None),
        Layout::HashedNTuple => StorageLayout::new(LayoutExtensionName::HashedNTupleLayout, None),
        Layout::HashedNTupleObjectId => StorageLayout::new(LayoutExtensionName::HashedNTupleObjectIdLayout, None),
    }
}

/// This is needed to keep enum_dispatch happy
impl Cmd for Init {
    fn exec(&self, _repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        unimplemented!()
    }
}

impl Cmd for New {
    fn exec(&self, repo: &OcflRepo, args: GlobalArgs) -> Result<()> {
        repo.create_object(&self.object_id,
                           algorithm(self.digest_algorithm),
                           &self.content_directory,
                           self.zero_padding)?;

        info!("Staged new OCFL object {}", self.object_id);

        Ok(())
    }
}

impl Cmd for Copy {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        if self.source_object.is_none() {
            // external copy
            repo.copy_files_external(self.destination_object.as_ref().unwrap(),
                                     &self.source,
                                     &self.destination,
                                     self.recursive,
                                     self.force)?;
        } else {
            // internal copy
            // TODO
        }

        // TODO output? maybe info logging internally

        Ok(())
    }
}

fn algorithm(algorithm: OptAlgorithm) -> DigestAlgorithm {
    match algorithm {
        OptAlgorithm::Sha256 => DigestAlgorithm::Sha256,
        OptAlgorithm::Sha512 => DigestAlgorithm::Sha512,
    }
}
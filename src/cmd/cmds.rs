use std::convert::TryInto;
use std::io;

use log::info;

use crate::cmd::opts::{
    CatCmd, CommitCmd, CopyCmd, DigestAlgorithm as OptAlgorithm, MoveCmd, NewCmd, RemoveCmd,
};
use crate::cmd::opts::{InitCmd, Layout, RocflArgs, Storage};
use crate::cmd::{println, Cmd, GlobalArgs};
use crate::ocfl::layout::{LayoutExtensionName, StorageLayout};
use crate::ocfl::{DigestAlgorithm, OcflRepo, Result};

impl Cmd for CatCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        repo.get_object_file(
            &self.object_id,
            &self.path.as_str().try_into()?,
            self.version,
            &mut io::stdout(),
        )
    }
}

pub fn init_repo(cmd: &InitCmd, args: &RocflArgs) -> Result<()> {
    match args.target_storage() {
        Storage::FileSystem => {
            let _ = OcflRepo::init_fs_repo(&args.root, create_layout(cmd.layout)?)?;
        }
        // TODO S3
        Storage::S3 => unimplemented!(),
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
        Layout::HashedNTupleObjectId => {
            StorageLayout::new(LayoutExtensionName::HashedNTupleObjectIdLayout, None)
        }
    }
}

/// This is needed to keep enum_dispatch happy
impl Cmd for InitCmd {
    fn exec(&self, _repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        unimplemented!()
    }
}

impl Cmd for NewCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        repo.create_object(
            &self.object_id,
            algorithm(self.digest_algorithm),
            &self.content_directory,
            self.zero_padding,
        )?;

        info!("Staged new OCFL object {}", self.object_id);

        Ok(())
    }
}

impl Cmd for CopyCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        if self.internal {
            repo.copy_files_internal(
                &self.object_id,
                self.version,
                &self.source,
                &self.destination,
                self.recursive,
                self.force,
            )
        } else {
            repo.copy_files_external(
                &self.object_id,
                &self.source,
                &self.destination,
                self.recursive,
                self.force,
            )
        }
    }
}

impl Cmd for MoveCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        if self.internal {
            repo.move_files_internal(&self.object_id, &self.source, &self.destination, self.force)
        } else {
            repo.move_files_external(&self.object_id, &self.source, &self.destination, self.force)
        }
    }
}

impl Cmd for RemoveCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        repo.remove_files(&self.object_id, &self.paths, self.recursive)
    }
}

impl Cmd for CommitCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        repo.commit(
            &self.object_id,
            &self.user_name,
            &self.user_address,
            &self.message,
        )?;

        Ok(())
    }
}

fn algorithm(algorithm: OptAlgorithm) -> DigestAlgorithm {
    match algorithm {
        OptAlgorithm::Sha256 => DigestAlgorithm::Sha256,
        OptAlgorithm::Sha512 => DigestAlgorithm::Sha512,
    }
}

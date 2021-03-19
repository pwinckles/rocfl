use std::convert::TryInto;
use std::io;

use log::info;

use crate::cmd::opts::{
    CatCmd, CommitCmd, CopyCmd, DigestAlgorithm as OptAlgorithm, Field, InitCmd, Layout, ListCmd,
    MoveCmd, NewCmd, PurgeCmd, RemoveCmd, RevertCmd, RocflArgs, ShowCmd, StatusCmd, Storage,
};
use crate::cmd::{print, println, Cmd, GlobalArgs};
use crate::ocfl::layout::{LayoutExtensionName, StorageLayout};
use crate::ocfl::{DigestAlgorithm, OcflRepo, Result};

impl Cmd for CatCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        if self.staged {
            repo.get_staged_object_file(
                &self.object_id,
                &self.path.as_str().try_into()?,
                &mut io::stdout(),
            )
        } else {
            repo.get_object_file(
                &self.object_id,
                &self.path.as_str().try_into()?,
                self.version,
                &mut io::stdout(),
            )
        }
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
            )
        } else {
            repo.copy_files_external(
                &self.object_id,
                &self.source,
                &self.destination,
                self.recursive,
            )
        }
    }
}

impl Cmd for MoveCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        if self.internal {
            repo.move_files_internal(&self.object_id, &self.source, &self.destination)
        } else {
            repo.move_files_external(&self.object_id, &self.source, &self.destination)
        }
    }
}

impl Cmd for RemoveCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        repo.remove_files(&self.object_id, &self.paths, self.recursive)
    }
}

impl Cmd for RevertCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        if self.paths.is_empty() {
            repo.revert(&self.object_id, &self.paths, self.recursive)
        } else {
            repo.revert_all(&self.object_id)
        }
    }
}

impl Cmd for CommitCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        repo.commit(
            &self.object_id,
            self.user_name.as_deref(),
            self.user_address.as_deref(),
            self.message.as_deref(),
            self.created,
        )?;

        Ok(())
    }
}

impl Cmd for StatusCmd {
    fn exec(&self, repo: &OcflRepo, args: GlobalArgs) -> Result<()> {
        if let Some(object_id) = self.object_id.as_ref() {
            let cmd = ShowCmd {
                object_id: object_id.to_string(),
                version: None,
                staged: true,
                minimal: false,
            };
            cmd.exec(repo, args)
        } else {
            let cmd = ListCmd {
                object_id: None,
                version: None,
                path: None,
                staged: true,
                all: false,
                digest: false,
                objects: false,
                header: true,
                long: true,
                reverse: false,
                physical: false,
                tsv: false,
                sort: Field::Name,
            };

            cmd.exec(repo, args)
        }
    }
}

impl Cmd for PurgeCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        if !self.force {
            print(format!("Permanently delete '{}'? [y/N]: ", self.object_id))?;
            let mut response = String::new();
            io::stdin().read_line(&mut response)?;
            if !response.trim().eq_ignore_ascii_case("y") {
                println("Aborted")?;
                return Ok(());
            }
        }

        repo.purge_object(&self.object_id)
    }
}

fn algorithm(algorithm: OptAlgorithm) -> DigestAlgorithm {
    match algorithm {
        OptAlgorithm::Sha256 => DigestAlgorithm::Sha256,
        OptAlgorithm::Sha512 => DigestAlgorithm::Sha512,
    }
}

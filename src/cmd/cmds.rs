use std::convert::TryInto;
use std::io;
use std::sync::atomic::AtomicBool;

use log::info;

use crate::cmd::opts::{
    CatCmd, CommitCmd, CopyCmd, DigestAlgorithm as OptAlgorithm, Field, InitCmd, ListCmd, MoveCmd,
    NewCmd, PurgeCmd, RemoveCmd, ResetCmd, ShowCmd, StatusCmd,
};
use crate::cmd::{print, println, Cmd, GlobalArgs};
use crate::ocfl::{CommitMeta, DigestAlgorithm, OcflRepo, Result};

impl Cmd for CatCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs, _terminate: &AtomicBool) -> Result<()> {
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

/// This is needed to keep enum_dispatch happy
impl Cmd for InitCmd {
    fn exec(&self, _repo: &OcflRepo, _args: GlobalArgs, _terminate: &AtomicBool) -> Result<()> {
        unimplemented!()
    }
}

impl Cmd for NewCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs, _terminate: &AtomicBool) -> Result<()> {
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
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs, _terminate: &AtomicBool) -> Result<()> {
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
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs, _terminate: &AtomicBool) -> Result<()> {
        if self.internal {
            repo.move_files_internal(&self.object_id, &self.source, &self.destination)
        } else {
            repo.move_files_external(&self.object_id, &self.source, &self.destination)
        }
    }
}

impl Cmd for RemoveCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs, _terminate: &AtomicBool) -> Result<()> {
        repo.remove_files(&self.object_id, &self.paths, self.recursive)
    }
}

impl Cmd for ResetCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs, _terminate: &AtomicBool) -> Result<()> {
        if self.paths.is_empty() {
            repo.reset(&self.object_id, &self.paths, self.recursive)
        } else {
            repo.reset_all(&self.object_id)
        }
    }
}

impl Cmd for CommitCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs, _terminate: &AtomicBool) -> Result<()> {
        let meta = CommitMeta::new()
            .with_user(self.user_name.clone(), self.user_address.clone())?
            .with_message(self.message.clone())
            .with_created(self.created);
        repo.commit(&self.object_id, meta, self.pretty_print)?;

        Ok(())
    }
}

impl Cmd for StatusCmd {
    fn exec(&self, repo: &OcflRepo, args: GlobalArgs, terminate: &AtomicBool) -> Result<()> {
        if let Some(object_id) = self.object_id.as_ref() {
            let cmd = ShowCmd {
                object_id: object_id.to_string(),
                version: None,
                staged: true,
                minimal: false,
            };
            cmd.exec(repo, args, terminate)
        } else {
            let cmd = ListCmd {
                object_id: None,
                version: None,
                path: None,
                staged: true,
                logical_dirs: false,
                digest: false,
                objects: false,
                header: true,
                long: true,
                reverse: false,
                physical: false,
                tsv: false,
                sort: Field::Name,
            };

            cmd.exec(repo, args, terminate)
        }
    }
}

impl Cmd for PurgeCmd {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs, _terminate: &AtomicBool) -> Result<()> {
        if !self.force {
            print(format!(
                "Permanently delete '{}'? This cannot be undone. [y/N]: ",
                self.object_id
            ))?;
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

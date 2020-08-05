use std::process::Command;

use anyhow::{anyhow, Context, Result};

use crate::cmd::opts::{Cat, RocflArgs};
use crate::ocfl::OcflRepo;

pub fn cat_command(repo: &OcflRepo, command: &Cat, args: &RocflArgs) -> Result<()> {
    CatCmd::new(repo, command, args).execute()
}

struct CatCmd<'a> {
    repo: &'a OcflRepo,
    command: &'a Cat,
    _args: &'a RocflArgs,
    cat: String,
}

impl<'a> CatCmd<'a> {
    fn new(repo: &'a OcflRepo, command: &'a Cat, _args: &'a RocflArgs) -> Self {
        let cat = if cfg!(windows) {
            "type"
        } else {
            "cat"
        };

        Self {
            repo,
            command,
            _args,
            cat: cat.to_owned(),
        }
    }

    fn execute(&self) -> Result<()> {
        let object = self.repo.get_object(&self.command.object_id, self.command.version.as_ref())
            .with_context(|| "Failed to find object")?;

        let storage_path = match object.state.get(&self.command.path) {
            Some(details) => &details.storage_path,
            None => return Err(anyhow!("Path {} not found in object {} version {}",
                self.command.path, self.command.object_id, object.version_details.version_num)),
        };

        // TODO s3?

        Command::new(&self.cat).arg(storage_path).status()?;

        Ok(())
    }
}
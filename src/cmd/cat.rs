use std::io;

use anyhow::Result;

use crate::cmd::opts::{Cat, RocflArgs};
use crate::ocfl::OcflRepo;

pub fn cat_command(repo: &OcflRepo, command: &Cat, args: &RocflArgs) -> Result<()> {
    CatCmd::new(repo, command, args).execute()
}

struct CatCmd<'a> {
    repo: &'a OcflRepo,
    command: &'a Cat,
    _args: &'a RocflArgs,
}

impl<'a> CatCmd<'a> {
    fn new(repo: &'a OcflRepo, command: &'a Cat, _args: &'a RocflArgs) -> Self {
        Self {
            repo,
            command,
            _args,
        }
    }

    fn execute(&self) -> Result<()> {
        self.repo.get_object_file(&self.command.object_id,
                                  &self.command.path,
                                  self.command.version.as_ref(),
                                  &mut io::stdout())
    }
}
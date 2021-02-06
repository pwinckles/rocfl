use std::io;

use anyhow::Result;

use crate::cmd::{Cmd, GlobalArgs};
use crate::cmd::opts::Cat;
use crate::ocfl::OcflRepo;

impl Cmd for Cat {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        repo.get_object_file(&self.object_id,
                             &self.path,
                             self.version.as_ref(),
                             &mut io::stdout())
    }
}
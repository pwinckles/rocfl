use std::fmt::Display;
use std::io::{self, ErrorKind, Write};

use anyhow::Result;
use enum_dispatch::enum_dispatch;
use rusoto_core::Region;

use crate::cmd::opts::*;
use crate::ocfl::OcflRepo;

pub mod opts;
mod cat;
mod diff;
mod list;
mod style;
mod table;

const DATE_FORMAT: &str = "%Y-%m-%d %H:%M";

pub fn exec_command(args: &RocflArgs) -> Result<()> {
    let repo = create_repo(&args)?;
    args.command.exec(&repo, args)
}

/// Trait executing a CLI command
#[enum_dispatch]
trait Cmd {
    fn exec(&self, repo: &OcflRepo, args: &RocflArgs) -> Result<()>;
}

fn println(value: impl Display) -> Result<()> {
    if let Err(e) = writeln!(io::stdout(), "{}", value) {
        match e.kind() {
            ErrorKind::BrokenPipe => Ok(()),
            _ => Err(e.into()),
        }
    } else {
        Ok(())
    }
}

fn create_repo(args: &RocflArgs) -> Result<OcflRepo> {
    if args.bucket.is_none() {
        OcflRepo::new_fs_repo(args.root.clone())
    } else {
        let prefix = match args.root.as_str() {
            "." => None,
            prefix => Some(prefix)
        };

        let region = match args.endpoint.is_some() {
            true => {
                Region::Custom {
                    name: args.region.as_ref().unwrap().to_owned(),
                    endpoint: args.endpoint.as_ref().unwrap().to_owned(),
                }
            }
            false => args.region.as_ref().unwrap().parse()?
        };

        OcflRepo::new_s3_repo(
            region,
            args.bucket.as_ref().unwrap(),
            prefix)
    }
}
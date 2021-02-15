use std::fmt::Display;
use std::io::{self, ErrorKind, Write};

use enum_dispatch::enum_dispatch;
#[cfg(feature = "s3")]
use rusoto_core::Region;

use crate::cmd::init::init_repo;
use crate::cmd::opts::*;
use crate::ocfl::{OcflRepo, Result};
#[cfg(not(feature = "s3"))]
use crate::ocfl::RocflError;

pub mod opts;
mod cat;
mod diff;
mod init;
mod new;
mod list;
mod style;
mod table;

const DATE_FORMAT: &str = "%Y-%m-%d %H:%M";

/// Executes a `rocfl` command
pub fn exec_command(args: &RocflArgs) -> Result<()> {
    match &args.command {
        Command::Init(command) => {
            // init cmd needs to be handled differently because the repo does not exist yet
            init_repo(command, args)
        }
        _ => {
            let repo = create_repo(&args)?;
            args.command.exec(&repo, GlobalArgs::new(args.quiet, args.verbose, args.no_styles))
        }
    }
}

/// Trait executing a CLI command
#[enum_dispatch]
trait Cmd {
    fn exec(&self, repo: &OcflRepo, args: GlobalArgs) -> Result<()>;
}

struct GlobalArgs {
    _quiet: bool,
    _verbose: bool,
    no_styles: bool,
}

impl GlobalArgs {
    fn new(quiet: bool, verbose: bool, no_styles: bool) -> Self {
        Self {
            _quiet: quiet,
            _verbose: verbose,
            no_styles,
        }
    }
}

fn println(value: impl Display) -> Result<()> {
    if let Err(e) = writeln!(io::stdout(), "{}", value) {
        match e.kind() {
            // This happens if the app is killed while writing
            ErrorKind::BrokenPipe => Ok(()),
            _ => Err(e.into()),
        }
    } else {
        Ok(())
    }
}

fn create_repo(args: &RocflArgs) -> Result<OcflRepo> {
    match args.target_storage() {
        Storage::FileSystem => OcflRepo::new_fs_repo(args.root.clone()),
        Storage::S3 => {
            #[cfg(not(feature = "s3"))]
                return Err(RocflError::General("This binary was not compiled with S3 support."));

            #[cfg(feature = "s3")]
                create_s3_repo(args)
        }
    }
}

#[cfg(feature = "s3")]
fn create_s3_repo(args: &RocflArgs) -> Result<OcflRepo> {
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
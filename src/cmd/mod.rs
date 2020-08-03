use std::io::Write;

use anyhow::{Error, Result};
use rusoto_core::Region;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

use crate::cmd::diff::{diff_command, log_command, show_command};
use crate::cmd::list::list_command;
use crate::cmd::opts::*;
use crate::ocfl::OcflRepo;

pub mod opts;
mod table;
mod list;
mod diff;

const DATE_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

pub fn exec_command(args: &RocflArgs) -> Result<()> {
    let repo = create_repo(&args)?;
    match &args.command {
        Command::List(list) => list_command(&repo, &list, args),
        Command::Log(log) => log_command(&repo, &log),
        Command::Show(show) => show_command(&repo, &show),
        Command::Diff(diff) => diff_command(&repo, &diff),
    }
}

pub fn print_err(error: &Error, quiet: bool) {
    if !quiet {
        let mut stderr = StandardStream::stderr(ColorChoice::Auto);
        match stderr.set_color(ColorSpec::new().set_fg(Some(Color::Red))) {
            Ok(_) => {
                if writeln!(&mut stderr, "Error: {:#}", error).is_err() {
                    eprintln!("Error: {:#}", error)
                }
                let _ = stderr.reset();
            }
            Err(_) => eprintln!("Error: {:#}", error)
        }
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
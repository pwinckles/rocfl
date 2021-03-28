use std::fmt::Display;
use std::io::{self, ErrorKind, Write};
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use enum_dispatch::enum_dispatch;
use log::error;
#[cfg(feature = "s3")]
use rusoto_core::Region;

use crate::cmd::opts::*;
#[cfg(not(feature = "s3"))]
use crate::ocfl::RocflError;
use crate::ocfl::{LayoutExtensionName, OcflRepo, Result, StorageLayout};

mod cmds;
mod diff;
mod list;
pub mod opts;
mod style;
mod table;

const DATE_FORMAT: &str = "%Y-%m-%d %H:%M";

/// Executes a `rocfl` command
pub fn exec_command(args: &RocflArgs) -> Result<()> {
    // TODO add the ability to load config from XDG. Global defaults and named repo overrides

    match &args.command {
        Command::Init(command) => {
            // init cmd needs to be handled differently because the repo does not exist yet
            init_repo(command, args)
        }
        _ => {
            let repo = Arc::new(create_repo(&args)?);
            let terminate = Arc::new(AtomicBool::new(false));

            let repo_ref = repo.clone();
            let terminate_ref = terminate.clone();

            ctrlc::set_handler(move || {
                if terminate_ref.load(Ordering::Acquire) {
                    error!("Force quitting. If a write operation was in progress, it is possible the resource was left in an inconsistent state.");
                    process::exit(1);
                } else {
                    println!("Stopping rocfl. If in the middle of a write operation, please wait for it to gracefully complete.");
                    terminate_ref.store(true, Ordering::Release);
                    repo_ref.close();
                }
            })?;

            args.command.exec(
                &repo,
                GlobalArgs::new(args.quiet, args.verbose, args.no_styles),
                &terminate,
            )
        }
    }
}

/// Trait executing a CLI command
#[enum_dispatch]
trait Cmd {
    /// Execute the command
    fn exec(&self, repo: &OcflRepo, args: GlobalArgs, terminate: &AtomicBool) -> Result<()>;
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

fn print(value: impl Display) -> Result<()> {
    if let Err(e) = write!(io::stdout(), "{}", value) {
        match e.kind() {
            // This happens if the app is killed while writing
            ErrorKind::BrokenPipe => Ok(()),
            _ => Err(e.into()),
        }
    } else {
        io::stdout().flush()?;
        Ok(())
    }
}

pub fn init_repo(cmd: &InitCmd, args: &RocflArgs) -> Result<()> {
    match args.target_storage() {
        Storage::FileSystem => {
            let _ = OcflRepo::init_fs_repo(&args.root, create_layout(cmd.layout)?)?;
        }
        Storage::S3 => {
            #[cfg(not(feature = "s3"))]
            return Err(RocflError::General(
                "This binary was not compiled with S3 support.",
            ));

            #[cfg(feature = "s3")]
            let _ = init_s3_repo(args, create_layout(cmd.layout)?)?;
        }
    }

    if !args.quiet {
        println("Initialized OCFL repository")?;
    }

    Ok(())
}

fn create_repo(args: &RocflArgs) -> Result<OcflRepo> {
    match args.target_storage() {
        Storage::FileSystem => OcflRepo::fs_repo(args.root.clone()),
        Storage::S3 => {
            #[cfg(not(feature = "s3"))]
            return Err(RocflError::General(
                "This binary was not compiled with S3 support.",
            ));

            #[cfg(feature = "s3")]
            create_s3_repo(args)
        }
    }
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

#[cfg(feature = "s3")]
fn create_s3_repo(args: &RocflArgs) -> Result<OcflRepo> {
    let prefix = match args.root.as_str() {
        "." => None,
        prefix => Some(prefix),
    };

    let region = resolve_region(args)?;

    // TODO XDG
    OcflRepo::s3_repo(region, args.bucket.as_ref().unwrap(), prefix, "/var/tmp")
}

#[cfg(feature = "s3")]
fn init_s3_repo(args: &RocflArgs, layout: StorageLayout) -> Result<OcflRepo> {
    let prefix = match args.root.as_str() {
        "." => None,
        prefix => Some(prefix),
    };

    let region = resolve_region(args)?;

    OcflRepo::init_s3_repo(
        region,
        args.bucket.as_ref().unwrap(),
        prefix,
        // TODO XDG
        "/var/tmp",
        layout,
    )
}

#[cfg(feature = "s3")]
fn resolve_region(args: &RocflArgs) -> Result<Region> {
    Ok(match args.endpoint.is_some() {
        true => Region::Custom {
            name: args.region.as_ref().unwrap().to_owned(),
            endpoint: args.endpoint.as_ref().unwrap().to_owned(),
        },
        false => args.region.as_ref().unwrap().parse()?,
    })
}

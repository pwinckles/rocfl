use std::fmt::Display;
use std::io::{self, ErrorKind, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{fs, process};

use enum_dispatch::enum_dispatch;
use log::error;
#[cfg(feature = "s3")]
use rusoto_core::Region;

use crate::cmd::opts::*;
#[cfg(not(feature = "s3"))]
use crate::ocfl::RocflError;
use crate::ocfl::{LayoutExtensionName, OcflRepo, Result, RocflError, StorageLayout};

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
            let _ = OcflRepo::init_fs_repo(
                &args.root,
                create_layout(cmd.layout, cmd.config_file.as_deref())?,
            )?;
        }
        Storage::S3 => {
            #[cfg(not(feature = "s3"))]
            return Err(RocflError::General(
                "This binary was not compiled with S3 support.".to_string(),
            ));

            #[cfg(feature = "s3")]
            let _ = init_s3_repo(args, create_layout(cmd.layout, cmd.config_file.as_deref())?)?;
        }
    }

    if !args.quiet {
        println(format!(
            "Initialized OCFL repository with layout {}",
            cmd.layout
        ))?;
    }

    Ok(())
}

fn create_repo(args: &RocflArgs) -> Result<OcflRepo> {
    match args.target_storage() {
        Storage::FileSystem => OcflRepo::fs_repo(args.root.clone()),
        Storage::S3 => {
            #[cfg(not(feature = "s3"))]
            return Err(RocflError::General(
                "This binary was not compiled with S3 support.".to_string(),
            ));

            #[cfg(feature = "s3")]
            create_s3_repo(args)
        }
    }
}

fn create_layout(layout_name: Layout, config_file: Option<&Path>) -> Result<Option<StorageLayout>> {
    let config_bytes = match read_layout_config(config_file) {
        Ok(bytes) => bytes,
        Err(e) => {
            return Err(RocflError::IllegalArgument(format!(
                "Failed to read layout config file: {}",
                e
            )));
        }
    };

    let layout = match layout_name {
        Layout::None => None,
        Layout::FlatDirect => Some(StorageLayout::new(
            LayoutExtensionName::FlatDirectLayout,
            config_bytes.as_deref(),
        )?),
        Layout::HashedNTuple => Some(StorageLayout::new(
            LayoutExtensionName::HashedNTupleLayout,
            config_bytes.as_deref(),
        )?),
        Layout::HashedNTupleObjectId => Some(StorageLayout::new(
            LayoutExtensionName::HashedNTupleObjectIdLayout,
            config_bytes.as_deref(),
        )?),
    };

    Ok(layout)
}

fn read_layout_config(config_file: Option<&Path>) -> Result<Option<Vec<u8>>> {
    let mut bytes = Vec::new();

    if let Some(file) = config_file {
        let _ = fs::File::open(file)?.read_to_end(&mut bytes)?;
        return Ok(Some(bytes));
    }

    Ok(None)
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
fn init_s3_repo(args: &RocflArgs, layout: Option<StorageLayout>) -> Result<OcflRepo> {
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

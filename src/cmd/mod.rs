use std::borrow::Cow;
use std::fmt::Display;
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{fs, process};

use ansi_term::{ANSIGenericString, Style};
use enum_dispatch::enum_dispatch;
use log::{error, info};
#[cfg(feature = "s3")]
use rusoto_core::Region;

use crate::cmd::opts::*;
use crate::config::{self, Config};
use crate::ocfl::{
    LayoutExtensionName, OcflRepo, Result, RocflError, SpecVersion as OcflSpecVersion,
    StorageLayout,
};

mod cmds;
mod diff;
mod list;
pub mod opts;
mod style;
mod table;
mod validate;

const DATE_FORMAT: &str = "%Y-%m-%d %H:%M";

/// Executes a `rocfl` command
pub fn exec_command(args: &RocflArgs, config: Config) -> Result<()> {
    let config = resolve_config(args, config);
    let config = default_values(config)?;

    info!("Resolved configuration: {:?}", config);

    config.validate()?;

    match &args.command {
        Command::Init(command) => {
            // init cmd needs to be handled differently because the repo does not exist yet
            init_repo(command, args, &config)
        }
        Command::Config(_command) => edit_config()
            .map_err(|e| RocflError::General(format!("Failed to edit config file: {}", e))),
        _ => {
            let repo = Arc::new(create_repo(&config)?);
            let terminate = Arc::new(AtomicBool::new(false));

            let repo_ref = repo.clone();
            let terminate_ref = terminate.clone();

            ctrlc::set_handler(move || {
                if terminate_ref.load(Ordering::Acquire) {
                    error!("Force quitting. If a write operation was in progress, it is possible the resource was left in an inconsistent state.");
                    process::exit(1);
                } else {
                    println("Stopping rocfl. If in the middle of a write operation, please wait for it to gracefully complete.");
                    terminate_ref.store(true, Ordering::Release);
                    repo_ref.close();
                }
            })?;

            args.command.exec(
                &repo,
                GlobalArgs::new(args.quiet, args.verbose, args.no_styles),
                &config,
                &terminate,
            )
        }
    }
}

/// Trait executing a CLI command
#[enum_dispatch]
trait Cmd {
    /// Execute the command
    fn exec(
        &self,
        repo: &OcflRepo,
        args: GlobalArgs,
        config: &Config,
        terminate: &AtomicBool,
    ) -> Result<()>;
}

struct GlobalArgs {
    quiet: bool,
    _verbose: bool,
    no_styles: bool,
}

impl GlobalArgs {
    fn new(quiet: bool, verbose: bool, no_styles: bool) -> Self {
        Self {
            quiet,
            _verbose: verbose,
            no_styles,
        }
    }
}

fn println(value: impl Display) {
    let _ = writeln!(io::stdout(), "{}", value);
}

fn paint<'b, I, S: 'b + ToOwned + ?Sized>(
    no_styles: bool,
    style: Style,
    text: I,
) -> ANSIGenericString<'b, S>
where
    I: Into<Cow<'b, S>>,
    <S as ToOwned>::Owned: std::fmt::Debug,
{
    if no_styles {
        style::DEFAULT.paint(text)
    } else {
        style.paint(text)
    }
}

pub fn init_repo(cmd: &InitCmd, args: &RocflArgs, config: &Config) -> Result<()> {
    let spec_version = map_spec_version(cmd.spec_version);

    if is_s3(config) {
        #[cfg(not(feature = "s3"))]
        return Err(RocflError::General(
            "This binary was not compiled with S3 support.".to_string(),
        ));

        #[cfg(feature = "s3")]
        let _ = init_s3_repo(
            config,
            spec_version,
            create_layout(cmd.layout, cmd.config_file.as_deref())?,
        )?;
    } else {
        let _ = OcflRepo::init_fs_repo(
            config.root.as_ref().unwrap(),
            config.staging_root.as_ref().map(Path::new),
            spec_version,
            create_layout(cmd.layout, cmd.config_file.as_deref())?,
        )?;
    }

    if !args.quiet {
        println(format!(
            "Initialized OCFL {} repository with layout {}",
            cmd.spec_version, cmd.layout
        ));
    }

    Ok(())
}

fn create_repo(config: &Config) -> Result<OcflRepo> {
    if is_s3(config) {
        #[cfg(not(feature = "s3"))]
        return Err(RocflError::General(
            "This binary was not compiled with S3 support.".to_string(),
        ));

        #[cfg(feature = "s3")]
        create_s3_repo(config)
    } else {
        OcflRepo::fs_repo(
            config.root.as_ref().unwrap(),
            config.staging_root.as_ref().map(Path::new),
        )
    }
}

fn create_layout(layout_name: Layout, config_file: Option<&Path>) -> Result<Option<StorageLayout>> {
    let config_bytes = match read_layout_config(config_file) {
        Ok(bytes) => bytes,
        Err(e) => {
            return Err(RocflError::InvalidValue(format!(
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
        Layout::FlatOmitPrefix => Some(StorageLayout::new(
            LayoutExtensionName::FlatOmitPrefixLayout,
            config_bytes.as_deref(),
        )?),
        Layout::NTupleOmitPrefix => Some(StorageLayout::new(
            LayoutExtensionName::NTupleOmitPrefixLayout,
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
fn create_s3_repo(config: &Config) -> Result<OcflRepo> {
    let region = resolve_region(config)?;

    OcflRepo::s3_repo(
        region,
        config.bucket.as_ref().unwrap(),
        config.root.as_deref(),
        config.staging_root.as_ref().unwrap(),
        config.profile.as_deref(),
    )
}

#[cfg(feature = "s3")]
fn init_s3_repo(
    config: &Config,
    spec_version: OcflSpecVersion,
    layout: Option<StorageLayout>,
) -> Result<OcflRepo> {
    let region = resolve_region(config)?;

    OcflRepo::init_s3_repo(
        region,
        config.bucket.as_ref().unwrap(),
        config.root.as_deref(),
        config.profile.as_deref(),
        config.staging_root.as_ref().unwrap(),
        spec_version,
        layout,
    )
}

#[cfg(feature = "s3")]
fn resolve_region(config: &Config) -> Result<Region> {
    Ok(match config.endpoint.is_some() {
        true => Region::Custom {
            name: config.region.as_ref().unwrap().to_owned(),
            endpoint: config.endpoint.as_ref().unwrap().to_owned(),
        },
        false => config.region.as_ref().unwrap().parse()?,
    })
}

fn resolve_config(args: &RocflArgs, mut config: Config) -> Config {
    if args.root.is_some() {
        config.root = args.root.clone();
    }
    if args.staging_root.is_some() {
        config.staging_root = args.staging_root.clone();
    }
    if args.bucket.is_some() {
        config.bucket = args.bucket.clone();
    }
    if args.region.is_some() {
        config.region = args.region.clone();
    }
    if args.endpoint.is_some() {
        config.endpoint = args.endpoint.clone();
    }
    if args.profile.is_some() {
        config.profile = args.profile.clone()
    }

    if let Command::Commit(commit) = &args.command {
        if commit.user_name.is_some() {
            config.author_name = commit.user_name.clone();
        }
        if commit.user_address.is_some() {
            config.author_address = commit.user_address.clone();
        }
    }

    if let Command::Upgrade(commit) = &args.command {
        if commit.user_name.is_some() {
            config.author_name = commit.user_name.clone();
        }
        if commit.user_address.is_some() {
            config.author_address = commit.user_address.clone();
        }
    }

    config
}

fn default_values(mut config: Config) -> Result<Config> {
    if is_s3(&config) {
        if config.staging_root.is_none() {
            config.staging_root = Some(config::s3_staging_path(&config)?);
        }
    } else if config.root.is_none() {
        config.root = Some(".".to_string());
    }

    Ok(config)
}

fn is_s3(config: &Config) -> bool {
    config.bucket.is_some()
}

fn edit_config() -> Result<()> {
    match config::config_path() {
        Some(config_path) => {
            if !config_path.exists() {
                fs::create_dir_all(config_path.parent().unwrap())?;
                let mut file = fs::File::create(&config_path)?;
                write!(
                    file,
                    "{}",
                    include_str!("../../resources/main/files/config.toml")
                )?;
            }

            edit::edit_file(&config_path)?;
            Ok(())
        }
        None => Err(RocflError::General(
            "Failed to find rocfl config".to_string(),
        )),
    }
}

fn map_spec_version(spec_version: SpecVersion) -> OcflSpecVersion {
    match spec_version {
        SpecVersion::Ocfl1_0 => OcflSpecVersion::Ocfl1_0,
        SpecVersion::Ocfl1_1 => OcflSpecVersion::Ocfl1_1,
    }
}

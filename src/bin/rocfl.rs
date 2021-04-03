use std::process;

use log::{error, LevelFilter};
use rocfl::cmd::opts::*;
use rocfl::ocfl::RocflError;
use rocfl::{cmd, config};
use structopt::StructOpt;

fn main() {
    let args = RocflArgs::from_args();

    let log_level = if args.quiet {
        LevelFilter::Off
    } else if args.verbose {
        LevelFilter::Info
    } else {
        LevelFilter::Warn
    };

    env_logger::builder()
        .filter_level(log_level)
        .format_timestamp(None)
        .format_module_path(false)
        .init();

    let config = match config::load_config(&args.name) {
        Ok(config) => config,
        Err(e) => {
            let path = config::config_path()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| "Unknown".to_string());
            error!("Failed to load rocfl config at {}: {}", path, e);
            process::exit(1);
        }
    };

    if let Err(e) = cmd::exec_command(&args, config) {
        match e {
            RocflError::CopyMoveError(errors) => {
                errors.0.iter().for_each(|error| error!("{}", error))
            }
            _ => error!("{:#}", e),
        }
        process::exit(1);
    }
}

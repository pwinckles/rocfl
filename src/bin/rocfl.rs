use std::process;

use clap::Parser;
use log::{error, LevelFilter};
use rocfl::cmd::opts::*;
use rocfl::config::Config;
use rocfl::ocfl::RocflError;
use rocfl::{cmd, config};

fn main() {
    let mut args = RocflArgs::parse();

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
        .format_target(false)
        .init();

    let config = match config::load_config(&args.name) {
        Ok(config) => config,
        Err(e) => {
            let path = config::config_path()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| "Unknown".to_string());
            error!("Failed to load rocfl config at {}: {}", path, e);
            Config::new()
        }
    };

    // If the output is being piped then we should disable styling
    if atty::isnt(atty::Stream::Stdout) {
        args.no_styles = true;
    }

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

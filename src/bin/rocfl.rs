use std::process;

use log::{error, LevelFilter};
use rocfl::cmd;
use rocfl::cmd::opts::*;
use rocfl::ocfl::RocflError;
use structopt::StructOpt;

fn main() {
    let args = RocflArgs::from_args();

    let log_level = if args.quiet {
        LevelFilter::Off
    } else if args.verbose {
        LevelFilter::Info
    } else {
        LevelFilter::Error
    };

    env_logger::builder()
        .filter_level(log_level)
        .format_timestamp(None)
        .format_module_path(false)
        .init();

    if let Err(e) = cmd::exec_command(&args) {
        match e {
            RocflError::CopyMoveError(errors) => {
                errors.0.iter().for_each(|error| error!("{}", error))
            }
            _ => error!("{:#}", e),
        }
        process::exit(1);
    }
}

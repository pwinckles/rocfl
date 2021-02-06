use std::process;

use log::error;
use log::LevelFilter;
use structopt::StructOpt;

use rocfl::cmd;
use rocfl::cmd::opts::*;

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
        error!("{}", e);
        process::exit(1);
    }
}

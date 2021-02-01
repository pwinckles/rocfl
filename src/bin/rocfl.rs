use std::process;

use structopt::StructOpt;

use rocfl::cmd;
use rocfl::cmd::opts::*;

fn main() {
    // TODO adjust log level based on --verbose
    // TODO revisit old error logging
    env_logger::builder()
        .format_timestamp(None)
        .format_module_path(false)
        .init();

    let args = RocflArgs::from_args();
    if let Err(e) = cmd::exec_command(&args) {
        cmd::eprintln(&e, args.quiet);
        process::exit(1);
    }
}

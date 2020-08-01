use std::process;

use structopt::StructOpt;

use rocfl::cmd;
use rocfl::cmd::opts::*;

fn main() {
    let args = RocflArgs::from_args();
    if let Err(e) = cmd::exec_command(&args) {
        cmd::print_err(&e, args.quiet);
        process::exit(1);
    }
}

use structopt::StructOpt;
use structopt::clap::AppSettings::{ColorAuto, ColoredHelp};
use roc::ocfl::{OcflRepo, Inventory};
use roc::ocfl::fs::FsOcflRepo;
use anyhow::{Result, Context};
use std::error::Error;
use std::io::Write;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

#[derive(Debug, StructOpt)]
#[structopt(name = "roc", author = "Peter Winckles <pwinckles@pm.me>")]
#[structopt(setting(ColorAuto), setting(ColoredHelp))]
pub struct AppArgs {
    /// Species the path to the OCFL storage root. Default: current directory.
    #[structopt(short = "R", long, value_name = "PATH")]
    pub root: Option<String>,

    /// Suppresses error messages
    #[structopt(short, long)]
    pub quiet: bool,

    /// Subcommand to execute
    #[structopt(subcommand)]
    pub command: Command,
}

/// A CLI for OCFL repositories.
#[derive(Debug, StructOpt)]
pub enum Command {
    #[structopt(name = "ls", author = "Peter Winckles <pwinckles@pm.me>")]
    List(List),
}

/// Lists objects or files within objects.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp))]
pub struct List {
    /// Enables long output format
    #[structopt(short, long)]
    pub long: bool,

    /// Displays the physical path to the resource
    #[structopt(short, long)]
    pub physical: bool,

    /// Specifies the version of the object to use. Default: HEAD version.
    #[structopt(short, long, value_name = "NUM")]
    pub version: Option<u32>,

    /// ID of the object to list
    #[structopt(name = "OBJECT")]
    pub object_id: Option<String>,
}

fn main() {
    let args = AppArgs::from_args();
    let repo = FsOcflRepo::new(args.root.clone()
        .unwrap_or_else(|| String::from(".")));

    match exec_command(&repo, &args) {
        Err(e) => panic!(format!("Error: {:#}", e)),
        _ => ()
    }
}

fn exec_command(repo: &FsOcflRepo, args: &AppArgs) -> Result<()> {
    match &args.command {
        Command::List(list) => list_command(&repo, &list, &args)?
    }
    Ok(())
}

// TODO implement command execution as a trait?
fn list_command(repo: &FsOcflRepo, command: &List, args: &AppArgs) -> Result<()> {
    if let Some(_object_id) = &command.object_id {
        unimplemented!("not yet implemented");
    } else {
        for object in repo.list_objects()
            .with_context(|| "Failed to list objects")? {
            match object {
                Ok(inventory) => print_object(&inventory, command.long),
                Err(e) => print_err(e.into(), args.quiet)
            }
        }
        Ok(())
    }
}

fn print_object(object: &Inventory, long: bool) {
    match long {
        true => println!("{:<}\t{:>5}\t{:<19}\t{:>}\t{:<}",
                         "o",
                         object.head,
                         object.versions.get(&object.head)
                             // TODO allow time to be formatted as UTC or local?
                             .and_then(|v| Some(v.created.format("%Y-%m-%d %H:%M:%S").to_string()))
                             .unwrap_or_else(|| String::from("")),
                         "",
                         object.id),
        false => println!("{}", object.id)
    }
}

fn print_err(error: Box<dyn Error>, quiet: bool) {
    if !quiet {
        let mut stderr = StandardStream::stderr(ColorChoice::Auto);
        match stderr.set_color(ColorSpec::new().set_fg(Some(Color::Red))) {
            Ok(_) => {
                if let Err(_) = writeln!(&mut stderr, "Error: {:#}", error) {
                    eprintln!("Error: {:#}", error)
                }
                let _ = stderr.reset();
            },
            Err(_) => eprintln!("Error: {:#}", error)
        }
    }
}

use structopt::StructOpt;
use structopt::clap::AppSettings::{ColorAuto, ColoredHelp};
use roc::ocfl::{OcflRepo, OcflObject};
use roc::ocfl::fs::FsOcflRepo;
use anyhow::{Result, Context};
use std::error::Error;
use std::io::Write;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use serde::export::Formatter;
use core::fmt;

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
                Ok(object) => print_object(&object, command),
                Err(e) => print_err(e.into(), args.quiet)
            }
        }
        Ok(())
    }
}

fn print_object(object: &OcflObject, command: &List) {
    println!("{}", FormatListing {
        listing: &Listing::from(object),
        command
    })
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

struct Listing<'a> {
    entry_type: String,
    version: &'a String,
    created: String,
    size: String,
    id: &'a String,
    path: &'a String,
}

impl<'a> From<&'a OcflObject> for Listing<'a> {
    fn from(object: &'a OcflObject) -> Self {
        Self {
            entry_type: String::from("o"),
            version: &object.head,
            created: object.head_version().created.format("%Y-%m-%d %H:%M:%S").to_string(),
            size: String::from(""),
            id: &object.id,
            path: &object.root,
        }
    }
}

struct FormatListing<'a> {
    listing: &'a Listing<'a>,
    command: &'a List
}

impl<'a> fmt::Display for FormatListing<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // TODO figure out length for id
        // TODO allow time to be formatted as UTC or local?

        if self.command.long {
            write!(f, "{entry_type}\t{version:>5}\t{created:<19}\t{size:>}\t{id:<42}",
                   entry_type = self.listing.entry_type,
                   version = self.listing.version,
                   created = self.listing.created,
                   size = self.listing.size,
                   id = self.listing.id)?
        } else {
            write!(f, "{:<42}", self.listing.id)?
        }

        if self.command.physical {
            write!(f, "\t{}", self.listing.path)?
        }

        Ok(())
    }
}


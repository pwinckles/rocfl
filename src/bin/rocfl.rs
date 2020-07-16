use structopt::StructOpt;
use structopt::clap::AppSettings::{ColorAuto, ColoredHelp};
use clap::arg_enum;
use anyhow::{Result, Context};
use std::error::Error;
use std::io::Write;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use serde::export::Formatter;
use core::fmt;
use std::convert::TryFrom;
use rocfl::{OcflObjectVersion, FileDetails, VersionId, OcflRepo, FsOcflRepo};
use std::cmp::Ordering;
use chrono::{DateTime, Local};
use globset::Glob;

#[derive(Debug, StructOpt)]
#[structopt(name = "rocfl", author = "Peter Winckles <pwinckles@pm.me>")]
#[structopt(setting(ColorAuto), setting(ColoredHelp))]
struct AppArgs {
    /// Species the path to the OCFL storage root.
    #[structopt(short = "R", long, value_name = "PATH", default_value = ".")]
    root: String,

    /// Suppresses error messages
    #[structopt(short, long)]
    quiet: bool,

    /// Subcommand to execute
    #[structopt(subcommand)]
    command: Command,
}

/// A CLI for OCFL repositories.
#[derive(Debug, StructOpt)]
enum Command {
    #[structopt(name = "ls", author = "Peter Winckles <pwinckles@pm.me>")]
    List(List),
}

/// Lists objects or files within objects.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp))]
struct List {
    /// Enables long output format: version, updated, name
    #[structopt(short, long)]
    long: bool,

    /// Displays the physical path to the item
    #[structopt(short, long)]
    physical: bool,

    /// Displays the digest of the item
    #[structopt(short, long)]
    digest: bool,

    /// Specifies the version of the object to list
    #[structopt(short, long, value_name = "NUM")]
    version: Option<u32>,

    /// Specifies the field to sort on. Sort is not supported when listing objects.
    #[structopt(short, long, value_name = "FIELD", possible_values = &Field::variants(), default_value = "name", case_insensitive = true)]
    sort: Field,

    /// Reverses the direction of the sort
    #[structopt(short, long)]
    reverse: bool,

    /// List only objects; not their contents
    #[structopt(short, long)]
    objects: bool,

    /// ID of the object to list. May be a glob when used with '-o'.
    #[structopt(name = "OBJECT")]
    object_id: Option<String>,

    /// Path glob of files to list. May only be specified if an object is also specified.
    #[structopt(name = "PATH")]
    path: Option<String>,
}

arg_enum! {
    #[derive(Debug)]
    enum Field {
        Name,
        Version,
        Updated,
        None
    }
}

impl Field {
    fn cmp_listings(&self, a: &Listing, b: &Listing) -> Ordering {
        match self {
            Self::Name => a.name.cmp(b.name),
            Self::Version => a.version.cmp(b.version),
            Self::Updated => a.updated.cmp(b.updated),
            Self::None => Ordering::Equal,
        }
    }
}

fn main() {
    let args = AppArgs::from_args();
    match exec_command(&args) {
        Err(e) => print_err(e.into(), args.quiet),
        _ => ()
    }
}

fn exec_command(args: &AppArgs) -> Result<()> {
    let repo = FsOcflRepo::new(args.root.clone())?;
    match &args.command {
        Command::List(list) => list_command(&repo, &list, &args)?
    }
    Ok(())
}

// TODO move to cmds module?
fn list_command(repo: &FsOcflRepo, command: &List, args: &AppArgs) -> Result<()> {
    if command.objects || command.object_id.is_none() {
        list_objects(repo, command, args)?;
    } else {
        list_object_contents(repo, command)?;
    }

    Ok(())
}

fn list_object_contents(repo: &FsOcflRepo, command: &List) -> Result<()> {
    let object_id = command.object_id.as_ref().unwrap();
    let version = parse_version(command.version)?;

    match repo.get_object(object_id, version.clone())
        .with_context(|| "Failed to list object")? {
        Some(object) => print_object_contents(&object, command)?,
        None => {
            match version {
                Some(version) => println!("Object {} version {} was not found", object_id, version),
                None => println!("Object {} was not found", object_id),
            }
        },
    }

    Ok(())
}

fn list_objects(repo: &FsOcflRepo, command: &List, args: &AppArgs) -> Result<()> {
    for object in repo.list_objects(command.object_id.as_deref())
        .with_context(|| "Failed to list objects")? {
        match object {
            Ok(object) => print_object(&object, command),
            Err(e) => print_err(e.into(), args.quiet)
        }
    }

    Ok(())
}

fn print_object(object: &OcflObjectVersion, command: &List) {
    println!("{}", FormatListing {
        listing: &Listing::from(object),
        command
    })
}

fn print_object_contents(object: &OcflObjectVersion, command: &List) -> Result<()> {
    let mut glob = None;
    if command.path.is_some() {
        glob = Some(Glob::new(command.path.as_ref().unwrap())?.compile_matcher());
    }

    let mut listings: Vec<Listing> = object.state.iter().map(|(path, details)| {
        Listing::new(path, details, &object.digest_algorithm)
    }).filter(|listing| {
        match &glob {
            Some(glob) => glob.is_match(&listing.name),
            None => true
        }
    }).collect();

    listings.sort_unstable_by(|a, b| {
        if command.reverse {
            command.sort.cmp_listings(b, a)
        } else {
            command.sort.cmp_listings(a, b)
        }
    });

    for listing in listings {
        println!("{}", FormatListing{
            listing: &listing,
            command
        })
    }

    Ok(())
}

fn print_err(error: Box<dyn Error>, quiet: bool) {
    // TODO figure out why anyhow alt formatting isn't working
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

fn parse_version(version_num: Option<u32>) -> Result<Option<VersionId>> {
    match version_num {
        Some(version_num) => Ok(Some(VersionId::try_from(version_num)?)),
        None => Ok(None)
    }
}

struct Listing<'a> {
    version: &'a VersionId,
    updated: &'a DateTime<Local>,
    name: &'a String,
    storage_path: &'a String,
    digest_algorithm: Option<&'a String>,
    digest: Option<&'a String>,
}

impl<'a> Listing<'a> {

    fn new(path: &'a String, details: &'a FileDetails, digest_algorithm: &'a String) -> Self {
        Self {
            version: &details.last_update.version,
            updated: &details.last_update.created,
            name: path,
            storage_path: &details.storage_path,
            digest_algorithm: Some(digest_algorithm),
            digest: Some(&details.digest),
        }
    }

    fn updated_str(&self) -> String {
        self.updated.format("%Y-%m-%d %H:%M:%S").to_string()
    }

}

impl<'a> From<&'a OcflObjectVersion> for Listing<'a> {
    fn from(object: &'a OcflObjectVersion) -> Self {
        Self {
            version: &object.version,
            updated: &object.created,
            name: &object.id,
            storage_path: &object.root,
            digest_algorithm: None,
            digest: None,
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
            write!(f, "{version:>5}\t{updated:<19}\t{name:<42}",
                   version = self.listing.version.version_str,  // For some reason the formatting is not applied to the output of VersionId::fmt()
                   updated = self.listing.updated_str(),
                   name = self.listing.name)?
        } else {
            write!(f, "{:<42}", self.listing.name)?
        }

        if self.command.physical {
            write!(f, "\t{}", self.listing.storage_path)?
        }

        if self.command.digest && self.listing.digest.is_some() {
            write!(f, "\t{}:{}", self.listing.digest_algorithm.unwrap(), self.listing.digest.unwrap())?
        }

        Ok(())
    }
}

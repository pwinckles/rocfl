use structopt::StructOpt;
use structopt::clap::AppSettings::{ColorAuto, ColoredHelp, DisableVersion};
use clap::arg_enum;
use lazy_static::lazy_static;
use anyhow::{Result, Context, Error};
use std::io::Write;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use serde::export::Formatter;
use core::fmt;
use rocfl::{ObjectVersion, FileDetails, VersionId, OcflRepo, FsOcflRepo, VersionDetails, ObjectVersionDetails, Diff as VersionDiff, DiffType};
use std::cmp::Ordering;
use globset::{GlobBuilder};
use std::fmt::Display;
use std::str::FromStr;
use std::num::ParseIntError;
use std::process::exit;
use std::rc::Rc;

#[derive(Debug, StructOpt)]
#[structopt(name = "rocfl", author = "Peter Winckles <pwinckles@pm.me>")]
#[structopt(setting(ColorAuto), setting(ColoredHelp))]
struct AppArgs {
    /// Specifies the path to the OCFL storage root.
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
    #[structopt(name = "ls")]
    List(List),
    #[structopt(name = "log")]
    Log(Log),
    #[structopt(name = "show")]
    Show(Show),
    #[structopt(name = "diff")]
    Diff(Diff),
}

/// Lists objects or files within objects.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
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
    #[structopt(short, long, value_name = "VERSION")]
    version: Option<VersionId>,

    /// Specifies the field to sort on. Sort is not supported when listing objects.
    #[structopt(short, long, value_name = "FIELD", possible_values = &Field::variants(), default_value = "name", case_insensitive = true)]
    sort: Field,

    /// Reverses the direction of the sort
    #[structopt(short, long)]
    reverse: bool,

    /// Lists only objects; not their contents
    #[structopt(short, long)]
    objects: bool,

    /// Wildcards in path glob expressions will not match separators
    #[structopt(short, long)]
    glob_literal_separator: bool,

    /// ID of the object to list. May be a glob when used with '-o'.
    #[structopt(name = "OBJECT")]
    object_id: Option<String>,

    /// Path glob of files to list. May only be specified if an object is also specified.
    #[structopt(name = "PATH")]
    path: Option<String>,
}

/// Displays the version history of an object or file.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
struct Log {
    /// Enables compact format
    #[structopt(short, long)]
    compact: bool,

    /// Reverses the direction the versions are displayed
    #[structopt(short, long)]
    reverse: bool,

    /// Limits the number of versions that are displayed
    #[structopt(short, long, value_name = "NUM", default_value)]
    num: Num,

    /// ID of the object
    #[structopt(name = "OBJECT")]
    object_id: String,

    /// Optional path to a file
    #[structopt(name = "PATH")]
    path: Option<String>,
}

/// Shows a summary of changes in a version.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
struct Show {
    /// Suppresses the version details output
    #[structopt(short, long)]
    minimal: bool,

    /// ID of the object
    #[structopt(name = "OBJECT")]
    object_id: String,

    /// Optional version to show
    #[structopt(name = "VERSION")]
    version: Option<VersionId>,
}

/// Shows the files that changed between two versions
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
struct Diff {
    /// ID of the object
    #[structopt(name = "OBJECT")]
    object_id: String,

    /// Left-hand side version
    #[structopt(name = "LEFT_VERSION")]
    left: VersionId,

    /// Right-hand side version
    #[structopt(name = "RIGHT_VERSION")]
    right: VersionId,
}

#[derive(Debug)]
struct Num(u32);

impl Default for Num {
    fn default() -> Self {
        Self {
            0: u32::MAX
        }
    }
}

impl FromStr for Num {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Num(u32::from_str(s)?))
    }
}

impl Display for Num {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;
        Ok(())
    }
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
            Self::Name => a.name.cmp(&b.name),
            Self::Version => a.version_details.version.cmp(&b.version_details.version),
            Self::Updated => a.version_details.created.cmp(&b.version_details.created),
            Self::None => Ordering::Equal,
        }
    }
}

const DATE_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

lazy_static! {
    static ref DEFAULT_USER: String = "NA".to_string();
}

fn main() {
    let args = AppArgs::from_args();
    match exec_command(&args) {
        Err(e) => {
            print_err(&e, args.quiet);
            exit(1);
        },
        _ => ()
    }
}

fn exec_command(args: &AppArgs) -> Result<()> {
    let repo = FsOcflRepo::new(args.root.clone())?;
    match &args.command {
        Command::List(list) => list_command(&repo, &list, args),
        Command::Log(log) => log_command(&repo, &log),
        Command::Show(show) => show_command(&repo, &show),
        Command::Diff(diff) => diff_command(&repo, &diff),
    }
}

fn list_command(repo: &FsOcflRepo, command: &List, args: &AppArgs) -> Result<()> {
    if command.objects || command.object_id.is_none() {
        list_objects(repo, command, args)
    } else {
        list_object_contents(repo, command)
    }
}

fn log_command(repo: &FsOcflRepo, command: &Log) -> Result<()> {
    let versions = match &command.path {
        Some(path) => repo.list_file_versions(&command.object_id, path)?,
        None => repo.list_object_versions(&command.object_id)?,
    };

    let iter: Box<dyn Iterator<Item=&VersionDetails>> = match command.reverse {
        true => Box::new(versions.iter().rev()),
        false => Box::new(versions.iter())
    };

    let mut count = 0;

    for version in iter {
        if count == command.num.0 {
            break;
        }
        println!("{}", FormatVersion::new(version, command.compact));
        count += 1;
    }

    Ok(())
}

fn show_command(repo: &FsOcflRepo, command: &Show) -> Result<()> {
    let object = repo.get_object_details(&command.object_id, command.version.as_ref())?;

    if !command.minimal {
        println!("{}", FormatVersion::new(&object.version_details, false));
    }

    diff_and_print(repo, &command.object_id, &object.version_details.version, None)
}

fn diff_command(repo: &FsOcflRepo, command: &Diff) -> Result<()> {
    if command.left == command.right {
        return Ok(());
    }

    diff_and_print(repo, &command.object_id, &command.left, Some(&command.right))
}

fn diff_and_print(repo: &FsOcflRepo, object_id: &str, left: &VersionId, right: Option<&VersionId>) -> Result<()> {
    let mut diffs: Vec<DiffLine> = repo.diff(object_id, left, right)?.into_iter().map(|diff| DiffLine(diff)).collect();
    diffs.sort_unstable();

    for diff in diffs {
        println!("{}", diff);
    }

    Ok(())
}

fn list_object_contents(repo: &FsOcflRepo, command: &List) -> Result<()> {
    let object_id = command.object_id.as_ref().unwrap();
    let object = repo.get_object(object_id, command.version.as_ref())
        .with_context(|| "Failed to list object")?;
    print_object_contents(object, command)
}

fn list_objects(repo: &FsOcflRepo, command: &List, args: &AppArgs) -> Result<()> {
    let iter = repo.list_objects(command.object_id.as_deref())
        .with_context(|| "Failed to list objects")?;

    match command.sort {
        Field::None => {
            for object in iter {
                match object {
                    Ok(object) => println!("{}", FormatListing::new(&Listing::from(object), command)),
                    Err(e) => print_err(&e, args.quiet)
                }
            }
        },
        _ => {
            let listings: Vec<Listing> = iter.filter(|object| {
                match object {
                    Ok(_object) => true,
                    Err(e) => {
                        print_err(e, args.quiet);
                        false
                    }
                }
            }).map(|object| {
                Listing::from(object.unwrap())
            }).collect();

            sort_and_print(listings, command);
        }
    }

    Ok(())
}

fn print_object_contents(object: ObjectVersion, command: &List) -> Result<()> {
    let mut glob = None;
    if command.path.is_some() {
        glob = Some(GlobBuilder::new(command.path.as_ref().unwrap())
            .literal_separator(command.glob_literal_separator)
            .backslash_escape(true).build()?.compile_matcher());
    }

    let digest_algorithm = Rc::new(object.digest_algorithm.clone());
    let listings: Vec<Listing> = object.state.into_iter().map(move |(path, details)| {
        Listing::new(path, details, Rc::clone(&digest_algorithm))
    }).filter(|listing| {
        match &glob {
            Some(glob) => glob.is_match(&listing.name),
            None => true
        }
    }).collect();

    sort_and_print(listings, command);

    Ok(())
}

fn sort_and_print(mut listings: Vec<Listing>, command: &List) {
    listings.sort_unstable_by(|a, b| {
        if command.reverse {
            command.sort.cmp_listings(b, a)
        } else {
            command.sort.cmp_listings(a, b)
        }
    });

    for listing in listings {
        println!("{}", FormatListing::new(&listing, command));
    }
}

fn print_err(error: &Error, quiet: bool) {
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

#[derive(Debug)]
struct Listing {
    version_details: Rc<VersionDetails>,
    name: String,
    storage_path: String,
    digest_algorithm: Option<Rc<String>>,
    digest: Option<Rc<String>>,
}

impl Listing {
    fn new(path: String, details: FileDetails, digest_algorithm: Rc<String>) -> Self {
        Self {
            version_details: Rc::clone(&details.last_update),
            name: path,
            storage_path: details.storage_path,
            digest_algorithm: Some(digest_algorithm),
            digest: Some(Rc::clone(&details.digest)),
        }
    }

    fn updated_str(&self) -> String {
        self.version_details.created.format(DATE_FORMAT).to_string()
    }
}

impl From<ObjectVersionDetails> for Listing {
    fn from(object: ObjectVersionDetails) -> Self {
        Self {
            version_details: Rc::new(object.version_details),
            name: object.id,
            storage_path: object.object_root,
            digest_algorithm: None,
            digest: None,
        }
    }
}

#[derive(Debug)]
struct FormatListing<'a> {
    listing: &'a Listing,
    command: &'a List,
}

impl<'a> FormatListing<'a> {
    fn new(listing: &'a Listing, command: &'a List) -> Self {
        Self {
            listing,
            command,
        }
    }
}

impl<'a> fmt::Display for FormatListing<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // TODO figure out length for id
        // TODO allow time to be formatted as UTC or local?

        if self.command.long {
            write!(f, "{version:>5}\t{updated:<19}\t{name:<42}",
                   // For some reason the formatting is not applied to the output of VersionId::fmt()
                   version = self.listing.version_details.version.to_string(),
                   updated = self.listing.updated_str(),
                   name = self.listing.name)?;
        } else {
            write!(f, "{:<42}", self.listing.name)?;
        }

        if self.command.physical {
            write!(f, "\t{}", self.listing.storage_path)?;
        }

        if self.command.digest && self.listing.digest.is_some() {
            write!(f, "\t{}:{}", self.listing.digest_algorithm.as_ref().unwrap(),
                   self.listing.digest.as_ref().unwrap())?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct FormatVersion<'a> {
    version: &'a VersionDetails,
    compact: bool,
}

impl<'a> FormatVersion<'a> {
    fn new(version: &'a VersionDetails, compact: bool) -> Self {
        Self {
            version,
            compact,
        }
    }
}

impl<'a> fmt::Display for FormatVersion<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.compact {
            write!(f, "{version:>5}\t{name}\t<{address}>\t{date:19}\t{message}",
                   version = self.version.version.to_string(),
                   name = self.version.user_name.as_ref().unwrap_or(&(*DEFAULT_USER)),
                   address = self.version.user_address.as_ref().unwrap_or(&(*DEFAULT_USER)),
                   date = self.version.created.format(DATE_FORMAT),
                   message = self.version.message.as_ref().unwrap_or(&"".to_string()))?;
        } else {
            write!(f, "{:width$} {}\n{:width$} {} <{}>\n{:width$} {}\n{:width$} {}\n",
                   "Version:", self.version.version.to_string(),
                   "Author:",
                   self.version.user_name.as_ref().unwrap_or(&(*DEFAULT_USER)),
                   self.version.user_address.as_ref().unwrap_or(&(*DEFAULT_USER)),
                   "Date:", self.version.created.to_rfc2822(),
                   "Message:", self.version.message.as_ref().unwrap_or(&"".to_string()),
                   width = 8)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct DiffLine(VersionDiff);

impl fmt::Display for DiffLine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.0.diff_type {
            DiffType::Added => write!(f, "A\t{}", self.0.path),
            DiffType::Modified => write!(f, "M\t{}", self.0.path),
            DiffType::Deleted => write!(f, "D\t{}", self.0.path),
        }
    }
}

impl PartialEq for DiffLine {
    fn eq(&self, other: &Self) -> bool {
        self.0.path == other.0.path
    }
}

impl Eq for DiffLine {}

impl PartialOrd for DiffLine {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DiffLine {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.path.cmp(&other.0.path)
    }
}

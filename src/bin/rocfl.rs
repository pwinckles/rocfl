use structopt::StructOpt;
use structopt::clap::AppSettings::{ColorAuto, ColoredHelp, DisableVersion};
use clap::arg_enum;
use lazy_static::lazy_static;
use anyhow::{Result, Context, Error};
use std::io::Write;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use serde::export::Formatter;
use core::fmt;
use rocfl::{ObjectVersion, FileDetails, VersionId, OcflRepo, FsOcflRepo, VersionDetails};
use std::cmp::Ordering;
use chrono::{DateTime, Local};
use globset::Glob;
use std::fmt::Display;
use std::str::FromStr;
use std::num::ParseIntError;
use std::process::exit;
use std::ops::Deref;

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

    // TODO flag for listing unique logical paths across all versions?

    /// Specifies the version of the object to list
    #[structopt(short, long, value_name = "VERSION")]
    version: Option<VersionId>,

    // TODO implement sort for object listing?
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

    // TODO flag to disable * from matching / in globs?
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
            Self::Name => a.name.cmp(b.name),
            Self::Version => a.version.cmp(b.version),
            Self::Updated => a.updated.cmp(b.updated),
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
            print_err(e.into(), args.quiet);
            exit(1);
        },
        _ => ()
    }
}

fn exec_command(args: &AppArgs) -> Result<()> {
    let repo = FsOcflRepo::new(args.root.clone())?;
    match &args.command {
        Command::List(list) => list_command(&repo, &list, args)?,
        Command::Log(log) => log_command(&repo, &log)?,
        Command::Show(show) => show_command(&repo, &show)?,
        Command::Diff(diff) => diff_command(&repo, &diff)?,
    }
    Ok(())
}

fn list_command(repo: &FsOcflRepo, command: &List, args: &AppArgs) -> Result<()> {
    if command.objects || command.object_id.is_none() {
        list_objects(repo, command, args)?;
    } else {
        list_object_contents(repo, command)?;
    }

    Ok(())
}

fn log_command(repo: &FsOcflRepo, command: &Log) -> Result<()> {
    let versions = match &command.path {
        Some(path) => repo.list_file_versions(&command.object_id, path)?,
        None => repo.list_object_versions(&command.object_id)?,
    };

    let mut count = 0;
    // TODO find a way to do this with less duplication
    if command.reverse {
        for version in versions.iter().rev() {
            if count == command.num.0 {
                break;
            } else {
                println!("{}", FormatVersion::new(version, command.compact));
                count += 1;
            }
        }
    } else {
        for version in versions.iter() {
            if count == command.num.0 {
                break;
            } else {
                println!("{}", FormatVersion::new(version, command.compact));
                count += 1;
            }
        }
    }

    Ok(())
}

fn show_command(repo: &FsOcflRepo, command: &Show) -> Result<()> {
    // TODO this call is computing the latest update of all files, which is unnecessary
    let target = repo.get_object(&command.object_id, command.version.clone())?;

    if !command.minimal {
        println!("{}", FormatVersion::new(&target.version_details, false));
    }

    let mut diffs = Vec::new();

    if target.version_details.version.version_num > 1 {
        let previous_id = target.version_details.version.previous().unwrap();
        let previous = repo.get_object(&command.object_id, Some(previous_id))?;
        diffs = diff(previous, target);
    } else {
        for (path, _details) in target.state {
            diffs.push(DiffLine::added(path))
        }
    }

    diffs.sort_unstable();

    for diff in diffs {
        println!("{}", diff);
    }

    Ok(())
}

fn diff_command(repo: &FsOcflRepo, command: &Diff) -> Result<()> {
    if command.left == command.right {
        return Ok(());
    }

    // TODO this call is computing the latest update of all files, which is unnecessary
    let left = repo.get_object(&command.object_id, Some(command.left.clone()))?;
    let right = repo.get_object(&command.object_id, Some(command.right.clone()))?;

    let diffs = diff(left, right);

    for diff in diffs {
        println!("{}", diff);
    }

    Ok(())
}

fn diff(mut left: ObjectVersion, right: ObjectVersion) -> Vec<DiffLine> {
    let mut diffs = Vec::new();

    for (path, details) in right.state {
        match left.state.remove(&path) {
            None => diffs.push(DiffLine::added(path)),
            Some(entry) => {
                if entry.digest.deref().ne(details.digest.deref()) {
                    diffs.push(DiffLine::modified(path))
                }
            }
        }
    }

    for (path, _details) in left.state {
        diffs.push(DiffLine::deleted(path))
    }

    diffs.sort_unstable();

    diffs
}

fn list_object_contents(repo: &FsOcflRepo, command: &List) -> Result<()> {
    let object_id = command.object_id.as_ref().unwrap();
    let object = repo.get_object(object_id, command.version.clone())
        .with_context(|| "Failed to list object")?;
    print_object_contents(&object, command)?;

    Ok(())
}

fn list_objects(repo: &FsOcflRepo, command: &List, args: &AppArgs) -> Result<()> {
    // TODO this call is computing the latest update of all files, which is unnecessary
    for object in repo.list_objects(command.object_id.as_deref())
        .with_context(|| "Failed to list objects")? {
        match object {
            Ok(object) => println!("{}", FormatListing::new(&Listing::from(&object), command)),
            Err(e) => print_err(e, args.quiet)
        }
    }

    Ok(())
}

fn print_object_contents(object: &ObjectVersion, command: &List) -> Result<()> {
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
        println!("{}", FormatListing::new(&listing, command));
    }

    Ok(())
}

fn print_err(error: Error, quiet: bool) {
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
        self.updated.format(DATE_FORMAT).to_string()
    }

}

impl<'a> From<&'a ObjectVersion> for Listing<'a> {
    fn from(object: &'a ObjectVersion) -> Self {
        Self {
            version: &object.version_details.version,
            updated: &object.version_details.created,
            name: &object.id,
            storage_path: &object.object_root,
            digest_algorithm: None,
            digest: None,
        }
    }
}

#[derive(Debug)]
struct FormatListing<'a> {
    listing: &'a Listing<'a>,
    command: &'a List,
}

impl<'a> FormatListing<'a> {
    fn new(listing: &'a Listing<'a>, command: &'a List) -> Self {
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
                   version = self.listing.version.to_string(),
                   updated = self.listing.updated_str(),
                   name = self.listing.name)?;
        } else {
            write!(f, "{:<42}", self.listing.name)?;
        }

        if self.command.physical {
            write!(f, "\t{}", self.listing.storage_path)?;
        }

        if self.command.digest && self.listing.digest.is_some() {
            write!(f, "\t{}:{}", self.listing.digest_algorithm.unwrap(), self.listing.digest.unwrap())?;
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
struct DiffLine {
    diff_type: DiffType,
    path: String,
}

#[derive(Debug)]
enum DiffType {
    Added,
    Modified,
    Deleted,
}

impl DiffLine {
    fn added(path: String) -> Self {
        Self {
            diff_type: DiffType::Added,
            path
        }
    }
    fn modified(path: String) -> Self {
        Self {
            diff_type: DiffType::Modified,
            path
        }
    }
    fn deleted(path: String) -> Self {
        Self {
            diff_type: DiffType::Deleted,
            path
        }
    }
}

impl fmt::Display for DiffLine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.diff_type {
            DiffType::Added => write!(f, "A\t{}", self.path)?,
            DiffType::Modified => write!(f, "M\t{}", self.path)?,
            DiffType::Deleted => write!(f, "D\t{}", self.path)?,
        }

        Ok(())
    }
}

impl PartialEq for DiffLine {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
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
        self.path.cmp(&other.path)
    }
}

use core::fmt;
use std::cmp::Ordering;
use std::fmt::Display;
use std::io;
use std::io::Write;
use std::num::ParseIntError;
use std::process::exit;
use std::rc::Rc;
use std::str::FromStr;

use anyhow::{Context, Error, Result};
use awsregion::Region;
use clap::arg_enum;
use globset::GlobBuilder;
use lazy_static::lazy_static;
use serde::export::Formatter;
use structopt::clap::AppSettings::{ColorAuto, ColoredHelp, DisableVersion};
use structopt::StructOpt;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

use rocfl::{Diff as VersionDiff, DiffType, FileDetails, ObjectVersion, ObjectVersionDetails, OcflRepo, VersionDetails, VersionNum};

#[derive(Debug, StructOpt)]
#[structopt(name = "rocfl", author = "Peter Winckles <pwinckles@pm.me>")]
#[structopt(setting(ColorAuto), setting(ColoredHelp))]
struct AppArgs {
    /// Specifies the path to the OCFL storage root.
    #[structopt(short, long, value_name = "PATH", default_value = ".")]
    root: String,

    /// Specifies the AWS region.
    #[structopt(short = "R", long, value_name = "region", requires = "bucket")]
    region: Option<String>,

    /// Specifies the S3 bucket to use.
    #[structopt(short, long, value_name = "BUCKET", requires = "region")]
    bucket: Option<String>,

    /// Specifies a custom S3 endpoint to use.
    #[structopt(long, value_name = "ENDPOINT")]
    endpoint: Option<String>,

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
    version: Option<VersionNum>,

    /// Specifies the field to sort on. Sort is not supported when listing objects.
    #[structopt(short, long, value_name = "FIELD", possible_values = &Field::variants(), default_value = "Name", case_insensitive = true)]
    sort: Field,

    /// Reverses the direction of the sort
    #[structopt(short, long)]
    reverse: bool,

    /// Lists only objects; not their contents
    #[structopt(short, long)]
    objects: bool,

    /// Wildcards in path glob expressions will not match '/'
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
    version: Option<VersionNum>,
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
    left: VersionNum,

    /// Right-hand side version
    #[structopt(name = "RIGHT_VERSION")]
    right: VersionNum,
}

#[derive(Debug)]
struct Num(u32);

arg_enum! {
    #[derive(Debug)]
    enum Field {
        Name,
        Version,
        Updated,
        None
    }
}

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
        write!(f, "{}", self.0)
    }
}

impl Field {
    fn cmp_listings(&self, a: &Listing, b: &Listing) -> Ordering {
        match self {
            Self::Name => a.name.cmp(&b.name),
            Self::Version => a.version_details.version_num.cmp(&b.version_details.version_num),
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
        }
        _ => ()
    }
}

fn exec_command(args: &AppArgs) -> Result<()> {
    let repo = create_repo(&args)?;
    match &args.command {
        Command::List(list) => list_command(&repo, &list, args),
        Command::Log(log) => log_command(&repo, &log),
        Command::Show(show) => show_command(&repo, &show),
        Command::Diff(diff) => diff_command(&repo, &diff),
    }
}

fn create_repo(args: &AppArgs) -> Result<OcflRepo> {
    if args.bucket.is_none() {
        OcflRepo::new_fs_repo(args.root.clone())
    } else {
        let prefix = match args.root.as_str() {
            "." => None,
            prefix => Some(prefix)
        };

        let region = match args.endpoint.is_some() {
            true => {
                Region::Custom {
                    region: args.region.as_ref().unwrap().to_owned(),
                    endpoint: args.endpoint.as_ref().unwrap().to_owned(),
                }
            }
            false => args.region.as_ref().unwrap().parse()?
        };

        OcflRepo::new_s3_repo(
            region,
            args.bucket.as_ref().unwrap(),
            prefix)
    }
}

fn list_command(repo: &OcflRepo, command: &List, args: &AppArgs) -> Result<()> {
    if command.objects || command.object_id.is_none() {
        list_objects(repo, command, args)
    } else {
        list_object_contents(repo, command)
    }
}

fn log_command(repo: &OcflRepo, command: &Log) -> Result<()> {
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
        println(FormatVersion::new(version, command.compact));
        count += 1;
    }

    Ok(())
}

fn show_command(repo: &OcflRepo, command: &Show) -> Result<()> {
    let object = repo.get_object_details(&command.object_id, command.version.as_ref())?;

    if !command.minimal {
        println(FormatVersion::new(&object.version_details, false));
    }

    diff_and_print(repo, &command.object_id, None, &object.version_details.version_num)
}

fn diff_command(repo: &OcflRepo, command: &Diff) -> Result<()> {
    if command.left == command.right {
        return Ok(());
    }

    diff_and_print(repo, &command.object_id, Some(&command.left), &command.right)
}

fn diff_and_print(repo: &OcflRepo, object_id: &str, left: Option<&VersionNum>, right: &VersionNum) -> Result<()> {
    let mut diffs: Vec<DiffLine> = repo.diff(object_id, left, right)?
        .into_iter().map(|diff| DiffLine(diff)).collect();

    diffs.sort_unstable();

    for diff in diffs {
        println(diff);
    }

    Ok(())
}

fn list_object_contents(repo: &OcflRepo, command: &List) -> Result<()> {
    let object_id = command.object_id.as_ref().unwrap();
    let object = repo.get_object(object_id, command.version.as_ref())
        .with_context(|| "Failed to list object")?;
    print_object_contents(object, command)
}

fn list_objects(repo: &OcflRepo, command: &List, args: &AppArgs) -> Result<()> {
    let iter = repo.list_objects(command.object_id.as_deref())
        .with_context(|| "Failed to list objects")?;

    match command.sort {
        Field::None => {
            for object in iter {
                match object {
                    Ok(object) => println(FormatListing::new(&Listing::from(object), command)),
                    Err(e) => print_err(&e, args.quiet)
                }
            }
        }
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

    let listings: Vec<Listing> = object.state.into_iter().map(move |(path, details)| {
        Listing::new(path, details)
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
        println(FormatListing::new(&listing, command));
    }
}

// https://github.com/rust-lang/rust/issues/46016
fn println(value: impl Display) {
    // Don't care about errors
    let _ = writeln!(io::stdout(), "{}", value);
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
            }
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

#[derive(Debug)]
struct FormatListing<'a> {
    listing: &'a Listing,
    command: &'a List,
}

#[derive(Debug)]
struct FormatVersion<'a> {
    version: &'a VersionDetails,
    compact: bool,
}

#[derive(Debug)]
struct DiffLine(VersionDiff);

impl Listing {
    fn new(path: String, details: FileDetails) -> Self {
        Self {
            version_details: details.last_update,
            name: path,
            storage_path: details.storage_path,
            digest_algorithm: Some(details.digest_algorithm),
            digest: Some(details.digest),
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
                   version = self.listing.version_details.version_num.to_string(),
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
            write!(f, "{version:>5}\t{name}\t{address}\t{date:19}\t{message}",
                   version = self.version.version_num.to_string(),
                   name = self.version.user_name.as_ref().unwrap_or(&(*DEFAULT_USER)),
                   address = self.version.user_address.as_ref().unwrap_or(&(*DEFAULT_USER)),
                   date = self.version.created.format(DATE_FORMAT),
                   message = self.version.message.as_ref().unwrap_or(&"".to_string()))?;
        } else {
            write!(f, "{:width$} {}\n{:width$} {} <{}>\n{:width$} {}\n{:width$} {}\n",
                   "Version:", self.version.version_num.to_string(),
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

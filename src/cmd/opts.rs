use std::fmt::{self, Display, Formatter};
use std::num::ParseIntError;
use std::str::FromStr;

use clap::arg_enum;
use structopt::clap::AppSettings::{ColorAuto, ColoredHelp, DisableVersion};
use structopt::StructOpt;

use crate::ocfl::VersionNum;

#[derive(Debug, StructOpt)]
#[structopt(name = "rocfl", author = "Peter Winckles <pwinckles@pm.me>")]
#[structopt(setting(ColorAuto), setting(ColoredHelp))]
pub struct RocflArgs {
    /// Specifies the path to the OCFL storage root.
    #[structopt(short, long, value_name = "PATH", default_value = ".")]
    pub root: String,

    /// Specifies the AWS region.
    #[structopt(short = "R", long, value_name = "region", requires = "bucket")]
    pub region: Option<String>,

    /// Specifies the S3 bucket to use.
    #[structopt(short, long, value_name = "BUCKET", requires = "region")]
    pub bucket: Option<String>,

    /// Specifies a custom S3 endpoint to use.
    #[structopt(short, long, value_name = "ENDPOINT")]
    pub endpoint: Option<String>,

    /// Suppresses error messages
    #[structopt(short, long)]
    pub quiet: bool,

    /// Increases log level
    #[structopt(short = "V", long)]
    pub verbose: bool,

    /// Disables all output styling
    #[structopt(short = "S", long)]
    pub no_styles: bool,

    /// Subcommand to execute
    #[structopt(subcommand)]
    pub command: Command,
}

/// A CLI for OCFL repositories.
#[derive(Debug, StructOpt)]
pub enum Command {
    #[structopt(name = "ls")]
    List(List),
    #[structopt(name = "log")]
    Log(Log),
    #[structopt(name = "show")]
    Show(Show),
    #[structopt(name = "diff")]
    Diff(Diff),
    #[structopt(name = "cat")]
    Cat(Cat),
}

/// Lists objects or files within objects.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct List {
    /// Enables long output format: Version, Updated, Name (Object ID or Logical Path)
    #[structopt(short, long)]
    pub long: bool,

    /// Displays the physical path to the item
    #[structopt(short, long)]
    pub physical: bool,

    /// Displays the digest of the item
    #[structopt(short, long)]
    pub digest: bool,

    /// Displays a header row
    #[structopt(short, long)]
    pub header: bool,

    /// Tab separate the output
    #[structopt(short, long)]
    pub tsv: bool,

    /// Specifies the version of the object to list
    #[structopt(short, long, value_name = "VERSION")]
    pub version: Option<VersionNum>,

    /// Specifies the field to sort on.
    #[structopt(short, long, value_name = "FIELD", possible_values = &Field::variants(), default_value = "Name", case_insensitive = true)]
    pub sort: Field,

    /// Reverses the direction of the sort
    #[structopt(short, long)]
    pub reverse: bool,

    /// Lists only objects; not their contents
    #[structopt(short, long)]
    pub objects: bool,

    /// Wildcards in path glob expressions will not match '/'
    #[structopt(short, long)]
    pub glob_literal_separator: bool,

    /// ID of the object to list. May be a glob when used with '-o'.
    #[structopt(name = "OBJECT")]
    pub object_id: Option<String>,

    /// Path glob of files to list. May only be specified if an object is also specified.
    #[structopt(name = "PATH")]
    pub path: Option<String>,
}

/// Displays the version history of an object or file.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct Log {
    /// Enables compact format
    #[structopt(short, long)]
    pub compact: bool,

    /// Displays a header row, only with compact format
    #[structopt(short, long)]
    pub header: bool,

    /// Tab separate the output, only with compact format
    #[structopt(short, long)]
    pub tsv: bool,

    /// Reverses the direction the versions are displayed
    #[structopt(short, long)]
    pub reverse: bool,

    /// Limits the number of versions that are displayed
    #[structopt(short, long, value_name = "NUM", default_value)]
    pub num: Num,

    /// ID of the object
    #[structopt(name = "OBJECT")]
    pub object_id: String,

    /// Optional path to a file
    #[structopt(name = "PATH")]
    pub path: Option<String>,
}

/// Shows a summary of changes in a version.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct Show {
    /// Suppresses the version details output
    #[structopt(short, long)]
    pub minimal: bool,

    /// ID of the object
    #[structopt(name = "OBJECT")]
    pub object_id: String,

    /// Optional version to show
    #[structopt(name = "VERSION")]
    pub version: Option<VersionNum>,
}

/// Shows the files that changed between two versions
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct Diff {
    /// ID of the object
    #[structopt(name = "OBJECT")]
    pub object_id: String,

    /// Left-hand side version
    #[structopt(name = "LEFT_VERSION")]
    pub left: VersionNum,

    /// Right-hand side version
    #[structopt(name = "RIGHT_VERSION")]
    pub right: VersionNum,
}

/// Cats the specified file
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct Cat {
    /// Specifies the version of the object to retrieve the file from
    #[structopt(short, long, value_name = "VERSION")]
    pub version: Option<VersionNum>,

    /// ID of the object
    #[structopt(name = "OBJECT")]
    pub object_id: String,

    /// Logical path of the file
    #[structopt(name = "PATH")]
    pub path: String,
}

#[derive(Debug)]
pub struct Num(pub usize);

arg_enum! {
    #[derive(Debug)]
    pub enum Field {
        Name,
        Version,
        Updated,
        Physical,
        Digest,
        None
    }
}

impl Default for Num {
    fn default() -> Self {
        Self {
            0: usize::MAX
        }
    }
}

impl FromStr for Num {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Num(usize::from_str(s)?))
    }
}

impl Display for Num {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
use std::fmt::{self, Display, Formatter};
use std::num::ParseIntError;
use std::str::FromStr;

use chrono::{DateTime, Local};
use clap::arg_enum;
use enum_dispatch::enum_dispatch;
use structopt::clap::AppSettings::{ColorAuto, ColoredHelp, DisableVersion};
use structopt::StructOpt;

use crate::ocfl::VersionNum;

// TODO review all of these docs so that the language agrees

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
#[enum_dispatch(Cmd)]
#[derive(Debug, StructOpt)]
pub enum Command {
    #[structopt(name = "ls")]
    List(ListCmd),
    #[structopt(name = "log")]
    Log(LogCmd),
    #[structopt(name = "show")]
    Show(ShowCmd),
    #[structopt(name = "diff")]
    Diff(DiffCmd),
    #[structopt(name = "cat")]
    Cat(CatCmd),
    #[structopt(name = "init")]
    Init(InitCmd),
    #[structopt(name = "new")]
    New(NewCmd),
    #[structopt(name = "cp")]
    Copy(CopyCmd),
    #[structopt(name = "mv")]
    Move(MoveCmd),
    #[structopt(name = "rm")]
    Remove(RemoveCmd),
    #[structopt(name = "reset")]
    Reset(ResetCmd),
    #[structopt(name = "commit")]
    Commit(CommitCmd),
    #[structopt(name = "status")]
    Status(StatusCmd),
    #[structopt(name = "purge")]
    Purge(PurgeCmd),
}

/// Lists objects or files within objects.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct ListCmd {
    /// Enables the interpretation of logical path parts as logical directories
    #[structopt(short = "D", long)]
    pub logical_dirs: bool,

    /// Enables long output format: Version, Updated, Name (Object ID or Logical Path)
    #[structopt(short, long)]
    pub long: bool,

    /// Displays the physical path to the item relative the storage root
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

    /// Lists staged objects or the contents of a specific staged object
    #[structopt(short = "S", long, conflicts_with = "version")]
    pub staged: bool,

    /// Specifies the version of the object to list
    #[structopt(short, long, value_name = "VERSION")]
    pub version: Option<VersionNum>,

    /// Specifies the field to sort on.
    #[structopt(short, long,
    value_name = "FIELD",
    possible_values = &Field::variants(),
    default_value = "Name",
    case_insensitive = true)]
    pub sort: Field,

    /// Reverses the direction of the sort
    #[structopt(short, long)]
    pub reverse: bool,

    /// Lists only objects; not their contents
    #[structopt(short, long)]
    pub objects: bool,

    /// ID of the object to list. May be a glob when used with '-o'.
    #[structopt(name = "OBJ_ID")]
    pub object_id: Option<String>,

    /// Path glob of files to list. May only be specified if an object is also specified.
    #[structopt(name = "PATH")]
    pub path: Option<String>,
}

/// Displays the version history of an object or file.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct LogCmd {
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
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,

    /// Optional path to a file
    #[structopt(name = "PATH")]
    pub path: Option<String>,
}

/// Shows a summary of changes in a version.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct ShowCmd {
    /// Shows the changes in the staged version of the object, if it exists
    #[structopt(short = "S", long, conflicts_with = "version")]
    pub staged: bool,

    /// Suppresses the version details output
    #[structopt(short, long)]
    pub minimal: bool,

    /// ID of the object
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,

    /// The version to show. The most recent version is shown by default
    #[structopt(name = "VERSION")]
    pub version: Option<VersionNum>,
}

/// Shows the files that changed between two versions
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct DiffCmd {
    /// ID of the object
    #[structopt(name = "OBJ_ID")]
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
pub struct CatCmd {
    /// Cats the contents of a staged file
    #[structopt(short = "S", long, conflicts_with = "version")]
    pub staged: bool,

    /// Specifies the version of the object to retrieve the file from
    #[structopt(short, long, value_name = "VERSION")]
    pub version: Option<VersionNum>,

    /// ID of the object
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,

    /// Logical path of the file
    #[structopt(name = "PATH")]
    pub path: String,
}

/// Creates a new OCFL repository.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct InitCmd {
    /// Specifies the OCFL storage layout extension to use
    #[structopt(short, long,
    value_name = "LAYOUT",
    possible_values = &Layout::variants(),
    default_value = "HashedNTuple",
    case_insensitive = true)]
    pub layout: Layout,
    // TODO add option for passing a layout file
    // TODO add the option for no layout
}

/// Stages a new OCFL object. The object does not exist until it is committed.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct NewCmd {
    /// Specifies the digest algorithm to use for the inventory digest
    #[structopt(short, long,
    value_name = "ALGORITHM",
    possible_values = &DigestAlgorithm::variants(),
    default_value = "Sha512",
    case_insensitive = true)]
    pub digest_algorithm: DigestAlgorithm,

    /// Specifies what to name the object's content directory
    #[structopt(short, long, value_name = "PATH", default_value = "content")]
    pub content_directory: String,

    /// Specifies the width for zero-padded versions, eg. v0001 has a width of 4
    #[structopt(short, long, value_name = "WIDTH", default_value = "0")]
    pub zero_padding: u32,

    /// ID of the object to create.
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,
}

/// Copies external files into an object or internal files to new locations. These changes are staged
/// and must be `committed` before they are reflected in a new OCFL object version.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct CopyCmd {
    /// Indicates that source directories should be copied recursively.
    #[structopt(short, long)]
    pub recursive: bool,

    /// Specifies that the source paths should be interpreted as logical paths internal to the
    /// object, and not as paths on the filesystem.
    #[structopt(short, long)]
    pub internal: bool,

    /// When '--internal' is used, this option specifies the version of the object the source
    /// paths are for. If not specified, the most recent version is used.
    #[structopt(short, long, value_name = "VERSION", requires = "internal")]
    pub version: Option<VersionNum>,

    /// The object ID of the object to copy files into
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,

    /// The files to copy. Glob patterns are supported.
    #[structopt(name = "SRC", required = true)]
    pub source: Vec<String>,

    /// The logical path to copy the source files to. Specify '/' to copy into the object's root.
    #[structopt(name = "DST", last = true)]
    pub destination: String,
}

/// Moves external files into an object or internal files to new locations. These changes are staged
/// and must be `committed` before they are reflected in a new OCFL object version.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct MoveCmd {
    /// Specifies that the source paths should be interpreted as logical paths internal to the
    /// object, and not as paths on the filesystem.
    #[structopt(short, long)]
    pub internal: bool,

    /// The object ID of the object to move files into
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,

    /// The files to move. Glob patterns are supported.
    #[structopt(name = "SRC", required = true)]
    pub source: Vec<String>,

    /// The logical path to move the source files to. Specify '/' to copy into the object's root.
    #[structopt(name = "DST", last = true)]
    pub destination: String,
}

/// Removes files from an object. The removed files still exist in previous versions, but are
/// no longer referenced in the current version. These changes are staged and must be `committed`
/// before they are reflected in a new OCFL object version.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct RemoveCmd {
    /// Indicates that logical directories should be removed recursively
    #[structopt(short, long)]
    pub recursive: bool,

    /// The ID of the object to remove files from
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,

    /// The logical paths of the files to remove. This may be a glob pattern
    #[structopt(name = "PATH", required = true)]
    pub paths: Vec<String>,
}

/// Commits all of an objects staged changes to a new OCFL version
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct CommitCmd {
    /// Indicates that the version's inventory.json file should be pretty printed
    #[structopt(short, long)]
    pub pretty_print: bool,

    /// The name of the user to attribute the changes to
    #[structopt(short = "n", long, value_name = "NAME")]
    pub user_name: Option<String>,

    /// The URI address of the user to attribute the changes to. For example, mailto:test@example.com
    #[structopt(short = "a", long, value_name = "ADDRESS", requires = "user-name")]
    pub user_address: Option<String>,

    /// A message describing the changes
    #[structopt(short, long, value_name = "MESSAGE")]
    pub message: Option<String>,

    /// The creation timestamp of the version. Timestamps must be formatted in accordance
    /// to RFC 3339, for example: 2020-12-23T10:11:12-06:00. Default: now
    #[structopt(short, long, value_name = "TIMESTAMP")]
    pub created: Option<DateTime<Local>>,

    /// The storage root relative path to the object's root. Should only be specified for new
    /// objects in repositories without defined storage layouts, and is otherwise ignored.
    #[structopt(short = "r", long, value_name = "OBJ_ROOT")]
    pub object_root: Option<String>,

    /// The ID of the object to commit changes for
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,
}

/// Resets changes staged to an object. Additions are removed, deletions are restored, and
/// modifications are returned to their original state.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct ResetCmd {
    /// Indicates that logical directories should be reset recursively
    #[structopt(short, long)]
    pub recursive: bool,

    /// The ID of the object to reset
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,

    /// The logical paths of the files to reset. This may be a glob pattern. If no paths are
    /// specified the entire object is reset.
    #[structopt(name = "PATH")]
    pub paths: Vec<String>,
}
/// Lists all of the objects with staged changes or shows the staged changes for a specific object.
/// This command is a simplified version of 'ls --staged' and 'show -staged'. Use the other commands
/// if you need more options.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct StatusCmd {
    /// The ID of the object to show staged changes for
    #[structopt(name = "OBJ_ID")]
    pub object_id: Option<String>,
}

/// Purges an object, completely removing it from the repository.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct PurgeCmd {
    /// Purges without prompting for confirmation
    #[structopt(short, long)]
    pub force: bool,

    /// The ID of the object to purge
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,
}

// TODO a command for rebasing staging if an object is updated after the staged version was created?

#[derive(Debug)]
pub struct Num(pub usize);

arg_enum! {
    #[derive(Debug, Clone, Copy)]
    pub enum Field {
        Name,
        Version,
        Updated,
        Physical,
        Digest,
        None
    }
}

arg_enum! {
    #[derive(Debug, Clone, Copy)]
    pub enum Layout {
        FlatDirect,
        HashedNTuple,
        HashedNTupleObjectId,
    }
}

arg_enum! {
    #[derive(Debug, Clone, Copy)]
    pub enum DigestAlgorithm {
        Sha256,
        Sha512,
    }
}

/// The target storage location
pub enum Storage {
    FileSystem,
    S3,
}

impl RocflArgs {
    /// Returns the target storage location of the command
    pub fn target_storage(&self) -> Storage {
        match self.bucket {
            Some(_) => Storage::S3,
            _ => Storage::FileSystem,
        }
    }
}

impl Default for Num {
    fn default() -> Self {
        Self { 0: usize::MAX }
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

use std::fmt::{self, Display, Formatter};
use std::num::ParseIntError;
use std::path::PathBuf;
use std::str::FromStr;
use std::string::ToString;

use chrono::{DateTime, Local};
use clap::arg_enum;
use enum_dispatch::enum_dispatch;
use structopt::clap::AppSettings::{ColorAuto, ColoredHelp, DisableVersion};
use structopt::StructOpt;
use strum::VariantNames;
use strum_macros::{Display as EnumDisplay, EnumString, EnumVariantNames};

use crate::ocfl::VersionNum;

/// A CLI for OCFL repositories
///
/// rocfl provides a variety of subcommands for interacting with OCFL repositories on the
/// local filesystem or in S3. Its goal is to provide a logical view of OCFL objects and
/// make them easy to interact with in a unix-like way.
///
/// rocfl is able to interact with repositories without a defined storage layout, but this
/// does come at a significant performance cost. Defining your OCFL storage layout is strongly
/// recommended.
///
/// Each subcommand has its own help page that provides details about how to use the command.
/// There are a number of global options that apply to most, if not all, subcommands that
/// are described here. A number of these options, such as repository location information,
/// can be defined in a configuration file so that they do not needed to be specified on
/// every invocation. The easiest way to do this is by invoking: 'rocfl config'.
#[derive(Debug, StructOpt)]
#[structopt(name = "rocfl", author = "Peter Winckles <pwinckles@pm.me>")]
#[structopt(setting(ColorAuto), setting(ColoredHelp))]
pub struct RocflArgs {
    /// Name of the repository to access
    ///
    /// Repository names are used to load repository specific configuration in the rocfl config
    /// file. For example, a repository's root could be defined in the config and referenced
    /// here by name so that the root does not need to be specified with every command.
    #[structopt(short, long, value_name = "NAME")]
    pub name: Option<String>,

    /// Absolute or relative path to the repository's storage root
    ///
    /// By default, this is the current directory.
    #[structopt(short, long, value_name = "ROOT_PATH")]
    pub root: Option<String>,

    /// Absolute or relative path to the staging directory
    ///
    /// By default, versions are staged in an extensions directory in the main repository.
    /// This is the recommended configuration. If the repository is in S3, then versions are
    /// staged in an OS specific user home directory. Staging directories should NOT be shared
    /// by multiple different repositories.
    #[structopt(short, long, value_name = "STAGING_PATH")]
    pub staging_root: Option<String>,

    /// AWS region identifier. Must specify when using S3.
    #[structopt(short = "R", long, value_name = "REGION")]
    pub region: Option<String>,

    /// S3 bucket name. Must specify when using S3.
    #[structopt(short, long, value_name = "BUCKET")]
    pub bucket: Option<String>,

    /// Custom S3 endpoint URL. Only specify when using a custom region.
    #[structopt(short, long, value_name = "ENDPOINT")]
    pub endpoint: Option<String>,

    /// AWS profile to load credentials from.
    #[structopt(short, long, value_name = "PROFILE")]
    pub profile: Option<String>,

    /// Suppress error messages
    #[structopt(short, long)]
    pub quiet: bool,

    /// Increase log level
    #[structopt(short = "V", long)]
    pub verbose: bool,

    /// Disable all output styling
    #[structopt(short = "S", long)]
    pub no_styles: bool,

    /// Subcommand to execute
    #[structopt(subcommand)]
    pub command: Command,
}

#[enum_dispatch(Cmd)]
#[derive(Debug, StructOpt)]
pub enum Command {
    #[structopt(name = "config")]
    Config(ConfigCmd),
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
    #[structopt(name = "validate")]
    Validate(ValidateCmd),
}

/// Edit rocfl configuration
///
/// The config file can have one global section, [global], that defines defaults across all
/// configurations, and any number of named sections, [NAME]. Each section can define any
/// of the following properties: author_name, author_address, root, staging_root, region,
/// bucket, and endpoint.
///
/// Global configuration is always active, and named configuration is activated by invoking
/// rocfl with '-n NAME'. When resolving configuration, command line arguments have highest
/// precedence, followed by named configuration, and finally global configuration.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct ConfigCmd {}

/// List objects or files within objects
///
/// When listing objects, rocfl must scan the entire repository, and can therefore be very slow
/// when operating on large repositories or repositories in S3.
///
/// This command supports glob expressions. When you use globs, it is usually a good idea to
/// quote them so that your shell does not attempt to expand them.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct ListCmd {
    /// Interpret logical path parts as logical directories
    ///
    /// Instead of listing all of the paths in the object, only the paths that are direct
    /// logical children of the query are returned. This is analogous to executing ls on
    /// the local filesystem.
    #[structopt(short = "D", long)]
    pub logical_dirs: bool,

    /// Enable long output
    ///
    /// Format: Version, Updated, Name (Object ID or Logical Path)
    #[structopt(short, long)]
    pub long: bool,

    /// Display the physical path to the item relative the repository storage root
    #[structopt(short, long)]
    pub physical: bool,

    /// Display the digest of the item in the format 'algorithm:digest'
    #[structopt(short, long)]
    pub digest: bool,

    /// Display a header row
    #[structopt(short, long)]
    pub header: bool,

    /// Tab separate the output
    #[structopt(short, long)]
    pub tsv: bool,

    /// List staged objects or the contents of a specific staged object
    #[structopt(short = "S", long, conflicts_with = "version")]
    pub staged: bool,

    /// Version of the object to list
    #[structopt(short, long, value_name = "VERSION")]
    pub version: Option<VersionNum>,

    /// Field to sort on
    #[structopt(short, long,
    value_name = "FIELD",
    possible_values = &Field::variants(),
    default_value = "Name",
    case_insensitive = true)]
    pub sort: Field,

    /// Reverse the order of the sort
    #[structopt(short, long)]
    pub reverse: bool,

    /// List only objects; not their contents. Useful when glob matching on object IDs
    #[structopt(short, long)]
    pub objects: bool,

    /// ID of the object to list. May be a glob when used with '-o'.
    #[structopt(name = "OBJ_ID")]
    pub object_id: Option<String>,

    /// Path glob of files to list. Requires an object to be specified.
    #[structopt(name = "PATH")]
    pub path: Option<String>,
}

/// Display version history of an object or file.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct LogCmd {
    /// Compact format
    #[structopt(short, long)]
    pub compact: bool,

    /// Display a header row, only with compact format
    #[structopt(short, long)]
    pub header: bool,

    /// Tab separate the output, only with compact format
    #[structopt(short, long)]
    pub tsv: bool,

    /// Reverse the order the versions are displayed
    #[structopt(short, long)]
    pub reverse: bool,

    /// Limit the number of versions displayed
    #[structopt(short, long, value_name = "NUM", default_value)]
    pub num: Num,

    /// ID of the object
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,

    /// Optional path to a file
    #[structopt(name = "PATH")]
    pub path: Option<String>,
}

/// Show a summary of changes in a version
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct ShowCmd {
    /// Show the changes in the staged version of the object, if it exists
    #[structopt(short = "S", long, conflicts_with = "version")]
    pub staged: bool,

    /// Suppress the version details output
    #[structopt(short, long)]
    pub minimal: bool,

    /// ID of the object
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,

    /// The version to show. The most recent version is shown by default
    #[structopt(name = "VERSION")]
    pub version: Option<VersionNum>,
}

/// Show the files that changed between two versions
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

/// Print the specified file to stdout
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct CatCmd {
    /// Cat the contents of a staged file
    #[structopt(short = "S", long, conflicts_with = "version")]
    pub staged: bool,

    /// The version of the object to retrieve the file from
    #[structopt(short, long, value_name = "VERSION")]
    pub version: Option<VersionNum>,

    /// ID of the object
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,

    /// Logical path of the file
    #[structopt(name = "PATH")]
    pub path: String,
}

/// Create a new OCFL repository
///
/// The repository is created in the current directory unless the global option '-r PATH'
/// was specified.
///
/// A new repository may only be created in an empty directory. By default, new repositories
/// are configured to use the storage layout extension 0004-hashed-n-tuple-storage-layout.
/// You should change this up front if you do not want to use this extension as it is
/// difficult to change a repository's layout after objects have been created.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct InitCmd {
    /// Path to a custom storage layout extension config JSON file.
    #[structopt(short, long, value_name = "LAYOUT_CONFIG")]
    pub config_file: Option<PathBuf>,

    /// OCFL storage layout extension to use
    ///
    /// The default extension configuration for the extension is used. Custom configuration
    /// may be specified using '--config-file'.
    #[structopt(short, long,
    value_name = "LAYOUT",
    possible_values = &Layout::VARIANTS,
    default_value = "0004-hashed-n-tuple-storage-layout",
    case_insensitive = true)]
    pub layout: Layout,
}

/// Stage a new OCFL object
///
/// New objects are created in staging and must be committed before they are available in the
/// main repository.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct NewCmd {
    /// Digest algorithm to use for the inventory digest
    #[structopt(short, long,
    value_name = "ALGORITHM",
    possible_values = &DigestAlgorithm::variants(),
    default_value = "Sha512",
    case_insensitive = true)]
    pub digest_algorithm: DigestAlgorithm,

    /// Name of the object's content directory
    #[structopt(short, long, value_name = "PATH", default_value = "content")]
    pub content_directory: String,

    /// Width for zero-padded version numbers, eg. v0001 has a width of 4
    #[structopt(short, long, value_name = "WIDTH", default_value = "0")]
    pub zero_padding: u32,

    /// ID of the object to create.
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,
}

/// Copy external or internal files into an object
///
/// If the target object does not already have a staged version, a new staged version is created,
/// and the files are copied to it. The changes must be committed before they are reflected in a
/// new OCFL version in the object in the main repository.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct CopyCmd {
    /// Source directories should be copied recursively.
    #[structopt(short, long)]
    pub recursive: bool,

    /// Source paths should be interpreted as logical paths internal to the object
    #[structopt(short, long)]
    pub internal: bool,

    /// Version of the object to copy the source paths from. Default: most recent
    ///
    /// Only applicable when copying files internally. For the purposes of this command,
    /// the most recent version is the staged version, if a staged version already exists, or
    /// the most recent version of the object in the main repository if there is no staged
    /// version.
    #[structopt(short, long, value_name = "VERSION", requires = "internal")]
    pub version: Option<VersionNum>,

    /// ID of the object to copy files into
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,

    /// Source files to copy. Glob patterns are supported.
    #[structopt(name = "SRC", required = true)]
    pub source: Vec<String>,

    /// Destination logical path. Specify '/' to copy into the object's root
    #[structopt(name = "DST", last = true)]
    pub destination: String,
}

/// Move external or internal files into an object
///
/// If the target object does not already have a staged version, a new staged version is created,
/// and the files are moved to it. The changes must be committed before they are reflected in a
/// new OCFL version in the object in the main repository.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct MoveCmd {
    /// Source paths should be interpreted as logical paths internal to the object
    #[structopt(short, long)]
    pub internal: bool,

    /// ID of the object to move files into
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,

    /// Source files to move. Glob patterns are supported.
    #[structopt(name = "SRC", required = true)]
    pub source: Vec<String>,

    /// Destination logical path. Specify '/' to move into the object's root
    #[structopt(name = "DST", last = true)]
    pub destination: String,
}

/// Remove riles from an object's state
///
/// The removed files still exist in previous versions, but are no longer referenced in the
/// current version. The changes must be committed before they are reflected in a new OCFL
/// version in the object in the main repository.
///
/// Removing files from a staged version that were new to that staged version will permanently
/// remove them from the object.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct RemoveCmd {
    /// Logical directories should be removed recursively
    #[structopt(short, long)]
    pub recursive: bool,

    /// ID of the object to remove files from
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,

    /// Logical paths of files to remove. Glob patterns are supported.
    #[structopt(name = "PATH", required = true)]
    pub paths: Vec<String>,
}

/// Commit an object's staged changes to a new OCFL version.
///
/// Creates a new OCFL version for all of the changes that were staged for an object, all
/// files are deduplicated, and installs the version into the main OCFL repository.
///
/// Metadata such as the version author's name, address, and message should be provided at this
/// time. These values are stamped into the new OCFL version's metadata.
///
/// If the repository is not using a known storage layout, and a new object is being committed,
/// then the storage root relative path to the object's root must be specified.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct CommitCmd {
    /// Pretty print the version's inventory.json file
    #[structopt(short, long)]
    pub pretty_print: bool,

    /// Name of the user to attribute the changes to
    #[structopt(short = "n", long, value_name = "NAME")]
    pub user_name: Option<String>,

    /// Address URI of the user to attribute the changes to. For example, mailto:test@example.com
    #[structopt(short = "a", long, value_name = "ADDRESS")]
    pub user_address: Option<String>,

    /// Message describing the changes
    #[structopt(short, long, value_name = "MESSAGE")]
    pub message: Option<String>,

    /// RFC 3339 creation timestamp of the version. Default: now
    ///
    /// Example timestamp: 2020-12-23T10:11:12-06:00
    #[structopt(short, long, value_name = "TIMESTAMP")]
    pub created: Option<DateTime<Local>>,

    /// Storage root relative path to the object's root
    ///
    /// Should only be specified for new objects in repositories without defined storage
    /// layouts, and is otherwise ignored.
    #[structopt(short = "r", long, value_name = "OBJ_ROOT")]
    pub object_root: Option<String>,

    /// ID of the object to commit changes for
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,
}

/// Reset an object's staged changes
///
/// Additions are removed, deletions are restored, and modifications are returned to their
/// original state. Use this to return an object to its state at the time the staged version
/// was originally created.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct ResetCmd {
    /// Logical directories should be reset recursively
    #[structopt(short, long)]
    pub recursive: bool,

    /// ID of the object to reset
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,

    /// Logical paths of the files to reset. Glob patterns are supported. If no paths are
    /// specified, the entire object is reset.
    #[structopt(name = "PATH")]
    pub paths: Vec<String>,
}

/// List objects with staged changes, or a specific object's changes
///
/// This command is a simplified version of 'ls --staged' and 'show -staged'. Use the other commands
/// if you need more options.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct StatusCmd {
    /// ID of the object to show staged changes for
    #[structopt(name = "OBJ_ID")]
    pub object_id: Option<String>,
}

/// Permanently delete an object
///
/// Purged objects are permanently deleted from the repository. This operation cannot be undone.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct PurgeCmd {
    /// Purge without prompting for confirmation
    #[structopt(short, long)]
    pub force: bool,

    /// ID of the object to purge
    #[structopt(name = "OBJ_ID")]
    pub object_id: String,
}

/// Validate an object or the entire repository
///
/// When run on a specific object, the object is validated against the OCFL spec, and any issues
/// are reported. When run against the entire repository, the repository structure is validated,
/// in addition to validating all of the objects in the repository.
#[derive(Debug, StructOpt)]
#[structopt(setting(ColorAuto), setting(ColoredHelp), setting(DisableVersion))]
pub struct ValidateCmd {
    /// Interpret positional parameters as paths to object roots relative the repository root
    #[structopt(short, long)]
    pub paths: bool,

    /// Disable fixity check on stored files
    #[structopt(short, long)]
    pub no_fixity_check: bool,

    // TODO allow multiple?
    /// IDs of the objects to validate, or paths object roots when used with '--paths'
    #[structopt(name = "OBJ_ID/PATH")]
    pub object_ids: Vec<String>,
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

#[derive(Debug, Clone, Copy, EnumDisplay, EnumString, EnumVariantNames)]
pub enum Layout {
    #[strum(serialize = "None", serialize = "none")]
    None,
    #[strum(serialize = "0002-flat-direct-storage-layout")]
    FlatDirect,
    #[strum(serialize = "0004-hashed-n-tuple-storage-layout")]
    HashedNTuple,
    #[strum(serialize = "0003-hash-and-id-n-tuple-storage-layout")]
    HashedNTupleObjectId,
    #[strum(serialize = "0006-flat-omit-prefix-storage-layout")]
    FlatOmitPrefix,
}

arg_enum! {
    #[derive(Debug, Clone, Copy)]
    pub enum DigestAlgorithm {
        Sha256,
        Sha512,
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

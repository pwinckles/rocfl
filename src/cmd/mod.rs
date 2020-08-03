use core::fmt;
use std::cmp::Ordering;
use std::fmt::Display;
use std::io;
use std::io::Write;

use anyhow::{Context, Error, Result};
use globset::GlobBuilder;
use lazy_static::lazy_static;
use rusoto_core::Region;
use serde::export::Formatter;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

use crate::cmd::opts::*;
use crate::cmd::print::{Alignment, AsRow, Column, ColumnId, Row, TableView, TextCell};
use crate::ocfl::{Diff as VersionDiff, DiffType, FileDetails, ObjectVersionDetails, OcflRepo, VersionDetails, VersionNum};

pub mod opts;
pub mod print;

const DATE_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

lazy_static! {
    static ref DEFAULT_USER: String = "NA".to_string();
}

// TODO
pub fn exec_command(args: &RocflArgs) -> Result<()> {
    let repo = create_repo(&args)?;
    match &args.command {
        Command::List(list) => list_command(&repo, &list, args),
        Command::Log(log) => log_command(&repo, &log),
        Command::Show(show) => show_command(&repo, &show),
        Command::Diff(diff) => diff_command(&repo, &diff),
    }
}

// TODO
pub fn print_err(error: &Error, quiet: bool) {
    if !quiet {
        let mut stderr = StandardStream::stderr(ColorChoice::Auto);
        match stderr.set_color(ColorSpec::new().set_fg(Some(Color::Red))) {
            Ok(_) => {
                if writeln!(&mut stderr, "Error: {:#}", error).is_err() {
                    eprintln!("Error: {:#}", error)
                }
                let _ = stderr.reset();
            }
            Err(_) => eprintln!("Error: {:#}", error)
        }
    }
}

fn create_repo(args: &RocflArgs) -> Result<OcflRepo> {
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
                    name: args.region.as_ref().unwrap().to_owned(),
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

fn list_command(repo: &OcflRepo, command: &List, args: &RocflArgs) -> Result<()> {
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

    for (count, version) in iter.enumerate() {
        if count == command.num.0 {
            break;
        }
        println(FormatVersion::new(version, command.compact));
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
        .into_iter().map(DiffLine).collect();

    diffs.sort_unstable();

    for diff in diffs {
        println(diff);
    }

    Ok(())
}

fn list_objects(repo: &OcflRepo, command: &List, args: &RocflArgs) -> Result<()> {
    let iter = repo.list_objects(command.object_id.as_deref())
        .with_context(|| "Failed to list objects")?;

    let mut objects: Vec<ObjectVersionDetails> = iter.filter(|result| {
        match result {
            Ok(_) => true,
            Err(e) => {
                print_err(e, args.quiet);
                false
            }
        }
    }).map(Result::unwrap).collect();

    objects.sort_unstable_by(|a, b| {
        if command.reverse {
            cmp_objects(&command.sort, b, a)
        } else {
            cmp_objects(&command.sort, a, b)
        }
    });

    let mut table = object_table(&command);
    objects.iter().for_each(|object| table.add_row(object));
    Ok(table.write_stdio()?)
}

fn list_object_contents(repo: &OcflRepo, command: &List) -> Result<()> {
    let object_id = command.object_id.as_ref().unwrap();
    let object = repo.get_object(object_id, command.version.as_ref())
        .with_context(|| "Failed to list object")?;

    let glob = match command.path.as_ref() {
        Some(path) => Some(GlobBuilder::new(path)
            .literal_separator(command.glob_literal_separator)
            .backslash_escape(true).build()?.compile_matcher()),
        None => None
    };

    let mut listings: Vec<ContentListing> = object.state.into_iter()
        .map(move |(path, details)| {
            ContentListing {
                logical_path: path,
                details
            }
        }).filter(|listing| {
        match &glob {
            Some(glob) => glob.is_match(&listing.logical_path),
            None => true
        }
    }).collect();

    listings.sort_unstable_by(|a, b| {
        if command.reverse {
            cmp_object_contents(&command.sort, b, a)
        } else {
            cmp_object_contents(&command.sort, a, b)
        }
    });

    let mut table = object_content_table(command);
    listings.iter().for_each(|listing| table.add_row(listing));
    Ok(table.write_stdio()?)
}

fn object_table(command: &List) -> TableView {
    let mut columns = Vec::new();

    if command.long {
        columns.push(Column::new(ColumnId::Version, "Version", Alignment::Right));
        columns.push(Column::new(ColumnId::Updated, "Updated", Alignment::Left));
    }

    columns.push(Column::new(ColumnId::ObjectId, "Object ID", Alignment::Left));

    if command.physical {
        columns.push(Column::new(ColumnId::PhysicalPath, "Physical Path", Alignment::Left));
    }

    TableView::new(columns, command.header)
}

fn object_content_table(command: &List) -> TableView {
    let mut columns = Vec::new();

    if command.long {
        columns.push(Column::new(ColumnId::Version, "Version", Alignment::Right));
        columns.push(Column::new(ColumnId::Updated, "Updated", Alignment::Left));
    }

    columns.push(Column::new(ColumnId::LogicalPath, "Logical Path", Alignment::Left));

    if command.physical {
        columns.push(Column::new(ColumnId::PhysicalPath, "Physical Path", Alignment::Left));
    }

    if command.digest {
        columns.push(Column::new(ColumnId::Digest, "Digest", Alignment::Left));
    }

    TableView::new(columns, command.header)
}

fn cmp_objects(field: &Field, a: &ObjectVersionDetails, b: &ObjectVersionDetails) -> Ordering {
    match field {
        Field::Name => natord::compare(&a.id, &b.id),
        Field::Version => a.version_details.version_num.cmp(&b.version_details.version_num),
        Field::Updated => a.version_details.created.cmp(&b.version_details.created),
        Field::Physical => a.object_root.cmp(&b.object_root),
        Field::Digest => Ordering::Equal,
        Field::None => Ordering::Equal,
    }
}

fn cmp_object_contents(field: &Field, a: &ContentListing, b: &ContentListing) -> Ordering {
    match field {
        Field::Name => natord::compare(&a.logical_path, &b.logical_path),
        Field::Version => a.details.last_update.version_num.cmp(&b.details.last_update.version_num),
        Field::Updated => a.details.last_update.created.cmp(&b.details.last_update.created),
        Field::Physical => natord::compare(&a.details.storage_path, &b.details.storage_path),
        Field::Digest => a.details.digest.cmp(&b.details.digest),
        Field::None => Ordering::Equal,
    }
}

// https://github.com/rust-lang/rust/issues/46016
fn println(value: impl Display) {
    // Don't care about errors
    let _ = writeln!(io::stdout(), "{}", value);
}

#[derive(Debug)]
struct ContentListing {
    logical_path: String,
    details: FileDetails,
}

#[derive(Debug)]
struct FormatVersion<'a> {
    version: &'a VersionDetails,
    compact: bool,
}

#[derive(Debug)]
struct DiffLine(VersionDiff);

impl<'a> AsRow<'a> for ContentListing {
    fn as_row(&'a self, columns: &[Column]) -> Row<'a> {
        let mut cells = Vec::new();

        for column in columns {
            let cell = match column.id {
                ColumnId::Version => TextCell::new_owned(
                    &self.details.last_update.version_num.to_string()),
                ColumnId::Updated => TextCell::new_owned(
                    &self.details.last_update.created.format(DATE_FORMAT).to_string()),
                ColumnId::LogicalPath =>TextCell::new_ref(&self.logical_path),
                ColumnId::PhysicalPath => TextCell::new_ref(&self.details.storage_path),
                ColumnId::Digest => TextCell::new_owned(&format!("{}:{}",
                                                               self.details.digest_algorithm,
                                                               self.details.digest)),
                _ => TextCell::blank()
            };

            cells.push(cell);
        }

        Row::new(cells)
    }
}

impl<'a> AsRow<'a> for ObjectVersionDetails {
    fn as_row(&'a self, columns: &[Column]) -> Row<'a> {
        let mut cells = Vec::new();

        for column in columns {
            let cell = match column.id {
                ColumnId::Version => TextCell::new_owned(
                    &self.version_details.version_num.to_string()),
                ColumnId::Updated => TextCell::new_owned(
                    &self.version_details.created.format(DATE_FORMAT).to_string()),
                ColumnId::ObjectId =>TextCell::new_ref(&self.id),
                ColumnId::PhysicalPath => TextCell::new_ref(&self.object_root),
                _ => TextCell::blank()
            };

            cells.push(cell);
        }

        Row::new(cells)
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

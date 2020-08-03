use core::fmt;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::io::{self, ErrorKind, Write};

use anyhow::Result;
use lazy_static::lazy_static;

use crate::cmd::DATE_FORMAT;
use crate::cmd::opts::{Diff, Log, Show};
use crate::cmd::table::{Alignment, AsRow, Column, ColumnId, Row, TableView, TextCell};
use crate::ocfl::{Diff as VersionDiff, DiffType, OcflRepo, VersionDetails, VersionNum};

lazy_static! {
    static ref DEFAULT_USER: String = "NA".to_string();
}

pub fn log_command(repo: &OcflRepo, command: &Log) -> Result<()> {
    let mut versions = match &command.path {
        Some(path) => repo.list_file_versions(&command.object_id, path)?,
        None => repo.list_object_versions(&command.object_id)?,
    };

    if command.reverse {
        versions.reverse();
    }

    versions.truncate(command.num.0);

    print_versions(&versions, command)
}

fn print_versions(versions: &[VersionDetails], command: &Log) -> Result<()> {
    if command.compact {
        let mut table = version_table();
        versions.iter().for_each(|version| table.add_row(version));
        Ok(table.write_stdio()?)
    } else {
        for version in versions {
            println(FormatVersion(version))?
        }
        Ok(())
    }
}

pub fn show_command(repo: &OcflRepo, command: &Show) -> Result<()> {
    let object = repo.get_object_details(&command.object_id, command.version.as_ref())?;

    if !command.minimal {
        println(FormatVersion(&object.version_details))?;
    }

    diff_and_print(repo, &command.object_id, None, &object.version_details.version_num)
}

pub fn diff_command(repo: &OcflRepo, command: &Diff) -> Result<()> {
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
        println(diff)?;
    }

    Ok(())
}

fn println(value: impl Display) -> Result<()> {
    if let Err(e) = writeln!(io::stdout(), "{}", value) {
        match e.kind() {
            ErrorKind::BrokenPipe => Ok(()),
            _ => Err(e.into()),
        }
    } else {
        Ok(())
    }
}

fn version_table<'a>() -> TableView<'a> {
    let mut columns = Vec::new();

    columns.push(Column::new(ColumnId::Version, "Version", Alignment::Right));
    columns.push(Column::new(ColumnId::Author, "Author", Alignment::Left));
    columns.push(Column::new(ColumnId::Address, "Address", Alignment::Left));
    columns.push(Column::new(ColumnId::Created, "Created", Alignment::Left));
    columns.push(Column::new(ColumnId::Message, "Message", Alignment::Left));

    TableView::new(columns, false)
}

#[derive(Debug)]
struct FormatVersion<'a>(&'a VersionDetails);

#[derive(Debug)]
struct DiffLine(VersionDiff);

impl fmt::Display for FormatVersion<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:width$} {}\n{:width$} {} <{}>\n{:width$} {}\n{:width$} {}\n",
               "Version:", self.0.version_num.to_string(),
               "Author:",
               self.0.user_name.as_ref().unwrap_or(&(*DEFAULT_USER)),
               self.0.user_address.as_ref().unwrap_or(&(*DEFAULT_USER)),
               "Date:", self.0.created.to_rfc2822(),
               "Message:", self.0.message.as_ref().unwrap_or(&"".to_string()),
               width = 8)
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

impl<'a> AsRow<'a> for VersionDetails {
    fn as_row(&'a self, columns: &[Column]) -> Row<'a> {
        let mut cells = Vec::new();

        for column in columns {
            let cell = match column.id {
                ColumnId::Version => TextCell::new_owned(&self.version_num.to_string()),
                ColumnId::Author => TextCell::new_owned(
                    self.user_name.as_ref().unwrap_or(&(*DEFAULT_USER))),
                ColumnId::Address =>TextCell::new_owned(
                    self.user_address.as_ref().unwrap_or(&(*DEFAULT_USER))),
                ColumnId::Created => TextCell::new_owned(
                    &self.created.format(DATE_FORMAT).to_string()),
                ColumnId::Message => TextCell::new_owned(
                    self.message.as_ref().unwrap_or(&"".to_string())),
                _ => TextCell::blank()
            };

            cells.push(cell);
        }

        Row::new(cells)
    }
}
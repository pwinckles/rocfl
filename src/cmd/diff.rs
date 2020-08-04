use core::fmt;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::io::{self, ErrorKind, Write};

use ansi_term::{Color, Style};
use anyhow::Result;
use lazy_static::lazy_static;

use crate::cmd::DATE_FORMAT;
use crate::cmd::opts::{Diff, Log, RocflArgs, Show};
use crate::cmd::table::{Alignment, AsRow, Column, ColumnId, Row, TableView, TextCell};
use crate::ocfl::{Diff as VersionDiff, DiffType, OcflRepo, VersionDetails, VersionNum};

lazy_static! {
    static ref DEFAULT_USER: String = "NA".to_string();
}

// TODO implement command structs

pub fn log_command(repo: &OcflRepo, command: &Log, args: &RocflArgs) -> Result<()> {
    let mut versions = match &command.path {
        Some(path) => repo.list_file_versions(&command.object_id, path)?,
        None => repo.list_object_versions(&command.object_id)?,
    };

    if command.reverse {
        versions.reverse();
    }

    versions.truncate(command.num.0);

    print_versions(&versions, command, args)
}

fn print_versions(versions: &[VersionDetails], command: &Log, args: &RocflArgs) -> Result<()> {
    if command.compact {
        let mut table = version_table(command, args);
        versions.iter().for_each(|version| table.add_row(version));
        Ok(table.write_stdio()?)
    } else {
        for version in versions {
            println(FormatVersion::new(version, !args.no_styles))?
        }
        Ok(())
    }
}

pub fn show_command(repo: &OcflRepo, command: &Show, args: &RocflArgs) -> Result<()> {
    let object = repo.get_object_details(&command.object_id, command.version.as_ref())?;

    if !command.minimal {
        println(FormatVersion::new(&object.version_details, !args.no_styles))?;
    }

    diff_and_print(repo, &command.object_id, None, &object.version_details.version_num, args)
}

pub fn diff_command(repo: &OcflRepo, command: &Diff, args: &RocflArgs) -> Result<()> {
    if command.left == command.right {
        return Ok(());
    }

    diff_and_print(repo, &command.object_id, Some(&command.left), &command.right, args)
}

fn diff_and_print(repo: &OcflRepo,
                  object_id: &str,
                  left: Option<&VersionNum>,
                  right: &VersionNum,
                  args: &RocflArgs) -> Result<()> {

    let mut diffs: Vec<DiffLine> = repo.diff(object_id, left, right)?
        .into_iter().map(|diff| DiffLine::new(diff, !args.no_styles)).collect();

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

fn version_table<'a, 'b>(command: &'a Log, args: &'a RocflArgs) -> TableView<'b> {
    let mut columns = Vec::new();

    columns.push(Column::new(ColumnId::Version, "Version", Alignment::Right));
    columns.push(Column::new(ColumnId::Author, "Author", Alignment::Left));
    columns.push(Column::new(ColumnId::Address, "Address", Alignment::Left));
    columns.push(Column::new(ColumnId::Created, "Created", Alignment::Left));
    columns.push(Column::new(ColumnId::Message, "Message", Alignment::Left));

    TableView::new(columns, &separator(command), command.header, !args.no_styles)
}

fn separator(command: &Log) -> String {
    if command.tsv {
        "\t".to_string()
    } else {
        " ".to_string()
    }
}

#[derive(Debug)]
struct FormatVersion<'a> {
    details: &'a VersionDetails,
    enable_styling: bool,
}

#[derive(Debug)]
struct DiffLine {
    diff: VersionDiff,
    enable_styling: bool,
}

impl<'a> FormatVersion<'a> {
    fn new(details: &'a VersionDetails, enable_styling: bool) -> Self {
        Self {
            details,
            enable_styling,
        }
    }
}

impl fmt::Display for FormatVersion<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let version = format!("Version {}", self.details.version_num.number);
        let style = if self.enable_styling {
            Style::new().fg(Color::Yellow)
        } else {
            Style::default()
        };

        write!(f, "{}\n{:width$} {} <{}>\n{:width$} {}\n{:width$} {}\n",
               style.paint(version),
               "Author:",
               self.details.user_name.as_ref().unwrap_or(&(*DEFAULT_USER)),
               self.details.user_address.as_ref().unwrap_or(&(*DEFAULT_USER)),
               "Date:", self.details.created.to_rfc2822(),
               "Message:", self.details.message.as_ref().unwrap_or(&"".to_owned()),
               width = 8)
    }
}

impl DiffLine {
    fn new(diff: VersionDiff, enable_styling: bool) -> Self {
        Self {
            diff,
            enable_styling,
        }
    }
}

impl fmt::Display for DiffLine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // TODO see about statically initializing these
        let (letter, style) = match self.diff.diff_type {
            DiffType::Added => ("A", Style::new().fg(Color::Green)),
            DiffType::Modified => ("M", Style::new().fg(Color::Cyan)),
            DiffType::Deleted => ("D", Style::new().fg(Color::Red)),
        };

        let style = if self.enable_styling {
            style
        } else {
            Style::default()
        };

        write!(f, "{}\t{}", style.paint(letter), self.diff.path)
    }
}

impl PartialEq for DiffLine {
    fn eq(&self, other: &Self) -> bool {
        self.diff.path == other.diff.path
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
        self.diff.path.cmp(&other.diff.path)
    }
}

impl<'a> AsRow<'a> for VersionDetails {
    fn as_row(&'a self, columns: &[Column]) -> Row<'a> {
        let mut cells = Vec::new();

        for column in columns {
            let cell = match column.id {
                ColumnId::Version => TextCell::new_owned(&self.version_num.to_string())
                    .with_style(Style::new().fg(Color::Green)),
                ColumnId::Author => TextCell::new_owned(
                    self.user_name.as_ref().unwrap_or(&(*DEFAULT_USER)))
                    .with_style(Style::new().bold()),
                ColumnId::Address =>TextCell::new_owned(
                    self.user_address.as_ref().unwrap_or(&(*DEFAULT_USER))),
                ColumnId::Created => TextCell::new_owned(
                    &self.created.format(DATE_FORMAT).to_string())
                    .with_style(Style::new().fg(Color::Yellow)),
                ColumnId::Message => TextCell::new_owned(
                    self.message.as_ref().unwrap_or(&"".to_string())),
                _ => TextCell::blank()
            };

            cells.push(cell);
        }

        Row::new(cells)
    }
}
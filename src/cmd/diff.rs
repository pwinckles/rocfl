use core::fmt;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt::Formatter;

use crate::cmd::opts::{DiffCmd, LogCmd, ShowCmd};
use crate::cmd::style;
use crate::cmd::table::{Alignment, AsRow, Column, ColumnId, Row, Separator, TableView, TextCell};
use crate::cmd::{println, Cmd, GlobalArgs, DATE_FORMAT};
use crate::ocfl::{Diff, OcflRepo, Result, VersionDetails};

const DEFAULT_USER: &str = "NA";

const ADDED: &str = "Added";
const MODIFIED: &str = "Modified";
const DELETED: &str = "Deleted";
const RENAMED: &str = "Renamed";

impl Cmd for LogCmd {
    fn exec(&self, repo: &OcflRepo, args: GlobalArgs) -> Result<()> {
        let mut versions = match &self.path {
            Some(path) => repo.list_file_versions(&self.object_id, &path.try_into()?)?,
            None => repo.list_object_versions(&self.object_id)?,
        };

        if self.reverse {
            versions.reverse();
        }

        versions.truncate(self.num.0);

        self.print_versions(&versions, args)
    }
}

impl LogCmd {
    fn print_versions(&self, versions: &[VersionDetails], args: GlobalArgs) -> Result<()> {
        if self.compact {
            let mut table = self.version_table(args);
            versions.iter().for_each(|version| table.add_row(version));
            Ok(table.write_stdio()?)
        } else {
            for version in versions {
                println(FormatVersion::new(version, !args.no_styles))?
            }
            Ok(())
        }
    }

    fn version_table(&self, args: GlobalArgs) -> TableView {
        let mut columns = Vec::new();

        columns.push(Column::new(ColumnId::Version, "Version", Alignment::Right));
        columns.push(Column::new(ColumnId::Author, "Author", Alignment::Left));
        columns.push(Column::new(ColumnId::Address, "Address", Alignment::Left));
        columns.push(Column::new(ColumnId::Created, "Created", Alignment::Left));
        columns.push(Column::new(ColumnId::Message, "Message", Alignment::Left));

        TableView::new(columns, self.separator(), self.header, !args.no_styles)
    }

    fn separator(&self) -> Separator {
        if self.tsv {
            Separator::TAB
        } else {
            Separator::SPACE
        }
    }
}

impl Cmd for ShowCmd {
    fn exec(&self, repo: &OcflRepo, args: GlobalArgs) -> Result<()> {
        if self.staged {
            // TODO add back status alias?
            if !self.minimal {
                let object = repo.get_staged_object_details(&self.object_id)?;
                println(FormatVersion::new(&object.version_details, !args.no_styles))?;
            }

            let diffs = repo.diff_staged(&self.object_id)?;

            if diffs.is_empty() {
                println("No staged changes found.")
            } else {
                display_diffs(diffs, &args)
            }
        } else {
            let object = repo.get_object_details(&self.object_id, self.version)?;

            if !self.minimal {
                println(FormatVersion::new(&object.version_details, !args.no_styles))?;
            }

            let right = object.version_details.version_num;

            let diffs = repo.diff(&self.object_id, None, right)?;

            display_diffs(diffs, &args)
        }
    }
}

impl Cmd for DiffCmd {
    fn exec(&self, repo: &OcflRepo, args: GlobalArgs) -> Result<()> {
        if self.left == self.right {
            return Ok(());
        }

        let diffs = repo.diff(&self.object_id, Some(self.left), self.right)?;

        display_diffs(diffs, &args)
    }
}

fn display_diffs(diffs: Vec<Diff>, args: &GlobalArgs) -> Result<()> {
    let mut diffs: Vec<DiffLine> = diffs.into_iter().map(DiffLine::new).collect();

    diffs.sort_unstable();

    let mut columns = Vec::new();
    columns.push(Column::new(
        ColumnId::Operation,
        "Operation",
        Alignment::Left,
    ));
    columns.push(Column::new(
        ColumnId::LogicalPath,
        "Logical Path",
        Alignment::Left,
    ));
    let mut table = TableView::new(columns, Separator::SPACE, true, !args.no_styles);

    diffs.iter().for_each(|diff| table.add_row(diff));
    table.write_stdio()?;

    Ok(())
}

struct FormatVersion<'a> {
    details: &'a VersionDetails,
    enable_styling: bool,
}

struct DiffLine {
    diff: Diff,
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
            &*style::YELLOW
        } else {
            &*style::DEFAULT
        };

        write!(
            f,
            "{}\n{:width$} {} <{}>\n{:width$} {}\n{:width$} {}\n",
            style.paint(version),
            "Author:",
            defaulted_str(&self.details.user_name, DEFAULT_USER),
            defaulted_str(&self.details.user_address, DEFAULT_USER),
            "Date:",
            self.details.created.to_rfc2822(),
            "Message:",
            self.details.message.as_ref().unwrap_or(&"".to_owned()),
            width = 8
        )
    }
}

impl DiffLine {
    fn new(diff: Diff) -> Self {
        Self { diff }
    }
}

impl<'a> AsRow<'a> for DiffLine {
    fn as_row(&'a self, columns: &[Column]) -> Row<'a> {
        let mut cells = Vec::new();

        for column in columns {
            let cell = match column.id {
                ColumnId::Operation => match &self.diff {
                    Diff::Added(_) => TextCell::new(ADDED).with_style(&*style::GREEN),
                    Diff::Modified(_) => TextCell::new(MODIFIED).with_style(&*style::CYAN),
                    Diff::Deleted(_) => TextCell::new(DELETED).with_style(&*style::RED),
                    Diff::Renamed { .. } => TextCell::new(RENAMED).with_style(&*style::CYAN),
                },
                ColumnId::LogicalPath => TextCell::new(self.path_display()),
                _ => TextCell::blank(),
            };

            cells.push(cell);
        }

        Row::new(cells)
    }
}

impl DiffLine {
    fn path_display(&self) -> Cow<str> {
        match &self.diff {
            Diff::Renamed { original, renamed } => Cow::Owned(format!(
                "{} -> {}",
                original
                    .iter()
                    .map(|e| e.as_ref().as_ref().as_ref())
                    .collect::<Vec<&str>>()
                    .join(", "),
                renamed
                    .iter()
                    .map(|e| e.as_ref().as_ref().as_ref())
                    .collect::<Vec<&str>>()
                    .join(", ")
            )),
            Diff::Added(path) => path.as_ref().as_ref().into(),
            Diff::Modified(path) => path.as_ref().as_ref().into(),
            Diff::Deleted(path) => path.as_ref().as_ref().into(),
        }
    }
}

impl PartialEq for DiffLine {
    fn eq(&self, other: &Self) -> bool {
        self.diff.path() == other.diff.path()
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
        self.diff.path().cmp(&other.diff.path())
    }
}

impl<'a> AsRow<'a> for VersionDetails {
    fn as_row(&'a self, columns: &[Column]) -> Row<'a> {
        let mut cells = Vec::new();

        for column in columns {
            let cell = match column.id {
                ColumnId::Version => {
                    TextCell::new(self.version_num.to_string()).with_style(&*style::GREEN)
                }
                ColumnId::Author => TextCell::new(defaulted_str(&self.user_name, DEFAULT_USER))
                    .with_style(&*style::BOLD),
                ColumnId::Address => TextCell::new(defaulted_str(&self.user_address, DEFAULT_USER)),
                ColumnId::Created => TextCell::new(self.created.format(DATE_FORMAT).to_string())
                    .with_style(&*style::YELLOW),
                ColumnId::Message => match &self.message {
                    Some(message) => TextCell::new(message),
                    None => TextCell::blank(),
                },
                _ => TextCell::blank(),
            };

            cells.push(cell);
        }

        Row::new(cells)
    }
}

fn defaulted_str<'a>(value: &'a Option<String>, default: &'a str) -> &'a str {
    match value {
        Some(value) => value.as_ref(),
        None => default,
    }
}

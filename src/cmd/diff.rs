use core::fmt;
use std::cmp::Ordering;
use std::fmt::Formatter;

use anyhow::Result;
use lazy_static::lazy_static;

use crate::cmd::{Cmd, DATE_FORMAT, GlobalArgs, println};
use crate::cmd::opts::{Diff, Log, Show};
use crate::cmd::style;
use crate::cmd::table::{Alignment, AsRow, Column, ColumnId, Row, TableView, TextCell};
use crate::ocfl::{Diff as VersionDiff, DiffType, OcflRepo, VersionDetails};

lazy_static! {
    static ref DEFAULT_USER: String = "NA".to_owned();
    static ref ADDED: String = "A".to_owned();
    static ref MODIFIED: String = "M".to_owned();
    static ref DELETED: String = "D".to_owned();
}

impl Cmd for Log {
    fn exec(&self, repo: &OcflRepo, args: GlobalArgs) -> Result<()> {
        let mut versions = match &self.path {
            Some(path) => repo.list_file_versions(&self.object_id, path)?,
            None => repo.list_object_versions(&self.object_id)?,
        };

        if self.reverse {
            versions.reverse();
        }

        versions.truncate(self.num.0);

        self.print_versions(&versions, args)
    }
}

impl Log {
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

        TableView::new(columns, &self.separator(), self.header, !args.no_styles)
    }

    fn separator(&self) -> String {
        if self.tsv {
            "\t".to_string()
        } else {
            " ".to_string()
        }
    }
}

impl Cmd for Show {
    fn exec(&self, repo: &OcflRepo, args: GlobalArgs) -> Result<()> {
        let object = repo.get_object_details(&self.object_id,
                                             self.version.as_ref())?;

        if !self.minimal {
            println(FormatVersion::new(&object.version_details, !args.no_styles))?;
        }

        let right = &object.version_details.version_num;

        let mut diffs: Vec<DiffLine> = repo.diff(&self.object_id, None, right)?
            .into_iter()
            .map(|diff| DiffLine::new(diff, !args.no_styles))
            .collect();

        diffs.sort_unstable();

        for diff in diffs {
            println(diff)?;
        }

        Ok(())
    }
}

impl Cmd for Diff {
    fn exec(&self, repo: &OcflRepo, args: GlobalArgs) -> Result<()> {
        if self.left == self.right {
            return Ok(());
        }

        let raw_diffs = repo.diff(&self.object_id,
                                  Some(&self.left),
                                  &self.right)?;

        let mut diffs: Vec<DiffLine> = raw_diffs.into_iter()
            .map(|diff| DiffLine::new(diff, !args.no_styles))
            .collect();

        diffs.sort_unstable();

        for diff in diffs {
            println(diff)?;
        }

        Ok(())
    }
}

struct FormatVersion<'a> {
    details: &'a VersionDetails,
    enable_styling: bool,
}

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
            &*style::YELLOW
        } else {
            &*style::DEFAULT
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
        let (letter, style) = match self.diff.diff_type {
            DiffType::Added => (&*ADDED, &*style::GREEN),
            DiffType::Modified => (&*MODIFIED, &*style::CYAN),
            DiffType::Deleted => (&*DELETED, &*style::RED),
        };

        let style = if self.enable_styling {
            style
        } else {
            &*style::DEFAULT
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
                    .with_style(&*style::GREEN),
                ColumnId::Author => TextCell::new_owned(
                    self.user_name.as_ref().unwrap_or(&*DEFAULT_USER))
                    .with_style(&*style::BOLD),
                ColumnId::Address =>TextCell::new_owned(
                    self.user_address.as_ref().unwrap_or(&*DEFAULT_USER)),
                ColumnId::Created => TextCell::new_owned(
                    &self.created.format(DATE_FORMAT).to_string())
                    .with_style(&*style::YELLOW),
                ColumnId::Message => TextCell::new_owned(
                    self.message.as_ref().unwrap_or(&"".to_string())),
                _ => TextCell::blank()
            };

            cells.push(cell);
        }

        Row::new(cells)
    }
}
use std::cmp::Ordering;

use globset::GlobBuilder;

use crate::cmd::{Cmd, DATE_FORMAT, GlobalArgs};
use crate::cmd::opts::*;
use crate::cmd::opts::List;
use crate::cmd::style;
use crate::cmd::table::{Alignment, AsRow, Column, ColumnId, Row, TableView, TextCell};
use crate::ocfl::{FileDetails, ObjectVersionDetails, OcflRepo};
use crate::ocfl::error::Result;

impl Cmd for List {
    fn exec(&self, repo: &OcflRepo, args: GlobalArgs) -> Result<()> {
        if self.objects || self.object_id.is_none() {
            self.list_objects(repo, args)
        } else {
            self.list_object_contents(repo, args)
        }
    }
}

impl List {
    fn list_objects(&self, repo: &OcflRepo, args: GlobalArgs) -> Result<()> {
        let iter = repo.list_objects(self.object_id.as_deref())?;

        let mut objects: Vec<ObjectVersionDetails> = iter.collect();

        objects.sort_unstable_by(|a, b| {
            if self.reverse {
                cmp_objects(&self.sort, b, a)
            } else {
                cmp_objects(&self.sort, a, b)
            }
        });

        let mut table = self.object_table(args);
        objects.iter().for_each(|object| table.add_row(object));
        Ok(table.write_stdio()?)
    }

    fn list_object_contents(&self, repo: &OcflRepo, args: GlobalArgs) -> Result<()> {
        let object_id = self.object_id.as_ref().unwrap();
        let object = repo.get_object(object_id, self.version)?;

        // TODO this should remove any leading slashes
        let glob = match self.path.as_ref() {
            Some(path) => Some(GlobBuilder::new(path)
                .literal_separator(self.glob_literal_separator)
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
            if self.reverse {
                cmp_object_contents(&self.sort, b, a)
            } else {
                cmp_object_contents(&self.sort, a, b)
            }
        });

        let mut table = self.object_content_table(args);
        listings.iter().for_each(|listing| table.add_row(listing));
        Ok(table.write_stdio()?)
    }

    fn object_table(&self, args: GlobalArgs) -> TableView {
        let mut columns = Vec::new();

        if self.long {
            columns.push(Column::new(ColumnId::Version, "Version", Alignment::Right));
            columns.push(Column::new(ColumnId::Created, "Updated", Alignment::Left));
        }

        columns.push(Column::new(ColumnId::ObjectId, "Object ID", Alignment::Left));

        if self.physical {
            columns.push(Column::new(ColumnId::PhysicalPath, "Physical Path", Alignment::Left));
        }

        TableView::new(columns, &self.separator(), self.header, !args.no_styles)
    }

    fn object_content_table(&self, args: GlobalArgs) -> TableView {
        let mut columns = Vec::new();

        if self.long {
            columns.push(Column::new(ColumnId::Version, "Version", Alignment::Right));
            columns.push(Column::new(ColumnId::Created, "Updated", Alignment::Left));
        }

        columns.push(Column::new(ColumnId::LogicalPath, "Logical Path", Alignment::Left));

        if self.physical {
            columns.push(Column::new(ColumnId::PhysicalPath, "Physical Path", Alignment::Left));
        }

        if self.digest {
            columns.push(Column::new(ColumnId::Digest, "Digest", Alignment::Left));
        }

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

struct ContentListing {
    logical_path: String,
    details: FileDetails,
}

impl<'a> AsRow<'a> for ContentListing {
    fn as_row(&'a self, columns: &[Column]) -> Row<'a> {
        let mut cells = Vec::new();

        for column in columns {
            let cell = match column.id {
                ColumnId::Version => TextCell::new_owned(
                    &self.details.last_update.version_num.to_string())
                    .with_style(&*style::GREEN),
                ColumnId::Created => TextCell::new_owned(
                    &self.details.last_update.created.format(DATE_FORMAT).to_string())
                    .with_style(&*style::YELLOW),
                ColumnId::LogicalPath =>TextCell::new_ref(&self.logical_path)
                    .with_style(&*style::BOLD),
                ColumnId::PhysicalPath => TextCell::new_ref(&self.details.storage_path),
                ColumnId::Digest => TextCell::new_owned(&format!("{}:{}",
                                                                 self.details.digest_algorithm.to_string(),
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
                    &self.version_details.version_num.to_string())
                    .with_style(&*style::GREEN),
                ColumnId::Created => TextCell::new_owned(
                    &self.version_details.created.format(DATE_FORMAT).to_string())
                    .with_style(&*style::YELLOW),
                ColumnId::ObjectId =>TextCell::new_ref(&self.id)
                    .with_style(&*style::BOLD),
                ColumnId::PhysicalPath => TextCell::new_ref(&self.object_root),
                _ => TextCell::blank()
            };

            cells.push(cell);
        }

        Row::new(cells)
    }
}
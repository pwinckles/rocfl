use std::cmp::Ordering;

use anyhow::{Context, Result};
use globset::GlobBuilder;

use crate::cmd::{DATE_FORMAT, print_err};
use crate::cmd::opts::*;
use crate::cmd::opts::{List, RocflArgs};
use crate::cmd::print::{Alignment, AsRow, Column, ColumnId, Row, TableView, TextCell};
use crate::ocfl::{FileDetails, ObjectVersionDetails, OcflRepo};

pub fn list_command(repo: &OcflRepo, command: &List, args: &RocflArgs) -> Result<()> {
    if command.objects || command.object_id.is_none() {
        list_objects(repo, command, args)
    } else {
        list_object_contents(repo, command)
    }
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

#[derive(Debug)]
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
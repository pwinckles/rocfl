use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;

use globset::GlobBuilder;

use crate::cmd::opts::{ListCmd, *};
use crate::cmd::table::{Alignment, AsRow, Column, ColumnId, Row, Separator, TableView, TextCell};
use crate::cmd::{style, Cmd, GlobalArgs, DATE_FORMAT};
use crate::ocfl::{
    FileDetails, InventoryPath, ObjectVersion, ObjectVersionDetails, OcflRepo, Result,
};

impl Cmd for ListCmd {
    fn exec(&self, repo: &OcflRepo, args: GlobalArgs) -> Result<()> {
        if self.objects || self.object_id.is_none() {
            self.list_objects(repo, args)
        } else {
            self.list_object_contents(repo, args)
        }
    }
}

impl ListCmd {
    fn list_objects(&self, repo: &OcflRepo, args: GlobalArgs) -> Result<()> {
        let iter = if self.staged {
            repo.list_staged_objects(self.object_id.as_deref())?
        } else {
            repo.list_objects(self.object_id.as_deref())?
        };

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
        let object = if self.staged {
            repo.get_staged_object(object_id)?
        } else {
            repo.get_object(object_id, self.version)?
        };

        let mut listings = self.filter_paths_to_listings(object)?;

        listings.sort_unstable_by(|a, b| {
            if self.reverse {
                cmp_listings(&self.sort, b, a)
            } else {
                cmp_listings(&self.sort, a, b)
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

        columns.push(Column::new(
            ColumnId::ObjectId,
            "Object ID",
            Alignment::Left,
        ));

        if self.physical {
            columns.push(Column::new(
                ColumnId::PhysicalPath,
                "Physical Path",
                Alignment::Left,
            ));
        }

        TableView::new(columns, self.separator(), self.header, !args.no_styles)
    }

    fn object_content_table(&self, args: GlobalArgs) -> TableView {
        let mut columns = Vec::new();

        if self.long {
            columns.push(Column::new(ColumnId::Version, "Version", Alignment::Right));
            columns.push(Column::new(ColumnId::Created, "Updated", Alignment::Left));
        }

        columns.push(Column::new(
            ColumnId::LogicalPath,
            "Logical Path",
            Alignment::Left,
        ));

        if self.physical {
            columns.push(Column::new(
                ColumnId::PhysicalPath,
                "Physical Path",
                Alignment::Left,
            ));
        }

        if self.digest {
            columns.push(Column::new(ColumnId::Digest, "Digest", Alignment::Left));
        }

        TableView::new(columns, self.separator(), self.header, !args.no_styles)
    }

    fn filter_paths_to_listings(&self, object: ObjectVersion) -> Result<Vec<Listing>> {
        let mut listings = Vec::new();

        let glob = match &self.path {
            Some(path) => {
                let trimmed = path.trim_start_matches('/');
                if trimmed.is_empty() {
                    "*".to_string()
                } else {
                    trimmed.to_string()
                }
            }
            None => "*".to_string(),
        };

        let glob_trailing_slash = glob.ends_with('/');

        let matcher = GlobBuilder::new(&glob)
            .literal_separator(self.logical_dirs)
            .backslash_escape(true)
            .build()?
            .compile_matcher();

        let logical_dirs = if self.logical_dirs {
            Some(create_logical_dirs(&object))
        } else {
            None
        };

        let mut not_matched = HashMap::new();

        for (path, details) in object.state {
            if matcher.is_match(path.as_ref().as_ref()) {
                listings.push(Listing::File(ContentListing {
                    logical_path: path.to_string(),
                    details,
                }));
            } else {
                not_matched.insert(path, details);
            }
        }

        if self.logical_dirs {
            let mut dir_matches = HashSet::new();
            let mut not_matched_dirs = HashSet::new();

            for dir in logical_dirs.unwrap() {
                if (glob_trailing_slash && matcher.is_match(format!("{}/", dir)))
                    || (!glob_trailing_slash && matcher.is_match(dir.as_ref()))
                {
                    dir_matches.insert(dir);
                } else {
                    not_matched_dirs.insert(dir);
                }
            }

            // If no files were matched and there is a single directory match, then expand the dir
            if listings.is_empty() && dir_matches.len() == 1 && glob != "*" {
                let sub_glob = if glob_trailing_slash {
                    format!("{}*", glob)
                } else {
                    format!("{}/*", glob)
                };

                let sub_matcher = GlobBuilder::new(&sub_glob)
                    .literal_separator(true)
                    .backslash_escape(true)
                    .build()?
                    .compile_matcher();

                for (path, details) in not_matched {
                    if sub_matcher.is_match(path.as_ref().as_ref()) {
                        listings.push(Listing::File(ContentListing {
                            logical_path: path.to_string(),
                            details,
                        }));
                    }
                }

                for dir in not_matched_dirs {
                    if sub_matcher.is_match(dir.as_ref()) {
                        listings.push(Listing::Dir(format!("{}/", dir)));
                    }
                }
            } else {
                for dir in dir_matches {
                    if !dir.as_ref().is_empty() {
                        listings.push(Listing::Dir(format!("{}/", dir)));
                    }
                }
            }
        }

        Ok(listings)
    }

    fn separator(&self) -> Separator {
        if self.tsv {
            Separator::TAB
        } else {
            Separator::SPACE
        }
    }
}

fn cmp_objects(field: &Field, a: &ObjectVersionDetails, b: &ObjectVersionDetails) -> Ordering {
    match field {
        Field::Name => natord::compare(&a.id, &b.id),
        Field::Version => a
            .version_details
            .version_num
            .cmp(&b.version_details.version_num),
        Field::Updated => a.version_details.created.cmp(&b.version_details.created),
        Field::Physical => a.object_root.cmp(&b.object_root),
        Field::Digest => Ordering::Equal,
        Field::None => Ordering::Equal,
    }
}

fn cmp_listings(field: &Field, a: &Listing, b: &Listing) -> Ordering {
    match (a, b) {
        (Listing::File(a), Listing::File(b)) => match field {
            Field::Name => natord::compare(&a.logical_path, &b.logical_path),
            Field::Version => a
                .details
                .last_update
                .version_num
                .cmp(&b.details.last_update.version_num),
            Field::Updated => a
                .details
                .last_update
                .created
                .cmp(&b.details.last_update.created),
            Field::Physical => natord::compare(&a.details.storage_path, &b.details.storage_path),
            Field::Digest => a.details.digest.cmp(&b.details.digest),
            Field::None => Ordering::Equal,
        },
        (Listing::File(a_file), Listing::Dir(b_dir)) => match field {
            Field::Name => natord::compare(&a_file.logical_path, b_dir),
            Field::None => Ordering::Equal,
            _ => Ordering::Greater,
        },
        (Listing::Dir(a_dir), Listing::Dir(b_dir)) => match field {
            Field::None => Ordering::Equal,
            _ => natord::compare(a_dir, b_dir),
        },
        (Listing::Dir(a_dir), Listing::File(b_file)) => match field {
            Field::Name => natord::compare(a_dir, &b_file.logical_path),
            Field::None => Ordering::Equal,
            _ => Ordering::Less,
        },
    }
}

fn create_logical_dirs(object: &ObjectVersion) -> HashSet<InventoryPath> {
    let mut dirs = HashSet::with_capacity(object.state.len());

    dirs.insert("".try_into().unwrap());

    for path in object.state.keys() {
        let mut parent = path.parent();
        while parent.as_ref() != "" {
            let next = parent.parent();
            dirs.insert(parent);
            parent = next;
        }
    }

    dirs
}

enum Listing {
    File(ContentListing),
    Dir(String),
}

struct ContentListing {
    logical_path: String,
    details: FileDetails,
}

impl<'a> AsRow<'a> for Listing {
    fn as_row(&'a self, columns: &[Column]) -> Row<'a> {
        match self {
            Listing::File(file) => file.as_row(columns),
            Listing::Dir(dir) => {
                let mut cells = Vec::new();

                for column in columns {
                    let cell = match column.id {
                        ColumnId::LogicalPath => {
                            TextCell::new(dir.as_str()).with_style(&*style::DEFAULT)
                        }
                        _ => TextCell::blank(),
                    };

                    cells.push(cell);
                }

                Row::new(cells)
            }
        }
    }
}

impl<'a> AsRow<'a> for ContentListing {
    fn as_row(&'a self, columns: &[Column]) -> Row<'a> {
        let mut cells = Vec::new();

        for column in columns {
            let cell = match column.id {
                ColumnId::Version => {
                    TextCell::new(self.details.last_update.version_num.to_string())
                        .with_style(&*style::GREEN)
                }
                ColumnId::Created => TextCell::new(
                    self.details
                        .last_update
                        .created
                        .format(DATE_FORMAT)
                        .to_string(),
                )
                .with_style(&*style::YELLOW),
                ColumnId::LogicalPath => {
                    TextCell::new(&self.logical_path).with_style(&*style::BOLD)
                }
                ColumnId::PhysicalPath => TextCell::new(&self.details.storage_path),
                ColumnId::Digest => TextCell::new(format!(
                    "{}:{}",
                    self.details.digest_algorithm.to_string(),
                    self.details.digest
                )),
                _ => TextCell::blank(),
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
                ColumnId::Version => TextCell::new(self.version_details.version_num.to_string())
                    .with_style(&*style::GREEN),
                ColumnId::Created => {
                    TextCell::new(self.version_details.created.format(DATE_FORMAT).to_string())
                        .with_style(&*style::YELLOW)
                }
                ColumnId::ObjectId => TextCell::new(&self.id).with_style(&*style::BOLD),
                ColumnId::PhysicalPath => TextCell::new(&self.object_root),
                _ => TextCell::blank(),
            };

            cells.push(cell);
        }

        Row::new(cells)
    }
}

use core::fmt;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::io::{self, Write};

use anyhow::Result;
use lazy_static::lazy_static;

use crate::cmd::DATE_FORMAT;
use crate::cmd::opts::{Diff, Log, Show};
use crate::ocfl::{Diff as VersionDiff, DiffType, OcflRepo, VersionDetails, VersionNum};

lazy_static! {
    static ref DEFAULT_USER: String = "NA".to_string();
}

pub fn log_command(repo: &OcflRepo, command: &Log) -> Result<()> {
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

pub fn show_command(repo: &OcflRepo, command: &Show) -> Result<()> {
    let object = repo.get_object_details(&command.object_id, command.version.as_ref())?;

    if !command.minimal {
        println(FormatVersion::new(&object.version_details, false));
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
        println(diff);
    }

    Ok(())
}

// https://github.com/rust-lang/rust/issues/46016
fn println(value: impl Display) {
    // Don't care about errors
    let _ = writeln!(io::stdout(), "{}", value);
}

#[derive(Debug)]
struct FormatVersion<'a> {
    version: &'a VersionDetails,
    compact: bool,
}

#[derive(Debug)]
struct DiffLine(VersionDiff);

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
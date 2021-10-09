use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::sync::atomic::AtomicBool;

use ansi_term::{ANSIGenericString, Style};

use crate::cmd::opts::ValidateCmd;
use crate::cmd::{print, println, style, Cmd, GlobalArgs};
use crate::config::Config;
use crate::ocfl::{
    ObjectValidationResult, OcflRepo, ProblemLocation, Result, StorageValidationResult,
    ValidationResult,
};

const UNKNOWN_ID: &str = "Unknown";

impl Cmd for ValidateCmd {
    fn exec(
        &self,
        repo: &OcflRepo,
        args: GlobalArgs,
        _config: &Config,
        _terminate: &AtomicBool,
    ) -> Result<()> {
        if !self.object_ids.is_empty() {
            let mut first = true;

            for object_id in &self.object_ids {
                let result = if self.paths {
                    repo.validate_object_at(object_id, !self.no_fixity_check)?
                } else {
                    repo.validate_object(object_id, !self.no_fixity_check)?
                };

                if first {
                    first = false;
                } else {
                    println("");
                }

                print(DisplayObjectValidationResult {
                    result: &result,
                    no_styles: args.no_styles,
                })
            }
        } else {
            let validator = repo.validate_repo(!self.no_fixity_check)?;

            print(DisplayStorageValidationResult {
                result: validator.storage_root_result(),
                location: "root",
                no_styles: args.no_styles,
            });

            for result in validator {
                println("");

                print(DisplayObjectValidationResult {
                    result: &result,
                    no_styles: args.no_styles,
                })
            }

            // TODO :(
            // print(DisplayStorageValidationResult{
            //     result: &validator.storage_hierarchy_result(),
            //     location: "hierarchy",
            //     no_styles: args.no_styles,
            // });

            // TODO display summary
        }

        // TODO different return code on error/warning?

        Ok(())
    }
}

trait Painter {
    fn no_styles(&self) -> bool;

    fn paint<'b, I, S: 'b + ToOwned + ?Sized>(
        &self,
        style: Style,
        text: I,
    ) -> ANSIGenericString<'b, S>
    where
        I: Into<Cow<'b, S>>,
        <S as ToOwned>::Owned: std::fmt::Debug,
    {
        if self.no_styles() {
            style::DEFAULT.paint(text)
        } else {
            style.paint(text)
        }
    }
}

struct DisplayStorageValidationResult<'a> {
    result: &'a StorageValidationResult,
    location: &'a str,
    no_styles: bool,
}

impl<'a> Painter for DisplayStorageValidationResult<'a> {
    fn no_styles(&self) -> bool {
        self.no_styles
    }
}

impl<'a> Display for DisplayStorageValidationResult<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.result.has_errors() || self.result.has_warnings() {
            writeln!(
                f,
                "{} {} has {} {} and {} {}",
                self.paint(*style::BOLD, "Storage"),
                self.paint(*style::BOLD, self.location),
                self.paint(*style::RED, self.result.errors().len().to_string()),
                self.paint(*style::RED, pluralize("error", self.result.errors())),
                self.paint(*style::YELLOW, self.result.warnings().len().to_string()),
                self.paint(*style::YELLOW, pluralize("warning", self.result.warnings()))
            )?;

            if self.result.has_errors() {
                writeln!(f, "  {}:", self.paint(*style::RED, "Errors"))?;
            }
            for (i, error) in self.result.errors().iter().enumerate() {
                writeln!(f, "    {}. [{}] {}", i + 1, error.code, error.text)?;
            }

            if self.result.has_warnings() {
                writeln!(f, "  {}:", self.paint(*style::YELLOW, "Warnings"))?;
            }
            for (i, warning) in self.result.warnings().iter().enumerate() {
                writeln!(f, "    {}. [{}] {}", i + 1, warning.code, warning.text)?;
            }
        } else {
            writeln!(
                f,
                "Storage {} is {}",
                self.location,
                self.paint(*style::GREEN, "valid")
            )?;
        }

        Ok(())
    }
}

struct DisplayObjectValidationResult<'a> {
    result: &'a ObjectValidationResult,
    no_styles: bool,
}

impl<'a> DisplayObjectValidationResult<'a> {
    fn display_object_id(&self) -> ANSIGenericString<'_, str> {
        self.paint(
            *style::BOLD,
            self.result
                .object_id
                .as_ref()
                .map(|id| id.as_ref())
                .unwrap_or(UNKNOWN_ID),
        )
    }
}

impl<'a> Painter for DisplayObjectValidationResult<'a> {
    fn no_styles(&self) -> bool {
        self.no_styles
    }
}

impl<'a> Display for DisplayObjectValidationResult<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.result.has_errors() || self.result.has_warnings() {
            writeln!(
                f,
                "Object {} has {} {} and {} {}",
                self.display_object_id(),
                self.paint(*style::RED, self.result.errors().len().to_string()),
                self.paint(*style::RED, pluralize("error", self.result.errors())),
                self.paint(*style::YELLOW, self.result.warnings().len().to_string()),
                self.paint(*style::YELLOW, pluralize("warning", self.result.warnings()))
            )?;

            if self.result.has_errors() {
                writeln!(f, "  {}:", self.paint(*style::RED, "Errors"))?;
            }
            for (i, error) in self.result.errors().iter().enumerate() {
                writeln!(
                    f,
                    "    {}. [{}] ({}) {}",
                    i + 1,
                    error.code,
                    display_location(error.location),
                    error.text
                )?;
            }

            if self.result.has_warnings() {
                writeln!(f, "  {}:", self.paint(*style::YELLOW, "Warnings"))?;
            }
            for (i, warning) in self.result.warnings().iter().enumerate() {
                writeln!(
                    f,
                    "    {}. [{}] ({}) {}",
                    i + 1,
                    warning.code,
                    display_location(warning.location),
                    warning.text
                )?;
            }
        } else {
            writeln!(
                f,
                "Object {} is {}",
                self.display_object_id(),
                self.paint(*style::GREEN, "valid")
            )?;
        }

        Ok(())
    }
}

fn display_location(location: ProblemLocation) -> String {
    match location {
        ProblemLocation::ObjectRoot => "root".to_string(),
        ProblemLocation::ObjectVersion(num) => num.to_string(),
        ProblemLocation::StorageRoot => "storage-root".to_string(),
        ProblemLocation::StorageHierarchy => "hierarchy".to_string(),
    }
}

fn pluralize<'a, 'b, T>(word: &'a str, list: &'b [T]) -> Cow<'a, str> {
    if list.len() == 1 {
        word.into()
    } else {
        format!("{}s", word).into()
    }
}

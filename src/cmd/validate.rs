use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::process;
use std::sync::atomic::AtomicBool;

use ansi_term::{ANSIGenericString, Style};
use log::error;

use crate::cmd::opts::{Level, ValidateCmd};
use crate::cmd::{paint, print, println, style, Cmd, GlobalArgs};
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
        terminate: &AtomicBool,
    ) -> Result<()> {
        // TODO perhaps use something like https://crates.io/crates/console to update the display

        if !self.object_ids.is_empty() {
            self.validate_objects(repo, args, terminate)?;
        } else {
            self.validate_repo(repo, args, terminate)?;
        }

        Ok(())
    }
}

impl ValidateCmd {
    fn validate_objects(
        &self,
        repo: &OcflRepo,
        args: GlobalArgs,
        _terminate: &AtomicBool,
    ) -> Result<()> {
        let mut has_printed = false;
        let mut obj_count = 0;
        let mut invalid_count = 0;
        let mut error_validating = false;

        for object_id in &self.object_ids {
            let mut result = if self.paths {
                match repo.validate_object_at(object_id, !self.no_fixity_check) {
                    Ok(result) => result,
                    Err(e) => {
                        error_validating = true;
                        error!("{:#}", e);
                        continue;
                    }
                }
            } else {
                match repo.validate_object(object_id, !self.no_fixity_check) {
                    Ok(result) => result,
                    Err(e) => {
                        error_validating = true;
                        error!("{:#}", e);
                        continue;
                    }
                }
            };

            self.suppress_errors_warnings(&mut result);

            obj_count += 1;
            if result.has_errors() {
                invalid_count += 1;
            }

            if self.should_print(&result) {
                if has_printed {
                    println("");
                } else {
                    has_printed = true;
                }

                print(DisplayObjectValidationResult {
                    result: &result,
                    no_styles: args.no_styles,
                    level: self.level,
                })
            }
        }

        if self.object_ids.len() > 1 {
            if has_printed {
                println("");
            }

            println(paint(args.no_styles, *style::BOLD, "Summary:"));
            println(format!("  Total objects:   {}", obj_count));
            println(format!("  Invalid objects: {}", invalid_count));
        }

        if invalid_count > 0 {
            process::exit(2);
        } else if error_validating {
            process::exit(1);
        }

        Ok(())
    }

    fn validate_repo(
        &self,
        repo: &OcflRepo,
        args: GlobalArgs,
        _terminate: &AtomicBool,
    ) -> Result<()> {
        let mut validator = repo.validate_repo(!self.no_fixity_check)?;

        let mut obj_count = 0;
        let mut invalid_count = 0;
        let mut has_printed = false;
        let mut error_validating = false;

        self.suppress_errors_warnings(validator.storage_hierarchy_result_mut());

        if self.should_print(validator.storage_root_result()) {
            has_printed = true;
            print(DisplayStorageValidationResult {
                result: validator.storage_root_result(),
                location: "root",
                no_styles: args.no_styles,
                level: self.level,
            });
        }

        for result in &mut validator {
            match result {
                Ok(mut result) => {
                    self.suppress_errors_warnings(&mut result);

                    obj_count += 1;
                    if result.has_errors() {
                        invalid_count += 1;
                    }

                    if self.should_print(&result) {
                        if has_printed {
                            println("");
                        } else {
                            has_printed = true;
                        }

                        print(DisplayObjectValidationResult {
                            result: &result,
                            no_styles: args.no_styles,
                            level: self.level,
                        })
                    }
                }
                Err(e) => {
                    error_validating = true;
                    error!("{:#}", e);
                    continue;
                }
            }
        }

        self.suppress_errors_warnings(validator.storage_hierarchy_result_mut());

        if self.should_print(validator.storage_hierarchy_result()) {
            if has_printed {
                println("");
            } else {
                has_printed = true;
            }

            print(DisplayStorageValidationResult {
                result: validator.storage_hierarchy_result(),
                location: "hierarchy",
                no_styles: args.no_styles,
                level: self.level,
            });
        }

        let storage_errors = validator.storage_root_result().errors().len()
            + validator.storage_hierarchy_result().errors().len();

        if has_printed {
            println("");
        }

        println(paint(args.no_styles, *style::BOLD, "Summary:"));
        println(format!("  Total objects:   {}", obj_count));
        println(format!("  Invalid objects: {}", invalid_count));
        println(format!("  Storage issues:  {}", storage_errors));

        if invalid_count > 0 || storage_errors > 0 {
            process::exit(2);
        } else if error_validating {
            process::exit(1);
        }

        Ok(())
    }

    fn should_print<T: ValidationResult>(&self, result: &T) -> bool {
        result.has_errors()
            || (result.has_warnings() && self.level != Level::Error)
            || self.level == Level::Info
    }

    /// Removes errors and warnings from a validation result if the user indicated they should be
    /// suppressed
    fn suppress_errors_warnings<T: ValidationResult>(&self, result: &mut T) {
        result
            .errors_mut()
            .retain(|e| !self.suppress_error.contains(&e.code));
        result
            .warnings_mut()
            .retain(|w| !self.suppress_warning.contains(&w.code));
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
    level: Level,
}

impl<'a> Painter for DisplayStorageValidationResult<'a> {
    fn no_styles(&self) -> bool {
        self.no_styles
    }
}

impl<'a> Display for DisplayStorageValidationResult<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.result.has_errors_or_warnings() {
            if self.result.has_errors() {
                writeln!(
                    f,
                    "{} {} is {}",
                    self.paint(*style::BOLD, "Storage"),
                    self.paint(*style::BOLD, self.location),
                    self.paint(*style::RED, "invalid")
                )?;
            } else {
                writeln!(
                    f,
                    "{} {} is {}",
                    self.paint(*style::BOLD, "Storage"),
                    self.paint(*style::BOLD, self.location),
                    self.paint(*style::YELLOW, "valid with warnings")
                )?;
            }

            let error_width = count_digits(self.result.errors().len());
            let warning_width = count_digits(self.result.warnings().len());

            if self.result.has_errors() {
                writeln!(f, "  {}:", self.paint(*style::RED, "Errors"))?;
            }
            for (i, error) in self.result.errors().iter().enumerate() {
                writeln!(
                    f,
                    "    {:width$}. [{}] {}",
                    i + 1,
                    error.code,
                    error.text,
                    width = error_width
                )?;
            }

            if self.level != Level::Error {
                if self.result.has_warnings() {
                    writeln!(f, "  {}:", self.paint(*style::YELLOW, "Warnings"))?;
                }
                for (i, warning) in self.result.warnings().iter().enumerate() {
                    writeln!(
                        f,
                        "    {:width$}. [{}] {}",
                        i + 1,
                        warning.code,
                        warning.text,
                        width = warning_width
                    )?;
                }
            }
        } else {
            writeln!(
                f,
                "{} {} is {}",
                self.paint(*style::BOLD, "Storage"),
                self.paint(*style::BOLD, self.location),
                self.paint(*style::GREEN, "valid")
            )?;
        }

        Ok(())
    }
}

struct DisplayObjectValidationResult<'a> {
    result: &'a ObjectValidationResult,
    no_styles: bool,
    level: Level,
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
        if self.result.has_errors_or_warnings() {
            if self.result.has_errors() {
                writeln!(
                    f,
                    "Object {} is {}",
                    self.display_object_id(),
                    self.paint(*style::RED, "invalid")
                )?;
            } else {
                writeln!(
                    f,
                    "Object {} is {}",
                    self.display_object_id(),
                    self.paint(*style::YELLOW, "valid with warnings")
                )?;
            }

            let error_width = count_digits(self.result.errors().len());
            let warning_width = count_digits(self.result.warnings().len());

            if self.result.has_errors() {
                writeln!(f, "  {}:", self.paint(*style::RED, "Errors"))?;
            }
            for (i, error) in self.result.errors().iter().enumerate() {
                writeln!(
                    f,
                    "    {:width$}. [{}] ({}) {}",
                    i + 1,
                    error.code,
                    display_location(error.location),
                    error.text,
                    width = error_width
                )?;
            }

            if self.level != Level::Error {
                if self.result.has_warnings() {
                    writeln!(f, "  {}:", self.paint(*style::YELLOW, "Warnings"))?;
                }
                for (i, warning) in self.result.warnings().iter().enumerate() {
                    writeln!(
                        f,
                        "    {:width$}. [{}] ({}) {}",
                        i + 1,
                        warning.code,
                        display_location(warning.location),
                        warning.text,
                        width = warning_width
                    )?;
                }
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

fn count_digits(num: usize) -> usize {
    let mut n = 1;
    let mut num = num;

    if num >= 100_000_000 {
        n += 8;
        num /= 100_000_000;
    }
    if num >= 10_000 {
        n += 4;
        num /= 10_000;
    }
    if num >= 100 {
        n += 2;
        num /= 100;
    }
    if num >= 10 {
        n += 1;
    }

    n
}

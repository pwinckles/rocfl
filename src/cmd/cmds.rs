use std::convert::TryInto;
use std::io;
use std::sync::atomic::AtomicBool;

use log::info;

use crate::cmd::opts::{
    CatCmd, CommitCmd, ConfigCmd, CopyCmd, DigestAlgorithm as OptAlgorithm, Field, InitCmd,
    ListCmd, MoveCmd, NewCmd, PurgeCmd, RemoveCmd, ResetCmd, ShowCmd, StatusCmd, ValidateCmd,
};
use crate::cmd::{print, println, Cmd, GlobalArgs};
use crate::config::Config;
use crate::ocfl::{CommitMeta, DigestAlgorithm, OcflRepo, Result};

impl Cmd for CatCmd {
    fn exec(
        &self,
        repo: &OcflRepo,
        _args: GlobalArgs,
        _config: &Config,
        _terminate: &AtomicBool,
    ) -> Result<()> {
        if self.staged {
            repo.get_staged_object_file(
                &self.object_id,
                &self.path.as_str().try_into()?,
                &mut io::stdout(),
            )
        } else {
            repo.get_object_file(
                &self.object_id,
                &self.path.as_str().try_into()?,
                self.version.into(),
                &mut io::stdout(),
            )
        }
    }
}

/// This is needed to keep enum_dispatch happy
impl Cmd for InitCmd {
    fn exec(
        &self,
        _repo: &OcflRepo,
        _args: GlobalArgs,
        _config: &Config,
        _terminate: &AtomicBool,
    ) -> Result<()> {
        unimplemented!()
    }
}

/// This is needed to keep enum_dispatch happy
impl Cmd for ConfigCmd {
    fn exec(
        &self,
        _repo: &OcflRepo,
        _args: GlobalArgs,
        _config: &Config,
        _terminate: &AtomicBool,
    ) -> Result<()> {
        unimplemented!()
    }
}

impl Cmd for NewCmd {
    fn exec(
        &self,
        repo: &OcflRepo,
        _args: GlobalArgs,
        _config: &Config,
        _terminate: &AtomicBool,
    ) -> Result<()> {
        repo.create_object(
            &self.object_id,
            algorithm(self.digest_algorithm),
            &self.content_directory,
            self.zero_padding,
        )?;

        info!("Staged new OCFL object {}", self.object_id);

        Ok(())
    }
}

impl Cmd for CopyCmd {
    fn exec(
        &self,
        repo: &OcflRepo,
        _args: GlobalArgs,
        _config: &Config,
        _terminate: &AtomicBool,
    ) -> Result<()> {
        if self.internal {
            repo.copy_files_internal(
                &self.object_id,
                self.version.into(),
                &self.source,
                &self.destination,
                self.recursive,
            )
        } else {
            repo.copy_files_external(
                &self.object_id,
                &self.source,
                &self.destination,
                self.recursive,
            )
        }
    }
}

impl Cmd for MoveCmd {
    fn exec(
        &self,
        repo: &OcflRepo,
        _args: GlobalArgs,
        _config: &Config,
        _terminate: &AtomicBool,
    ) -> Result<()> {
        if self.internal {
            repo.move_files_internal(&self.object_id, &self.source, &self.destination)
        } else {
            repo.move_files_external(&self.object_id, &self.source, &self.destination)
        }
    }
}

impl Cmd for RemoveCmd {
    fn exec(
        &self,
        repo: &OcflRepo,
        _args: GlobalArgs,
        _config: &Config,
        _terminate: &AtomicBool,
    ) -> Result<()> {
        repo.remove_files(&self.object_id, &self.paths, self.recursive)
    }
}

impl Cmd for ResetCmd {
    fn exec(
        &self,
        repo: &OcflRepo,
        _args: GlobalArgs,
        _config: &Config,
        _terminate: &AtomicBool,
    ) -> Result<()> {
        if !self.paths.is_empty() {
            repo.reset(&self.object_id, &self.paths, self.recursive)
        } else {
            repo.reset_all(&self.object_id)
        }
    }
}

impl Cmd for CommitCmd {
    fn exec(
        &self,
        repo: &OcflRepo,
        _args: GlobalArgs,
        config: &Config,
        _terminate: &AtomicBool,
    ) -> Result<()> {
        let meta = CommitMeta::new()
            .with_user(config.author_name.clone(), config.author_address.clone())?
            .with_message(self.message.clone())
            .with_created(self.created);
        repo.commit(
            &self.object_id,
            meta,
            self.object_root.as_ref().map(|r| r.as_ref()),
            self.pretty_print,
        )?;

        Ok(())
    }
}

impl Cmd for StatusCmd {
    fn exec(
        &self,
        repo: &OcflRepo,
        args: GlobalArgs,
        config: &Config,
        terminate: &AtomicBool,
    ) -> Result<()> {
        if let Some(object_id) = self.object_id.as_ref() {
            let cmd = ShowCmd {
                object_id: object_id.to_string(),
                version: None,
                staged: true,
                minimal: false,
            };
            cmd.exec(repo, args, config, terminate)
        } else {
            let cmd = ListCmd {
                object_id: None,
                version: None,
                path: None,
                staged: true,
                logical_dirs: false,
                digest: false,
                objects: false,
                header: true,
                long: true,
                reverse: false,
                physical: false,
                tsv: false,
                sort: Field::Name,
            };

            cmd.exec(repo, args, config, terminate)
        }
    }
}

impl Cmd for PurgeCmd {
    fn exec(
        &self,
        repo: &OcflRepo,
        _args: GlobalArgs,
        _config: &Config,
        _terminate: &AtomicBool,
    ) -> Result<()> {
        if !self.force {
            print(format!(
                "Permanently delete '{}'? This cannot be undone. [y/N]: ",
                self.object_id
            ));
            let mut response = String::new();
            io::stdin().read_line(&mut response)?;
            if !response.trim().eq_ignore_ascii_case("y") {
                println("Aborted");
                return Ok(());
            }
        }

        repo.purge_object(&self.object_id)
    }
}

impl Cmd for ValidateCmd {
    fn exec(
        &self,
        repo: &OcflRepo,
        _args: GlobalArgs,
        _config: &Config,
        _terminate: &AtomicBool,
    ) -> Result<()> {
        if let Some(object_id) = &self.object_id {
            let result = repo.validate_object(object_id, self.no_fixity_check)?;

            fn format_version(version: &Option<String>) -> String {
                match version {
                    Some(version) => format!(" ({})", version),
                    None => "".to_string(),
                }
            }

            // TODO use error/warn?
            if result.has_errors() || result.has_warnings() {
                // TODO pluralization
                if !result.has_errors() {
                    println(format!("Object {} has {} warnings", object_id, result.warnings.len()));
                } else {
                    println(format!(
                        "Object {} has {} errors and {} warnings",
                        object_id,
                        result.errors.len(),
                        result.warnings.len()
                    ));
                }

                if result.has_errors() {
                    println("Errors:");
                }
                result.errors.iter().enumerate().for_each(|(i, error)| {
                    // TODO this should probably have Display
                    println(format!(
                        "  {}. [{}]{} {}",
                        i+1,
                        error.code,
                        format_version(&error.version_num),
                        error.text
                    ));
                });

                if result.has_warnings() {
                    println("Warnings:");
                }
                result.warnings.iter().enumerate().for_each(|(i, warning)| {
                    // TODO this should probably have Display
                    println(format!(
                        "  {}. [{}]{} {}",
                        i+1,
                        warning.code,
                        format_version(&warning.version_num),
                        warning.text
                    ));
                });

                // TODO different return code?
            } else {
                // TODO I think there's a clever way around the string formatting issue in the book
                println(format!("Object {} is valid", object_id));
            }
        } else {
            todo!()
        }

        Ok(())
    }
}

fn algorithm(algorithm: OptAlgorithm) -> DigestAlgorithm {
    match algorithm {
        OptAlgorithm::Sha256 => DigestAlgorithm::Sha256,
        OptAlgorithm::Sha512 => DigestAlgorithm::Sha512,
    }
}

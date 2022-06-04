use std::convert::TryInto;
use std::io;
use std::io::{BufWriter, Write};
use std::sync::atomic::AtomicBool;

use log::info;

use crate::cmd::opts::{
    CatCmd, CommitCmd, ConfigCmd, CopyCmd, DigestAlgorithm as OptAlgorithm, Field, InfoCmd,
    InitCmd, ListCmd, MoveCmd, NewCmd, PurgeCmd, RemoveCmd, ResetCmd, ShowCmd, StatusCmd,
    UpgradeCmd,
};
use crate::cmd::{map_spec_version, println, style, Cmd, GlobalArgs};
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
            self.spec_version.map(map_spec_version),
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

impl Cmd for UpgradeCmd {
    fn exec(
        &self,
        repo: &OcflRepo,
        args: GlobalArgs,
        config: &Config,
        _terminate: &AtomicBool,
    ) -> Result<()> {
        if let Some(object_id) = &self.object_id {
            let meta = CommitMeta::new()
                .with_user(config.author_name.clone(), config.author_address.clone())?
                .with_message(self.message.clone())
                .with_created(self.created);
            repo.upgrade_object(
                object_id,
                map_spec_version(self.spec_version),
                meta,
                self.pretty_print,
            )?;
            Ok(())
        } else {
            repo.upgrade_repo(map_spec_version(self.spec_version))?;
            if !args.quiet {
                println(format!("Upgraded OCFL repository to {}", self.spec_version));
            }
            Ok(())
        }
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
        let mut out = BufWriter::new(io::stdout());

        if !self.force {
            let _ = write!(
                out,
                "Permanently delete '{}'? This cannot be undone. [y/N]: ",
                self.object_id
            );
            let _ = out.flush();
            let mut response = String::new();
            io::stdin().read_line(&mut response)?;
            if !response.trim().eq_ignore_ascii_case("y") {
                let _ = writeln!(out, "Aborted");
                return Ok(());
            }
        }

        repo.purge_object(&self.object_id)
    }
}

impl Cmd for InfoCmd {
    fn exec(
        &self,
        repo: &OcflRepo,
        args: GlobalArgs,
        _config: &Config,
        _terminate: &AtomicBool,
    ) -> Result<()> {
        let style = if args.no_styles {
            &*style::DEFAULT
        } else {
            &*style::BOLD
        };

        if let Some(object_id) = self.object_id.as_deref() {
            let mut info = if self.staged {
                repo.describe_staged_object(object_id)?
            } else {
                repo.describe_object(object_id)?
            };

            let mut out = BufWriter::new(io::stdout());

            let _ = writeln!(
                out,
                "{}     {}",
                style.paint("Spec Version:"),
                info.spec_version
            );
            let _ = writeln!(
                out,
                "{} {}",
                style.paint("Digest Algorithm:"),
                info.digest_algorithm
                    .unwrap_or_else(|| "unknown".to_string())
            );

            if info.extensions.is_empty() {
                let _ = writeln!(out, "{}       none", style.paint("Extensions:"));
            } else {
                info.extensions.sort();
                let _ = writeln!(out, "{}", style.paint("Extensions:"));
                for extension in info.extensions {
                    let _ = writeln!(out, "  {}", extension);
                }
            }

            out.flush()?;
        } else {
            let mut info = repo.describe_repo()?;

            let mut out = BufWriter::new(io::stdout());
            let _ = writeln!(
                out,
                "{}   {}",
                style.paint("Spec Version:"),
                info.spec_version
            );
            let _ = writeln!(
                out,
                "{} {}",
                style.paint("Storage Layout:"),
                info.layout.unwrap_or_else(|| "unknown".to_string())
            );

            if info.extensions.is_empty() {
                let _ = writeln!(out, "{}     none", style.paint("Extensions:"));
            } else {
                info.extensions.sort();
                let _ = writeln!(out, "{}", style.paint("Extensions:"));
                for extension in info.extensions {
                    let _ = writeln!(out, "  {}", extension);
                }
            }

            out.flush()?;
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

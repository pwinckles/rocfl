use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use directories::ProjectDirs;
use serde::Deserialize;

use crate::ocfl::{DigestAlgorithm, Result, RocflError};

const CONFIG_FILE: &str = "config.toml";
const GLOBAL: &str = "global";

/// Representation of user configuration
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub author_name: Option<String>,
    pub author_address: Option<String>,
    pub root: Option<String>,
    pub staging_root: Option<String>,
    pub region: Option<String>,
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub profile: Option<String>,
}

impl Config {
    pub fn new() -> Self {
        Self {
            author_name: None,
            author_address: None,
            root: None,
            staging_root: None,
            region: None,
            bucket: None,
            endpoint: None,
            profile: None,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.bucket.is_some() {
            if self.region.is_none() {
                return Err(RocflError::InvalidConfiguration(
                    "Region must be specified when using S3".to_string(),
                ));
            }
        } else if self.region.is_some() || self.endpoint.is_some() {
            return Err(RocflError::InvalidConfiguration(
                "Region and endpoint should not be set when not using S3. \
                If you intended to use S3, then you must specify a bucket."
                    .to_string(),
            ));
        }

        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

/// Parses the user's rocfl config, if it exists
pub fn load_config(name: &Option<String>) -> Result<Config> {
    if let Some(config_file) = config_path() {
        if config_file.exists() {
            let config = parse_config(&config_file)?;
            return Ok(resolve_config(name, config));
        }
    }
    Ok(Config::new())
}

/// The path to the rocfl config file, or None if the config directory cannot be resolved.
/// The file may not exist.
pub fn config_path() -> Option<PathBuf> {
    project_dirs().map(|dirs| dirs.config_dir().join(CONFIG_FILE))
}

/// Reference to the rocfl project directories. These directories do **not** necessarily exist
pub fn project_dirs() -> Option<ProjectDirs> {
    ProjectDirs::from("org", "rocfl", "rocfl")
}

/// Constructs a path to the default S3 staging location. This function should only be called
/// when bucket is set. The path is **not** created
pub fn s3_staging_path(config: &Config) -> Result<String> {
    match project_dirs() {
        Some(dirs) => {
            let mut staging = dirs.data_dir().join("s3").join("staging");
            staging.push(s3_identifier(config)?);
            Ok(staging.to_string_lossy().to_string())
        }
        None => Err(RocflError::General(
            "Failed to locate a suitable directory for staging objects. Please specify a directory using '--staging-root'".to_string()))
    }
}

fn s3_identifier(config: &Config) -> Result<String> {
    let mut name = config.bucket.clone().unwrap();
    if let Some(root) = &config.root {
        name.push('/');
        name.push_str(root);
    }
    let hash = DigestAlgorithm::Sha256.hash_hex(&mut name.as_bytes())?;
    Ok(hash.to_string())
}

fn parse_config(config_file: impl AsRef<Path>) -> Result<HashMap<String, Config>> {
    let mut buffer = Vec::new();
    fs::File::open(config_file.as_ref())?.read_to_end(&mut buffer)?;
    let config: HashMap<String, Config> = toml::from_slice(&buffer)?;
    Ok(config)
}

fn resolve_config(name: &Option<String>, mut config: HashMap<String, Config>) -> Config {
    let global_config = config.remove(GLOBAL);
    let repo_config = match name {
        None => None,
        Some(name) => config.remove(name),
    };

    match (global_config, repo_config) {
        (Some(global), None) => global,
        (None, Some(repo)) => repo,
        (None, None) => Config::new(),
        (Some(global), Some(repo)) => {
            let mut resolved = Config::new();

            resolved.author_name = resolve_field(global.author_name, repo.author_name);
            resolved.author_address = resolve_field(global.author_address, repo.author_address);
            resolved.root = resolve_field(global.root, repo.root);
            resolved.staging_root = resolve_field(global.staging_root, repo.staging_root);
            resolved.region = resolve_field(global.region, repo.region);
            resolved.bucket = resolve_field(global.bucket, repo.bucket);
            resolved.endpoint = resolve_field(global.endpoint, repo.endpoint);
            resolved.profile = resolve_field(global.profile, repo.profile);

            resolved
        }
    }
}

fn resolve_field(global_field: Option<String>, repo_field: Option<String>) -> Option<String> {
    if repo_field.is_some() {
        repo_field
    } else {
        global_field
    }
}

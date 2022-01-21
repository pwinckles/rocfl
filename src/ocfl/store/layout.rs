//! OCFL storage layout extension implementations

use std::borrow::Cow;

use once_cell::sync::Lazy;
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde::{Deserialize, Serialize};
use strum_macros::{Display as EnumDisplay, EnumString};

use crate::ocfl::digest::DigestAlgorithm;
use crate::ocfl::error::{Result, RocflError};

const MAX_0003_ENCAPSULATION_LENGTH: usize = 100;

static NON_ALPHA_PLUS: Lazy<AsciiSet> = Lazy::new(|| NON_ALPHANUMERIC.remove(b'-').remove(b'_'));

/// The storage layout maps object IDs to locations within the storage root
#[derive(Debug)]
pub struct StorageLayout {
    extension: LayoutExtension,
}

/// Enum of known storage layout extensions
#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, EnumString, EnumDisplay)]
pub enum LayoutExtensionName {
    #[strum(serialize = "0002-flat-direct-storage-layout")]
    #[serde(rename = "0002-flat-direct-storage-layout")]
    FlatDirectLayout,
    #[strum(serialize = "0004-hashed-n-tuple-storage-layout")]
    #[serde(rename = "0004-hashed-n-tuple-storage-layout")]
    HashedNTupleLayout,
    #[strum(serialize = "0003-hash-and-id-n-tuple-storage-layout")]
    #[serde(rename = "0003-hash-and-id-n-tuple-storage-layout")]
    HashedNTupleObjectIdLayout,
    #[strum(serialize = "0006-flat-omit-prefix-storage-layout")]
    #[serde(rename = "0006-flat-omit-prefix-storage-layout")]
    FlatOmitPrefixLayout,
    #[strum(serialize = "0007-n-tuple-omit-prefix-storage-layout")]
    #[serde(rename = "0007-n-tuple-omit-prefix-storage-layout")]
    NTupleOmitPrefixLayout,
}

impl StorageLayout {
    pub fn new(name: LayoutExtensionName, config_bytes: Option<&[u8]>) -> Result<Self> {
        let attempt = || -> Result<LayoutExtension> {
            match name {
                LayoutExtensionName::FlatDirectLayout => {
                    Ok(FlatDirectLayoutExtension::new(config_bytes)?.into())
                }
                LayoutExtensionName::HashedNTupleLayout => {
                    Ok(HashedNTupleLayoutExtension::new(config_bytes)?.into())
                }
                LayoutExtensionName::HashedNTupleObjectIdLayout => {
                    Ok(HashedNTupleObjectIdLayoutExtension::new(config_bytes)?.into())
                }
                LayoutExtensionName::FlatOmitPrefixLayout => {
                    Ok(FlatOmitPrefixLayoutExtension::new(config_bytes)?.into())
                }
                LayoutExtensionName::NTupleOmitPrefixLayout => {
                    Ok(NTupleOmitPrefixLayoutExtension::new(config_bytes)?.into())
                }
            }
        };

        match attempt() {
            Ok(extension) => Ok(StorageLayout { extension }),
            Err(e) => Err(RocflError::General(format!(
                "Failed to parse layout config: {}",
                e
            ))),
        }
    }

    /// Maps an object ID to an object root directory
    pub fn map_object_id(&self, object_id: &str) -> String {
        self.extension.map_object_id(object_id)
    }

    /// Returns the extension name of the layout extension in use
    pub fn extension_name(&self) -> LayoutExtensionName {
        self.extension.extension_name()
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        self.extension.serialize()
    }
}

/// [Flat Direct Storage Layout Extension](https://ocfl.github.io/extensions/0002-flat-direct-storage-layout.html)
#[derive(Debug)]
struct FlatDirectLayoutExtension {
    config: FlatDirectLayoutConfig,
}

/// [Hashed N-Tuple Storage Layout Extension](https://ocfl.github.io/extensions/0004-hashed-n-tuple-storage-layout.html)
#[derive(Debug)]
struct HashedNTupleLayoutExtension {
    config: HashedNTupleLayoutConfig,
}

/// [Hashed N-Tuple with Object ID Encapsulation Storage Layout Extension](https://ocfl.github.io/extensions/0003-hash-and-id-n-tuple-storage-layout.html)
#[derive(Debug)]
struct HashedNTupleObjectIdLayoutExtension {
    config: HashedNTupleObjectIdLayoutConfig,
}

/// [Flat Omit Prefix Storage Layout Extension](https://ocfl.github.io/extensions/0006-flat-omit-prefix-storage-layout.html)
#[derive(Debug)]
struct FlatOmitPrefixLayoutExtension {
    config: FlatOmitPrefixLayoutConfig,
    case_matters: bool,
    normalized_delimiter: String,
}

/// [N Tuple Omit Prefix Storage Layout Extension](https://ocfl.github.io/extensions/0007-n-tuple-omit-prefix-storage-layout.html)
#[derive(Debug)]
struct NTupleOmitPrefixLayoutExtension {
    config: NTupleOmitPrefixLayoutConfig,
    case_matters: bool,
    normalized_delimiter: String,
    width: usize,
}

/// [Flat Direct Storage Layout Config](https://ocfl.github.io/extensions/0002-flat-direct-storage-layout.html)
#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase", default)]
struct FlatDirectLayoutConfig {
    extension_name: LayoutExtensionName,
}

/// [Hashed N-Tuple Storage Layout Config](https://ocfl.github.io/extensions/0004-hashed-n-tuple-storage-layout.html)
#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase", default)]
struct HashedNTupleLayoutConfig {
    extension_name: LayoutExtensionName,

    #[serde(default = "default_algorithm")]
    digest_algorithm: DigestAlgorithm,

    #[serde(default = "default_tuple")]
    tuple_size: usize,

    #[serde(default = "default_tuple")]
    number_of_tuples: usize,

    #[serde(default = "default_short_root")]
    short_object_root: bool,
}

/// [Hashed N-Tuple with Object ID Encapsulation Storage Layout Config](https://ocfl.github.io/extensions/0003-hash-and-id-n-tuple-storage-layout.html)
#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase", default)]
struct HashedNTupleObjectIdLayoutConfig {
    extension_name: LayoutExtensionName,

    #[serde(default = "default_algorithm")]
    digest_algorithm: DigestAlgorithm,

    #[serde(default = "default_tuple")]
    tuple_size: usize,

    #[serde(default = "default_tuple")]
    number_of_tuples: usize,
}

/// [Flat Omit Prefix Storage Layout Config](https://ocfl.github.io/extensions/0006-flat-omit-prefix-storage-layout.html)
#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct FlatOmitPrefixLayoutConfig {
    extension_name: LayoutExtensionName,
    delimiter: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, EnumString, EnumDisplay)]
enum Padding {
    #[strum(serialize = "left")]
    #[serde(rename = "left")]
    Left,
    #[strum(serialize = "right")]
    #[serde(rename = "right")]
    Right,
}

/// [N Tuple Omit Prefix Storage Layout](https://ocfl.github.io/extensions/0007-n-tuple-omit-prefix-storage-layout.html)
#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct NTupleOmitPrefixLayoutConfig {
    extension_name: LayoutExtensionName,
    delimiter: String,

    #[serde(default = "default_tuple")]
    tuple_size: usize,

    #[serde(default = "default_tuple")]
    number_of_tuples: usize,

    #[serde(default = "default_padding")]
    zero_padding: Padding,

    #[serde(default = "default_reverse")]
    reverse_object_root: bool,
}

#[derive(Debug)]
enum LayoutExtension {
    FlatDirect(FlatDirectLayoutExtension),
    HashedNTuple(HashedNTupleLayoutExtension),
    HashedNTupleObjectId(HashedNTupleObjectIdLayoutExtension),
    FlatOmitPrefix(FlatOmitPrefixLayoutExtension),
    NTupleOmitPrefix(NTupleOmitPrefixLayoutExtension),
}

impl FlatDirectLayoutConfig {
    fn validate(&self) -> Result<()> {
        validate_extension_name(&LayoutExtensionName::FlatDirectLayout, &self.extension_name)
    }
}

impl Default for FlatDirectLayoutConfig {
    fn default() -> Self {
        Self {
            extension_name: LayoutExtensionName::FlatDirectLayout,
        }
    }
}

impl HashedNTupleLayoutConfig {
    fn validate(&self) -> Result<()> {
        validate_extension_name(
            &LayoutExtensionName::HashedNTupleLayout,
            &self.extension_name,
        )?;
        validate_tuple_config(self.tuple_size, self.number_of_tuples)?;
        validate_digest_algorithm(
            self.digest_algorithm,
            self.tuple_size,
            self.number_of_tuples,
        )
    }
}

impl Default for HashedNTupleLayoutConfig {
    fn default() -> Self {
        Self {
            extension_name: LayoutExtensionName::HashedNTupleLayout,
            digest_algorithm: DigestAlgorithm::Sha256,
            tuple_size: 3,
            number_of_tuples: 3,
            short_object_root: false,
        }
    }
}

impl HashedNTupleObjectIdLayoutConfig {
    fn validate(&self) -> Result<()> {
        validate_extension_name(
            &LayoutExtensionName::HashedNTupleObjectIdLayout,
            &self.extension_name,
        )?;
        validate_tuple_config(self.tuple_size, self.number_of_tuples)?;
        validate_digest_algorithm(
            self.digest_algorithm,
            self.tuple_size,
            self.number_of_tuples,
        )
    }
}

impl Default for HashedNTupleObjectIdLayoutConfig {
    fn default() -> Self {
        Self {
            extension_name: LayoutExtensionName::HashedNTupleObjectIdLayout,
            digest_algorithm: DigestAlgorithm::Sha256,
            tuple_size: 3,
            number_of_tuples: 3,
        }
    }
}

impl FlatOmitPrefixLayoutConfig {
    fn validate(&self) -> Result<()> {
        validate_extension_name(
            &LayoutExtensionName::FlatOmitPrefixLayout,
            &self.extension_name,
        )?;

        if self.delimiter.is_empty() {
            return Err(RocflError::InvalidConfiguration(
                "delimiter was empty but it must be non-empty".to_string(),
            ));
        }

        Ok(())
    }
}

impl NTupleOmitPrefixLayoutConfig {
    fn validate(&self) -> Result<()> {
        validate_extension_name(
            &LayoutExtensionName::NTupleOmitPrefixLayout,
            &self.extension_name,
        )?;

        if self.delimiter.is_empty() {
            return Err(RocflError::InvalidConfiguration(
                "delimiter was empty but it must be non-empty".to_string(),
            ));
        }

        if self.tuple_size < 1 || self.tuple_size > 32 {
            return Err(RocflError::InvalidConfiguration(format!(
                "tupleSize must be between 1 and 32, inclusive, but was {}.",
                self.tuple_size
            )));
        }

        if self.number_of_tuples < 1 || self.number_of_tuples > 32 {
            return Err(RocflError::InvalidConfiguration(format!(
                "numberOfTuples must be between 1 and 32, inclusive, but was {}.",
                self.tuple_size
            )));
        }

        Ok(())
    }
}

impl LayoutExtension {
    fn map_object_id(&self, object_id: &str) -> String {
        match self {
            LayoutExtension::FlatDirect(ext) => ext.map_object_id(object_id),
            LayoutExtension::HashedNTuple(ext) => ext.map_object_id(object_id),
            LayoutExtension::HashedNTupleObjectId(ext) => ext.map_object_id(object_id),
            LayoutExtension::FlatOmitPrefix(ext) => ext.map_object_id(object_id),
            LayoutExtension::NTupleOmitPrefix(ext) => ext.map_object_id(object_id),
        }
    }

    fn extension_name(&self) -> LayoutExtensionName {
        match self {
            LayoutExtension::FlatDirect(ext) => ext.config.extension_name,
            LayoutExtension::HashedNTuple(ext) => ext.config.extension_name,
            LayoutExtension::HashedNTupleObjectId(ext) => ext.config.extension_name,
            LayoutExtension::FlatOmitPrefix(ext) => ext.config.extension_name,
            LayoutExtension::NTupleOmitPrefix(ext) => ext.config.extension_name,
        }
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        match self {
            LayoutExtension::FlatDirect(ext) => Ok(serde_json::to_vec_pretty(&ext.config)?),
            LayoutExtension::HashedNTuple(ext) => Ok(serde_json::to_vec_pretty(&ext.config)?),
            LayoutExtension::HashedNTupleObjectId(ext) => {
                Ok(serde_json::to_vec_pretty(&ext.config)?)
            }
            LayoutExtension::FlatOmitPrefix(ext) => Ok(serde_json::to_vec_pretty(&ext.config)?),
            LayoutExtension::NTupleOmitPrefix(ext) => Ok(serde_json::to_vec_pretty(&ext.config)?),
        }
    }
}

impl From<FlatDirectLayoutExtension> for LayoutExtension {
    fn from(extension: FlatDirectLayoutExtension) -> Self {
        LayoutExtension::FlatDirect(extension)
    }
}

impl From<HashedNTupleLayoutExtension> for LayoutExtension {
    fn from(extension: HashedNTupleLayoutExtension) -> Self {
        LayoutExtension::HashedNTuple(extension)
    }
}

impl From<HashedNTupleObjectIdLayoutExtension> for LayoutExtension {
    fn from(extension: HashedNTupleObjectIdLayoutExtension) -> Self {
        LayoutExtension::HashedNTupleObjectId(extension)
    }
}

impl From<FlatOmitPrefixLayoutExtension> for LayoutExtension {
    fn from(extension: FlatOmitPrefixLayoutExtension) -> Self {
        LayoutExtension::FlatOmitPrefix(extension)
    }
}

impl From<NTupleOmitPrefixLayoutExtension> for LayoutExtension {
    fn from(extension: NTupleOmitPrefixLayoutExtension) -> Self {
        LayoutExtension::NTupleOmitPrefix(extension)
    }
}

impl FlatDirectLayoutExtension {
    fn new(config_bytes: Option<&[u8]>) -> Result<Self> {
        let config = match config_bytes {
            Some(config_bytes) => {
                let config: FlatDirectLayoutConfig = serde_json::from_slice(config_bytes)?;
                config.validate()?;
                config
            }
            None => FlatDirectLayoutConfig::default(),
        };

        Ok(Self { config })
    }

    /// One-to-one mapping from object ID to object root path
    fn map_object_id(&self, object_id: &str) -> String {
        object_id.to_string()
    }
}

impl HashedNTupleLayoutExtension {
    fn new(config_bytes: Option<&[u8]>) -> Result<Self> {
        let config = match config_bytes {
            Some(config_bytes) => {
                let config: HashedNTupleLayoutConfig = serde_json::from_slice(config_bytes)?;
                config.validate()?;
                config
            }
            None => HashedNTupleLayoutConfig::default(),
        };

        Ok(Self { config })
    }

    /// Object IDs are hashed and then divided into tuples to create a pair-tree like layout
    fn map_object_id(&self, object_id: &str) -> String {
        let digest: String = self
            .config
            .digest_algorithm
            .hash_hex(&mut object_id.as_bytes())
            .unwrap()
            .into();

        if self.config.tuple_size == 0 {
            return digest;
        }

        let mut path = to_tuples(
            &digest,
            self.config.tuple_size,
            self.config.number_of_tuples,
        );

        if self.config.short_object_root {
            let start = self.config.tuple_size * self.config.number_of_tuples;
            path.push_str(&digest[start..]);
        } else {
            path.push_str(&digest);
        }

        path
    }
}

impl HashedNTupleObjectIdLayoutExtension {
    fn new(config_bytes: Option<&[u8]>) -> Result<Self> {
        let config = match config_bytes {
            Some(config_bytes) => {
                let config: HashedNTupleObjectIdLayoutConfig =
                    serde_json::from_slice(config_bytes)?;
                config.validate()?;
                config
            }
            None => HashedNTupleObjectIdLayoutConfig::default(),
        };

        Ok(Self { config })
    }

    /// Object IDs are hashed and then divided into tuples to create a pair-tree like layout. The
    /// difference here is that the object encapsulation directory is the url-encoded object ID
    fn map_object_id(&self, object_id: &str) -> String {
        let digest: String = self
            .config
            .digest_algorithm
            .hash_hex(&mut object_id.as_bytes())
            .unwrap()
            .into();

        if self.config.tuple_size == 0 {
            return digest;
        }

        let mut path = to_tuples(
            &digest,
            self.config.tuple_size,
            self.config.number_of_tuples,
        );

        // sadly, this produced uppercase hex; lowercase is required
        let encoded = utf8_percent_encode(object_id, &NON_ALPHA_PLUS).to_string();
        let lower = lower_percent_escape(&encoded);

        if lower.len() <= MAX_0003_ENCAPSULATION_LENGTH {
            path.push_str(&lower);
        } else {
            path.push_str(&lower[..MAX_0003_ENCAPSULATION_LENGTH]);
            path.push('-');
            path.push_str(&digest);
        }

        path
    }
}

impl FlatOmitPrefixLayoutExtension {
    fn new(config_bytes: Option<&[u8]>) -> Result<Self> {
        let config = match config_bytes {
            Some(config_bytes) => {
                let config: FlatOmitPrefixLayoutConfig = serde_json::from_slice(config_bytes)?;
                config.validate()?;
                config
            }
            None => {
                return Err(RocflError::InvalidConfiguration(
                    "Storage layout extension configuration must be specified".to_string(),
                ))
            }
        };

        let case_matters = config.delimiter.to_lowercase() != config.delimiter.to_uppercase();

        let normalized_delimiter = if case_matters {
            config.delimiter.to_lowercase()
        } else {
            config.delimiter.clone()
        };

        Ok(Self {
            config,
            case_matters,
            normalized_delimiter,
        })
    }

    /// Object IDs have a prefix removed and the remaining part is returned
    fn map_object_id(&self, object_id: &str) -> String {
        let test_id = if self.case_matters {
            Cow::Owned(object_id.to_lowercase())
        } else {
            Cow::Borrowed(object_id)
        };

        match test_id.rfind(&self.normalized_delimiter) {
            None => object_id.to_string(),
            Some(index) => {
                let length = self.normalized_delimiter.len();
                if object_id.len() == index + length {
                    // Unfortunately, this needs to panic because I don't want to introduce a Result here
                    panic!("The id '{}' cannot be mapped to a storage path using layout {} because it ends with the delimiter '{}'",
                           object_id, self.config.extension_name, self.config.delimiter);
                } else {
                    object_id[index + length..].to_string()
                }
            }
        }
    }
}

impl NTupleOmitPrefixLayoutExtension {
    fn new(config_bytes: Option<&[u8]>) -> Result<Self> {
        let config = match config_bytes {
            Some(config_bytes) => {
                let config: NTupleOmitPrefixLayoutConfig = serde_json::from_slice(config_bytes)?;
                config.validate()?;
                config
            }
            None => {
                return Err(RocflError::InvalidConfiguration(
                    "Storage layout extension configuration must be specified".to_string(),
                ))
            }
        };

        let case_matters = config.delimiter.to_lowercase() != config.delimiter.to_uppercase();

        let normalized_delimiter = if case_matters {
            config.delimiter.to_lowercase()
        } else {
            config.delimiter.clone()
        };

        Ok(Self {
            width: config.tuple_size * config.number_of_tuples,
            config,
            case_matters,
            normalized_delimiter,
        })
    }

    /// Object IDs have a prefix removed and the remaining part turned into an n tuple and returned
    /// as the object root path
    fn map_object_id(&self, object_id: &str) -> String {
        if !object_id.is_ascii() {
            // TODO it would seem that map_object_id() needs to return a Result :(
            panic!("The id '{}' cannot be mapped to a storage path using layout {} because it contains non-ASCII characters",
                   object_id, self.config.extension_name);
        }

        let test_id = if self.case_matters {
            Cow::Owned(object_id.to_lowercase())
        } else {
            Cow::Borrowed(object_id)
        };

        let id_part = match test_id.rfind(&self.normalized_delimiter) {
            None => object_id,
            Some(index) => {
                let length = self.normalized_delimiter.len();
                if object_id.len() == index + length {
                    // Unfortunately, this needs to panic because I don't want to introduce a Result here
                    panic!("The id '{}' cannot be mapped to a storage path using layout {} because it ends with the delimiter '{}'",
                           object_id, self.config.extension_name, self.config.delimiter);
                } else {
                    &object_id[index + length..]
                }
            }
        };

        let mut padded_part = if self.config.zero_padding == Padding::Left {
            format!("{:0>width$}", id_part, width = self.width)
        } else {
            format!("{:0<width$}", id_part, width = self.width)
        };

        if self.config.reverse_object_root {
            padded_part = padded_part.chars().rev().collect::<String>()
        }

        let mut path = to_tuples(
            &padded_part,
            self.config.tuple_size,
            self.config.number_of_tuples,
        );

        path.push_str(id_part);
        path
    }
}

/// Splits the value into N tuples of M size, joined with a /, and ending with a trailing /
fn to_tuples(value: &str, tuple_size: usize, number_of_tuples: usize) -> String {
    let mut path = String::new();

    for i in 0..number_of_tuples {
        let start = i * tuple_size;
        let end = start + tuple_size;
        path.push_str(&value[start..end]);
        path.push('/');
    }

    path
}

/// Transforms an uppercase percent encoded string to lower case, only touching characters that are
/// part of an escape sequence.
///
/// This method assumes that ALL unicode characters have been percent encoded. It is NOT SAFE
/// to use on strings that contain unicode.
fn lower_percent_escape(original: &str) -> Cow<str> {
    if let Some(first) = original.find('%') {
        let start = first + 1;
        let mut out = Vec::with_capacity(original.len());
        out.extend_from_slice(original[..start].as_bytes());
        let search = original[start..].bytes();

        let mut count = 2;

        for c in search {
            let mut lc = c;

            if count > 0 {
                lc = c.to_ascii_lowercase();
                count -= 1;
            } else if c == b'%' {
                count = 2;
            }

            out.push(lc);
        }

        // This is safe because this method is only intended to be used AFTER percent encoding the
        // input string
        Cow::Owned(unsafe { String::from_utf8_unchecked(out) })
    } else {
        original.into()
    }
}

fn validate_extension_name(
    expected: &LayoutExtensionName,
    actual: &LayoutExtensionName,
) -> Result<()> {
    if actual != expected {
        Err(RocflError::InvalidConfiguration(format!(
            "Expected layout extension name {}; Found: {}",
            expected,
            actual
        )))
    } else {
        Ok(())
    }
}

fn validate_tuple_config(tuple_size: usize, number_of_tuples: usize) -> Result<()> {
    if (tuple_size == 0 || number_of_tuples == 0) && (tuple_size != 0 || number_of_tuples != 0) {
        Err(RocflError::InvalidConfiguration(format!(
            "If tupleSize (={}) or numberOfTuples (={}) is set to 0, then both must be 0.",
            tuple_size, number_of_tuples
        )))
    } else {
        Ok(())
    }
}

fn validate_digest_algorithm(
    algorithm: DigestAlgorithm,
    tuple_size: usize,
    number_of_tuples: usize,
) -> Result<()> {
    let digest: String = algorithm.hash_hex(&mut "test".as_bytes()).unwrap().into();
    let total_tuples_length = tuple_size * number_of_tuples;

    if digest.len() < total_tuples_length {
        Err(RocflError::InvalidConfiguration(format!(
            "tupleSize={} and numberOfTuples={} requires a minimum of {} characters. \
             The digest algorithm {} only produces {}.",
            tuple_size,
            number_of_tuples,
            total_tuples_length,
            algorithm,
            digest.len()
        )))
    } else {
        Ok(())
    }
}

// These functions are needed for serde default values

fn default_tuple() -> usize {
    3
}

fn default_short_root() -> bool {
    false
}

fn default_reverse() -> bool {
    false
}

fn default_algorithm() -> DigestAlgorithm {
    DigestAlgorithm::Sha256
}

fn default_padding() -> Padding {
    Padding::Left
}

#[cfg(test)]
mod tests {
    use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};

    use super::{
        lower_percent_escape, HashedNTupleLayoutExtension, HashedNTupleObjectIdLayoutExtension,
    };
    use crate::ocfl::store::layout::{
        FlatOmitPrefixLayoutExtension, NTupleOmitPrefixLayoutExtension, Padding,
    };
    use crate::ocfl::Result;

    const ID_1: &str = "info:example/test-123";
    const ID_2: &str = "..Hor/rib:lè-$id";
    const ID_3: &str = "۵ݨݯژښڙڜڛڝڠڱݰݣݫۯ۞ۆݰ";

    #[test]
    fn lower_case_percent_escape() {
        assert_eq!(
            "T%25HIS%20is%20a%20%c5%a4%c4%99%c8%98%cd%b2%21%40%23%2e",
            lower_percent_escape(
                &utf8_percent_encode("T%HIS is a ŤęȘͲ!@#.", &NON_ALPHANUMERIC).to_string()
            )
        );
        assert_eq!(
            "THIShasNOencodings",
            lower_percent_escape(
                &utf8_percent_encode("THIShasNOencodings", &NON_ALPHANUMERIC).to_string()
            )
        );
    }

    #[test]
    fn map_id_with_default_config_0003() {
        let ext = HashedNTupleObjectIdLayoutExtension::new(None).unwrap();

        assert_eq!(
            "1e4/d16/d89/info%3aexample%2ftest-123",
            ext.map_object_id(ID_1)
        );
        assert_eq!(
            "373/529/21a/%2e%2eHor%2frib%3al%c3%a8-%24id",
            ext.map_object_id(ID_2)
        );
        assert_eq!("72d/744/ab2/%db%b5%dd%a8%dd%af%da%98%da%9a%da%99%da%9c%da%9b%da%9d%da%a0%da%b1%dd\
        %b0%dd%a3%dd%ab%db%af%db%9e%db%-72d744ab28e696afd14423026efe0ca8954e8f1b3fd21e86f06e89375b4de005",
                   ext.map_object_id(ID_3));
    }

    #[test]
    fn map_id_with_different_tuple_size_0003() {
        let ext = hashed_ntuple_id_ext("sha256", 2, 3).unwrap();

        assert_eq!(
            "1e/4d/16/info%3aexample%2ftest-123",
            ext.map_object_id(ID_1)
        );
        assert_eq!(
            "37/35/29/%2e%2eHor%2frib%3al%c3%a8-%24id",
            ext.map_object_id(ID_2)
        );
        assert_eq!("72/d7/44/%db%b5%dd%a8%dd%af%da%98%da%9a%da%99%da%9c%da%9b%da%9d%da%a0%da%b1%dd%b0\
        %dd%a3%dd%ab%db%af%db%9e%db%-72d744ab28e696afd14423026efe0ca8954e8f1b3fd21e86f06e89375b4de005",
                   ext.map_object_id(ID_3));
    }

    #[test]
    fn map_id_with_different_tuple_count_0003() {
        let ext = hashed_ntuple_id_ext("sha256", 3, 2).unwrap();

        assert_eq!("1e4/d16/info%3aexample%2ftest-123", ext.map_object_id(ID_1));
        assert_eq!(
            "373/529/%2e%2eHor%2frib%3al%c3%a8-%24id",
            ext.map_object_id(ID_2)
        );
        assert_eq!("72d/744/%db%b5%dd%a8%dd%af%da%98%da%9a%da%99%da%9c%da%9b%da%9d%da%a0%da%b1%dd%b0\
        %dd%a3%dd%ab%db%af%db%9e%db%-72d744ab28e696afd14423026efe0ca8954e8f1b3fd21e86f06e89375b4de005",
                   ext.map_object_id(ID_3));
    }

    #[test]
    fn map_id_with_different_algorithm_0003() {
        let ext = hashed_ntuple_id_ext("md5", 3, 3).unwrap();

        assert_eq!(
            "787/a3c/e39/info%3aexample%2ftest-123",
            ext.map_object_id(ID_1)
        );
        assert_eq!(
            "284/654/5c5/%2e%2eHor%2frib%3al%c3%a8-%24id",
            ext.map_object_id(ID_2)
        );
        assert_eq!(
            "7fd/b24/28e/%db%b5%dd%a8%dd%af%da%98%da%9a%da%99%da%9c%da%9b%da%9d%da%a0%da%b1%dd\
        %b0%dd%a3%dd%ab%db%af%db%9e%db%-7fdb2428e841e023e24be158d9e2dfeb",
            ext.map_object_id(ID_3)
        );
    }

    #[test]
    #[should_panic(expected = "unknown variant `md6`")]
    fn fail_0003_init_when_invalid_digest() {
        let _ = hashed_ntuple_id_ext("md6", 3, 3).unwrap();
    }

    #[test]
    #[should_panic(expected = "then both must be 0")]
    fn fail_0003_init_when_invalid_tuple_1() {
        let _ = hashed_ntuple_id_ext("sha256", 0, 3).unwrap();
    }

    #[test]
    #[should_panic(expected = "then both must be 0")]
    fn fail_0003_init_when_invalid_tuple_2() {
        let _ = hashed_ntuple_id_ext("sha256", 3, 0).unwrap();
    }

    #[test]
    #[should_panic(expected = "minimum of 100 characters")]
    fn fail_0003_init_when_digest_not_long_enough() {
        let _ = hashed_ntuple_id_ext("sha256", 10, 10).unwrap();
    }

    #[test]
    fn map_id_with_default_config_0004() {
        let ext = HashedNTupleLayoutExtension::new(None).unwrap();

        assert_eq!(
            "1e4/d16/d89/1e4d16d8940c54e7a88a8562fa5a55bafc0902128abb163f39fae3bda53425ae",
            ext.map_object_id(ID_1)
        );
        assert_eq!(
            "373/529/21a/37352921ac393c83cb43065acd6229228b6d82823790ab4e372da5e0295851a0",
            ext.map_object_id(ID_2)
        );
        assert_eq!(
            "72d/744/ab2/72d744ab28e696afd14423026efe0ca8954e8f1b3fd21e86f06e89375b4de005",
            ext.map_object_id(ID_3)
        );
    }

    #[test]
    fn map_id_with_different_tuple_size_0004() {
        let ext = hashed_ntuple_ext("sha256", 2, 3, false).unwrap();

        assert_eq!(
            "1e/4d/16/1e4d16d8940c54e7a88a8562fa5a55bafc0902128abb163f39fae3bda53425ae",
            ext.map_object_id(ID_1)
        );
        assert_eq!(
            "37/35/29/37352921ac393c83cb43065acd6229228b6d82823790ab4e372da5e0295851a0",
            ext.map_object_id(ID_2)
        );
        assert_eq!(
            "72/d7/44/72d744ab28e696afd14423026efe0ca8954e8f1b3fd21e86f06e89375b4de005",
            ext.map_object_id(ID_3)
        );
    }

    #[test]
    fn map_id_with_different_tuple_count_0004() {
        let ext = hashed_ntuple_ext("sha256", 3, 2, false).unwrap();

        assert_eq!(
            "1e4/d16/1e4d16d8940c54e7a88a8562fa5a55bafc0902128abb163f39fae3bda53425ae",
            ext.map_object_id(ID_1)
        );
        assert_eq!(
            "373/529/37352921ac393c83cb43065acd6229228b6d82823790ab4e372da5e0295851a0",
            ext.map_object_id(ID_2)
        );
        assert_eq!(
            "72d/744/72d744ab28e696afd14423026efe0ca8954e8f1b3fd21e86f06e89375b4de005",
            ext.map_object_id(ID_3)
        );
    }

    #[test]
    fn map_id_with_short_root_0004() {
        let ext = hashed_ntuple_ext("sha256", 3, 3, true).unwrap();

        assert_eq!(
            "1e4/d16/d89/40c54e7a88a8562fa5a55bafc0902128abb163f39fae3bda53425ae",
            ext.map_object_id(ID_1)
        );
        assert_eq!(
            "373/529/21a/c393c83cb43065acd6229228b6d82823790ab4e372da5e0295851a0",
            ext.map_object_id(ID_2)
        );
        assert_eq!(
            "72d/744/ab2/8e696afd14423026efe0ca8954e8f1b3fd21e86f06e89375b4de005",
            ext.map_object_id(ID_3)
        );
    }

    #[test]
    fn map_id_with_different_algorithm_0004() {
        // md5
        let ext = hashed_ntuple_ext("md5", 3, 3, false).unwrap();

        assert_eq!(
            "787/a3c/e39/787a3ce39753c8a5bbbf0d8b623e54bc",
            ext.map_object_id(ID_1)
        );
        assert_eq!(
            "284/654/5c5/2846545c50a3ea528c61fa73f158e4bc",
            ext.map_object_id(ID_2)
        );
        assert_eq!(
            "7fd/b24/28e/7fdb2428e841e023e24be158d9e2dfeb",
            ext.map_object_id(ID_3)
        );

        // sha1
        let ext = hashed_ntuple_ext("sha1", 3, 3, false).unwrap();

        assert_eq!(
            "903/844/22e/90384422ea7703eed693b79c23871eff4650bc2f",
            ext.map_object_id(ID_1)
        );
        assert_eq!(
            "178/5a5/0ed/1785a50ed995e95cad9489a2926bd0f0a3b5e799",
            ext.map_object_id(ID_2)
        );
        assert_eq!(
            "f3c/67e/836/f3c67e8367f0b67fb3f9951d74b8f955cb59a3b4",
            ext.map_object_id(ID_3)
        );

        // sha512
        let ext = hashed_ntuple_ext("sha512", 3, 3, false).unwrap();

        assert_eq!(
            "a43/39e/be5/a4339ebe5aeb1766748f86130c9f1a338706fc9972a453674c6d51074954a2d9d822\
        68166d05b78eb15a18f30f97e13a3c6a37f00ae29d3c6815bed9b8d7050b",
            ext.map_object_id(ID_1)
        );
        assert_eq!(
            "3a9/f56/a75/3a9f56a75ca66b24341967ed8f3e1900225f64c452e7111f51b13c7a1b0b8054f395\
        c7787d710c6000257da3d95e0f4518e0f05bff9d5187786aecdab02412f2",
            ext.map_object_id(ID_2)
        );
        assert_eq!(
            "fa2/29e/b18/fa229eb18fb3aaf013ca8cfe4536c9d169c3543d3e442bd8662e52a253d1a72522c4\
        30606a7062dfc086e6132eea7bf9614f83b77107efd91896ece2b1389d98",
            ext.map_object_id(ID_3)
        );

        // sha512/256
        let ext = hashed_ntuple_ext("sha512/256", 3, 3, false).unwrap();

        assert_eq!(
            "570/0dc/cca/5700dccca5547746cbfe291156d8c9d9ee750026a7868c76d3689478d31e2caf",
            ext.map_object_id(ID_1)
        );
        assert_eq!(
            "163/6d3/e4e/1636d3e4e3b9ef85b0c095047159e424bdb4e99d8f0ed9d2283aa6f63ce85cec",
            ext.map_object_id(ID_2)
        );
        assert_eq!(
            "0b7/fbc/00c/0b7fbc00c46dacddaf64912b53aca14855a8230c6ab0e34beea2d60dae8bf6d7",
            ext.map_object_id(ID_3)
        );

        // blake2b-512
        let ext = hashed_ntuple_ext("blake2b-512", 3, 3, false).unwrap();

        assert_eq!(
            "8de/6c4/2cc/8de6c42ccbd068fcc274fad7ee5257f14e4ef1696b040144691e725bb1a779eb2d8\
        c89df2d0b48b441d1810677e2ebb7cf11243ff0df7bc026a997d414e65f5f",
            ext.map_object_id(ID_1)
        );
        assert_eq!(
            "751/b54/6e2/751b546e2782ce58cdb197134b63f2743f8742373e346973b2c3674a70abc27cfba\
        aed7b4f68d0e44ced88bf2d4302255e094ae6d2f674e90e60338340962f99",
            ext.map_object_id(ID_2)
        );
        assert_eq!(
            "39d/20e/f35/39d20ef3533754b580d4097a7a72f7b133b9c3216a35e91f82dd2f8c264a18606ed\
        e2c68d54055311a68f467be6e915cff1e66934a5a61c9d2e2bb66a30a5652",
            ext.map_object_id(ID_3)
        );

        // blake2b-160
        let ext = hashed_ntuple_ext("blake2b-160", 3, 3, false).unwrap();

        assert_eq!(
            "10b/ab8/f38/10bab8f38bb05add59ea3756b23054dea173471f",
            ext.map_object_id(ID_1)
        );
        assert_eq!(
            "43a/194/e5a/43a194e5ad637fb4827930e34b4df5b59d701348",
            ext.map_object_id(ID_2)
        );
        assert_eq!(
            "264/555/ae9/264555ae9b36b1570d4cf33f14fac596bf300a72",
            ext.map_object_id(ID_3)
        );

        // blake2b-256
        let ext = hashed_ntuple_ext("blake2b-256", 3, 3, false).unwrap();

        assert_eq!(
            "5a6/43a/8ce/5a643a8ce4b75bcf6c13257abe115a0c8c47e62b73c87074710223a196a38983",
            ext.map_object_id(ID_1)
        );
        assert_eq!(
            "a22/cb2/5f8/a22cb25f8b16b8221e325763cdc99c5a32c86ee03269a146b3c21bf4a216387a",
            ext.map_object_id(ID_2)
        );
        assert_eq!(
            "be5/df0/609/be5df060977b13927193acd17a0342e6b8f76353a85da14fef684ce5aba9bf25",
            ext.map_object_id(ID_3)
        );

        // blake2b-384
        let ext = hashed_ntuple_ext("blake2b-384", 3, 3, false).unwrap();

        assert_eq!(
            "ed1/3e8/068/ed13e80681eb1553d5feb01a77ec399cafc295791717adc3936eb9a59cf6894d8a3\
        99df0ce0a8f120dfac230ecff367d",
            ext.map_object_id(ID_1)
        );
        assert_eq!(
            "4c0/47c/bec/4c047cbec0eba530142ac2d93ff11ea6564016b577e21a1a5862f19c942a57b9e92\
        f77fd72b702cb8cd28d12210f63e4",
            ext.map_object_id(ID_2)
        );
        assert_eq!(
            "52e/e8e/9e0/52ee8e9e08eb49586e241e6f665d62e287389fae4e4cd8f958ac1a010ae6566c359\
        d845249ae6475688c8e3f08a6397b",
            ext.map_object_id(ID_3)
        );
    }

    #[test]
    #[should_panic(expected = "unknown variant `md6`")]
    fn fail_0004_init_when_invalid_digest() {
        let _ = hashed_ntuple_ext("md6", 3, 3, false).unwrap();
    }

    #[test]
    #[should_panic(expected = "then both must be 0")]
    fn fail_0004_init_when_invalid_tuple_1() {
        let _ = hashed_ntuple_ext("sha256", 0, 3, false).unwrap();
    }

    #[test]
    #[should_panic(expected = "then both must be 0")]
    fn fail_0004_init_when_invalid_tuple_2() {
        let _ = hashed_ntuple_ext("sha256", 3, 0, false).unwrap();
    }

    #[test]
    #[should_panic(expected = "minimum of 100 characters")]
    fn fail_0004_init_when_digest_not_long_enough() {
        let _ = hashed_ntuple_ext("sha256", 10, 10, false).unwrap();
    }

    #[test]
    fn flat_omit_mapping_single_char() {
        let ext = flat_omit_ext(":").unwrap();

        assert_eq!("example/test-123", ext.map_object_id(ID_1));
        assert_eq!("lè-$id", ext.map_object_id(ID_2));
        assert_eq!(ID_3, ext.map_object_id(ID_3));
        assert_eq!(
            "6e8bc430-9c3a-11d9-9669-0800200c9a66",
            ext.map_object_id("urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66")
        );
    }

    #[test]
    #[should_panic(expected = "ends with the delimiter")]
    fn fail_flat_omit_mapping_when_ends_with_delimiter() {
        let ext = flat_omit_ext(":").unwrap();
        assert_eq!(
            "",
            ext.map_object_id("urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66:")
        );
    }

    #[test]
    fn flat_omit_mapping_multi_char() {
        let ext = flat_omit_ext("edu/").unwrap();

        assert_eq!(
            "3448793",
            ext.map_object_id("https://institution.edu/3448793")
        );
        assert_eq!(
            "f8.05v",
            ext.map_object_id("https://institution.edu/abc/edu/f8.05v")
        );
        assert_eq!(
            "https://example.com/",
            ext.map_object_id("https://example.com/")
        );
    }

    #[test]
    #[should_panic(expected = "delimiter was empty")]
    fn flat_omit_fail_when_delimiter_empty() {
        let _ = flat_omit_ext("").unwrap();
    }

    #[test]
    fn ntuple_omit_mapping_single_char() {
        let ext = ntuple_omit_ext(":", 4, 2, Padding::Left, true).unwrap();

        assert_eq!(
            "6927/8821/12887296",
            ext.map_object_id("namespace:12887296")
        );
        assert_eq!(
            "66a9/c002/6e8bc430-9c3a-11d9-9669-0800200c9a66",
            ext.map_object_id("urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66")
        );
        assert_eq!("321c/ba00/abc123", ext.map_object_id("abc123"));
    }

    #[test]
    fn ntuple_omit_mapping_multi_char() {
        let ext = ntuple_omit_ext("edu/", 3, 3, Padding::Right, false).unwrap();

        assert_eq!(
            "344/879/300/3448793",
            ext.map_object_id("https://institution.edu/3448793")
        );
        assert_eq!(
            "344/879/300/3448793",
            ext.map_object_id("https://institution.EDU/3448793")
        );
        assert_eq!(
            "f8./05v/000/f8.05v",
            ext.map_object_id("https://institution.edu/abc/edu/f8.05v")
        );
    }

    #[test]
    #[should_panic(expected = "delimiter was empty")]
    fn ntuple_omit_fail_when_delimiter_empty() {
        let _ = ntuple_omit_ext("", 4, 2, Padding::Left, true).unwrap();
    }

    #[test]
    #[should_panic(expected = "non-ASCII characters")]
    fn ntuple_omit_fail_when_contains_non_ascii_chars() {
        let ext = ntuple_omit_ext(":", 4, 2, Padding::Left, true).unwrap();
        ext.map_object_id(ID_2);
    }

    fn hashed_ntuple_ext(
        algorithm: &str,
        tuple_size: usize,
        number_of_tuples: usize,
        short: bool,
    ) -> Result<HashedNTupleLayoutExtension> {
        HashedNTupleLayoutExtension::new(Some(
            format!(
                "{{
            \"extensionName\": \"0004-hashed-n-tuple-storage-layout\",
            \"digestAlgorithm\": \"{}\",
            \"tupleSize\": {},
            \"numberOfTuples\": {},
            \"shortObjectRoot\": {}
        }}",
                algorithm, tuple_size, number_of_tuples, short
            )
            .as_bytes(),
        ))
    }

    fn hashed_ntuple_id_ext(
        algorithm: &str,
        tuple_size: usize,
        number_of_tuples: usize,
    ) -> Result<HashedNTupleObjectIdLayoutExtension> {
        HashedNTupleObjectIdLayoutExtension::new(Some(
            format!(
                "{{
            \"extensionName\": \"0003-hash-and-id-n-tuple-storage-layout\",
            \"digestAlgorithm\": \"{}\",
            \"tupleSize\": {},
            \"numberOfTuples\": {}
        }}",
                algorithm, tuple_size, number_of_tuples
            )
            .as_bytes(),
        ))
    }

    fn flat_omit_ext(delimiter: &str) -> Result<FlatOmitPrefixLayoutExtension> {
        FlatOmitPrefixLayoutExtension::new(Some(
            format!(
                "{{
            \"extensionName\": \"0006-flat-omit-prefix-storage-layout\",
            \"delimiter\": \"{}\"
        }}",
                delimiter
            )
            .as_bytes(),
        ))
    }

    fn ntuple_omit_ext(
        delimiter: &str,
        tuple_size: usize,
        number_of_tuples: usize,
        padding: Padding,
        reverse: bool,
    ) -> Result<NTupleOmitPrefixLayoutExtension> {
        NTupleOmitPrefixLayoutExtension::new(Some(
            format!(
                "{{
            \"extensionName\": \"0007-n-tuple-omit-prefix-storage-layout\",
            \"delimiter\": \"{}\",
            \"tupleSize\": {},
            \"numberOfTuples\": {},
            \"zeroPadding\": \"{}\",
            \"reverseObjectRoot\": {}
        }}",
                delimiter, tuple_size, number_of_tuples, padding, reverse
            )
            .as_bytes(),
        ))
    }
}

//! OCFL storage layout extension implementations

use std::borrow::Cow;

use anyhow::Result;
use blake2::Blake2b;
use digest::{Digest, DynDigest};
use enum_dispatch::enum_dispatch;
use lazy_static::lazy_static;
use md5::Md5;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde::Deserialize;
use sha1::Sha1;
use sha2::{Sha256, Sha512, Sha512Trunc256};
use strum_macros::{Display as EnumDisplay, EnumString};

use crate::ocfl::{RocflError, Validate};

const DEFAULT_DIGEST_ALGORITHM: &str = "sha256";
const MAX_0003_ENCAPSULATION_LENGTH: usize = 100;

lazy_static! {
    static ref NON_ALPHA_PLUS: AsciiSet = NON_ALPHANUMERIC.remove(b'-').remove(b'_');
}

// ================================================== //
//             public structs+enums+traits            //
// ================================================== //

/// The storage layout maps object IDs to locations within the storage root
#[derive(Debug)]
pub struct StorageLayout {
    extension: LayoutExtension,
}

/// Enum of known storage layout extensions
#[derive(Deserialize, Debug, PartialEq, EnumString, EnumDisplay)]
pub enum LayoutExtensionName {
    #[strum(serialize = "0002-flat-direct-storage-layout")]
    #[serde(rename = "0002-flat-direct-storage-layout")]
    FlatDirectLayout,
    #[strum(serialize = "0004-hashed-n-tuple-storage-layout")]
    #[serde(rename = "0004-hashed-n-tuple-storage-layout")]
    HashedNTupleLayout,
    #[strum(serialize = "0003-hash-and-id-n-tuple-storage-layout")]
    #[serde(rename = "0003-hash-and-id-n-tuple-storage-layout")]
    HashedNTupleObjectIdLayout
}

// ================================================== //
//                   public impls+fns                 //
// ================================================== //

impl StorageLayout {
    pub fn new(name: &LayoutExtensionName, config_bytes: Option<&[u8]>) -> Result<Self> {
        let extension = match name {
            LayoutExtensionName::FlatDirectLayout => FlatDirectLayoutExtension::new(config_bytes)?.into(),
            LayoutExtensionName::HashedNTupleLayout => HashedNTupleLayoutExtension::new(config_bytes)?.into(),
            LayoutExtensionName::HashedNTupleObjectIdLayout => HashedNTupleObjectIdLayoutExtension::new(config_bytes)?.into(),
        };

        Ok(StorageLayout {
            extension
        })
    }

    /// Maps an object ID to an object root directory
    pub fn map_object_id(&self, object_id: &str) -> String {
        self.extension.map_object_id(object_id)
    }
}

// ================================================== //
//            private structs+enums+traits            //
// ================================================== //

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

/// [Flat Direct Storage Layout Config](https://ocfl.github.io/extensions/0002-flat-direct-storage-layout.html)
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", default)]
struct FlatDirectLayoutConfig {
    extension_name: LayoutExtensionName,
}

/// [Hashed N-Tuple Storage Layout Config](https://ocfl.github.io/extensions/0004-hashed-n-tuple-storage-layout.html)
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", default)]
struct HashedNTupleLayoutConfig {
    extension_name: LayoutExtensionName,
    digest_algorithm: String,
    tuple_size: usize,
    number_of_tuples: usize,
    short_object_root: bool,
}

/// [Hashed N-Tuple with Object ID Encapsulation Storage Layout Config](https://ocfl.github.io/extensions/0003-hash-and-id-n-tuple-storage-layout.html)
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", default)]
struct HashedNTupleObjectIdLayoutConfig {
    extension_name: LayoutExtensionName,
    digest_algorithm: String,
    tuple_size: usize,
    number_of_tuples: usize,
}

#[enum_dispatch(MapObjectId)]
#[derive(Debug)]
enum LayoutExtension {
    FlatDirect(FlatDirectLayoutExtension),
    HashedNTuple(HashedNTupleLayoutExtension),
    HashedNTupleObjectId(HashedNTupleObjectIdLayoutExtension),
}

#[enum_dispatch]
trait MapObjectId {
    fn map_object_id(&self, object_id: &str) -> String;
}

// ================================================== //
//                private impls+fns                   //
// ================================================== //

impl Default for FlatDirectLayoutConfig {
    fn default() -> Self {
        Self {
            extension_name: LayoutExtensionName::FlatDirectLayout,
        }
    }
}

impl Default for HashedNTupleLayoutConfig {
    fn default() -> Self {
        Self {
            extension_name: LayoutExtensionName::HashedNTupleLayout,
            digest_algorithm: DEFAULT_DIGEST_ALGORITHM.to_string(),
            tuple_size: 3,
            number_of_tuples: 3,
            short_object_root: false,
        }
    }
}

impl Default for HashedNTupleObjectIdLayoutConfig {
    fn default() -> Self {
        Self {
            extension_name: LayoutExtensionName::HashedNTupleObjectIdLayout,
            digest_algorithm: DEFAULT_DIGEST_ALGORITHM.to_string(),
            tuple_size: 3,
            number_of_tuples: 3,
        }
    }
}

impl Validate for FlatDirectLayoutConfig {
    fn validate(&self) -> Result<()> {
        validate_extension_name(&LayoutExtensionName::FlatDirectLayout, &self.extension_name)
    }
}

impl Validate for HashedNTupleLayoutConfig {
    fn validate(&self) -> Result<()> {
        validate_extension_name(&LayoutExtensionName::HashedNTupleLayout, &self.extension_name)?;
        validate_tuple_config(self.tuple_size, self.number_of_tuples)?;
        validate_digest_algorithm(&self.digest_algorithm, self.tuple_size, self.number_of_tuples)
    }
}

impl Validate for HashedNTupleObjectIdLayoutConfig {
    fn validate(&self) -> Result<()> {
        validate_extension_name(&LayoutExtensionName::HashedNTupleObjectIdLayout, &self.extension_name)?;
        validate_tuple_config(self.tuple_size, self.number_of_tuples)?;
        validate_digest_algorithm(&self.digest_algorithm, self.tuple_size, self.number_of_tuples)
    }
}

impl FlatDirectLayoutExtension {
    fn new(config_bytes: Option<&[u8]>) -> Result<Self> {
        let config = match config_bytes {
            Some(config_bytes) => {
                let config: FlatDirectLayoutConfig = serde_json::from_slice(config_bytes)?;
                config.validate()?;
                config
            },
            None => FlatDirectLayoutConfig::default()
        };

        Ok(Self {
            config
        })
    }
}

/// One-to-one mapping from object ID to object root path
impl MapObjectId for FlatDirectLayoutExtension {
    fn map_object_id(&self, object_id: &str) -> String {
        // TODO this is not validating that the object id can be safely mapped to a path
        //      do we care as long as we aren't authoring objects?
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
            },
            None => HashedNTupleLayoutConfig::default()
        };

        Ok(Self {
            config
        })
    }
}

/// Object IDs are hashed and then divided into tuples to create a pair-tree like layout
impl MapObjectId for HashedNTupleLayoutExtension {
    fn map_object_id(&self, object_id: &str) -> String {
        let mut hasher = algorithm_to_hasher(&self.config.digest_algorithm).unwrap();
        hasher.update(object_id.as_bytes());
        let digest = hex::encode(hasher.finalize());

        if self.config.tuple_size == 0 {
            return digest
        }

        let mut path = digest_to_tuples(&digest, self.config.tuple_size, self.config.number_of_tuples);

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
                let config: HashedNTupleObjectIdLayoutConfig = serde_json::from_slice(config_bytes)?;
                config.validate()?;
                config
            },
            None => HashedNTupleObjectIdLayoutConfig::default()
        };

        Ok(Self {
            config
        })
    }
}

/// Object IDs are hashed and then divided into tuples to create a pair-tree like layout. The
/// difference here is that the object encapsulation directory is the url-encoded object ID
impl MapObjectId for HashedNTupleObjectIdLayoutExtension {
    fn map_object_id(&self, object_id: &str) -> String {
        let mut hasher = algorithm_to_hasher(&self.config.digest_algorithm).unwrap();
        hasher.update(&object_id.as_bytes());
        let digest = hex::encode(hasher.finalize());

        if self.config.tuple_size == 0 {
            return digest
        }

        let mut path = digest_to_tuples(&digest, self.config.tuple_size, self.config.number_of_tuples);

        // sadly, this produced uppercase hex; lowercase is required
        let encoded = utf8_percent_encode(&object_id, &NON_ALPHA_PLUS).to_string();
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

/// Maps the digest algorithm names defined in the OCFL spec to implementations
fn algorithm_to_hasher(algorithm: &str) -> Result<Box<dyn DynDigest>> {
    match algorithm {
        "md5" => Ok(Box::new(Md5::new())),
        "sha1" => Ok(Box::new(Sha1::new())),
        "sha256" => Ok(Box::new(Sha256::new())),
        "sha512" => Ok(Box::new(Sha512::new())),
        "blake2b-512" => Ok(Box::new(Blake2b::new())),
        // TODO the other blake2b formats are not currently supported because of annoying Rust trait issues
        "sha512/256" => Ok(Box::new(Sha512Trunc256::new())),
        _ => Err(RocflError::InvalidConfiguration(
            format!("Unsupported digest algorithm: {}", algorithm)).into())
    }
}

/// Splits the digest into N tuples of M size, joined with a /
fn digest_to_tuples(digest: &str, tuple_size: usize, number_of_tuples: usize) -> String {
    let mut path = String::new();

    for i in 0..number_of_tuples {
        let start = i * tuple_size;
        let end = start + tuple_size;
        path.push_str(&digest[start..end]);
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
        Cow::Owned(unsafe {
            String::from_utf8_unchecked(out)
        })
    } else {
        original.into()
    }
}

fn validate_extension_name(expected: &LayoutExtensionName, actual: &LayoutExtensionName) -> Result<()> {
    if actual != expected {
        Err(RocflError::InvalidConfiguration(
            format!("Expected layout extension name {}; Found: {}",
                    expected.to_string(),
                    actual.to_string())).into())
    } else {
        Ok(())
    }
}

fn validate_tuple_config(tuple_size: usize, number_of_tuples: usize) -> Result<()> {
    if (tuple_size == 0 || number_of_tuples == 0)
        && (tuple_size != 0 || number_of_tuples != 0) {
        Err(RocflError::InvalidConfiguration(
            format!("If tupleSize (={}) or numberOfTuples (={}) is set to 0, then both must be 0.",
                    tuple_size, number_of_tuples)).into())
    } else {
        Ok(())
    }
}

fn validate_digest_algorithm(algorithm: &str, tuple_size: usize, number_of_tuples: usize) -> Result<()>{
    let mut hasher = algorithm_to_hasher(&algorithm)?;
    hasher.update("test".as_bytes());
    let digest = hex::encode(hasher.finalize());

    let total_tuples_length = tuple_size * number_of_tuples;

    if digest.len() < total_tuples_length {
        Err(RocflError::InvalidConfiguration(
            format!("tupleSize={} and numberOfTuples={} requires a minimum of {} characters. \
             The digest algorithm {} only produces {}.",
                    tuple_size, number_of_tuples,
                    total_tuples_length, algorithm, digest.len())).into())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use anyhow::Result;
    use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

    use crate::ocfl::layout::{HashedNTupleLayoutExtension, HashedNTupleObjectIdLayoutExtension, lower_percent_escape, MapObjectId};

    const ID_1: &str = "info:example/test-123";
    const ID_2: &str = "..Hor/rib:lè-$id";
    const ID_3: &str = "۵ݨݯژښڙڜڛڝڠڱݰݣݫۯ۞ۆݰ";

    #[test]
    fn lower_case_percent_escape() {
        assert_eq!("T%25HIS%20is%20a%20%c5%a4%c4%99%c8%98%cd%b2%21%40%23%2e",
                   lower_percent_escape(&utf8_percent_encode("T%HIS is a ŤęȘͲ!@#.", &NON_ALPHANUMERIC).to_string()));
        assert_eq!("THIShasNOencodings",
                   lower_percent_escape(&utf8_percent_encode("THIShasNOencodings", &NON_ALPHANUMERIC).to_string()));
    }

    #[test]
    fn map_id_with_default_config_0003() {
        let ext = HashedNTupleObjectIdLayoutExtension::new(None).unwrap();

        assert_eq!("1e4/d16/d89/info%3aexample%2ftest-123",
                   ext.map_object_id(ID_1));
        assert_eq!("373/529/21a/%2e%2eHor%2frib%3al%c3%a8-%24id",
                   ext.map_object_id(ID_2));
        assert_eq!("72d/744/ab2/%db%b5%dd%a8%dd%af%da%98%da%9a%da%99%da%9c%da%9b%da%9d%da%a0%da%b1%dd\
        %b0%dd%a3%dd%ab%db%af%db%9e%db%-72d744ab28e696afd14423026efe0ca8954e8f1b3fd21e86f06e89375b4de005",
                   ext.map_object_id(ID_3));
    }

    #[test]
    fn map_id_with_different_tuple_size_0003() {
        let ext = hashed_ntuple_id_ext("sha256", 2, 3).unwrap();

        assert_eq!("1e/4d/16/info%3aexample%2ftest-123",
                   ext.map_object_id(ID_1));
        assert_eq!("37/35/29/%2e%2eHor%2frib%3al%c3%a8-%24id",
                   ext.map_object_id(ID_2));
        assert_eq!("72/d7/44/%db%b5%dd%a8%dd%af%da%98%da%9a%da%99%da%9c%da%9b%da%9d%da%a0%da%b1%dd%b0\
        %dd%a3%dd%ab%db%af%db%9e%db%-72d744ab28e696afd14423026efe0ca8954e8f1b3fd21e86f06e89375b4de005",
                   ext.map_object_id(ID_3));
    }

    #[test]
    fn map_id_with_different_tuple_count_0003() {
        let ext = hashed_ntuple_id_ext("sha256", 3, 2).unwrap();

        assert_eq!("1e4/d16/info%3aexample%2ftest-123",
                   ext.map_object_id(ID_1));
        assert_eq!("373/529/%2e%2eHor%2frib%3al%c3%a8-%24id",
                   ext.map_object_id(ID_2));
        assert_eq!("72d/744/%db%b5%dd%a8%dd%af%da%98%da%9a%da%99%da%9c%da%9b%da%9d%da%a0%da%b1%dd%b0\
        %dd%a3%dd%ab%db%af%db%9e%db%-72d744ab28e696afd14423026efe0ca8954e8f1b3fd21e86f06e89375b4de005",
                   ext.map_object_id(ID_3));
    }

    #[test]
    fn map_id_with_different_algorithm_0003() {
        let ext = hashed_ntuple_id_ext("md5", 3, 3).unwrap();

        assert_eq!("787/a3c/e39/info%3aexample%2ftest-123",
                   ext.map_object_id(ID_1));
        assert_eq!("284/654/5c5/%2e%2eHor%2frib%3al%c3%a8-%24id",
                   ext.map_object_id(ID_2));
        assert_eq!("7fd/b24/28e/%db%b5%dd%a8%dd%af%da%98%da%9a%da%99%da%9c%da%9b%da%9d%da%a0%da%b1%dd\
        %b0%dd%a3%dd%ab%db%af%db%9e%db%-7fdb2428e841e023e24be158d9e2dfeb",
                   ext.map_object_id(ID_3));
    }

    #[test]
    fn fail_0003_init_when_invalid_digest() {
        let ext = hashed_ntuple_id_ext("md6", 3, 3);
        expected_err(ext.err().unwrap().into(), "Unsupported digest algorithm: md6");
    }

    #[test]
    fn fail_0003_init_when_invalid_tuple() {
        let ext = hashed_ntuple_id_ext("sha256", 0, 3);
        expected_err(ext.err().unwrap().into(), "then both must be 0");

        let ext = hashed_ntuple_id_ext("sha256", 3, 0);
        expected_err(ext.err().unwrap().into(), "then both must be 0");
    }

    #[test]
    fn fail_0003_init_when_digest_not_long_enough() {
        let ext = hashed_ntuple_id_ext("sha256", 10, 10);
        expected_err(ext.err().unwrap().into(), "minimum of 100 characters");
    }

    #[test]
    fn map_id_with_default_config_0004() {
        let ext = HashedNTupleLayoutExtension::new(None).unwrap();

        assert_eq!("1e4/d16/d89/1e4d16d8940c54e7a88a8562fa5a55bafc0902128abb163f39fae3bda53425ae",
                   ext.map_object_id(ID_1));
        assert_eq!("373/529/21a/37352921ac393c83cb43065acd6229228b6d82823790ab4e372da5e0295851a0",
                   ext.map_object_id(ID_2));
        assert_eq!("72d/744/ab2/72d744ab28e696afd14423026efe0ca8954e8f1b3fd21e86f06e89375b4de005",
                   ext.map_object_id(ID_3));
    }

    #[test]
    fn map_id_with_different_tuple_size_0004() {
        let ext = hashed_ntuple_ext("sha256", 2, 3, false).unwrap();

        assert_eq!("1e/4d/16/1e4d16d8940c54e7a88a8562fa5a55bafc0902128abb163f39fae3bda53425ae",
                   ext.map_object_id(ID_1));
        assert_eq!("37/35/29/37352921ac393c83cb43065acd6229228b6d82823790ab4e372da5e0295851a0",
                   ext.map_object_id(ID_2));
        assert_eq!("72/d7/44/72d744ab28e696afd14423026efe0ca8954e8f1b3fd21e86f06e89375b4de005",
                   ext.map_object_id(ID_3));
    }

    #[test]
    fn map_id_with_different_tuple_count_0004() {
        let ext = hashed_ntuple_ext("sha256", 3, 2, false).unwrap();

        assert_eq!("1e4/d16/1e4d16d8940c54e7a88a8562fa5a55bafc0902128abb163f39fae3bda53425ae",
                   ext.map_object_id(ID_1));
        assert_eq!("373/529/37352921ac393c83cb43065acd6229228b6d82823790ab4e372da5e0295851a0",
                   ext.map_object_id(ID_2));
        assert_eq!("72d/744/72d744ab28e696afd14423026efe0ca8954e8f1b3fd21e86f06e89375b4de005",
                   ext.map_object_id(ID_3));
    }

    #[test]
    fn map_id_with_short_root_0004() {
        let ext = hashed_ntuple_ext("sha256", 3, 3, true).unwrap();

        assert_eq!("1e4/d16/d89/40c54e7a88a8562fa5a55bafc0902128abb163f39fae3bda53425ae",
                   ext.map_object_id(ID_1));
        assert_eq!("373/529/21a/c393c83cb43065acd6229228b6d82823790ab4e372da5e0295851a0",
                   ext.map_object_id(ID_2));
        assert_eq!("72d/744/ab2/8e696afd14423026efe0ca8954e8f1b3fd21e86f06e89375b4de005",
                   ext.map_object_id(ID_3));
    }

    #[test]
    fn map_id_with_different_algorithm_0004() {
        // md5
        let ext = hashed_ntuple_ext("md5", 3, 3, false).unwrap();

        assert_eq!("787/a3c/e39/787a3ce39753c8a5bbbf0d8b623e54bc",
                   ext.map_object_id(ID_1));
        assert_eq!("284/654/5c5/2846545c50a3ea528c61fa73f158e4bc",
                   ext.map_object_id(ID_2));
        assert_eq!("7fd/b24/28e/7fdb2428e841e023e24be158d9e2dfeb",
                   ext.map_object_id(ID_3));

        // sha1
        let ext = hashed_ntuple_ext("sha1", 3, 3, false).unwrap();

        assert_eq!("903/844/22e/90384422ea7703eed693b79c23871eff4650bc2f",
                   ext.map_object_id(ID_1));
        assert_eq!("178/5a5/0ed/1785a50ed995e95cad9489a2926bd0f0a3b5e799",
                   ext.map_object_id(ID_2));
        assert_eq!("f3c/67e/836/f3c67e8367f0b67fb3f9951d74b8f955cb59a3b4",
                   ext.map_object_id(ID_3));

        // sha512
        let ext = hashed_ntuple_ext("sha512", 3, 3, false).unwrap();

        assert_eq!("a43/39e/be5/a4339ebe5aeb1766748f86130c9f1a338706fc9972a453674c6d51074954a2d9d822\
        68166d05b78eb15a18f30f97e13a3c6a37f00ae29d3c6815bed9b8d7050b",
                   ext.map_object_id(ID_1));
        assert_eq!("3a9/f56/a75/3a9f56a75ca66b24341967ed8f3e1900225f64c452e7111f51b13c7a1b0b8054f395\
        c7787d710c6000257da3d95e0f4518e0f05bff9d5187786aecdab02412f2",
                   ext.map_object_id(ID_2));
        assert_eq!("fa2/29e/b18/fa229eb18fb3aaf013ca8cfe4536c9d169c3543d3e442bd8662e52a253d1a72522c4\
        30606a7062dfc086e6132eea7bf9614f83b77107efd91896ece2b1389d98",
                   ext.map_object_id(ID_3));

        // sha512/256
        let ext = hashed_ntuple_ext("sha512/256", 3, 3, false).unwrap();

        assert_eq!("570/0dc/cca/5700dccca5547746cbfe291156d8c9d9ee750026a7868c76d3689478d31e2caf",
                   ext.map_object_id(ID_1));
        assert_eq!("163/6d3/e4e/1636d3e4e3b9ef85b0c095047159e424bdb4e99d8f0ed9d2283aa6f63ce85cec",
                   ext.map_object_id(ID_2));
        assert_eq!("0b7/fbc/00c/0b7fbc00c46dacddaf64912b53aca14855a8230c6ab0e34beea2d60dae8bf6d7",
                   ext.map_object_id(ID_3));

        // blake2b-512
        let ext = hashed_ntuple_ext("blake2b-512", 3, 3, false).unwrap();

        assert_eq!("8de/6c4/2cc/8de6c42ccbd068fcc274fad7ee5257f14e4ef1696b040144691e725bb1a779eb2d8\
        c89df2d0b48b441d1810677e2ebb7cf11243ff0df7bc026a997d414e65f5f",
                   ext.map_object_id(ID_1));
        assert_eq!("751/b54/6e2/751b546e2782ce58cdb197134b63f2743f8742373e346973b2c3674a70abc27cfba\
        aed7b4f68d0e44ced88bf2d4302255e094ae6d2f674e90e60338340962f99",
                   ext.map_object_id(ID_2));
        assert_eq!("39d/20e/f35/39d20ef3533754b580d4097a7a72f7b133b9c3216a35e91f82dd2f8c264a18606ed\
        e2c68d54055311a68f467be6e915cff1e66934a5a61c9d2e2bb66a30a5652",
                   ext.map_object_id(ID_3));
    }

    #[test]
    fn fail_0004_init_when_invalid_digest() {
        let ext = hashed_ntuple_ext("md6", 3, 3, false);
        expected_err(ext.err().unwrap().into(), "Unsupported digest algorithm: md6");
    }

    #[test]
    fn fail_0004_init_when_invalid_tuple() {
        let ext = hashed_ntuple_ext("sha256", 0, 3, false);
        expected_err(ext.err().unwrap().into(), "then both must be 0");

        let ext = hashed_ntuple_ext("sha256", 3, 0, false);
        expected_err(ext.err().unwrap().into(), "then both must be 0");
    }

    #[test]
    fn fail_0004_init_when_digest_not_long_enough() {
        let ext = hashed_ntuple_ext("sha256", 10, 10, false);
        expected_err(ext.err().unwrap().into(), "minimum of 100 characters");
    }

    fn hashed_ntuple_ext(algorithm: &str, tuple_size: usize, number_of_tuples: usize, short: bool)
        -> Result<HashedNTupleLayoutExtension> {
        HashedNTupleLayoutExtension::new(Some(format!("{{
            \"extensionName\": \"0004-hashed-n-tuple-storage-layout\",
            \"digestAlgorithm\": \"{}\",
            \"tupleSize\": {},
            \"numberOfTuples\": {},
            \"shortObjectRoot\": {}
        }}", algorithm, tuple_size, number_of_tuples, short).as_bytes()))
    }

    fn hashed_ntuple_id_ext(algorithm: &str, tuple_size: usize, number_of_tuples: usize)
        -> Result<HashedNTupleObjectIdLayoutExtension> {
        HashedNTupleObjectIdLayoutExtension::new(Some(format!("{{
            \"extensionName\": \"0003-hash-and-id-n-tuple-storage-layout\",
            \"digestAlgorithm\": \"{}\",
            \"tupleSize\": {},
            \"numberOfTuples\": {}
        }}", algorithm, tuple_size, number_of_tuples).as_bytes()))
    }

    fn expected_err(err: Box<dyn Error>, expected: &str) -> () {
        let msg = err.to_string();
        assert!(msg.contains(expected), "actual error message: {}", msg);
    }

}
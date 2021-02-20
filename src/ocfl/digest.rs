use core::{cmp, fmt};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::io;
use std::io::Read;

use blake2::{Blake2b, VarBlake2b};
use digest::{Digest, DynDigest, Update, VariableOutput};
use md5::Md5;
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Sha256, Sha512, Sha512Trunc256};
use strum_macros::{Display as EnumDisplay, EnumString};

use crate::ocfl::error::{Result, RocflError};

/// Enum of all valid digest algorithms
#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Copy, Clone, EnumString, EnumDisplay)]
pub enum DigestAlgorithm {
    #[serde(rename = "md5")]
    #[strum(serialize = "md5")]
    Md5,
    #[serde(rename = "sha1")]
    #[strum(serialize = "sha1")]
    Sha1,
    #[serde(rename = "sha256")]
    #[strum(serialize = "sha256")]
    Sha256,
    #[serde(rename = "sha512")]
    #[strum(serialize = "sha512")]
    Sha512,
    #[serde(rename = "sha512/256")]
    #[strum(serialize = "sha512/256")]
    Sha512_256,
    #[serde(rename = "blake2b-512")]
    #[strum(serialize = "blake2b-512")]
    Blake2b512,
    #[serde(rename = "blake2b-160")]
    #[strum(serialize = "blake2b-160")]
    Blake2b160,
    #[serde(rename = "blake2b-256")]
    #[strum(serialize = "blake2b-256")]
    Blake2b256,
    #[serde(rename = "blake2b-384")]
    #[strum(serialize = "blake2b-384")]
    Blake2b384,
}

/// Reader wrapper that calculates a digest while reading
pub struct DigestReader<R: Read> {
    digest: Box<dyn DynDigest>,
    inner: R,
}

/// A digest encoded as a hex string
#[derive(Debug, Eq, Clone)]
pub struct HexDigest(String);

impl DigestAlgorithm {
    /// Hashes the input and returns its hex encoded digest
    pub fn hash_hex(&self, data: impl AsRef<[u8]>) -> HexDigest {
        // This ugliness is because the variable length blake2b algorithms don't work with DynDigest
        let bytes = match self {
            DigestAlgorithm::Md5 => {
                let mut hasher = Md5::new();
                Digest::update(&mut hasher, data);
                hasher.finalize().to_vec()
            }
            DigestAlgorithm::Sha1 => {
                let mut hasher = Sha1::new();
                Digest::update(&mut hasher, data);
                hasher.finalize().to_vec()
            }
            DigestAlgorithm::Sha256 => {
                let mut hasher = Sha256::new();
                Digest::update(&mut hasher, data);
                hasher.finalize().to_vec()
            }
            DigestAlgorithm::Sha512 => {
                let mut hasher = Sha512::new();
                Digest::update(&mut hasher, data);
                hasher.finalize().to_vec()
            }
            DigestAlgorithm::Sha512_256 => {
                let mut hasher = Sha512Trunc256::new();
                Digest::update(&mut hasher, data);
                hasher.finalize().to_vec()
            }
            DigestAlgorithm::Blake2b512 => {
                let mut hasher = Blake2b::new();
                Digest::update(&mut hasher, data);
                hasher.finalize().to_vec()
            }
            DigestAlgorithm::Blake2b160 => {
                let mut hasher = VarBlake2b::new(20).unwrap();
                hasher.update(data);
                hasher.finalize_boxed().to_vec()
            }
            DigestAlgorithm::Blake2b256 => {
                let mut hasher = VarBlake2b::new(32).unwrap();
                hasher.update(data);
                hasher.finalize_boxed().to_vec()
            }
            DigestAlgorithm::Blake2b384 => {
                let mut hasher = VarBlake2b::new(48).unwrap();
                hasher.update(data);
                hasher.finalize_boxed().to_vec()
            }
        };

        bytes.into()
    }

    /// Wraps the specified reader in a `DigestReader`. Does not support blake2b because of the
    /// DynDigest problem.
    pub fn reader<R: Read>(&self, reader: R) -> Result<DigestReader<R>> {
        let digest: Box<dyn DynDigest> = match self {
            DigestAlgorithm::Md5 => Box::new(Md5::new()),
            DigestAlgorithm::Sha1 => Box::new(Sha1::new()),
            DigestAlgorithm::Sha256 => Box::new(Sha256::new()),
            DigestAlgorithm::Sha512 => Box::new(Sha512::new()),
            DigestAlgorithm::Sha512_256 => Box::new(Sha512Trunc256::new()),
            _ => return Err(RocflError::General("Blake2b is not supported for streaming digest.".to_string())),
        };

        Ok(DigestReader::new(digest, reader))
    }
}

impl<R: Read> DigestReader<R> {
    pub fn new(digest: Box<dyn DynDigest>, reader: R) -> Self {
        Self {
            digest,
            inner: reader,
        }
    }

    pub fn finalize_hex(self) -> HexDigest {
        self.digest.finalize().to_vec().into()
    }
}

impl<R: Read> Read for DigestReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let result = self.inner.read(buf)?;

        if result > 0 {
            self.digest.update(&buf);
        }

        Ok(result)
    }
}

impl From<Vec<u8>> for HexDigest {
    fn from(bytes: Vec<u8>) -> Self {
        Self(hex::encode(bytes))
    }
}

impl From<HexDigest> for String {
    fn from(digest: HexDigest) -> Self {
        digest.0
    }
}

impl Ord for HexDigest {
    /// Case insensitive string comparison
    fn cmp(&self, other: &Self) -> Ordering {
        // Based on SliceOrd::compare()
        // This is slightly more efficient than converting the entire str to lower case and then
        // comparing because only a single iteration is needed.

        let left = self.0.as_bytes();
        let right = other.0.as_bytes();

        let l = cmp::min(left.len(), right.len());

        // Slice to the loop iteration range to enable bound check
        // elimination in the compiler
        let lhs = &left[..l];
        let rhs = &right[..l];

        for i in 0..l {
            match lhs[i].to_ascii_lowercase().cmp(&rhs[i].to_ascii_lowercase()) {
                Ordering::Equal => (),
                non_eq => return non_eq,
            }
        }

        left.len().cmp(&right.len())
    }
}

impl PartialOrd for HexDigest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HexDigest {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Hash for HexDigest {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_ascii_lowercase().hash(state);
    }
}

impl Display for HexDigest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

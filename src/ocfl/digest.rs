use core::{cmp, fmt};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::io;
use std::io::{Read, Write};

use blake2::digest::consts::{U20, U32, U48};
use blake2::{Blake2b, Blake2b512};
use digest::{Digest, DynDigest};
use md5::Md5;
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Sha256, Sha512, Sha512_256};
use strum_macros::{Display as EnumDisplay, EnumString};

use crate::ocfl::error::Result;

type Blake2b160 = Blake2b<U20>;
type Blake2b256 = Blake2b<U32>;
type Blake2b384 = Blake2b<U48>;

/// Enum of all valid digest algorithms
#[derive(
    Deserialize, Serialize, Debug, Hash, Eq, PartialEq, Copy, Clone, EnumString, EnumDisplay,
)]
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

/// Writer wrapper that calculates a digest while writing
pub struct DigestWriter<W: Write> {
    digest: Box<dyn DynDigest>,
    inner: W,
}

/// Writer wrapper that calculates multiple digests while writing
pub struct MultiDigestWriter<W: Write> {
    digests: HashMap<DigestAlgorithm, Box<dyn DynDigest>>,
    inner: W,
}

/// A digest encoded as a hex string
#[derive(Deserialize, Serialize, Debug, Eq, Clone)]
pub struct HexDigest(String);

impl DigestAlgorithm {
    /// Hashes the input and returns its hex encoded digest
    pub fn hash_hex(&self, data: &mut impl Read) -> Result<HexDigest> {
        let mut hasher = self.reader(data);
        io::copy(&mut hasher, &mut io::sink())?;
        Ok(hasher.finalize_hex())
    }

    /// Wraps the specified reader in a `DigestReader`
    pub fn reader<R: Read>(&self, reader: R) -> DigestReader<R> {
        DigestReader::new(self.new_digest(), reader)
    }

    /// Wraps the specified writer in a `DigestWriter`
    pub fn writer<W: Write>(&self, writer: W) -> DigestWriter<W> {
        DigestWriter::new(self.new_digest(), writer)
    }

    fn new_digest(&self) -> Box<dyn DynDigest> {
        match self {
            DigestAlgorithm::Md5 => Box::new(Md5::new()),
            DigestAlgorithm::Sha1 => Box::new(Sha1::new()),
            DigestAlgorithm::Sha256 => Box::new(Sha256::new()),
            DigestAlgorithm::Sha512 => Box::new(Sha512::new()),
            DigestAlgorithm::Sha512_256 => Box::new(Sha512_256::new()),
            DigestAlgorithm::Blake2b512 => Box::new(Blake2b512::new()),
            DigestAlgorithm::Blake2b160 => Box::new(Blake2b160::new()),
            DigestAlgorithm::Blake2b256 => Box::new(Blake2b256::new()),
            DigestAlgorithm::Blake2b384 => Box::new(Blake2b384::new()),
        }
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
            self.digest.update(&buf[0..result]);
        }

        Ok(result)
    }
}

impl<W: Write> DigestWriter<W> {
    pub fn new(digest: Box<dyn DynDigest>, writer: W) -> Self {
        Self {
            digest,
            inner: writer,
        }
    }

    pub fn finalize_hex(self) -> HexDigest {
        self.digest.finalize().to_vec().into()
    }
}

impl<W: Write> Write for DigestWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let result = self.inner.write(buf)?;

        if result > 0 {
            self.digest.update(&buf[0..result]);
        }

        Ok(result)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<W: Write> MultiDigestWriter<W> {
    pub fn new(algorithms: &[DigestAlgorithm], writer: W) -> Self {
        let mut digests = HashMap::with_capacity(algorithms.len());
        for algorithm in algorithms {
            digests.insert(*algorithm, algorithm.new_digest());
        }

        Self {
            digests,
            inner: writer,
        }
    }

    pub fn inner(&self) -> &W {
        &self.inner
    }

    pub fn finalize_hex(self) -> HashMap<DigestAlgorithm, HexDigest> {
        let mut results = HashMap::with_capacity(self.digests.len());
        for (algorithm, digest) in self.digests {
            results.insert(algorithm, digest.finalize().to_vec().into());
        }
        results
    }
}

impl<W: Write> Write for MultiDigestWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let result = self.inner.write(buf)?;

        if result > 0 {
            let part = &buf[0..result];
            self.digests
                .values_mut()
                .for_each(|digest| digest.update(part));
        }

        Ok(result)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl From<Vec<u8>> for HexDigest {
    fn from(bytes: Vec<u8>) -> Self {
        Self(hex::encode(bytes))
    }
}

impl From<&str> for HexDigest {
    fn from(digest: &str) -> Self {
        Self(digest.to_string())
    }
}

impl From<String> for HexDigest {
    fn from(digest: String) -> Self {
        Self(digest)
    }
}

impl From<HexDigest> for String {
    fn from(digest: HexDigest) -> Self {
        digest.0
    }
}

impl AsRef<str> for HexDigest {
    fn as_ref(&self) -> &str {
        self.0.as_str()
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
            match lhs[i]
                .to_ascii_lowercase()
                .cmp(&rhs[i].to_ascii_lowercase())
            {
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
        self.0.eq_ignore_ascii_case(&other.0)
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

#[cfg(test)]
mod tests {
    use std::io;

    use crate::ocfl::digest::MultiDigestWriter;
    use crate::ocfl::error::Result;
    use crate::ocfl::DigestAlgorithm;

    #[test]
    fn calculate_digest_while_reading() -> Result<()> {
        let input = "testing\n".to_string();
        let mut output: Vec<u8> = Vec::new();

        let mut reader = DigestAlgorithm::Sha512.reader(input.as_bytes());

        io::copy(&mut reader, &mut output)?;

        let expected =
            "24f950aac7b9ea9b3cb728228a0c82b67c39e96b4b344798870d5daee93e3ae5931baae8c7c\
        acfea4b629452c38026a81d138bc7aad1af3ef7bfd5ec646d6c28"
                .to_string();
        let actual = reader.finalize_hex();

        assert_eq!(input, String::from_utf8(output).unwrap());
        assert_eq!(
            DigestAlgorithm::Sha512.hash_hex(&mut input.as_bytes())?,
            actual
        );
        assert_eq!(expected, actual.to_string());

        Ok(())
    }

    #[test]
    fn calculate_digest_while_writing() -> Result<()> {
        let input = "testing\n".to_string();
        let output: Vec<u8> = Vec::new();

        let mut writer = DigestAlgorithm::Sha512.writer(output);

        io::copy(&mut input.as_bytes(), &mut writer)?;

        let expected =
            "24f950aac7b9ea9b3cb728228a0c82b67c39e96b4b344798870d5daee93e3ae5931baae8c7c\
        acfea4b629452c38026a81d138bc7aad1af3ef7bfd5ec646d6c28"
                .to_string();
        let actual = writer.finalize_hex();

        assert_eq!(expected, actual.to_string());

        Ok(())
    }

    #[test]
    fn calculate_multiple_digests_while_writing() -> Result<()> {
        let input = "testing\n".to_string();
        let output: Vec<u8> = Vec::new();

        let mut writer = MultiDigestWriter::new(
            &[
                DigestAlgorithm::Md5,
                DigestAlgorithm::Sha256,
                DigestAlgorithm::Sha512,
            ],
            output,
        );

        io::copy(&mut input.as_bytes(), &mut writer)?;

        let expected_sha512 =
            "24f950aac7b9ea9b3cb728228a0c82b67c39e96b4b344798870d5daee93e3ae5931baae8c7c\
        acfea4b629452c38026a81d138bc7aad1af3ef7bfd5ec646d6c28"
                .to_string();
        let expected_sha256 =
            "12a61f4e173fb3a11c05d6471f74728f76231b4a5fcd9667cef3af87a3ae4dc2".to_string();
        let expected_md5 = "eb1a3227cdc3fedbaec2fe38bf6c044a".to_string();

        let actual = writer.finalize_hex();

        assert_eq!(
            expected_sha512,
            actual.get(&DigestAlgorithm::Sha512).unwrap().to_string()
        );
        assert_eq!(
            expected_sha256,
            actual.get(&DigestAlgorithm::Sha256).unwrap().to_string()
        );
        assert_eq!(
            expected_md5,
            actual.get(&DigestAlgorithm::Md5).unwrap().to_string()
        );

        Ok(())
    }

    #[test]
    fn blake2b_test() {
        let digest = DigestAlgorithm::Blake2b160
            .hash_hex(&mut "test".as_bytes())
            .unwrap();
        assert_eq!(
            "a34fc3b6d2cce8beb3216c2bbb5e55739e8121ed".to_string(),
            digest.to_string()
        );

        let digest = DigestAlgorithm::Blake2b256
            .hash_hex(&mut "test".as_bytes())
            .unwrap();
        assert_eq!(
            "928b20366943e2afd11ebc0eae2e53a93bf177a4fcf35bcc64d503704e65e202".to_string(),
            digest.to_string()
        );

        let digest = DigestAlgorithm::Blake2b384
            .hash_hex(&mut "test".as_bytes())
            .unwrap();
        assert_eq!("8a84b8666c8fcfb69f2ec41f578d7c85fbdb504ea6510fb05b50fcbf7ed8153c77943bc2da73abb136834e1a0d4f22cb".to_string(),
                   digest.to_string());

        let digest = DigestAlgorithm::Blake2b512
            .hash_hex(&mut "test".as_bytes())
            .unwrap();
        assert_eq!("a71079d42853dea26e453004338670a53814b78137ffbed07603a41d76a483aa9bc33b582f77d30a65e6f29a896c0411f38312e1d66e0bf16386c86a89bea572".to_string(),
                   digest.to_string());
    }
}

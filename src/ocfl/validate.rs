use crate::ocfl::error::{Result, RocflError};
use crate::ocfl::DigestAlgorithm;

pub fn validate_object_id(object_id: &str) -> Result<()> {
    if object_id.is_empty() {
        return Err(RocflError::InvalidValue(
            "Object IDs may not be blank".to_string(),
        ));
    }
    Ok(())
}

pub fn validate_digest_algorithm(digest_algorithm: DigestAlgorithm) -> Result<()> {
    if digest_algorithm != DigestAlgorithm::Sha512 && digest_algorithm != DigestAlgorithm::Sha256 {
        return Err(RocflError::InvalidValue(format!(
            "The inventory digest algorithm must be sha512 or sha256. Found: {}",
            digest_algorithm.to_string()
        )));
    }
    Ok(())
}

pub fn validate_content_dir(content_dir: &str) -> Result<()> {
    if content_dir.eq(".") || content_dir.eq("..") || content_dir.contains('/') {
        return Err(RocflError::InvalidValue(format!(
            "The content directory cannot equal '.' or '..' and cannot contain a '/'. Found: {}",
            content_dir
        )));
    }
    Ok(())
}

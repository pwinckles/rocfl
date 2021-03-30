use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use log::error;

use crate::ocfl::error::Result;
use crate::ocfl::{util, DigestAlgorithm, RocflError};

/// An object lock manager that works by attempting to atomically create files using a hash
/// of the object's id. The lock files should be automatically removed when the lock goes out
/// of scope.
pub struct LockManager {
    locks_dir: PathBuf,
    digest_algorithm: DigestAlgorithm,
}

pub struct ObjectLock {
    lock_path: PathBuf,
}

impl LockManager {
    /// Creates a new lock manager. `locks_dir` must already exist.
    pub fn new(locks_dir: impl AsRef<Path>) -> Self {
        Self {
            locks_dir: locks_dir.as_ref().to_path_buf(),
            digest_algorithm: DigestAlgorithm::Sha256,
        }
    }

    /// Acquires a lock on the given object. If the lock cannot be acquired,
    /// `RocflError::LockAcquire` is returned. The lock is _not_ reentrant.
    pub fn acquire(&self, object_id: &str) -> Result<ObjectLock> {
        let hash = self.digest_algorithm.hash_hex(&mut object_id.as_bytes())?;
        let lock_path = self.locks_dir.join(format!("{}.lock", hash.as_ref()));

        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
        {
            Ok(_) => Ok(ObjectLock { lock_path }),
            Err(_) => Err(RocflError::LockAcquire(
                object_id.to_string(),
                lock_path.to_string_lossy().into(),
            )),
        }
    }
}

impl Drop for ObjectLock {
    fn drop(&mut self) {
        if let Err(e) = util::remove_file_ignore_not_found(&self.lock_path) {
            error!(
                "Failed to remove lock file {}: {}",
                self.lock_path.to_string_lossy(),
                e
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_fs::TempDir;

    use crate::ocfl::lock::LockManager;
    use crate::ocfl::RocflError;

    #[test]
    fn acquire_lock_when_available() {
        let temp = TempDir::new().unwrap();
        let manager = LockManager::new(temp.path());

        let object_id = "testing";

        let _lock = manager.acquire(object_id).unwrap();

        assert_cannot_acquire_lock(object_id, &manager);
    }

    #[test]
    fn acquire_multiple_locks() {
        let temp = TempDir::new().unwrap();
        let manager = LockManager::new(temp.path());

        let object_1_id = "one";
        let object_2_id = "two";

        let _lock1 = manager.acquire(object_1_id).unwrap();
        let _lock2 = manager.acquire(object_2_id).unwrap();

        assert_cannot_acquire_lock(object_1_id, &manager);
        assert_cannot_acquire_lock(object_2_id, &manager);
    }

    #[test]
    fn release_lock_when_out_of_scope() {
        let temp = TempDir::new().unwrap();
        let manager = LockManager::new(temp.path());

        let object_1_id = "one";
        let object_2_id = "two";

        {
            let _lock1 = manager.acquire(object_1_id).unwrap();
            assert_cannot_acquire_lock(object_1_id, &manager);

            {
                let _lock2 = manager.acquire(object_2_id).unwrap();
                assert_cannot_acquire_lock(object_2_id, &manager);
                assert_cannot_acquire_lock(object_1_id, &manager);
            }

            let _lock2 = manager.acquire(object_2_id).unwrap();
        }

        let _lock1 = manager.acquire(object_1_id).unwrap();
    }

    fn assert_cannot_acquire_lock(object_id: &str, manager: &LockManager) {
        match manager.acquire(object_id) {
            Err(RocflError::LockAcquire(..)) => (),
            _ => {
                panic!("Expected the lock to be unavailable")
            }
        }
    }
}

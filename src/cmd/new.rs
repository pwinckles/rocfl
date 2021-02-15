use crate::cmd::{Cmd, GlobalArgs};
use crate::cmd::opts::{DigestAlgorithm as OptAlgorithm, New};
use crate::ocfl::{DigestAlgorithm, OcflRepo, Result};

// TODO move all of these simple commands into a single file?

impl Cmd for New {
    fn exec(&self, repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        repo.create_object(&self.object_id,
                           algorithm(self.digest_algorithm),
                           &self.content_directory,
                           self.zero_padding)

        // TODO print anything?
    }
}

fn algorithm(algorithm: OptAlgorithm) -> DigestAlgorithm {
    match algorithm {
        OptAlgorithm::Sha256 => DigestAlgorithm::Sha256,
        OptAlgorithm::Sha512 => DigestAlgorithm::Sha512,
    }
}
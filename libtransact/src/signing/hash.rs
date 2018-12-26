use sha2::{Digest, Sha512};

use crate::signing::Error;
use crate::signing::Signer;

pub struct HashSigner {
    dummy_public_key: Vec<u8>,
}

impl HashSigner {
    pub fn new() -> Self {
        HashSigner::default()
    }
}

impl Default for HashSigner {
    fn default() -> Self {
        HashSigner {
            dummy_public_key: String::from("hash_signer").into_bytes(),
        }
    }
}

impl Signer for HashSigner {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, Error> {
        let mut hasher = Sha512::new();
        hasher.input(message);
        Ok(hasher.result().to_vec())
    }

    fn public_key(&self) -> &[u8] {
        &self.dummy_public_key
    }
}

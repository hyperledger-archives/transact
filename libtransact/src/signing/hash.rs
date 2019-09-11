/*
 * Copyright 2018 Bitwise IO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -----------------------------------------------------------------------------
 */

//! A SHA-512 Hash Signer
//!
//! The HashSigner provides a simple implementation of the Signer trait, by simply producing a
//! SHA-512 hash of the message bytes.  This implementation allows for the use of transact without
//! the need of a cryptographic library for public-private key signing.

use sha2::{Digest, Sha512};

use crate::signing::Error;
use crate::signing::Signer;

pub struct HashSigner {
    public_key: Vec<u8>,
}

impl HashSigner {
    pub fn new(public_key: Vec<u8>) -> Self {
        Self { public_key }
    }
}

impl Default for HashSigner {
    fn default() -> Self {
        HashSigner {
            public_key: String::from("hash_signer").into_bytes(),
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
        &self.public_key
    }
}

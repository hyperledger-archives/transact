/*
 * Copyright 2019 Sovrin Foundation
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

use ursa::keys::{PrivateKey, PublicKey};
use ursa::signatures::{secp256k1::EcdsaSecp256k1Sha256, SignatureScheme};

use crate::signing::Error;
use crate::signing::Signer;

pub struct UrsaSigner {
    scheme: EcdsaSecp256k1Sha256,
    pk: PublicKey,
    sk: PrivateKey,
}

impl UrsaSigner {
    pub fn new() -> Self {
        let scheme = EcdsaSecp256k1Sha256::new();
        let (pk, sk) = scheme.keypair(None).unwrap();
        UrsaSigner { scheme, pk, sk }
    }
}

impl Default for UrsaSigner {
    fn default() -> Self {
        UrsaSigner::new()
    }
}

impl Signer for UrsaSigner {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, Error> {
        self.scheme
            .sign(message, &self.sk)
            .map_err(|e| Error::SigningError(format!("{:?}", e)))
    }

    fn public_key(&self) -> &[u8] {
        self.pk.0.as_slice()
    }
}

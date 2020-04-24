/*
 * Copyright 2019 Cargill Incorporated
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

#[cfg(feature = "contract-address-double-key-hash")]
pub mod double_key_hash;
mod error;
#[cfg(feature = "contract-address-key-hash")]
pub mod key_hash;
#[cfg(feature = "contract-address-triple-key-hash")]
pub mod triple_key_hash;

pub use crate::contract::address::error::AddresserError;

use sha2::{Digest, Sha512};

pub const ADDRESS_LENGTH: usize = 70;

pub trait Addresser<K> {
    /// Returns a radix address calculated from the given key
    ///
    /// # Arguments
    ///
    /// * `key` - A natural key, as defined by K, used to calculate an address
    ///
    fn compute(&self, key: &K) -> Result<String, AddresserError>;

    /// Returns a human readable string of the given key
    ///
    /// # Arguments
    ///
    /// * `key` - A natural key, as defined by K
    ///
    fn normalize(&self, key: &K) -> String;
}

pub fn hash(hash_length: usize, key: &str) -> String {
    let mut sha = Sha512::new();
    sha.input(key.as_bytes());
    hex::encode(sha.result().to_vec())[..hash_length].to_string()
}

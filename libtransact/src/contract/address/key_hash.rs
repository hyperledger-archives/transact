// Copyright 2019 Cargill Incorporated
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::contract::address::{hash, Addresser, AddresserError, ADDRESS_LENGTH};

pub struct KeyHashAddresser {
    prefix: String,
}

impl KeyHashAddresser {
    pub fn new(prefix: String) -> KeyHashAddresser {
        KeyHashAddresser { prefix }
    }
}

impl Addresser<String> for KeyHashAddresser {
    fn compute(&self, key: &String) -> Result<String, AddresserError> {
        let hash_length = ADDRESS_LENGTH - self.prefix.len();

        Ok(String::from(&self.prefix) + &hash(hash_length, key))
    }

    fn normalize(&self, key: &String) -> String {
        key.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// The KeyHashAddresser is constructed by providing a 6 character `prefix.` The KeyHashAddresser
    /// represents keys as single strings. The `compute` method must combine the `prefix` and the
    /// provided natural key to create a valid radix address. This radix address is valid if:
    ///
    /// 1. The address is equal to the ADDRESS_LENGTH const
    /// 2. The prefix is present in the beginning of the address
    /// 3. The remaining characters match the same amount of characters from the hash of the provided
    ///    natural key.
    ///
    /// This test also ensures that the instantiated KeyHashAddresser can transform the natural key
    /// into a single string, using the `normalize` method.
    fn test_key_hash_addresser() {
        // Instantiate a KeyHashAddresser, using a simple 6 character prefix
        let addresser = KeyHashAddresser::new("prefix".to_string());
        // Calculate an address using `a` as the natural key
        let addr = addresser.compute(&"a".to_string()).unwrap();

        // Verify the beginning characters match the provided prefix and the address length.
        assert_eq!(addr[..6], "prefix".to_string());
        assert_eq!(addr.len(), ADDRESS_LENGTH);

        // Verify the remaining section of the calculated address matches the direct hash of the key.
        let key_hash = hash(64, "a");
        let remaining = ADDRESS_LENGTH - 6;
        assert_eq!(addr[6..ADDRESS_LENGTH], key_hash[..remaining]);

        // Verify the instantiated KeyHashAddresser outputs the correctly normalized key.
        let normalized = addresser.normalize(&"b".to_string());
        assert_eq!(normalized, "b".to_string());
    }
}

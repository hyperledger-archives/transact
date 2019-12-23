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

pub struct DoubleKeyHashAddresser {
    prefix: String,
    first_hash_length: usize,
}

impl DoubleKeyHashAddresser {
    pub fn new(prefix: String, first_hash_length: Option<usize>) -> DoubleKeyHashAddresser {
        DoubleKeyHashAddresser {
            prefix: prefix.clone(),
            first_hash_length: first_hash_length.unwrap_or((ADDRESS_LENGTH - prefix.len()) / 2),
        }
    }
}

impl Addresser<(String, String)> for DoubleKeyHashAddresser {
    fn compute(&self, key: &(String, String)) -> Result<String, AddresserError> {
        let hash_length = ADDRESS_LENGTH - self.prefix.len();
        let second_hash_length = hash_length - self.first_hash_length;
        if (self.prefix.len() + self.first_hash_length + second_hash_length) != ADDRESS_LENGTH {
            return Err(AddresserError {
                message: format!("Hash length does not equal {}", ADDRESS_LENGTH),
            });
        }
        let first_hash = &hash(self.first_hash_length, &key.0);
        let second_hash = &hash(second_hash_length, &key.1);

        Ok(String::from(&self.prefix) + first_hash + second_hash)
    }

    fn normalize(&self, key: &(String, String)) -> String {
        key.0.to_string() + "_" + &key.1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// This test constructs a DoubleKeyHashAddresser with a 6 character `prefix` and a None option
    /// for the `first_hash_length.` This test ensures a radix address passes basic criteria, as
    /// well as criteria specific to this DoubleKeyHashAddresser as well as the provided key
    /// `('a', 'b')`. Specifically, this test validates:
    ///
    /// 1. The address equal to the ADDRESS_LENGTH const
    /// 2. The prefix is present in the beginning of the address
    /// 3. The next characters, the length of which matches the default value `first_hash_length`
    ///    used to construct the DoubleKeyHashAddresser, match the hash of the first key, 'a', of
    ///    the tuple provided to the `compute` method
    /// 4. The remaining characters, the length of which matches the ADDRESS_LENGTH const less the
    ///    length of the provided `prefix` and the default `first_hash_length`, match the hash of
    ///    the second key, 'b', of the tuple provided to the `compute` method
    ///
    /// This test also ensures that the instantiated DoubleKeyHashAddresser can transform the natural
    /// key into a single string with the individual keys within the tuple of two strings separated
    /// by an underscore(`_`), using the `normalize` method.
    fn test_double_key_default_length() {
        // Creating a DoubleKeyHashAddresser with a 6 character `prefix` and None option for the
        // `first_hash_length`
        let addresser = DoubleKeyHashAddresser::new("prefix".to_string(), None);
        // Create the hashes of the individual keys to verify the constructed address
        let key1 = "a";
        let key1_hash = hash(32, key1);
        let key2 = "b";
        let key2_hash = hash(32, key2);
        // Compute the address
        let addr = addresser
            .compute(&(key1.to_string(), key2.to_string()))
            .unwrap();
        // Verify the `prefix` characters and the length
        assert_eq!(addr[..6], "prefix".to_string());
        assert_eq!(addr.len(), ADDRESS_LENGTH);
        // Verify the remaining characters match the hash of each key created above
        assert_eq!(addr[6..38], key1_hash[..32]);
        assert_eq!(addr[38..], key2_hash[..32]);
        // Verify the `normalize` method generates the correct single string
        let normalized = addresser.normalize(&(key1.to_string(), key2.to_string()));
        assert_eq!(normalized, "a_b".to_string());
    }

    #[test]
    /// This test constructs a DoubleKeyHashAddresser with a 6 character `prefix` and an optional
    /// value of 16 for the `first_hash_length.` This test ensures a radix address passes basic
    /// criteria, as well as criteria specific to this DoubleKeyHashAddresser as well as the
    /// provided key `('a', 'b')`. Specifically, this test validates:
    ///
    /// 1. The address equal to the ADDRESS_LENGTH const
    /// 2. The prefix is present in the beginning of the address
    /// 3. The next characters, the length of which matches the value `first_hash_length` (16)
    ///    used to construct the DoubleKeyHashAddresser, match the hash of the first key, 'a', of
    ///    the tuple provided to the `compute` method
    /// 4. The remaining characters, the length of which matches the ADDRESS_LENGTH const less the
    ///    length of the provided `prefix` and the `first_hash_length`, match the hash of
    ///    the second key, 'b', of the tuple provided to the `compute` method
    ///
    /// This test also ensures that the instantiated DoubleKeyHashAddresser can transform the natural
    /// key into a single string with the individual keys within the tuple of two strings separated
    /// by an underscore(`_`), using the `normalize` method.
    fn test_double_key_custom_length() {
        // Creating a DoubleKeyHashAddresser with a 6 character `prefix` and value of 16 for the
        // `first_hash_length`
        let addresser = DoubleKeyHashAddresser::new("prefix".to_string(), Some(16));
        // Create the hashes of the individual keys to verify the constructed address
        let key1 = "a";
        let key1_hash = hash(16, key1);
        let key2 = "b";
        let key2_hash = hash(48, key2);
        // Compute the address
        let addr = addresser
            .compute(&(key1.to_string(), key2.to_string()))
            .unwrap();
        // Verify the `prefix` characters and the length
        assert_eq!(addr[..6], "prefix".to_string());
        assert_eq!(addr.len(), ADDRESS_LENGTH);
        // Verify the remaining characters match the hash of each key created above
        assert_eq!(addr[6..22], key1_hash[..16]);
        assert_eq!(addr[22..], key2_hash[..48]);
        // Verify the `normalize` method generates the correct single string
        let normalized = addresser.normalize(&(key1.to_string(), key2.to_string()));
        assert_eq!(normalized, "a_b".to_string());
    }
}

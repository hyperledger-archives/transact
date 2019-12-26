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

pub struct TripleKeyHashAddresser {
    prefix: String,
    first_hash_length: usize,
    second_hash_length: usize,
}

impl TripleKeyHashAddresser {
    pub fn new(
        prefix: String,
        first_hash_length: Option<usize>,
        second_hash_length: Option<usize>,
    ) -> Result<TripleKeyHashAddresser, AddresserError> {
        let (first, second) =
            calculate_hash_lengths(prefix.len(), first_hash_length, second_hash_length)?;
        Ok(TripleKeyHashAddresser {
            prefix,
            first_hash_length: first,
            second_hash_length: second,
        })
    }
}

impl Addresser<(String, String, String)> for TripleKeyHashAddresser {
    fn compute(&self, key: &(String, String, String)) -> Result<String, AddresserError> {
        let last_hash_length =
            ADDRESS_LENGTH - self.prefix.len() - (self.first_hash_length + self.second_hash_length);

        let first_hash = &hash(self.first_hash_length, &key.0);
        let second_hash = &hash(self.second_hash_length, &key.1);
        let third_hash = &hash(last_hash_length, &key.2);

        Ok(String::from(&self.prefix) + first_hash + second_hash + third_hash)
    }

    fn normalize(&self, key: &(String, String, String)) -> String {
        key.0.to_string() + "_" + &key.1 + "_" + &key.2
    }
}

// Used to calculate the lengths of the key hashes to be used to create an address by the
// TripleKeyHashAddresser.
fn calculate_hash_lengths(
    prefix_length: usize,
    first_length: Option<usize>,
    second_length: Option<usize>,
) -> Result<(usize, usize), AddresserError> {
    // Validate the length of the provided prefix is not greater than the ADDRESS_LENGTH.
    if prefix_length > ADDRESS_LENGTH {
        return Err(AddresserError {
            message: format!(
                "Prefix length ({}) is greater than total address length ({})",
                prefix_length, ADDRESS_LENGTH
            ),
        });
    }
    match (first_length, second_length) {
        (Some(first), Some(second)) => {
            // Validate the hash lengths plus the prefix length is not greater than ADDRESS_LENGTH.
            if prefix_length + first + second > ADDRESS_LENGTH {
                return Err(AddresserError {
                    message: format!(
                        "Prefix length ({}) and hash lengths ({}) combined are greater than \
                         total address length ({})",
                        prefix_length,
                        (first + second),
                        ADDRESS_LENGTH
                    ),
                });
            }
            Ok((first, second))
        }
        (None, Some(second)) => {
            // Validate the hash length plus the prefix length is not greater than ADDRESS_LENGTH.
            if prefix_length + second > ADDRESS_LENGTH {
                return Err(AddresserError {
                    message: format!(
                        "Prefix length ({}) and hash length ({}) combined are greater than \
                         total address length ({})",
                        prefix_length, second, ADDRESS_LENGTH
                    ),
                });
            }
            // If the prefix length and hash length are not greater than ADDRESS_LENGTH, the
            // other hash length can be calculated and returned.
            let calculated_length = (ADDRESS_LENGTH - prefix_length - second) / 2;
            Ok((calculated_length, second))
        }
        (Some(first), None) => {
            // Validate the hash length plus the prefix length is not greater than ADDRESS_LENGTH.
            if prefix_length + first > ADDRESS_LENGTH {
                return Err(AddresserError {
                    message: format!(
                        "Prefix length ({}) and hash length ({}) combined are greater than \
                         total address length ({})",
                        prefix_length, first, ADDRESS_LENGTH
                    ),
                });
            }
            // If the prefix length and hash ength are not greater than ADDRESS_LENGTH, the other
            // hash length can be calculated and returned.
            let calculated_length = (ADDRESS_LENGTH - prefix_length - first) / 2;
            Ok((first, calculated_length))
        }
        (None, None) => {
            // Calculate the first and second hash length.
            let calculated_first = (ADDRESS_LENGTH - prefix_length) / 3;
            let calculated_second = calculated_first;
            // Validate the calculated hash lengths plus the prefix length is not greater than the
            // ADDRESS_LENGTH.
            if prefix_length + calculated_first + calculated_second > ADDRESS_LENGTH {
                return Err(AddresserError {
                    message: format!(
                        "Prefix length ({}) and hash lengths ({}) combined are greater than \
                         total address length ({})",
                        prefix_length,
                        (calculated_first + calculated_second),
                        ADDRESS_LENGTH
                    ),
                });
            }
            // If the validation is passed, return the calculated hash lengths.
            Ok((calculated_first, calculated_second))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// This test constructs a TripleKeyHashAddresser with a 6 character `prefix` and a None option
    /// for the `first_hash_length` and `second_hash_length.` The default value for these hash lengths
    /// is the ADDRESS_LENGTH const minus the length of the prefix, then divided by 3. This test
    /// ensures this addresser computes a valid radix address from the provided key, `('a', 'b', 'c')`.
    /// Specifically, this test validates:
    ///
    /// 1. The address length matches the ADDRESS_LENGTH const
    /// 2. The prefix is present in the beginning of the address
    /// 3. The next characters, the length of which matches the default value `first_hash_length`,
    ///    match the hash of the first key, 'a', of the tuple provided to the `compute` method
    /// 4. The next characters, the length of which matches the default `second_hash_length`,
    ///    match the hash of the second key, 'b', of the tuple provided to the `compute` method
    /// 5. The remaining characters, the length of which matches the ADDRESS_LENGTH const less the
    ///    length of the provided `prefix`, the default `first_hash_length` and `second_hash_length`,
    ///    match the hash of the last key, 'c', of the tuple provided to the `compute` method
    ///
    /// This test also ensures that the instantiated TripleKeyHashAddresser can transform the natural
    /// key into a single string with the individual keys within the tuple of three strings separated
    /// by an underscore(`_`), using the `normalize` method.
    fn test_triple_key_default_length() {
        // Creating a DoubleKeyHashAddresser with a 6 character `prefix` and None options for the
        // `first_hash_length` and `second_hash_length`
        let addresser = TripleKeyHashAddresser::new("prefix".to_string(), None, None)
            .expect("Unable to construct TripleKeyHashAddresser");
        // Create the hashes of the individual keys to verify the constructed address
        let key1 = "a";
        let key1_hash = hash(21, key1);
        let key2 = "b";
        let key2_hash = hash(21, key2);
        let key3 = "c";
        let key3_hash = hash(22, key3);
        // Compute the address
        let addr = addresser
            .compute(&(key1.to_string(), key2.to_string(), key3.to_string()))
            .unwrap();
        // Verify the `prefix` characters and the length
        assert_eq!(addr[..6], "prefix".to_string());
        assert_eq!(addr.len(), ADDRESS_LENGTH);
        // Verify the remaining characters match the hash of each key created above
        assert_eq!(addr[6..27], key1_hash[..21]);
        assert_eq!(addr[27..48], key2_hash[..21]);
        assert_eq!(addr[48..], key3_hash[..22]);
        // Verify the `normalize` method generates the correct single string
        let normalized =
            addresser.normalize(&(key1.to_string(), key2.to_string(), key3.to_string()));
        assert_eq!(normalized, "a_b_c".to_string());
    }

    #[test]
    /// This test constructs a TripleKeyHashAddresser with a 6 character `prefix` and a Some option
    /// with a value of 14 for the `first_hash_length` and a None option for `second_hash_length.`
    /// The `second_hash_length` should be equal to the ADDRESS_LENGTH minus the length of the
    /// prefix and the `first_hash_length`, then divided by two.
    /// This test ensures this addresser computes a valid radix address from the provided key,
    /// `('a', 'b', 'c')`. Specifically, this test validates:
    ///
    /// 1. The address length matches the ADDRESS_LENGTH const
    /// 2. The prefix is present in the beginning of the address
    /// 3. The next characters, the length of which matches the value `first_hash_length` (14)
    ///    used to construct the TripleKeyHashAddresser, match the hash of the first key, 'a', of
    ///    the tuple provided to the `compute` method
    /// 4. The next characters, the length of which matches the calculated `second_hash_length`,
    ///    match the hash of the second key, 'b', of the tuple provided to the `compute` method
    /// 5. The remaining characters, the length of which matches the ADDRESS_LENGTH const less the
    ///    length of the provided `prefix`, the `first_hash_length` and `second_hash_length`,
    ///    match the hash of the last key, 'c', of the tuple provided to the `compute` method
    ///
    /// This test also ensures that the instantiated TripleKeyHashAddresser can transform the natural
    /// key into a single string with the individual keys within the tuple of three strings separated
    /// by an underscore(`_`), using the `normalize` method.
    fn test_triple_key_custom_first_length() {
        // Creating a DoubleKeyHashAddresser with a 6 character `prefix,` a Some option for the
        // `first_hash_length` and a None option for the `second_hash_length`
        let addresser = TripleKeyHashAddresser::new("prefix".to_string(), Some(14), None)
            .expect("Unable to construct TripleKeyHashAddresser");
        // Create the hashes of the individual keys to verify the constructed address
        let key1 = "a";
        let key1_hash = hash(14, key1);
        let key2 = "b";
        let key2_hash = hash(25, key2);
        let key3 = "c";
        let key3_hash = hash(25, key3);
        // Compute the address
        let addr = addresser
            .compute(&(key1.to_string(), key2.to_string(), key3.to_string()))
            .unwrap();
        // Verify the `prefix` characters and the length
        assert_eq!(addr[..6], "prefix".to_string());
        assert_eq!(addr.len(), ADDRESS_LENGTH);
        // Verify the remaining characters match the hash of each key created above
        assert_eq!(addr[6..20], key1_hash[..14]);
        assert_eq!(addr[20..45], key2_hash[..25]);
        assert_eq!(addr[45..], key3_hash[..25]);
        // Verify the `normalize` method generates the correct single string
        let normalized =
            addresser.normalize(&(key1.to_string(), key2.to_string(), key3.to_string()));
        assert_eq!(normalized, "a_b_c".to_string());
    }

    #[test]
    /// This test constructs a TripleKeyHashAddresser with a 6 character `prefix` and a None optional
    /// for the `first_hash_length` and a Some option for `second_hash_length` with a value of 14.
    /// The `first_hash_length` should be equal to the ADDRESS_LENGTH minus the length of the prefix
    /// and the `second_hash_length`, then divided by two.
    /// This test ensures this addresser computes a valid radix address from the provided key,
    /// `('a', 'b', 'c')`. Specifically, this test validates:
    ///
    /// 1. The address length matches the ADDRESS_LENGTH const
    /// 2. The prefix is present in the beginning of the address
    /// 3. The next characters, the length of which matches the calculated `first_hash_length`,
    ///    match the hash of the first key, 'a', of the tuple provided to the `compute` method
    /// 4. The next characters, the length of which matches the `second_hash_length` value (14),
    ///    match the hash of the second key, 'a', of the tuple provided to the `compute` method
    /// 5. The remaining characters, the length of which matches the ADDRESS_LENGTH const less the
    ///    length of the provided `prefix`, the `first_hash_length` and `second_hash_length`,
    ///    match the hash of the last key, 'c', of the tuple provided to the `compute` method
    ///
    /// This test also ensures that the instantiated TripleKeyHashAddresser can transform the natural
    /// key into a single string with the individual keys within the tuple of three strings separated
    /// by an underscore(`_`), using the `normalize` method.
    fn test_triple_key_custom_second_length() {
        // Creating a DoubleKeyHashAddresser with a 6 character `prefix,` a Some option for the
        // `second_hash_length` and a None option for the `first_hash_length`
        let addresser = TripleKeyHashAddresser::new("prefix".to_string(), None, Some(14))
            .expect("Unable to construct TripleKeyHashAddresser");
        // Create the hashes of the individual keys to verify the constructed address
        let key1 = "a";
        let key1_hash = hash(25, key1);
        let key2 = "b";
        let key2_hash = hash(14, key2);
        let key3 = "c";
        let key3_hash = hash(25, key3);
        // Compute the address
        let addr = addresser
            .compute(&(key1.to_string(), key2.to_string(), key3.to_string()))
            .unwrap();
        // Verify the `prefix` characters and the length
        assert_eq!(addr[..6], "prefix".to_string());
        assert_eq!(addr.len(), ADDRESS_LENGTH);
        // Verify the remaining characters match the hash of each key created above
        assert_eq!(addr[6..31], key1_hash[..25]);
        assert_eq!(addr[31..45], key2_hash[..14]);
        assert_eq!(addr[45..], key3_hash[..25]);
        // Verify the `normalize` method generates the correct single string
        let normalized =
            addresser.normalize(&(key1.to_string(), key2.to_string(), key3.to_string()));
        assert_eq!(normalized, "a_b_c".to_string());
    }

    #[test]
    /// This test constructs a TripleKeyHashAddresser with a 6 character `prefix` and a Some option
    /// with a value of 10 for `first_hash_length` and a Some option with a value of 10 for
    /// `second_hash_length.`
    /// This test ensures this addresser computes a valid radix address from the provided key,
    /// `('a', 'b', 'c')`. Specifically, this test validates:
    ///
    /// 1. The address length matches the ADDRESS_LENGTH const
    /// 2. The prefix is present in the beginning of the address
    /// 3. The next characters, the length of which matches the `first_hash_length` value (10),
    ///    match the hash of the first key, 'a', of the tuple provided to the `compute` method
    /// 4. The next characters, the length of which matches the `second_hash_length` value (10),
    ///    match the hash of the second key, 'a', of the tuple provided to the `compute` method
    /// 5. The remaining characters, the length of which matches the ADDRESS_LENGTH const less the
    ///    length of the provided `prefix`, the `first_hash_length` and `second_hash_length`,
    ///    match the hash of the last key, 'c', of the tuple provided to the `compute` method
    ///
    /// This test also ensures that the instantiated TripleKeyHashAddresser can transform the natural
    /// key into a single string with the individual keys within the tuple of three strings separated
    /// by an underscore(`_`), using the `normalize` method.
    fn test_triple_key_custom_lengths() {
        // Creating a DoubleKeyHashAddresser with a 6 character `prefix,` and Some options for the
        // `first_hash_length` and `second_hash_length`
        let addresser = TripleKeyHashAddresser::new("prefix".to_string(), Some(10), Some(10))
            .expect("Unable to construct TripleKeyHashAddresser");
        // Create the hashes of the individual keys to verify the constructed address
        let key1 = "a";
        let key1_hash = hash(10, key1);
        let key2 = "b";
        let key2_hash = hash(10, key2);
        let key3 = "c";
        let key3_hash = hash(44, key3);
        // Compute the address
        let addr = addresser
            .compute(&(key1.to_string(), key2.to_string(), key3.to_string()))
            .unwrap();
        // Verify the `prefix` characters and the length
        assert_eq!(addr[..6], "prefix".to_string());
        assert_eq!(addr.len(), ADDRESS_LENGTH);
        // Verify the remaining characters match the hash of each key created above
        assert_eq!(addr[6..16], key1_hash[..10]);
        assert_eq!(addr[16..26], key2_hash[..10]);
        assert_eq!(addr[26..], key3_hash[..44]);
        // Verify the `normalize` method generates the correct single string
        let normalized =
            addresser.normalize(&(key1.to_string(), key2.to_string(), key3.to_string()));
        assert_eq!(normalized, "a_b_c".to_string());
    }

    #[test]
    /// Tests the `calculate_hash_lengths` function to ensure it provides the correct values from
    /// various inputs. Specifically, this test validates the correct output when provided with a
    /// `prefix_length` of 6, a Some option with various values as the `first_hash_length`,
    /// and a None option as the `second_hash_length.` As the `second_hash_length` is None, the
    /// resulting value should be equal to ADDRESS_LENGTH less the `prefix_length` and
    /// `first_hash_length`, then divided by 2.
    ///
    /// This test validates the correct calculation for the `second_hash_length` and the matching
    /// value of the Some option for the `first_hash_length`
    fn test_calculate_hash_custom_first_length() {
        let (first_length, second_length) = calculate_hash_lengths(6, Some(21), None).unwrap();
        assert_eq!(first_length, 21);
        let remaining = ADDRESS_LENGTH - 6 - 21;
        assert_eq!(second_length, (remaining / 2));

        let (first_length, second_length) = calculate_hash_lengths(6, Some(41), None).unwrap();
        assert_eq!(first_length, 41);
        let remaining = ADDRESS_LENGTH - 6 - 41;
        assert_eq!(second_length, (remaining / 2));

        let (first_length, second_length) = calculate_hash_lengths(6, Some(61), None).unwrap();
        assert_eq!(first_length, 61);
        let remaining = ADDRESS_LENGTH - 6 - 61;
        assert_eq!(second_length, (remaining / 2));
    }

    #[test]
    /// Tests the `calculate_hash_lengths` function to ensure it provides the correct values from
    /// various inputs. Specifically, this test validates the correct output when provided with a
    /// `prefix_length` of 6, a Some option with various values as the `second_hash_length`,
    /// and a None option as the `first_hash_length.` As the `first_hash_length` is None, the
    /// resulting value should be equal to ADDRESS_LENGTH less the `prefix_length` and
    /// `second_hash_length`, then divided by 2.
    ///
    /// This test validates the correct calculation for the `first_hash_length` and the matching
    /// value of the Some option for the `second_hash_length`
    fn test_calculate_hash_custom_second_length() {
        let (first_length, second_length) = calculate_hash_lengths(6, None, Some(21)).unwrap();
        let remaining = ADDRESS_LENGTH - 6 - 21;
        assert_eq!(first_length, (remaining / 2));
        assert_eq!(second_length, 21);

        let (first_length, second_length) = calculate_hash_lengths(6, None, Some(41)).unwrap();
        let remaining = ADDRESS_LENGTH - 6 - 41;
        assert_eq!(first_length, (remaining / 2));
        assert_eq!(second_length, 41);

        let (first_length, second_length) = calculate_hash_lengths(6, None, Some(61)).unwrap();
        let remaining = ADDRESS_LENGTH - 6 - 61;
        assert_eq!(first_length, (remaining / 2));
        assert_eq!(second_length, 61);
    }

    #[test]
    /// Tests the `calculate_hash_lengths` function to ensure it provides the correct values from
    /// various inputs. Specifically, this test validates the correct output when provided with a
    /// `prefix_length` of 6, and Some options with various values for the `first_hash_length` and
    /// `second_hash_length.`
    ///
    /// This test validates the matching value of the Some option for the `first_hash_length` and
    /// `second_hash_length`
    fn test_calculate_hash_custom_lengths() {
        let (first_length, second_length) = calculate_hash_lengths(6, Some(42), Some(12)).unwrap();
        assert_eq!(first_length, 42);
        assert_eq!(second_length, 12);

        let (first_length, second_length) = calculate_hash_lengths(6, Some(12), Some(42)).unwrap();
        assert_eq!(first_length, 12);
        assert_eq!(second_length, 42);

        let (first_length, second_length) = calculate_hash_lengths(6, Some(20), Some(20)).unwrap();
        assert_eq!(first_length, 20);
        assert_eq!(second_length, 20);
    }

    #[test]
    /// Tests the `calculate_hash_lengths` function to ensure it provides the correct values from
    /// various inputs. Specifically, this test validates the correct output when provided with
    /// various values for the `prefix_length`, and a None option for the `first_hash_length` and
    /// `second_hash_length.` As both the provided hash lengths are None, they must be calculated.
    /// The resulting values should both be equal to ADDRESS_LENGTH less the `prefix_length`, then
    /// divided by 3.
    ///
    /// This test validates the correct calculation for the `first_hash_length` and `second_hash_length`
    fn test_calculate_hash_no_custom_lengths() {
        let (first_length, second_length) = calculate_hash_lengths(6, None, None).unwrap();
        let remaining = ADDRESS_LENGTH - 6;
        assert_eq!(first_length, (remaining / 3));
        assert_eq!(second_length, (remaining / 3));

        let (first_length, second_length) = calculate_hash_lengths(30, None, None).unwrap();
        let remaining = ADDRESS_LENGTH - 30;
        assert_eq!(first_length, (remaining / 3));
        assert_eq!(second_length, (remaining / 3));

        let (first_length, second_length) = calculate_hash_lengths(50, None, None).unwrap();
        let remaining = ADDRESS_LENGTH - 50;
        assert_eq!(first_length, (remaining / 3));
        assert_eq!(second_length, (remaining / 3));
    }

    #[test]
    #[should_panic]
    /// This test constructs a TripleKeyHashAddresser with a 6 character `prefix` and an optional
    /// value of the ADDRESS_LENGTH for the `first_hash_length` and None for the `second_hash_length.`
    /// This test ensures that an error will be returned as the length of the prefix and the custom
    /// length combined are greater than the const ADDRESS_LENGTH, currently set to 70.
    ///
    /// This test will attempt to construct a TripleKeyHashAddresser with an invalid custom hash
    /// length and should return an error. Also validates the expected error message.
    fn test_invalid_first_custom_length_construction() {
        // Creating a TripleKeyHashAddresser with a 6 character `prefix` and the `first_hash_length`
        // equal to the ADDRESS_LENGTH const which will return an error as the prefix length and
        // custom length combined are greater than the ADDRESS_LENGTH.
        let addresser =
            TripleKeyHashAddresser::new("prefix".to_string(), Some(ADDRESS_LENGTH), None);

        // Assert the Addresser constructor returned an error.
        assert!(addresser.is_err());
        // Unwrap to validate that this will panic.
        addresser.unwrap();
    }

    #[test]
    #[should_panic]
    /// This test constructs a TripleKeyHashAddresser with a 6 character `prefix` and an optional
    /// value of the ADDRESS_LENGTH for the `second_hash_length` and None for the `first_hash_length.`
    /// This test ensures that an error will be returned as the length of the prefix and the custom
    /// length combined are greater than the const ADDRESS_LENGTH, currently set to 70.
    ///
    /// This test will attempt to construct a TripleKeyHashAddresser with an invalid custom hash
    /// length and should return an error.
    fn test_invalid_second_custom_length_construction() {
        // Creating a TripleKeyHashAddresser with a 6 character `prefix` and the `second_hash_length`
        // equal to the ADDRESS_LENGTH const which will return an error as the prefix length and
        // custom length combined are greater than the ADDRESS_LENGTH.
        let addresser =
            TripleKeyHashAddresser::new("prefix".to_string(), None, Some(ADDRESS_LENGTH));

        // Assert the Addresser constructor returned an error.
        assert!(addresser.is_err());
        // Unwrap to validate that this will panic.
        addresser.unwrap();
    }

    #[test]
    #[should_panic]
    /// This test constructs a TripleKeyHashAddresser with a 6 character `prefix` and an optional
    /// value of half the ADDRESS_LENGTH const for both `second_hash_length` and `first_hash_length.`
    /// This test ensures that an error will be returned as the length of the prefix and the custom
    /// lengths combined are greater than the const ADDRESS_LENGTH, currently set to 70.
    ///
    /// This test will attempt to construct a TripleKeyHashAddresser with invalid custom hash
    /// lengths and should return an error.
    fn test_invalid_custom_lengths_construction() {
        // Creating a TripleKeyHashAddresser with a 6 character `prefix` and value of half the
        // ADDRESS_LENGTH const for the `first_hash_length` and `second_hash_length` which will
        // return an error as the prefixlength and custom lengths combined are greater than the
        // ADDRESS_LENGTH.
        let addresser = TripleKeyHashAddresser::new(
            "prefix".to_string(),
            Some(ADDRESS_LENGTH / 2),
            Some(ADDRESS_LENGTH / 2),
        );

        // Assert the Addresser constructor returned an error.
        assert!(addresser.is_err());
        // Unwrap to validate that this will panic.
        addresser.unwrap();
    }

    #[test]
    #[should_panic]
    /// This test constructs a TripleKeyHashAddresser with a 72 character `prefix` and a None
    /// value for both `second_hash_length` and `first_hash_length.` This test ensures that an error
    /// will be returned as the length of the prefix and the custom lengths combined are greater
    /// than the const ADDRESS_LENGTH, currently set to 70.
    ///
    /// This test will attempt to construct a TripleKeyHashAddresser with invalid prefix
    /// length and should return an error.
    fn test_invalid_prefix_length_construction() {
        // Creating a TripleKeyHashAddresser with a 72 character `prefix` and value of None for the
        // `first_hash_length` and `second_hash_length` which will return an error as the prefix
        //  length is greater than the ADDRESS_LENGTH const.
        let addresser = TripleKeyHashAddresser::new(
            "prefixprefixprefixprefixprefixprefixprefixprefixprefixprefixprefixprefix".to_string(),
            None,
            None,
        );

        // Assert the Addresser constructor returned an error.
        assert!(addresser.is_err());
        // Unwrap to validate that this will panic.
        addresser.unwrap();
    }
}

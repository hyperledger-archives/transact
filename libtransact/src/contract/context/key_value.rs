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

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;

use crate::contract::address::Addresser;
use crate::contract::context::error::ContractContextError;
use crate::handler::TransactionContext;
use crate::protocol::key_value_state::{
    StateEntry, StateEntryBuilder, StateEntryList, StateEntryListBuilder, StateEntryValue,
    StateEntryValueBuilder, ValueType,
};
use crate::protos::{FromBytes, IntoBytes};

/// KeyValueTransactionContext used to implement a simplified state consisting of natural keys and
/// a ValueType, an enum object used to represent a range of primitive data types. Uses an
/// implementation of the Addresser trait to calculate radix addresses to be stored in the
/// KeyValueTransactionContext's internal transaction context.
pub struct KeyValueTransactionContext<'a, A, K>
where
    A: Addresser<K>,
{
    context: &'a mut dyn TransactionContext,
    addresser: A,
    // PhantomData<K> is necessary for the K generic to be used with the Addresser trait, as K is not
    // used in any other elements of the KeyValueTransactionContext struct.
    _key: PhantomData<K>,
}

impl<'a, A, K> KeyValueTransactionContext<'a, A, K>
where
    A: Addresser<K>,
    K: Eq + Hash,
{
    /// Creates a new KeyValueTransactionContext
    /// Implementations of the TransactionContext trait and Addresser trait must be provided.
    pub fn new(
        context: &'a mut dyn TransactionContext,
        addresser: A,
    ) -> KeyValueTransactionContext<'a, A, K> {
        KeyValueTransactionContext {
            context,
            addresser,
            _key: PhantomData,
        }
    }

    /// Serializes the provided values and attempts to set this object at an address calculated
    /// from the provided natural key.
    ///
    /// Returns an `Ok(())` if the entry is successfully stored.
    ///
    /// # Arguments
    ///
    /// * `key` - The natural key that specifies where to set the state entry
    /// * `values` - The HashMap to be stored in state at the provided natural key
    pub fn set_state_entry(
        &self,
        key: &K,
        values: HashMap<String, ValueType>,
    ) -> Result<(), ContractContextError> {
        let mut new_entries = HashMap::new();
        new_entries.insert(key, values);
        self.set_state_entries(new_entries)
    }

    /// Attempts to retrieve the data from state at the address calculated from the provided
    /// natural key.
    ///
    /// Returns an optional HashMap which represents the value stored in state, returning `None`
    /// if the state entry is not found.
    ///
    /// # Arguments
    ///
    /// * `key` - The natural key to fetch
    pub fn get_state_entry(
        &self,
        key: &K,
    ) -> Result<Option<HashMap<String, ValueType>>, ContractContextError> {
        Ok(self
            .get_state_entries(vec![key])?
            .into_iter()
            .map(|(_, val)| val)
            .next())
    }

    /// Attempts to unset the data in state at the address calculated from the provided natural key.
    ///
    /// Returns an optional normalized key of the successfully deleted state entry, returning
    /// `None` if the state entry is not found.
    ///
    /// # Arguments
    ///
    /// * `key` - The natural key to delete
    pub fn delete_state_entry(&self, key: K) -> Result<Option<String>, ContractContextError> {
        Ok(self.delete_state_entries(vec![key])?.into_iter().next())
    }

    /// Serializes each value in the provided map and attempts to set in state each of these objects
    /// at the address calculated from its corresponding natural key.
    ///
    /// Returns an `Ok(())` if the provided entries were successfully stored.
    ///
    /// # Arguments
    ///
    /// * `entries` - HashMap with the map to be stored in state at the associated natural key
    pub fn set_state_entries(
        &self,
        entries: HashMap<&K, HashMap<String, ValueType>>,
    ) -> Result<(), ContractContextError> {
        let keys = entries.keys().map(ToOwned::to_owned).collect::<Vec<&K>>();
        let addresses = keys
            .iter()
            .map(|k| {
                self.addresser
                    .compute(&k)
                    .map_err(ContractContextError::AddresserError)
            })
            .collect::<Result<Vec<String>, ContractContextError>>()?;
        // Creating a map of the StateEntryList to the address it is stored at
        let entry_list_map = self.get_state_entry_lists(&addresses)?;

        // Iterating over the provided HashMap to see if there is an existing StateEntryList at the
        // corresponding address. If there is one found, add the new StateEntry to the StateEntryList.
        // If there is none found, creates a new StateEntryList entry for that address. Then,
        // serializes the newly created StateEntryList to be set in the internal context.
        let entries_list = entries
            .iter()
            .map(|(key, values)| {
                let addr = self.addresser.compute(key)?;
                let state_entry = self.create_state_entry(key, values.to_owned())?;
                match entry_list_map.get(&addr) {
                    Some(entry_list) => {
                        let mut existing_entries = entry_list.entries().to_vec();
                        existing_entries.push(state_entry);
                        let entry_list = StateEntryListBuilder::new()
                            .with_state_entries(existing_entries)
                            .build()
                            .map_err(|err| {
                                ContractContextError::ProtocolBuildError(Box::new(err))
                            })?;
                        Ok((addr, entry_list.into_bytes()?))
                    }
                    None => {
                        let entry_list = StateEntryListBuilder::new()
                            .with_state_entries(vec![state_entry])
                            .build()
                            .map_err(|err| {
                                ContractContextError::ProtocolBuildError(Box::new(err))
                            })?;
                        Ok((addr, entry_list.into_bytes()?))
                    }
                }
            })
            .collect::<Result<Vec<(String, Vec<u8>)>, ContractContextError>>()?;
        self.context.set_state_entries(entries_list)?;

        Ok(())
    }

    /// Attempts to retrieve the data from state at the addresses calculated from the provided list
    /// of natural keys.
    ///
    /// Returns a HashMap that links a normalized key with a HashMap. The associated HashMap represents
    /// the data retrieved from state. Only returns the data from addresses that have been set.
    ///
    /// # Arguments
    ///
    /// * `keys` - A list of natural keys to be fetched from state
    pub fn get_state_entries(
        &self,
        keys: Vec<&K>,
    ) -> Result<HashMap<String, HashMap<String, ValueType>>, ContractContextError> {
        let addresses = keys
            .iter()
            .map(|k| {
                self.addresser
                    .compute(&k)
                    .map_err(ContractContextError::AddresserError)
            })
            .collect::<Result<Vec<String>, ContractContextError>>()?;
        let normalized_keys = keys
            .iter()
            .map(|k| self.addresser.normalize(&k))
            .collect::<Vec<String>>();

        let state_entries: Vec<StateEntry> = self.flatten_state_entries(&addresses)?;

        // Now going to filter the StateEntry objects that actually have a matching normalized key
        // and convert the normalized key and value to be added to the returned HashMap.
        state_entries
            .iter()
            .filter(|entry| normalized_keys.contains(&entry.normalized_key().to_string()))
            .map(|entry| {
                let values = entry
                    .state_entry_values()
                    .iter()
                    .map(|val| (val.key().to_string(), val.value().to_owned()))
                    .collect::<HashMap<String, ValueType>>();
                Ok((entry.normalized_key().to_string(), values))
            })
            .collect::<Result<HashMap<String, HashMap<String, ValueType>>, ContractContextError>>()
    }

    /// Attempts to unset the data in state at the addresses calculated from each natural key in the
    /// provided list.
    ///
    /// Returns a list of normalized keys of successfully deleted state entries.
    ///
    /// # Arguments
    ///
    /// * `keys` - A list of natural keys to be deleted from state
    pub fn delete_state_entries(&self, keys: Vec<K>) -> Result<Vec<String>, ContractContextError> {
        let key_map: HashMap<String, String> = keys
            .iter()
            .map(|k| Ok((self.addresser.normalize(k), self.addresser.compute(k)?)))
            .collect::<Result<HashMap<String, String>, ContractContextError>>()?;
        let state_entry_lists: HashMap<String, StateEntryList> = self.get_state_entry_lists(
            &key_map
                .values()
                .map(ToOwned::to_owned)
                .collect::<Vec<String>>(),
        )?;

        let mut deleted_keys = Vec::new();
        let mut new_entry_lists = Vec::new();
        let mut delete_lists = Vec::new();
        key_map.iter().for_each(|(nkey, addr)| {
            // Fetching the StateEntryList at the corresponding address
            if let Some(list) = state_entry_lists.get(addr) {
                // The StateEntry objects will be filtered out of the StateEntryList if it has the
                // normalized key. This normalized key is added to a list of successfully filtered
                // entries to be returned.
                if list.contains(nkey.to_string()) {
                    let filtered = list
                        .entries()
                        .to_vec()
                        .into_iter()
                        .filter(|e| e.normalized_key() != nkey)
                        .collect::<Vec<StateEntry>>();
                    if filtered.is_empty() {
                        delete_lists.push(addr.to_string());
                    } else {
                        new_entry_lists.push((addr.to_string(), filtered));
                    }
                    deleted_keys.push(nkey.to_string());
                }
            }
        });
        // Delete any StateEntryLists that have an empty list of entries
        self.context.delete_state_entries(delete_lists.as_slice())?;
        // Setting the newly filtered StateEntryLists into state using the internal context
        self.context.set_state_entries(
            new_entry_lists
                .iter()
                .map(|(addr, filtered_list)| {
                    let new_entry_list = StateEntryListBuilder::new()
                        .with_state_entries(filtered_list.to_vec())
                        .build()
                        .map_err(|err| ContractContextError::ProtocolBuildError(Box::new(err)))?;
                    Ok((addr.to_string(), new_entry_list.into_bytes()?))
                })
                .collect::<Result<Vec<(String, Vec<u8>)>, ContractContextError>>()?,
        )?;

        Ok(deleted_keys)
    }

    /// Adds a blob to the execution result for this transaction.
    ///
    /// # Arguments
    ///
    /// * `data` - Transaction-family-specific data which can be interpreted by application clients.
    pub fn add_receipt_data(&self, data: Vec<u8>) -> Result<(), ContractContextError> {
        Ok(self.context.add_receipt_data(data)?)
    }

    /// add_event adds a new event to the execution result for this transaction.
    ///
    /// # Arguments
    ///
    /// * `event_type` -  A globally unique event identifier that is used to subscribe to events.
    ///         It describes what, in general, has occured.
    /// * `attributes` - Additional information about the event. Attributes can be used by
    ///         subscribers to filter the type of events they receive.
    /// * `data` - Transaction-family-specific data which can be interpreted by application clients.
    pub fn add_event(
        &self,
        event_type: String,
        attributes: Vec<(String, String)>,
        data: Vec<u8>,
    ) -> Result<(), ContractContextError> {
        Ok(self.context.add_event(event_type, attributes, data)?)
    }

    /// Collects the StateEntryList objects from state and then uses flat_map to collect the
    /// StateEntry objects held within each StateEntryList.
    fn flatten_state_entries(
        &self,
        addresses: &[String],
    ) -> Result<Vec<StateEntry>, ContractContextError> {
        Ok(self
            .get_state_entry_lists(addresses)?
            .values()
            .flat_map(|entry_list| entry_list.entries().to_vec())
            .collect::<Vec<StateEntry>>())
    }

    /// Collects the StateEntryList objects from the bytes fetched from state, then deserializes
    /// these into the native StateEntryList object.
    fn get_state_entry_lists(
        &self,
        addresses: &[String],
    ) -> Result<HashMap<String, StateEntryList>, ContractContextError> {
        self.context
            .get_state_entries(&addresses)?
            .iter()
            .map(|(addr, bytes_entry)| {
                Ok((addr.to_string(), StateEntryList::from_bytes(bytes_entry)?))
            })
            .collect::<Result<HashMap<String, StateEntryList>, ContractContextError>>()
    }

    /// Creates a singular StateEntry object from the provided key and values.
    fn create_state_entry(
        &self,
        key: &K,
        values: HashMap<String, ValueType>,
    ) -> Result<StateEntry, ContractContextError> {
        let state_values: Vec<StateEntryValue> = values
            .iter()
            .map(|(key, value)| {
                StateEntryValueBuilder::new()
                    .with_key(key.to_string())
                    .with_value(value.clone())
                    .build()
                    .map_err(|err| ContractContextError::ProtocolBuildError(Box::new(err)))
            })
            .collect::<Result<Vec<StateEntryValue>, ContractContextError>>()?;
        Ok(StateEntryBuilder::new()
            .with_normalized_key(self.addresser.normalize(key.to_owned()))
            .with_state_entry_values(state_values)
            .build()
            .map_err(|err| ContractContextError::ProtocolBuildError(Box::new(err)))?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex};

    use crate::contract::address::{
        double_key_hash::DoubleKeyHashAddresser, key_hash::KeyHashAddresser,
        triple_key_hash::TripleKeyHashAddresser,
    };

    use crate::handler::ContextError;

    /// Simple state backed by a HashMap.
    struct TestState {
        state: HashMap<String, Vec<u8>>,
    }

    /// Simple state implementation with basic methods to get, set, and delete state values.
    impl TestState {
        pub fn new() -> Self {
            TestState {
                state: HashMap::new(),
            }
        }

        fn get_entries(
            &self,
            addresses: &[String],
        ) -> Result<Vec<(String, Vec<u8>)>, ContextError> {
            let mut values = Vec::new();
            addresses.iter().for_each(|key| {
                if let Some(value) = self.state.get(key) {
                    values.push((key.to_string(), value.to_vec()))
                }
            });
            Ok(values)
        }

        fn set_entries(&mut self, entries: Vec<(String, Vec<u8>)>) -> Result<(), ContextError> {
            entries.iter().for_each(|(key, value)| {
                match self.state.insert(key.to_string(), value.to_vec()) {
                    _ => (),
                }
            });
            Ok(())
        }

        fn delete_entries(&mut self, addresses: &[String]) -> Result<Vec<String>, ContextError> {
            let mut deleted = Vec::new();
            addresses.iter().for_each(|key| {
                if let Some(_) = self.state.remove(key) {
                    deleted.push(key.to_string());
                }
            });
            Ok(deleted)
        }

        fn add_receipt_data(&self, _data: Vec<u8>) -> Result<(), ContextError> {
            Ok(())
        }

        fn add_event(
            &self,
            _event_type: String,
            _attributes: Vec<(String, String)>,
            _data: Vec<u8>,
        ) -> Result<(), ContextError> {
            Ok(())
        }
    }

    /// TestContext using the simple TestState backend to be used as the internal context to
    /// construct a KeyValueTransactionContext for the tests.
    struct TestContext {
        internal_state: Arc<Mutex<TestState>>,
    }

    impl TestContext {
        pub fn new() -> Self {
            TestContext {
                internal_state: Arc::new(Mutex::new(TestState::new())),
            }
        }
    }

    /// TransactionContext trait implementation for the TestContext.
    impl TransactionContext for TestContext {
        fn get_state_entries(
            &self,
            addresses: &[String],
        ) -> Result<Vec<(String, Vec<u8>)>, ContextError> {
            self.internal_state
                .lock()
                .expect("Test lock was poisoned in get method")
                .get_entries(addresses)
        }

        fn set_state_entries(&self, entries: Vec<(String, Vec<u8>)>) -> Result<(), ContextError> {
            self.internal_state
                .lock()
                .expect("Test lock was poisoned in set method")
                .set_entries(entries)
        }

        fn delete_state_entries(&self, addresses: &[String]) -> Result<Vec<String>, ContextError> {
            self.internal_state
                .lock()
                .expect("Test lock was poisoned in delete method")
                .delete_entries(addresses)
        }

        fn add_receipt_data(&self, data: Vec<u8>) -> Result<(), ContextError> {
            self.internal_state
                .lock()
                .expect("Test lock was poisoned in add_receipt_data method")
                .add_receipt_data(data)
        }

        fn add_event(
            &self,
            event_type: String,
            attributes: Vec<(String, String)>,
            data: Vec<u8>,
        ) -> Result<(), ContextError> {
            self.internal_state
                .lock()
                .expect("Test lock was poisoned in add_event method")
                .add_event(event_type, attributes, data)
        }
    }

    /// Function to create a HashMap from `key` and `value` to create a StateEntryValue for a
    /// StateEntry message.
    fn create_entry_value_map(key: String, value: ValueType) -> HashMap<String, ValueType> {
        let mut value_map = HashMap::new();
        value_map.insert(key, value);
        value_map
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a KeyHashAddresser,
    /// will successfully perform the `set_state_entry` method, after the following steps:
    ///
    /// 1. Create a TestContext and a KeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a KeyHashAddresser, so a key
    ///    is represented by a single string
    /// 2. Create a HashMap, a value to be set in state, with a string key and a ValueType value
    ///
    /// This test then verifies a successful return from the `set_state_entry` when providing a
    /// single string for the `key` argument and the HashMap for the `values` argument
    fn test_simple_set_state_entry() {
        // Create a TestContext and KeyHashAddresser, then construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = KeyHashAddresser::new("prefix".to_string());
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap to be set in state
        let value = ValueType::Int32(32);
        let mut state_value = HashMap::new();
        state_value.insert("key1".to_string(), value);

        // Verify the `set_state_entry` method returns an `Ok(())` result
        assert!(simple_state
            .set_state_entry(&"a".to_string(), state_value)
            .is_ok());
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a KeyHashAddresser,
    /// will successfully perform the `get_state_entry` method, after the following steps:
    ///
    /// 1. Create a TestContext and a KeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a KeyHashAddresser, so a key
    ///    is represented by a single string
    /// 2. Create a HashMap, a value to be set in state, with a string key and a ValueType value
    /// 3. Set the created HashMap in state at a key, 'a', using the `set_state_entry` method
    ///
    /// This test then verifies the `get_state_entry` returns a Some option and the value of the
    /// returned option is equivalent to the HashMap created, including the key and ValueType
    fn test_simple_get_state_entry() {
        // Create a TestContext and KeyHashAddresser, then construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = KeyHashAddresser::new("prefix".to_string());
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap, with a string key and ValueType value
        let value = ValueType::Int32(32);
        let mut state_value = HashMap::new();
        state_value.insert("key1".to_string(), value);

        // Set the HashMap, created above, at the key 'a'
        simple_state
            .set_state_entry(&"a".to_string(), state_value)
            .expect("Unable to set state entry in get_state_entry test");

        // Use the `get_state_entry` method to access the state entry at the 'a' key
        let values = simple_state.get_state_entry(&"a".to_string()).unwrap();

        // Verify the return value is equal to the HashMap used when the entry was originally set
        assert!(values.is_some());
        assert_eq!(
            values.unwrap().get(&"key1".to_string()),
            Some(&ValueType::Int32(32))
        );
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a KeyHashAddresser,
    /// will successfully perform the `delete_state_entry` method, after the following steps:
    ///
    /// 1. Create a TestContext and a KeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a KeyHashAddresser, so a key
    ///    is represented by a single string
    /// 2. Create a HashMap, a value to be set in state, with a string key and a ValueType value
    /// 3. Set the created HashMap in state at a key, 'a', using the `set_state_entry` method
    ///
    /// This test verifies the `delete_state_entry` successfully deletes the intended state entry
    /// by checking that the initial call returns the correct key wrapped in a Some option
    /// and a subsequent attempt to delete the entry at the same key returns a None option
    fn test_simple_delete_state_entry() {
        // Create a TestContext and KeyHashAddresser, then construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = KeyHashAddresser::new("prefix".to_string());
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap, with a string key and ValueType value
        let value = ValueType::Int32(32);
        let mut state_value = HashMap::new();
        state_value.insert("key1".to_string(), value);

        // Set the HashMap, created above, at the key 'a'
        simple_state
            .set_state_entry(&"a".to_string(), state_value)
            .expect("Unable to set state entry in delete_state_entry test");

        // Call `get_state_entry` to ensure the value has been succesfully set
        simple_state
            .get_state_entry(&"a".to_string())
            .expect("Unable to get state entry in delete_state_entry test");

        // Call `delete_state_entry` and verify the return value is a Some option and the value of
        // this option is the correct key, 'a'
        let deleted = simple_state.delete_state_entry("a".to_string()).unwrap();
        assert!(deleted.is_some());
        assert_eq!(deleted, Some("a".to_string()));

        // Call `delete_state_entry` with the same key, 'a', to verify the return value is a None
        // option
        let already_deleted = simple_state.delete_state_entry("a".to_string()).unwrap();
        assert!(already_deleted.is_none());

        // Verify access to the 'a' key using the `get_state_entry` method returns a None option
        let deleted_value = simple_state.get_state_entry(&"a".to_string()).unwrap();
        assert!(deleted_value.is_none());
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a KeyHashAddresser,
    /// will successfully perform the `set_state_entries` method, after the following steps:
    ///
    /// 1. Create a TestContext and a KeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a KeyHashAddresser, so a key
    ///    is represented by a single string
    /// 2. Create two HashMaps, entries to be set in state, with values represented by a ValueType
    ///
    /// This test then verifies a successful return from the `set_state_entries` when provided a
    /// HashMap of a natural key (the key the entry is meant to be set at) associated with a value,
    /// represented by a HashMap (intended to be used in the state entry)
    fn test_simple_set_state_entries() {
        // Create a TestContext and KeyHashAddresser, then construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = KeyHashAddresser::new("prefix".to_string());
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap to contain the multiple entries to set in state
        let mut entries = HashMap::new();

        // Create two individual HashMaps, to be associated with unique keys
        let first_key = &"a".to_string();
        let first_value_map = create_entry_value_map("key1".to_string(), ValueType::Int32(32));
        let second_key = &"b".to_string();
        let second_value_map =
            create_entry_value_map("key1".to_string(), ValueType::String("String".to_string()));

        // Insert the two HashMaps at unique keys to the HashMap to be set in state
        entries.insert(first_key, first_value_map);
        entries.insert(second_key, second_value_map);

        // Verify the `set_state_entries` method returns an `Ok(())` result
        let set_result = simple_state.set_state_entries(entries);
        assert!(set_result.is_ok());
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a KeyHashAddresser,
    /// will successfully perform the `get_state_entries` method, after the following steps:
    ///
    /// 1. Create a TestContext and a KeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a KeyHashAddresser, so a key
    ///    is represented by a single string
    /// 2. Create two HashMaps, values to be set in state, with values represented by a ValueType
    /// 3. Set these created HashMaps in state using the `set_state_entries` method
    ///
    /// This test then verifies the `get_state_entries` returns a HashMap containing the keys ('a'
    /// and 'b') and each is associated with the unique HashMaps used to originally set the entries
    fn test_simple_get_state_entries() {
        // Create a TestContext and KeyHashAddresser, then construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = KeyHashAddresser::new("prefix".to_string());
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap to contain the multiple entries to set in state
        let mut entries = HashMap::new();

        // Create two individual HashMaps, to be associated with unique keys
        let first_key = &"a".to_string();
        let first_value_map = create_entry_value_map("key1".to_string(), ValueType::Int32(32));
        let second_key = &"b".to_string();
        let second_value_map =
            create_entry_value_map("key1".to_string(), ValueType::String("String".to_string()));

        // Insert the two HashMaps at unique keys to the HashMap to be set in state
        entries.insert(first_key, first_value_map.clone());
        entries.insert(second_key, second_value_map.clone());

        // Use the `set_state_entries` method to place the `entries` HashMap in state
        simple_state
            .set_state_entries(entries)
            .expect("Unable to set state entries in get_state_entries test");

        // Use the `get_state_entries` method to access the state entries at the 'a' and 'b' key
        let values = simple_state
            .get_state_entries([first_key, second_key].to_vec())
            .unwrap();

        // Verify each key has the same associated value as when the entries were originally set
        assert_eq!(values.get("a").unwrap(), &first_value_map);
        assert_eq!(values.get("b").unwrap(), &second_value_map);
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a KeyHashAddresser,
    /// will successfully perform the `delete_state_entries` method, after the following steps:
    ///
    /// 1. Create a TestContext and a KeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a KeyHashAddresser, so a key
    ///    is represented by a single string
    /// 2. Create two HashMaps, values to be set in state, with values represented by a ValueType
    /// 3. Set these created HashMaps in state using the `set_state_entries` method
    ///
    /// This test verifies `delete_state_entries` successfully deletes the intended state entries
    /// by checking that the initial call returns a list containing the correct keys and a
    /// subsequent attempt to delete the entries at the same keys returns an empty list
    fn test_simple_delete_state_entries() {
        // Create a TestContext and KeyHashAddresser, then construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = KeyHashAddresser::new("prefix".to_string());
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap to contain the multiple entries to set in state
        let mut entries = HashMap::new();

        // Create two individual HashMaps, to be associated with unique keys
        let first_key = &"a".to_string();
        let first_value_map = create_entry_value_map("key1".to_string(), ValueType::Int32(32));
        let second_key = &"b".to_string();
        let second_value_map =
            create_entry_value_map("key1".to_string(), ValueType::String("String".to_string()));

        // Insert the two HashMaps at unique keys to the HashMap to be set in state
        entries.insert(first_key, first_value_map.clone());
        entries.insert(second_key, second_value_map.clone());

        // Call the `set_state_entries` method to place the `entries` HashMap in state
        simple_state
            .set_state_entries(entries)
            .expect("Unable to set state entries in delete_state_entries test");

        // Call the `get_state_entries` method to ensure the state entries have been set
        simple_state
            .get_state_entries([first_key, second_key].to_vec())
            .expect("Unable to get state entries in delete_state_entries test");

        // Call `delete_state_entries` and verify the return value is a list of the keys 'a' and 'b'
        let deleted = simple_state
            .delete_state_entries(["a".to_string(), "b".to_string()].to_vec())
            .expect("Unable to delete state entries");
        assert!(deleted.contains(&"a".to_string()));
        assert!(deleted.contains(&"b".to_string()));

        // Call `delete_state_entries` with the same keys and verify an empty list is returned
        let already_deleted = simple_state
            .delete_state_entries(["a".to_string(), "b".to_string()].to_vec())
            .unwrap();
        assert!(already_deleted.is_empty());
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a DoubleKeyHashAddresser,
    /// will successfully perform the `set_state_entry` method, after the following steps:
    ///
    /// 1. Create a TestContext and a KeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a DoubleKeyHashAddresser, so
    ///    a key is represented by a tuple of two strings
    /// 2. Create a HashMap, a value to be set in state, with some key and a value represented by a
    ///    ValueType
    ///
    /// This test then verifies a successful return from the `set_state_entry` when providing a
    /// tuple of two strings for the `key` argument and the HashMap for the `values` argument
    fn test_double_set_state_entry() {
        // Create a TestContext and DoubleKeyHashAddresser to construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = DoubleKeyHashAddresser::new("prefix".to_string(), None)
            .expect("Unable to construct Addresser");
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap to be set in state
        let value = ValueType::Int64(64);
        let mut state_value = HashMap::new();
        state_value.insert("key1".to_string(), value);

        // Verify the `set_state_entry` method returns an `Ok(())` result
        assert!(simple_state
            .set_state_entry(&("a".to_string(), "b".to_string()), state_value)
            .is_ok());
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a DoubleKeyHashAddresser,
    /// will successfully perform the `get_state_entry` method, after the following steps:
    ///
    /// 1. Create a TestContext and a KeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a DoubleKeyHashAddresser, so
    ///    a key is represented by a tuple of two strings
    /// 2. Create a HashMap, a value to be set in state, with a key and a ValueType as the value
    /// 3. Set the created HashMap in state at a key, ('a', 'b'), using the `set_state_entry` method
    ///
    /// This test then verifies the `get_state_entry` returns a Some option and the value of the
    /// returned option is equivalent to the HashMap created, including the key and ValueType
    fn test_double_get_state_entry() {
        // Create a TestContext and DoubleKeyHashAddresser to construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = DoubleKeyHashAddresser::new("prefix".to_string(), None)
            .expect("Unable to construct Addresser");
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap, with a string key and ValueType value
        let value = ValueType::Int64(64);
        let mut state_value = HashMap::new();
        state_value.insert("key1".to_string(), value);

        // Use `set_state_entry` to set the state entry at the ('a', 'b') key
        simple_state
            .set_state_entry(&("a".to_string(), "b".to_string()), state_value)
            .expect("Unable to set state entry in get_state_entry test");

        // Use the `get_state_entry` method to access the state entry at the ('a', 'b') key
        let values = simple_state
            .get_state_entry(&("a".to_string(), "b".to_string()))
            .unwrap();

        // Verify the return value is equal to the HashMap used when the entry was originally set
        assert!(values.is_some());
        assert_eq!(
            values.unwrap().get(&"key1".to_string()),
            Some(&ValueType::Int64(64))
        );
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a DoubleKeyHashAddresser,
    /// will successfully perform the `delete_state_entry` method, after the following steps:
    ///
    /// 1. Create a TestContext and a KeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a DoubleKeyHashAddresser, so
    ///    a key is represented by a tuple of two strings
    /// 2. Create a HashMap, a value to be set in state, with a key and a ValueType as the value
    /// 3. Set the created HashMap in state at a key, ('a', 'b'), using the `set_state_entry` method
    ///
    /// This test verifies the `delete_state_entry` successfully deletes the intended state entry
    /// by checking that the initial call returns the normalized key wrapped in a Some option
    fn test_double_delete_state_entry() {
        // Create a TestContext and DoubleKeyHashAddresser to construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = DoubleKeyHashAddresser::new("prefix".to_string(), None)
            .expect("Unable to construct Addresser");
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap, with a string key and ValueType value
        let value = ValueType::Int64(64);
        let mut state_value = HashMap::new();
        state_value.insert("key1".to_string(), value);

        // Set the HashMap, created above, at the key ('a', 'b')
        simple_state
            .set_state_entry(&("a".to_string(), "b".to_string()), state_value)
            .expect("Unable to set state entry in get_state_entry test");

        // Call `get_state_entry` to ensure the value has been succesfully set
        simple_state
            .get_state_entry(&("a".to_string(), "b".to_string()))
            .expect("Unable to get state entry in delete_state_entries test");

        // Call `delete_state_entry` and verify the return value is a Some option and the value of
        // this option is the correct normalized key, 'a_b'
        let deleted = simple_state
            .delete_state_entry(("a".to_string(), "b".to_string()))
            .unwrap();
        assert!(deleted.is_some());
        assert_eq!(deleted, Some(format!("{}_{}", "a", "b")));
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a DoubleKeyHashAddresser,
    /// will successfully perform the `set_state_entries` method, after the following steps:
    ///
    /// 1. Create a TestContext and a KeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a DoubleKeyHashAddresser, so
    ///    a key is represented by a tuple of two strings
    /// 2. Create two HashMaps, values to be set in state, with values represented by a ValueType
    ///
    /// This test then verifies a successful return from the `set_state_entries` when provided a
    /// HashMap of a natural key (the key the entry is meant to be set at) associated with a value,
    /// represented by a HashMap (intended to be used in the state entry)
    fn test_double_set_state_entries() {
        // Create a TestContext and DoubleKeyHashAddresser to construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = DoubleKeyHashAddresser::new("prefix".to_string(), None)
            .expect("Unable to construct Addresser");
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap to contain the multiple entries to set in state
        let mut entries = HashMap::new();

        // Create two individual HashMaps, to be associated with unique keys
        let first_key = &("a".to_string(), "b".to_string());
        let first_value_map = create_entry_value_map("key1".to_string(), ValueType::Int32(32));
        let second_key = &("c".to_string(), "d".to_string());
        let second_value_map =
            create_entry_value_map("key1".to_string(), ValueType::String("String".to_string()));

        // Insert the two HashMaps at unique keys to the HashMap to be set in state
        entries.insert(first_key, first_value_map);
        entries.insert(second_key, second_value_map);

        // Verify the `set_state_entries` method returns an `Ok(())` result
        assert!(simple_state.set_state_entries(entries).is_ok());
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a DoubleKeyHashAddresser,
    /// will successfully perform the `get_state_entries` method, after the following steps:
    ///
    /// 1. Create a TestContext and a KeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a DoubleKeyHashAddresser, so
    ///    a key is represented by a tuple of two strings
    /// 2. Create two HashMaps, values to be set in state, with values represented by a ValueType
    /// 3. Set these created HashMaps in state using the `set_state_entries` method
    ///
    /// This test verifies the `get_state_entries` returns a HashMap containing the keys, ('a', 'b')
    /// and ('c', 'd'), and each is associated with the unique HashMaps used to originally set the entries
    fn test_double_get_state_entries() {
        // Create a TestContext and DoubleKeyHashAddresser to construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = DoubleKeyHashAddresser::new("prefix".to_string(), None)
            .expect("Unable to construct Addresser");
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap to contain the multiple entries to set in state
        let mut entries = HashMap::new();

        // Create two individual HashMaps, to be associated with unique keys
        let first_key = &("a".to_string(), "b".to_string());
        let first_value_map = create_entry_value_map("key1".to_string(), ValueType::Int32(32));
        let second_key = &("c".to_string(), "d".to_string());
        let second_value_map =
            create_entry_value_map("key1".to_string(), ValueType::String("String".to_string()));

        // Insert the two HashMaps at unique keys to the HashMap to be set in state
        entries.insert(first_key, first_value_map.clone());
        entries.insert(second_key, second_value_map.clone());

        // Use the `set_state_entries` method to place the `entries` HashMap in state
        simple_state
            .set_state_entries(entries)
            .expect("Unable to set_state_entries in get_state_entries test");

        // Use the `get_state_entries` method to access the state entries at the ('a', 'b') and
        // ('c', 'd') keys
        let values = simple_state
            .get_state_entries([first_key, second_key].to_vec())
            .expect("Unable to get state entries in get_state_entries test");

        // Verify each normalized key has the same associated value as when the entries were
        // originally set
        assert_eq!(
            values.get(&format!("{}_{}", "a", "b")).unwrap(),
            &first_value_map
        );
        assert_eq!(
            values.get(&format!("{}_{}", "c", "d")).unwrap(),
            &second_value_map
        );
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a DoubleKeyHashAddresser,
    /// will successfully perform the `delete_state_entries` method, after the following steps:
    ///
    /// 1. Create a TestContext and a KeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a DoubleKeyHashAddresser, so
    ///    a key is represented by a tuple of two strings
    /// 2. Create two HashMaps, values to be set in state, with values represented by a ValueType
    /// 3. Set these created HashMaps in state using the `set_state_entries` method
    ///
    /// This test verifies `delete_state_entries` successfully deletes the intended state entries
    /// by checking that the call returns a list containing the correct normalized keys
    fn test_double_delete_state_entries() {
        // Create a TestContext and DoubleKeyHashAddresser to construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = DoubleKeyHashAddresser::new("prefix".to_string(), None)
            .expect("Unable to construct Addresser");
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap to contain the multiple entries to set in state
        let mut entries = HashMap::new();

        // Create two individual HashMaps, to be associated with unique keys
        let first_key = ("a".to_string(), "b".to_string());
        let first_value_map = create_entry_value_map("key1".to_string(), ValueType::Int32(32));
        let second_key = ("c".to_string(), "d".to_string());
        let second_value_map =
            create_entry_value_map("key1".to_string(), ValueType::String("String".to_string()));

        // Insert the two HashMaps at unique keys to the HashMap to be set in state
        entries.insert(&first_key, first_value_map.clone());
        entries.insert(&second_key, second_value_map.clone());

        // Call the `set_state_entries` method to place the `entries` HashMap in state
        simple_state
            .set_state_entries(entries)
            .expect("Unable to set_state_entries in delete_state_entries test");

        // Call the `get_state_entries` method to ensure the state entries have been set
        simple_state
            .get_state_entries([&first_key, &second_key].to_vec())
            .expect("Unable to get_state_entries in the delete_state_entries test");

        // Call `delete_state_entries` and verify the return value is a list containing the normalized
        // keys 'a_b' and 'c_d'
        let deleted = simple_state
            .delete_state_entries([first_key, second_key].to_vec())
            .expect("Unable to delete state entries");
        assert!(deleted.contains(&format!("{}_{}", "a", "b")));
        assert!(deleted.contains(&format!("{}_{}", "c", "d")));
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a TripleKeyHashAddresser,
    /// will successfully perform the `set_state_entry` method, after the following steps:
    ///
    /// 1. Create a TestContext and a KeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a TripleKeyHashAddresser, so
    ///    a key is represented by a tuple of three strings
    /// 2. Create a HashMap, a value to be set in state, with some key and a value represented by a
    ///    ValueType
    ///
    /// This test then verifies a successful return from the `set_state_entry` when providing a
    /// tuple of three strings for the `key` argument and the HashMap for the `values` argument
    fn test_triple_set_state_entry() {
        // Create a TestContext and TripleKeyHashAddresser to construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = TripleKeyHashAddresser::new("prefix".to_string(), None, None)
            .expect("Unable to construct Addresser");
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap to be set in state
        let value = ValueType::Int64(64);
        let mut state_value = HashMap::new();
        state_value.insert("key1".to_string(), value);

        // Verify the `set_state_entry` method returns an `Ok(())` result
        assert!(simple_state
            .set_state_entry(
                &("a".to_string(), "b".to_string(), "c".to_string()),
                state_value
            )
            .is_ok());
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a TripleKeyHashAddresser,
    /// will successfully perform the `get_state_entry` method, after the following steps:
    ///
    /// 1. Create a TestContext and a TripleKeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a TripleKeyHashAddresser, so
    ///    a key is represented by a tuple of three strings
    /// 2. Create a HashMap, a value to be set in state, with a key and a ValueType as the value
    /// 3. Set the created HashMap in state at a key, ('a', 'b', 'c'), using `set_state_entry`
    ///
    /// This test then verifies the `get_state_entry` returns a Some option and the value of the
    /// returned option is equivalent to the HashMap created, including the key and ValueType
    fn test_triple_get_state_entry() {
        // Create a TestContext and TripleKeyHashAddresser to construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = TripleKeyHashAddresser::new("prefix".to_string(), None, None)
            .expect("Unable to construct Addresser");
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap, with a string key and ValueType value
        let value = ValueType::Int64(64);
        let mut state_value = HashMap::new();
        state_value.insert("key1".to_string(), value);

        // Use `set_state_entry` to set the state entry at the ('a', 'b', 'c') key
        simple_state
            .set_state_entry(
                &("a".to_string(), "b".to_string(), "c".to_string()),
                state_value,
            )
            .expect("Unable to set state entry in get_state_entry test");

        // Use the `get_state_entry` method to access the state entry at the ('a', 'b', 'c') key
        let values = simple_state
            .get_state_entry(&("a".to_string(), "b".to_string(), "c".to_string()))
            .unwrap();

        // Verify the return value is equal to the HashMap used when the entry was originally set
        assert!(values.is_some());
        assert_eq!(
            values.unwrap().get(&"key1".to_string()),
            Some(&ValueType::Int64(64))
        );
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a TripleKeyHashAddresser,
    /// will successfully perform the `delete_state_entry` method, after the following steps:
    ///
    /// 1. Create a TestContext and a TripleKeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a TripleKeyHashAddresser, so
    ///    a key is represented by a tuple of three strings
    /// 2. Create a HashMap, a value to be set in state, with a key and a ValueType as the value
    /// 3. Set the created HashMap in state at a key, ('a', 'b', 'c'), using `set_state_entry`
    ///
    /// This test verifies the `delete_state_entry` successfully deletes the intended state entry
    /// by checking that the initial call returns the normalized key wrapped in a Some option
    fn test_triple_delete_state_entry() {
        // Create a TestContext and TripleKeyHashAddresser to construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = TripleKeyHashAddresser::new("prefix".to_string(), None, None)
            .expect("Unable to construct Addresser");
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap, with a string key and ValueType value
        let value = ValueType::Int64(64);
        let mut state_value = HashMap::new();
        state_value.insert("key1".to_string(), value);

        // Set the HashMap, created above, at the key ('a', 'b', 'c')
        simple_state
            .set_state_entry(
                &("a".to_string(), "b".to_string(), "c".to_string()),
                state_value,
            )
            .expect("Unable to set state entry in get_state_entry test");

        // Call `get_state_entry` to ensure the value has been succesfully set
        simple_state
            .get_state_entry(&("a".to_string(), "b".to_string(), "c".to_string()))
            .expect("Unable to get state entry in delete_state_entries test");

        // Call `delete_state_entry` and verify the return value is a Some option and the value of
        // this option is the correct normalized key, 'a_b_c'
        let deleted = simple_state
            .delete_state_entry(("a".to_string(), "b".to_string(), "c".to_string()))
            .unwrap();
        assert!(deleted.is_some());
        assert_eq!(deleted, Some(format!("{}_{}_{}", "a", "b", "c")));
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a TripleKeyHashAddresser,
    /// will successfully perform the `set_state_entries` method, after the following steps:
    ///
    /// 1. Create a TestContext and a TripleKeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a TripleKeyHashAddresser, so
    ///    a key is represented by a tuple of three strings
    /// 2. Create two HashMaps, values to be set in state, with values represented by a ValueType
    ///
    /// This test then verifies a successful return from the `set_state_entries` when provided a
    /// HashMap of a natural key (the key the entry is meant to be set at) associated with a value,
    /// represented by a HashMap (intended to be used in the state entry)
    fn test_triple_set_state_entries() {
        // Create a TestContext and TripleKeyHashAddresser to construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = TripleKeyHashAddresser::new("prefix".to_string(), None, None)
            .expect("Unable to construct Addresser");
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap to contain the multiple entries to set in state
        let mut entries = HashMap::new();

        // Create two individual HashMaps, to be associated with unique keys
        let first_key = &("a".to_string(), "b".to_string(), "c".to_string());
        let first_value_map = create_entry_value_map("key1".to_string(), ValueType::Int32(32));
        let second_key = &("c".to_string(), "d".to_string(), "c".to_string());
        let second_value_map =
            create_entry_value_map("key1".to_string(), ValueType::String("String".to_string()));

        // Insert the two HashMaps at unique keys to the HashMap to be set in state
        entries.insert(first_key, first_value_map);
        entries.insert(second_key, second_value_map);

        // Verify the `set_state_entries` method returns an `Ok(())` result
        assert!(simple_state.set_state_entries(entries).is_ok());
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a TripleKeyHashAddresser,
    /// will successfully perform the `get_state_entries` method, after the following steps:
    ///
    /// 1. Create a TestContext and a TripleKeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a TripleKeyHashAddresser, so
    ///    a key is represented by a tuple of three strings
    /// 2. Create two HashMaps, values to be set in state, with values represented by a ValueType
    /// 3. Set these created HashMaps in state using the `set_state_entries` method
    ///
    /// This test verifies the `get_state_entries` returns a HashMap containing the keys, ('a', 'b', 'c')
    /// and ('d', 'e', 'f'), and each is associated with the unique HashMaps used to originally set
    /// the entries
    fn test_triple_get_state_entries() {
        // Create a TestContext and TripleKeyHashAddresser to construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = TripleKeyHashAddresser::new("prefix".to_string(), None, None)
            .expect("Unable to construct Addresser");
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap to contain the multiple entries to set in state
        let mut entries = HashMap::new();

        // Create two individual HashMaps, to be associated with unique keys
        let first_key = &("a".to_string(), "b".to_string(), "c".to_string());
        let first_value_map = create_entry_value_map("key1".to_string(), ValueType::Int32(32));
        let second_key = &("d".to_string(), "e".to_string(), "f".to_string());
        let second_value_map =
            create_entry_value_map("key1".to_string(), ValueType::String("String".to_string()));

        // Insert the two HashMaps at unique keys to the HashMap to be set in state
        entries.insert(first_key, first_value_map.clone());
        entries.insert(second_key, second_value_map.clone());

        // Use the `set_state_entries` method to place the `entries` HashMap in state
        simple_state
            .set_state_entries(entries)
            .expect("Unable to set_state_entries in get_state_entries test");

        // Use the `get_state_entries` method to access the state entries at the ('a', 'b', 'c') and
        // ('d', 'e', 'f') keys
        let values = simple_state
            .get_state_entries([first_key, second_key].to_vec())
            .expect("Unable to get state entries in get_state_entries test");

        // Verify each normalized key has the same associated value as when the entries were
        // originally set
        assert_eq!(
            values.get(&format!("{}_{}_{}", "a", "b", "c")).unwrap(),
            &first_value_map
        );
        assert_eq!(
            values.get(&format!("{}_{}_{}", "d", "e", "f")).unwrap(),
            &second_value_map
        );
    }

    #[test]
    /// This test verifies that a KeyValueTransactionContext, constructed with a TripleKeyHashAddresser,
    /// will successfully perform the `delete_state_entries` method, after the following steps:
    ///
    /// 1. Create a TestContext and a TripleKeyHashAddresser to construct a KeyValueTransactionContext
    ///    Note: This KeyValueTransactionContext is constructed using a TripleKeyHashAddresser, so
    ///    a key is represented by a tuple of three strings
    /// 2. Create two HashMaps, values to be set in state, with values represented by a ValueType
    /// 3. Set these created HashMaps in state using the `set_state_entries` method
    ///
    /// This test verifies `delete_state_entries` successfully deletes the intended state entries
    /// by checking that the call returns a list containing the correct normalized keys
    fn test_triple_delete_state_entries() {
        // Create a TestContext and TripleKeyHashAddresser to construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = TripleKeyHashAddresser::new("prefix".to_string(), None, None)
            .expect("Unable to construct Addresser");
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Create a HashMap to contain the multiple entries to set in state
        let mut entries = HashMap::new();

        // Create two individual HashMaps, to be associated with unique keys
        let first_key = ("a".to_string(), "b".to_string(), "c".to_string());
        let first_value_map = create_entry_value_map("key1".to_string(), ValueType::Int32(32));
        let second_key = ("d".to_string(), "e".to_string(), "f".to_string());
        let second_value_map =
            create_entry_value_map("key1".to_string(), ValueType::String("String".to_string()));

        // Insert the two HashMaps at unique keys to the HashMap to be set in state
        entries.insert(&first_key, first_value_map.clone());
        entries.insert(&second_key, second_value_map.clone());

        // Call the `set_state_entries` method to place the `entries` HashMap in state
        simple_state
            .set_state_entries(entries)
            .expect("Unable to set_state_entries in delete_state_entries test");

        // Call the `get_state_entries` method to ensure the state entries have been set
        simple_state
            .get_state_entries([&first_key, &second_key].to_vec())
            .expect("Unable to get_state_entries in the delete_state_entries test");

        // Call `delete_state_entries` and verify the return value is a list containing the normalized
        // keys 'a_b_c' and 'd_e_f'
        let deleted = simple_state
            .delete_state_entries([first_key, second_key].to_vec())
            .expect("Unable to delete state entries");
        assert!(deleted.contains(&format!("{}_{}_{}", "a", "b", "c")));
        assert!(deleted.contains(&format!("{}_{}_{}", "d", "e", "f")));
    }

    #[test]
    /// This test verifies the `add_receipt_data` method returns an Ok(()), even though it is
    /// currently not supported in Sawtooth Sabre. This test should return a simple Ok(())
    fn test_add_receipt_data_ok() {
        // Create a TestContext and TripleKeyHashAddresser to construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = TripleKeyHashAddresser::new("prefix".to_string(), None, None)
            .expect("Unable to construct Addresser");
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Call the `add_receipt_data` method
        let res = simple_state.add_receipt_data(vec![]);

        // Verify this result is an Ok(())
        assert!(res.is_ok());
    }

    #[test]
    /// This test verifies the `add_event` method returns an Ok(()), even though it is currently
    /// not supported in Sawtooth Sabre. This test should return a simple Ok(())
    fn test_add_event_ok() {
        // Create a TestContext and TripleKeyHashAddresser to construct a KeyValueTransactionContext
        let mut context = TestContext::new();
        let addresser = TripleKeyHashAddresser::new("prefix".to_string(), None, None)
            .expect("Unable to construct Addresser");
        let simple_state = KeyValueTransactionContext::new(&mut context, addresser);

        // Call the `add_event` method
        let res = simple_state.add_event("event".to_string(), vec![], vec![]);

        // Verify this result is an Ok(())
        assert!(res.is_ok());
    }
}

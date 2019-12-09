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

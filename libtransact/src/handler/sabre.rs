/*
 * Copyright 2017 Bitwise IO, Inc.
 * Copyright 2019-2021 Cargill Incorporated
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

//! A struct for wrapping a `sabre_sdk::TransactionContext`.
//!
//! This will allow for a transact `TransactionHandler` to accept a
//! `sabre_sdk::TransactionContext` and be used in a Sabre smart contract.

use super::{error::ContextError, TransactionContext};
use sabre_sdk::TransactionContext as SabreTransactionContext;

/// A wrapper around a `'a mut dyn sabre_sdk::TransactionContext` that implements
/// `transact::handler::TransactionHandler`
pub struct SabreContext<'a> {
    pub context: &'a mut dyn SabreTransactionContext,
}

impl<'a> TransactionContext for SabreContext<'a> {
    /// get_state_entry queries the validator state for data at the
    /// address given. If the  address is set, the data is returned.
    ///
    /// # Arguments
    ///
    /// * `address` - the address to fetch
    fn get_state_entry(&self, address: &str) -> Result<Option<Vec<u8>>, ContextError> {
        Ok(self
            .context
            .get_state_entries(&[address.to_string()])
            .map_err(|err| ContextError::ResponseAttributeError(err.to_string()))?
            .into_iter()
            .map(|(_, val)| val)
            .next())
    }

    /// get_state_entries queries the validator state for data at each of the
    /// addresses in the given list. The addresses that have been set
    /// are returned.
    ///
    /// # Arguments
    ///
    /// * `addresses` - the addresses to fetch
    fn get_state_entries(
        &self,
        addresses: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, ContextError> {
        self.context
            .get_state_entries(addresses)
            .map_err(|err| ContextError::ResponseAttributeError(err.to_string()))
    }

    /// set_state_entry requests that the provided address is set in the validator state to its
    /// corresponding value.
    ///
    /// # Arguments
    ///
    /// * `address` - address of where to store the data
    /// * `data` - payload is the data to store at the address
    fn set_state_entry(&self, address: String, data: Vec<u8>) -> Result<(), ContextError> {
        self.context
            .set_state_entries(vec![(address, data)])
            .map_err(|err| ContextError::ResponseAttributeError(err.to_string()))
    }

    /// set_state_entries requests that each address in the provided map be
    /// set in validator state to its corresponding value.
    ///
    /// # Arguments
    ///
    /// * `entries` - entries are a hashmap where the key is an address and value is the data
    fn set_state_entries(&self, entries: Vec<(String, Vec<u8>)>) -> Result<(), ContextError> {
        self.context
            .set_state_entries(entries)
            .map_err(|err| ContextError::ResponseAttributeError(err.to_string()))
    }

    /// delete_state_entry requests that the provided address be unset
    /// in validator state. A list of successfully deleted addresses
    /// is returned.
    ///
    /// # Arguments
    ///
    /// * `address` - the address to delete
    fn delete_state_entry(&self, address: &str) -> Result<Option<String>, ContextError> {
        Ok(self
            .delete_state_entries(&[address.to_string()])
            .map_err(|err| ContextError::ResponseAttributeError(err.to_string()))?
            .into_iter()
            .next())
    }

    /// delete_state_entries requests that each of the provided addresses be unset
    /// in validator state. A list of successfully deleted addresses
    /// is returned.
    ///
    /// # Arguments
    ///
    /// * `addresses` - the addresses to delete
    fn delete_state_entries(&self, addresses: &[String]) -> Result<Vec<String>, ContextError> {
        self.context
            .delete_state_entries(addresses)
            .map_err(|err| ContextError::ResponseAttributeError(err.to_string()))
    }

    /// add_receipt_data adds a blob to the execution result for this transaction
    ///
    /// # Arguments
    ///
    /// * `data` - the data to add
    ///
    /// This is not currently supported by `sabre_sdk::TransactionContext` so it is
    /// left unimplemented.
    fn add_receipt_data(&self, _data: Vec<u8>) -> Result<(), ContextError> {
        unimplemented!()
    }

    /// add_event adds a new event to the execution result for this transaction.
    ///
    /// # Arguments
    ///
    /// * `event_type` -  This is used to subscribe to events. It should be globally unique and
    ///         describe what, in general, has occurred.
    /// * `attributes` - Additional information about the event that is transparent to the
    ///          validator. Attributes can be used by subscribers to filter the type of events
    ///          they receive.
    /// * `data` - Additional information about the event that is opaque to the validator.
    fn add_event(
        &self,
        event_type: String,
        attributes: Vec<(String, String)>,
        data: Vec<u8>,
    ) -> Result<(), ContextError> {
        self.context
            .add_event(event_type, attributes, &data)
            .map_err(|err| ContextError::ResponseAttributeError(err.to_string()))
    }
}

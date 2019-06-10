/*
 * Copyright 2017 Bitwise IO, Inc.
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

//! Traits for handling the execution of a transaction.
//!
//! The TransactionHandler trait provides the interface for implementing smart contract engines.
//! These handlers must be stateless and deterministic.  They are provided, along with the
//! transaction itself, a TransactonContext implementation which provides access to reading and
//! writing from state, as well appending events and other opaque data to the receipt.

mod error;

pub use crate::handler::error::{ApplyError, ContextError};
use crate::protocol::transaction::TransactionPair;

pub trait TransactionContext {
    /// get_state_entry queries the validator state for data at the
    /// address given. If the  address is set, the data is returned.
    ///
    /// # Arguments
    ///
    /// * `address` - the address to fetch
    fn get_state_entry(&self, address: &str) -> Result<Option<Vec<u8>>, ContextError> {
        Ok(self
            .get_state_entries(&[address.to_string()])?
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
    ) -> Result<Vec<(String, Vec<u8>)>, ContextError>;

    /// set_state_entry requests that the provided address is set in the validator state to its
    /// corresponding value.
    ///
    /// # Arguments
    ///
    /// * `address` - address of where to store the data
    /// * `data` - payload is the data to store at the address
    fn set_state_entry(&self, address: String, data: Vec<u8>) -> Result<(), ContextError> {
        self.set_state_entries(vec![(address, data)])
    }

    /// set_state_entries requests that each address in the provided map be
    /// set in validator state to its corresponding value.
    ///
    /// # Arguments
    ///
    /// * `entries` - entries are a hashmap where the key is an address and value is the data
    fn set_state_entries(&self, entries: Vec<(String, Vec<u8>)>) -> Result<(), ContextError>;

    /// delete_state_entry requests that the provided address be unset
    /// in validator state. A list of successfully deleted addresses
    /// is returned.
    ///
    /// # Arguments
    ///
    /// * `address` - the address to delete
    fn delete_state_entry(&self, address: &str) -> Result<Option<String>, ContextError> {
        Ok(self
            .delete_state_entries(&[address.to_string()])?
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
    fn delete_state_entries(&self, addresses: &[String]) -> Result<Vec<String>, ContextError>;

    /// add_receipt_data adds a blob to the execution result for this transaction
    ///
    /// # Arguments
    ///
    /// * `data` - the data to add
    fn add_receipt_data(&self, data: Vec<u8>) -> Result<(), ContextError>;

    /// add_event adds a new event to the execution result for this transaction.
    ///
    /// # Arguments
    ///
    /// * `event_type` -  This is used to subscribe to events. It should be globally unique and
    ///         describe what, in general, has occured.
    /// * `attributes` - Additional information about the event that is transparent to the
    ///          validator. Attributes can be used by subscribers to filter the type of events
    ///          they receive.
    /// * `data` - Additional information about the event that is opaque to the validator.
    fn add_event(
        &self,
        event_type: String,
        attributes: Vec<(String, String)>,
        data: Vec<u8>,
    ) -> Result<(), ContextError>;
}

pub trait TransactionHandler: Send {
    /// TransactionHandler that defines the business logic for a new transaction family.
    /// The family_name, family_versions, and namespaces functions are
    /// used by the processor to route processing requests to the handler.

    /// family_name should return the name of the transaction family that this
    /// handler can process, e.g. "intkey"
    fn family_name(&self) -> &str;

    /// family_versions should return a list of versions this transaction
    /// family handler can process, e.g. ["1.0"]
    fn family_versions(&self) -> &[String];

    /// Apply is the single method where all the business logic for a
    /// transaction family is defined. The method will be called by the
    /// transaction processor upon receiving a TpProcessRequest that the
    /// handler understands and will pass in the TpProcessRequest and an
    /// initialized instance of the Context type.
    fn apply(
        &self,
        transaction: &TransactionPair,
        context: &mut dyn TransactionContext,
    ) -> Result<(), ApplyError>;
}

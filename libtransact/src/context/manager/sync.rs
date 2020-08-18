/*
 * Copyright 2019 Bitwise IO, Inc.
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

//! This module provides a thread-safe ContextManager.
//!
//! For many uses of the context manager, it will need to be shared between multiple threads,
//! with some threads reading and writing to a context while others create contexts.
use std::sync::{Arc, Mutex};

use crate::context::error::ContextManagerError;
use crate::context::{manager, ContextId, ContextLifecycle};
use crate::protocol::receipt::{Event, TransactionReceipt};
use crate::state::Read;

/// A thread-safe ContextManager.
#[derive(Clone)]
pub struct ContextManager {
    internal_manager: Arc<Mutex<manager::ContextManager>>,
}

impl ContextManager {
    /// Constructs a new Context Manager around a given state Read.
    ///
    /// The Read defines the state on which the context built.
    pub fn new(database: Box<dyn Read<StateId = String, Key = String, Value = Vec<u8>>>) -> Self {
        ContextManager {
            internal_manager: Arc::new(Mutex::new(manager::ContextManager::new(database))),
        }
    }

    /// Return a set of values from a context.
    ///
    /// The values are returned as key-value tuples
    ///
    ///
    /// # Errors
    ///
    /// Returns an error if the context id does not exist, or an error occurs while reading
    /// from the underlying state.
    pub fn get(
        &self,
        context_id: &ContextId,
        keys: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, ContextManagerError> {
        self.internal_manager
            .lock()
            .expect("Lock in the get method was poisoned")
            .get(context_id, keys)
    }

    /// # Errors
    ///
    /// Returns an error if the context id does not exist, or an error occurs while reading
    /// from the underlying state.
    pub fn set_state(
        &self,
        context_id: &ContextId,
        key: String,
        value: Vec<u8>,
    ) -> Result<(), ContextManagerError> {
        self.internal_manager
            .lock()
            .expect("Lock in set_state was poisoned")
            .set_state(context_id, key, value)
    }

    pub fn delete_state(
        &self,
        context_id: &ContextId,
        key: &str,
    ) -> Result<Option<Vec<u8>>, ContextManagerError> {
        self.internal_manager
            .lock()
            .expect("Lock in delete_state was poisoned")
            .delete_state(context_id, key)
    }

    pub fn add_event(
        &self,
        context_id: &ContextId,
        event: Event,
    ) -> Result<(), ContextManagerError> {
        self.internal_manager
            .lock()
            .expect("Lock in add_event was poisoned")
            .add_event(context_id, event)
    }

    pub fn add_data(
        &self,
        context_id: &ContextId,
        data: Vec<u8>,
    ) -> Result<(), ContextManagerError> {
        self.internal_manager
            .lock()
            .expect("Lock in add_data was poisoned")
            .add_data(context_id, data)
    }
}

impl ContextLifecycle for ContextManager {
    /// Creates a Context, and returns the resulting ContextId.
    fn create_context(&mut self, dependent_contexts: &[ContextId], state_id: &str) -> ContextId {
        self.internal_manager
            .lock()
            .expect("Lock in create_context was poisoned")
            .create_context(dependent_contexts, state_id)
    }

    fn drop_context(&mut self, context_id: ContextId) {
        self.internal_manager
            .lock()
            .expect("Lock in drop_context was poisoned")
            .drop_context(context_id)
    }

    fn get_transaction_receipt(
        &self,
        context_id: &ContextId,
        transaction_id: &str,
    ) -> Result<TransactionReceipt, ContextManagerError> {
        self.internal_manager
            .lock()
            .expect("Lock in get_transaction_receipt was poisoned")
            .get_transaction_receipt(context_id, transaction_id)
    }

    fn clone_box(&self) -> Box<dyn ContextLifecycle> {
        Box::new(self.clone())
    }
}

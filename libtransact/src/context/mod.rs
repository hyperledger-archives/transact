/*
 * Copyright 2019 Bitwise IO, Inc.
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
//! Transaction context management.
//!
//! In Transact, state reads and writes are scoped (sandboxed) to a specific "context" that
//! contains a reference to a state ID (such as a Merkle-Radix state root hash) and one or more
//! previous contexts. The context manager implements the context lifecycle and services the calls
//! that read, write, and delete data from state.

/// Unique id that references a "Context" from which a `Transaction` can query state and
/// modify events, data, and state.
pub type ContextId = [u8; 16];

mod error;
pub mod manager;

use crate::context::manager::ContextManagerError;
use crate::protocol::receipt::{Event, StateChange, TransactionReceipt};
use std::mem;
use uuid::Uuid;

/// ContextManager functionality used by the Scheduler.
pub trait ContextLifecycle: Send {
    /// Create a new Context, returning a unique ContextId.
    fn create_context(&mut self, dependent_contexts: &[ContextId], state_id: &str) -> ContextId;

    fn drop_context(&mut self, context_id: ContextId);

    fn get_transaction_receipt(
        &self,
        context_id: &ContextId,
        transaction_id: &str,
    ) -> Result<TransactionReceipt, ContextManagerError>;

    /// Clone implementation for `ContextLifecycle`. The implementation of the `Clone` trait for
    /// `Box<dyn ContextLifecycle>` calls this method.
    fn clone_box(&self) -> Box<dyn ContextLifecycle>;
}

impl Clone for Box<dyn ContextLifecycle> {
    fn clone(&self) -> Box<dyn ContextLifecycle> {
        self.clone_box()
    }
}

#[derive(Debug, Clone, Default)]
pub struct Context {
    base_contexts: Vec<ContextId>,
    state_changes: Vec<StateChange>,
    id: ContextId,
    data: Vec<Vec<u8>>,
    events: Vec<Event>,
    state_id: String,
}

impl Context {
    pub fn new(state_id: &str, base_contexts: Vec<ContextId>) -> Self {
        Context {
            base_contexts,
            state_id: state_id.to_string(),
            state_changes: Vec::new(),
            id: *Uuid::new_v4().as_bytes(),
            data: Vec::new(),
            events: Vec::new(),
        }
    }

    pub fn base_contexts(&self) -> &[ContextId] {
        &self.base_contexts
    }
    pub fn events(&self) -> &Vec<Event> {
        &self.events
    }

    pub fn state_changes(&self) -> &Vec<StateChange> {
        &self.state_changes
    }

    pub fn id(&self) -> &ContextId {
        &self.id
    }

    pub fn data(&self) -> &Vec<Vec<u8>> {
        &self.data
    }

    pub fn state_id(&self) -> &String {
        &self.state_id
    }

    pub fn add_event(&mut self, event: Event) {
        if !self.events().contains(&event) {
            self.events.push(event);
        }
    }

    pub fn add_data(&mut self, data: Vec<u8>) {
        if !self.data().contains(&data) {
            self.data.push(data);
        }
    }

    pub fn get_state(&self, key: &str) -> Option<&[u8]> {
        if let Some(StateChange::Set { value: v, .. }) = self
            .state_changes
            .iter()
            .rev()
            .find(|state_change| state_change.has_key(&key))
        {
            return Some(v);
        }
        None
    }

    /// Adds StateChange::Set without deleting previous StateChanges associated with the Key
    pub fn set_state(&mut self, key: String, value: Vec<u8>) {
        let new_state_change = StateChange::Set { key, value };
        self.state_changes.push(new_state_change);
    }

    /// Adds StateChange::Delete and returns the value associated to the key being deleted
    pub fn delete_state(&mut self, key: &str) -> Option<Vec<u8>> {
        let found_state_change = self
            .state_changes
            .iter_mut()
            .rev()
            .find(|state_change| state_change.has_key(&key));
        if let Some(StateChange::Set { .. }) = found_state_change {
            // If a StateChange::Set is found associated with the key, the value set is returned.
            let mut new_state_change: StateChange = StateChange::Delete {
                key: key.to_string(),
            };
            mem::swap(found_state_change.unwrap(), &mut new_state_change);
            if let StateChange::Set { value: v, .. } = new_state_change {
                return Some(v);
            }
        } else if found_state_change.is_none() {
            // If no StateChange, Set or Delete, is found associated with the key, a new Delete
            // is added to the list of StateChanges with the value returned as None.
            self.state_changes.push(StateChange::Delete {
                key: key.to_string(),
            });
        }
        None
    }

    /// Checks to see if the Key is referenced by any StateChanges within the Context
    pub fn contains(&self, key: &str) -> bool {
        for state_change in self.state_changes().iter().rev() {
            match state_change {
                StateChange::Set { key: k, .. } => {
                    if k == key {
                        return true;
                    }
                }
                StateChange::Delete { key: k } => {
                    if k == key {
                        return false;
                    }
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::receipt::StateChange;

    static KEY1: &str = "111111111111111111111111111111111111111111111111111111111111111111";
    static KEY2: &str = "222222222222222222222222222222222222222222222222222222222222222222";
    static KEY3: &str = "333333333333333333333333333333333333333333333333333333333333333333";
    static BYTES1: [u8; 4] = [0x01, 0x02, 0x03, 0x04];
    static BYTES2: [u8; 4] = [0x05, 0x06, 0x07, 0x08];
    static BYTES3: [u8; 4] = [0x09, 0x0a, 0x0b, 0x0c];

    #[test]
    fn get_state() {
        let first_key = &KEY1.to_string();
        let first_value = &BYTES1.to_vec();
        let base_contexts = Vec::new();
        let mut context = Context::new(&KEY3, base_contexts);
        context.set_state(first_key.to_string(), first_value.to_vec());
        assert!(context.contains(&first_key));
        let state_value = context.get_state(&first_key);
        assert_eq!(state_value, Some(first_value.as_slice()));
    }

    #[test]
    fn test_compare_state_change() {
        let first_set: StateChange = StateChange::Set {
            key: KEY1.to_string(),
            value: BYTES1.to_vec(),
        };
        let second_set: StateChange = StateChange::Set {
            key: KEY2.to_string(),
            value: BYTES2.to_vec(),
        };
        let delete_first: StateChange = StateChange::Delete {
            key: KEY1.to_string(),
        };
        let delete_second: StateChange = StateChange::Delete {
            key: KEY2.to_string(),
        };
        let first_set_key = KEY1.to_string();
        assert_eq!(first_set.has_key(&first_set_key), true);
        assert_eq!(second_set.has_key(&first_set_key), false);
        assert_eq!(delete_first.has_key(&first_set_key), true);
        assert_eq!(delete_second.has_key(&first_set_key), false);
    }

    #[test]
    fn test_contains() {
        let base_contexts = Vec::new();
        let mut context = Context::new(&KEY3, base_contexts);
        context.set_state(KEY1.to_string(), BYTES1.to_vec());
        assert!(context.contains(&KEY1));

        context.set_state(KEY1.to_string(), BYTES2.to_vec());
        assert!(context.contains(&KEY1));

        context.set_state(KEY2.to_string(), BYTES3.to_vec());
        let deleted_value = context.delete_state(&KEY1);
        assert_eq!(deleted_value, Some(BYTES2.to_vec()));

        assert!(context.contains(&KEY2));
        assert!(!context.contains(&KEY1));
    }

    #[test]
    fn verify_state_changes() {
        let mut context = Context::new(&KEY3, Vec::new());
        context.set_state(KEY1.to_string(), BYTES1.to_vec());
        context.set_state(KEY1.to_string(), BYTES2.to_vec());
        context.set_state(KEY2.to_string(), BYTES3.to_vec());
        assert_eq!(context.state_changes().len(), 3);

        let deleted_value = context.delete_state(&KEY1);
        assert_ne!(deleted_value, Some(BYTES3.to_vec()));

        assert_eq!(context.state_changes().len(), 3);
        let first_key_set = context
            .state_changes()
            .iter()
            .cloned()
            .find(|change| change.has_key(&KEY1));
        if let Some(StateChange::Set { key: k, value: v }) = first_key_set {
            assert_eq!(k, KEY1.to_string());
            assert_ne!(Some(v), deleted_value);
        }
        if let StateChange::Set { key: k, value: v } = &context.state_changes()[1] {
            assert_eq!(k, KEY1);
            assert_eq!(Some(v.clone()), deleted_value);
        }
    }
}

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

//! This module provides a threaded ContextManager.
mod core;
mod error;

pub use crate::context::manager::thread::core::{
    ContextManagerJoinHandle, ContextOperationMessage, ContextOperationResponse,
};
pub use crate::context::manager::thread::error::ContextManagerCoreError;

use std::sync::mpsc;
use std::sync::mpsc::{RecvError, Sender};

use crate::context::error::ContextManagerError;
use crate::context::manager::thread::core::ContextOperationResult;
use crate::context::{ContextId, ContextLifecycle};
use crate::protocol::receipt::{Event, TransactionReceipt};

/// A ContextManager which interacts with a threaded version of the ContextManager operations.
#[derive(Clone)]
pub struct ContextManager {
    /// Used to send ContextOperationMessage to the Context Manager core
    core_sender: Sender<ContextOperationMessage>,
}

impl ContextManager {
    fn new(sender: Sender<ContextOperationMessage>) -> Self {
        ContextManager {
            core_sender: sender,
        }
    }

    /// Returns a list of key and value pairs from the specified context's state using the
    /// list of keys.
    pub fn get_state(
        &self,
        context_id: &ContextId,
        keys: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, ContextManagerError> {
        let (handler_sender, handler_receiver) = mpsc::channel();
        match self.core_sender.send(ContextOperationMessage::Get {
            context_id: *context_id,
            keys: keys.to_vec(),
            handler_sender,
        }) {
            Ok(_) => {
                if let ContextOperationResponse::ValidResult {
                    result: Some(ContextOperationResult::Get { values }),
                } = handler_receiver.recv()?
                {
                    Ok(values)
                } else {
                    Err(ContextManagerError::InternalError(Box::new(
                        ContextManagerCoreError::CoreReceiveError(RecvError),
                    )))
                }
            }
            Err(err) => Err(ContextManagerError::InternalError(Box::new(
                ContextManagerCoreError::HandlerSendError(err),
            ))),
        }
    }

    /// Sets a key and value pair in the specified context's state.
    pub fn set_state(
        &self,
        context_id: &ContextId,
        key: String,
        value: Vec<u8>,
    ) -> Result<(), ContextManagerError> {
        let (handler_sender, handler_receiver) = mpsc::channel();

        match self.core_sender.send(ContextOperationMessage::SetState {
            context_id: *context_id,
            key,
            value,
            handler_sender,
        }) {
            Ok(_) => {
                if let ContextOperationResponse::ValidResult { result: None } =
                    handler_receiver.recv()?
                {
                    Ok(())
                } else {
                    Err(ContextManagerError::InternalError(Box::new(
                        ContextManagerCoreError::CoreReceiveError(RecvError),
                    )))
                }
            }
            Err(err) => Err(ContextManagerError::InternalError(Box::new(
                ContextManagerCoreError::HandlerSendError(err),
            ))),
        }
    }

    /// Removes a specific key and all associated values from the specified context's state.
    pub fn delete_state(
        &self,
        context_id: &ContextId,
        key: &str,
    ) -> Result<Option<Vec<u8>>, ContextManagerError> {
        let (handler_sender, handler_receiver) = mpsc::channel();

        match self.core_sender.send(ContextOperationMessage::DeleteState {
            context_id: *context_id,
            key: key.to_string(),
            handler_sender,
        }) {
            Ok(_) => {
                if let ContextOperationResponse::ValidResult {
                    result: Some(ContextOperationResult::DeleteState { value }),
                } = handler_receiver.recv()?
                {
                    Ok(value)
                } else {
                    Err(ContextManagerError::InternalError(Box::new(
                        ContextManagerCoreError::CoreReceiveError(RecvError),
                    )))
                }
            }
            Err(err) => Err(ContextManagerError::InternalError(Box::new(
                ContextManagerCoreError::HandlerSendError(err),
            ))),
        }
    }

    /// Adds an Event to the specified context's list of events.
    pub fn add_event(
        &self,
        context_id: &ContextId,
        event: Event,
    ) -> Result<(), ContextManagerError> {
        let (handler_sender, handler_receiver) = mpsc::channel();

        match self.core_sender.send(ContextOperationMessage::AddEvent {
            context_id: *context_id,
            event,
            handler_sender,
        }) {
            Ok(_) => {
                if let ContextOperationResponse::ValidResult { result: None } =
                    handler_receiver.recv()?
                {
                    Ok(())
                } else {
                    Err(ContextManagerError::InternalError(Box::new(
                        ContextManagerCoreError::CoreReceiveError(RecvError),
                    )))
                }
            }
            Err(err) => Err(ContextManagerError::InternalError(Box::new(
                ContextManagerCoreError::HandlerSendError(err),
            ))),
        }
    }

    /// Adds data, represented as `Vec<u8>`, to the specified context's list of data.
    pub fn add_data(
        &self,
        context_id: &ContextId,
        data: Vec<u8>,
    ) -> Result<(), ContextManagerError> {
        let (handler_sender, handler_receiver) = mpsc::channel();

        match self.core_sender.send(ContextOperationMessage::AddData {
            context_id: *context_id,
            data,
            handler_sender,
        }) {
            Ok(_) => {
                if let ContextOperationResponse::ValidResult { result: None } =
                    handler_receiver.recv()?
                {
                    Ok(())
                } else {
                    Err(ContextManagerError::InternalError(Box::new(
                        ContextManagerCoreError::CoreReceiveError(RecvError),
                    )))
                }
            }
            Err(err) => Err(ContextManagerError::InternalError(Box::new(
                ContextManagerCoreError::HandlerSendError(err),
            ))),
        }
    }
}

impl ContextLifecycle for ContextManager {
    /// Creates a new context, returning the resulting ContextId.
    fn create_context(
        &mut self,
        dependent_contexts: &[ContextId],
        state_id: &str,
    ) -> Result<ContextId, ContextManagerError> {
        let (handler_sender, handler_receiver) = mpsc::channel();

        match self
            .core_sender
            .send(ContextOperationMessage::CreateContext {
                dependent_contexts: dependent_contexts.to_vec(),
                state_id: state_id.to_string(),
                handler_sender,
            }) {
            Ok(_) => {
                if let ContextOperationResponse::ValidResult {
                    result: Some(ContextOperationResult::CreateContext { context_id }),
                } = handler_receiver.recv()?
                {
                    Ok(context_id)
                } else {
                    Err(ContextManagerError::InternalError(Box::new(
                        ContextManagerCoreError::CoreReceiveError(RecvError),
                    )))
                }
            }
            Err(err) => Err(ContextManagerError::InternalError(Box::new(
                ContextManagerCoreError::HandlerSendError(err),
            ))),
        }
    }

    /// Drops the specified context from the ContextManager.
    fn drop_context(&mut self, _context_id: ContextId) -> Result<(), ContextManagerError> {
        unimplemented!();
    }

    ///Creates a TransactionReceipt based on the information available within the specified Context.
    fn get_transaction_receipt(
        &self,
        context_id: &ContextId,
        transaction_id: &str,
    ) -> Result<TransactionReceipt, ContextManagerError> {
        let (handler_sender, handler_receiver) = mpsc::channel();

        match self
            .core_sender
            .send(ContextOperationMessage::GetTransactionReceipt {
                context_id: *context_id,
                transaction_id: transaction_id.to_string(),
                handler_sender,
            }) {
            Ok(_) => {
                if let ContextOperationResponse::ValidResult {
                    result:
                        Some(ContextOperationResult::GetTransactionReceipt {
                            transaction_receipt,
                        }),
                } = handler_receiver.recv()?
                {
                    Ok(transaction_receipt)
                } else {
                    Err(ContextManagerError::InternalError(Box::new(
                        ContextManagerCoreError::CoreReceiveError(RecvError),
                    )))
                }
            }
            Err(err) => Err(ContextManagerError::InternalError(Box::new(
                ContextManagerCoreError::HandlerSendError(err),
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::protocol::receipt::{Event, EventBuilder, StateChange};
    use crate::state::hashmap::HashMapState;

    static KEY1: &str = "111111111111111111111111111111111111111111111111111111111111111111";
    static KEY2: &str = "222222222222222222222222222222222222222222222222222222222222222222";
    static KEY3: &str = "333333333333333333333333333333333333333333333333333333333333333333";
    static KEY4: &str = "444444444444444444444444444444444444444444444444444444444444444444";

    static BYTES1: [u8; 4] = [0x01, 0x02, 0x03, 0x04];
    static BYTES2: [u8; 4] = [0x05, 0x06, 0x07, 0x08];
    static BYTES3: [u8; 4] = [0x09, 0x10, 0x11, 0x12];

    static EVENT_TYPE1: &str = "sawtooth/block-commit";
    static ATTR1: (&str, &str) = (
        "block_id",
        "f40b90d06b4a9074af2ab09e0187223da7466be75ec0f472 \
         f2edd5f22960d76e402e6c07c90b7816374891d698310dd25d9b88dce7dbcba8219d9f7c9cae1861",
    );
    static ATTR2: (&str, &str) = ("block_num", "3");

    fn make_event() -> Event {
        EventBuilder::new()
            .with_event_type(EVENT_TYPE1.to_string())
            .with_attributes(vec![
                (ATTR1.0.to_string(), ATTR1.1.to_string()),
                (ATTR2.0.to_string(), ATTR2.1.to_string()),
            ])
            .with_data(BYTES1.to_vec())
            .build()
            .expect("Unable to build Event")
    }

    fn check_state_change(state_change: StateChange) {
        match state_change {
            StateChange::Set { key, value } => {
                assert_eq!(KEY2, key);
                assert_eq!(BYTES2.to_vec(), value);
            }
            StateChange::Delete { key } => {
                assert_eq!(KEY1, key);
            }
        }
    }

    fn check_transaction_receipt(transaction_receipt: TransactionReceipt, event: Event) {
        for state_change in transaction_receipt.state_changes {
            check_state_change(state_change)
        }
        assert_eq!(vec!(event), transaction_receipt.events);
        assert_eq!(vec!(BYTES3.to_vec()), transaction_receipt.data);
    }

    /// Tests the functionality of the threaded Context Manager implemented through
    /// the ContextLifecycle trait. The Context Manager's join handle is then used to shut down the
    /// Context Manager thread.
    #[test]
    fn test_context_lifecycle() {
        let state = HashMapState::new();
        let (join_handle, mut context_manager) = ContextManagerJoinHandle::new(Box::new(state));

        let state_id = HashMapState::state_id(&HashMap::new());

        let context_id = context_manager
            .create_context(&[], &state_id)
            .expect("Unable to create Context");

        let txn_receipt = context_manager.get_transaction_receipt(&context_id, KEY1);
        assert!(txn_receipt.is_ok());
        let transaction_id = txn_receipt.unwrap().transaction_id;
        assert_eq!(transaction_id, KEY1.to_string());

        assert!(join_handle.shutdown().is_ok());
    }

    /// Tests the threaded Context Manager functions dealing specifically with manipulating state,
    /// including setting and deleting values from state, and fetching state to prove the correct
    /// values are returned. The Context Manager's join handle is then used to shut down the Context
    /// Manager thread.
    #[test]
    fn test_context_state_changes() {
        let state = HashMapState::new();
        let (join_handle, mut context_manager) = ContextManagerJoinHandle::new(Box::new(state));
        let state_id = HashMapState::state_id(&HashMap::new());

        let context_id = context_manager
            .create_context(&[], &state_id)
            .expect("Unable to create Cotnext");

        let set_result = context_manager.set_state(&context_id, KEY1.to_string(), BYTES1.to_vec());
        assert!(set_result.is_ok());
        let set_result_2 =
            context_manager.set_state(&context_id, KEY2.to_string(), BYTES2.to_vec());
        assert!(set_result_2.is_ok());
        let set_result_3 =
            context_manager.set_state(&context_id, KEY3.to_string(), BYTES3.to_vec());
        assert!(set_result_3.is_ok());

        let delete_result = context_manager
            .delete_state(&context_id, KEY2)
            .expect("Unable to delete state");
        assert!(delete_result.is_some());
        assert_eq!(Some(BYTES2.to_vec()), delete_result);
        let delete_none_result = context_manager
            .delete_state(&context_id, KEY2)
            .expect("Unable to delete state");
        assert!(delete_none_result.is_none());

        let keys = [KEY1.to_string(), KEY2.to_string(), KEY3.to_string()];
        let mut key_values = context_manager
            .get_state(&context_id, &keys)
            .expect("Unable to get state");
        assert_eq!(key_values.len(), 2);
        assert_eq!(
            key_values.pop().expect("Key value not available"),
            (KEY1.to_string(), BYTES1.to_vec())
        );
        assert_eq!(
            key_values.pop().expect("Key value not available"),
            (KEY3.to_string(), BYTES3.to_vec())
        );

        assert!(join_handle.shutdown().is_ok());
    }

    /// Tests the functionality of the threaded Context Manager to be utilized on multiple threads
    /// by setting state values using a clone of the generated Context Manager as well as adding
    /// data to the cloned Context Manager on a thread spawned within the test. The original
    /// Context Manager is then used to create a TransactionReceipt which holds the data and state
    /// values set by the cloned Context Manager in the separate thread. The Context Manager's join
    /// handle is then used to shut down the Context Manager thread.
    #[test]
    fn test_multi_thread_integration() {
        let state = HashMapState::new();
        let (join_handle, mut context_manager) = ContextManagerJoinHandle::new(Box::new(state));
        let state_id = HashMapState::state_id(&HashMap::new());

        let context_id = context_manager
            .create_context(&[], &state_id)
            .expect("Unable to create Context");
        let cm = context_manager.clone();

        let _ = std::thread::Builder::new()
            .name(String::from("Thread-test-populate-context"))
            .spawn(move || {
                assert!(cm.add_event(&context_id, make_event()).is_ok());
                assert!(cm.add_data(&context_id, BYTES3.to_vec()).is_ok());
                assert!(cm
                    .set_state(&context_id, KEY1.to_string(), BYTES1.to_vec())
                    .is_ok());
                assert!(cm.delete_state(&context_id, KEY1).is_ok());
                assert!(cm
                    .set_state(&context_id, KEY2.to_string(), BYTES2.to_vec())
                    .is_ok());
            })
            .expect("Unable to create thread")
            .join();

        let txn_receipt = context_manager
            .get_transaction_receipt(&context_id, KEY4)
            .expect("Unable to get transaction receipt");
        check_transaction_receipt(txn_receipt, make_event());

        assert!(join_handle.shutdown().is_ok());
    }
}

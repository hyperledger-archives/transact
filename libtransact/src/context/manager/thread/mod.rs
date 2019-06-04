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
use crate::context::{manager::thread::core::ContextOperationResult, ContextId};
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

    /// Returns a ContextId of the newly created Context.
    pub fn create_context(
        &self,
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
    pub fn drop_context(&self, _context_id: ContextId) {
        unimplemented!();
    }

    /// Returns a TransactionReceipt based on the specified context.
    pub fn get_transaction_receipt(
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

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

use std::sync::mpsc::{Receiver, Sender};
use std::thread;

pub use crate::context::manager::thread::ContextManagerCoreError;
use crate::context::{manager::ContextManager, ContextId, ContextLifecycle};
use crate::protocol::receipt::{Event, TransactionReceipt};
use crate::state::Read;

/// An enum of messages which can be sent to the ContextManagerCore through a
/// Sender<ContextOperationMessage>
pub enum ContextOperationMessage {
    Get {
        handler_sender: Sender<ContextOperationResponse>,
        context_id: ContextId,
        keys: Vec<String>,
    },
    SetState {
        handler_sender: Sender<ContextOperationResponse>,
        context_id: ContextId,
        key: String,
        value: Vec<u8>,
    },
    DeleteState {
        handler_sender: Sender<ContextOperationResponse>,
        context_id: ContextId,
        key: String,
    },
    AddEvent {
        handler_sender: Sender<ContextOperationResponse>,
        context_id: ContextId,
        event: Event,
    },
    AddData {
        handler_sender: Sender<ContextOperationResponse>,
        context_id: ContextId,
        data: Vec<u8>,
    },
    GetTransactionReceipt {
        handler_sender: Sender<ContextOperationResponse>,
        context_id: ContextId,
        transaction_id: String,
    },
    CreateContext {
        handler_sender: Sender<ContextOperationResponse>,
        dependent_contexts: Vec<ContextId>,
        state_id: String,
    },
    Shutdown,
}

/// An enum of response messages from the ContextManagerCore, which holds the result of
/// ContextManager methods.
pub enum ContextOperationResponse {
    ValidResult {
        result: Option<ContextOperationResult>,
    },
    InvalidResult {
        error_message: String,
    },
}

pub enum ContextOperationResult {
    Get {
        values: Vec<(String, Vec<u8>)>,
    },
    DeleteState {
        value: Option<Vec<u8>>,
    },
    GetTransactionReceipt {
        transaction_receipt: TransactionReceipt,
    },
    CreateContext {
        context_id: ContextId,
    },
}

struct ContextManagerCore {
    /// Internal ContextManager owned by the threaded version.
    manager: ContextManager,

    /// Receiver for all messages to the ContextManagerCore.
    core_receiver: Receiver<ContextOperationMessage>,
}

impl ContextManagerCore {
    fn new(
        database: Box<dyn Read<StateId = String, Key = String, Value = Vec<u8>>>,
        core_receiver: Receiver<ContextOperationMessage>,
    ) -> Self {
        let internal_manager = ContextManager::new(database);
        ContextManagerCore {
            manager: internal_manager,
            core_receiver,
        }
    }

    fn run(&mut self) -> Result<(), ContextManagerCoreError> {
        loop {
            match self.core_receiver.recv()? {
                ContextOperationMessage::GetTransactionReceipt {
                    context_id,
                    transaction_id,
                    handler_sender,
                } => {
                    match self
                        .manager
                        .get_transaction_receipt(&context_id, &transaction_id)
                    {
                        Ok(txn_receipt) => {
                            handler_sender.send(ContextOperationResponse::ValidResult {
                                result: Some(ContextOperationResult::GetTransactionReceipt {
                                    transaction_receipt: txn_receipt,
                                }),
                            })?;
                        }
                        Err(err) => {
                            handler_sender.send(ContextOperationResponse::InvalidResult {
                                error_message: err.to_string(),
                            })?;
                        }
                    }
                }
                ContextOperationMessage::CreateContext {
                    dependent_contexts,
                    state_id,
                    handler_sender,
                } => {
                    let context_id = self.manager.create_context(&dependent_contexts, &state_id);
                    handler_sender.send(ContextOperationResponse::ValidResult {
                        result: Some(ContextOperationResult::CreateContext { context_id }),
                    })?;
                }
                ContextOperationMessage::Get {
                    context_id,
                    keys,
                    handler_sender,
                } => match self.manager.get(&context_id, &keys) {
                    Ok(values) => {
                        handler_sender.send(ContextOperationResponse::ValidResult {
                            result: Some(ContextOperationResult::Get { values }),
                        })?;
                    }
                    Err(err) => {
                        handler_sender.send(ContextOperationResponse::InvalidResult {
                            error_message: err.to_string(),
                        })?;
                    }
                },
                ContextOperationMessage::SetState {
                    context_id,
                    key,
                    value,
                    handler_sender,
                } => match self.manager.set_state(&context_id, key, value) {
                    Ok(()) => {
                        handler_sender
                            .send(ContextOperationResponse::ValidResult { result: None })?;
                    }
                    Err(err) => {
                        handler_sender.send(ContextOperationResponse::InvalidResult {
                            error_message: err.to_string(),
                        })?;
                    }
                },
                ContextOperationMessage::DeleteState {
                    context_id,
                    key,
                    handler_sender,
                } => match self.manager.delete_state(&context_id, &key) {
                    Ok(value) => {
                        handler_sender.send(ContextOperationResponse::ValidResult {
                            result: Some(ContextOperationResult::DeleteState { value }),
                        })?;
                    }
                    Err(err) => {
                        handler_sender.send(ContextOperationResponse::InvalidResult {
                            error_message: err.to_string(),
                        })?;
                    }
                },
                ContextOperationMessage::AddEvent {
                    context_id,
                    event,
                    handler_sender,
                } => match self.manager.add_event(&context_id, event) {
                    Ok(()) => {
                        handler_sender
                            .send(ContextOperationResponse::ValidResult { result: None })?;
                    }
                    Err(err) => {
                        handler_sender.send(ContextOperationResponse::InvalidResult {
                            error_message: err.to_string(),
                        })?;
                    }
                },
                ContextOperationMessage::AddData {
                    context_id,
                    data,
                    handler_sender,
                } => match self.manager.add_data(&context_id, data) {
                    Ok(()) => {
                        handler_sender
                            .send(ContextOperationResponse::ValidResult { result: None })?;
                    }
                    Err(err) => {
                        handler_sender.send(ContextOperationResponse::InvalidResult {
                            error_message: err.to_string(),
                        })?;
                    }
                },
                ContextOperationMessage::Shutdown => {
                    break;
                }
            }
        }
        Ok(())
    }

    fn start(mut self) -> std::thread::JoinHandle<()> {
        thread::Builder::new()
            .name(String::from("Thread-ContextManager"))
            .spawn(move || {
                if let Err(err) = self.run() {
                    error!("ContextManagerCore ended due to error: {}", err);
                }
            })
            .expect("Could not build a thread for Context Manager")
    }
}

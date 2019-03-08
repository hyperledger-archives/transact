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

use crate::context::{manager::ContextManager, ContextId};
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
    _manager: ContextManager,

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
            _manager: internal_manager,
            core_receiver,
        }
    }

    fn run(&mut self) {
        loop {
            match self.core_receiver.recv() {
                Ok(ContextOperationMessage::GetTransactionReceipt { .. }) => {
                    unimplemented!();
                }
                Ok(ContextOperationMessage::CreateContext { .. }) => {
                    unimplemented!();
                }
                Ok(ContextOperationMessage::Get { .. }) => {
                    unimplemented!();
                }
                Ok(ContextOperationMessage::SetState { .. }) => {
                    unimplemented!();
                }
                Ok(ContextOperationMessage::DeleteState { .. }) => {
                    unimplemented!();
                }
                Ok(ContextOperationMessage::AddEvent { .. }) => {
                    unimplemented!();
                }
                Ok(ContextOperationMessage::AddData { .. }) => {
                    unimplemented!();
                }
                Ok(ContextOperationMessage::Shutdown) => {
                    break;
                }
                Err(err) => {
                    error!("ContextManagerCore recv error: {}", err);
                    break;
                }
            }
        }
    }

    fn start(mut self) -> std::thread::JoinHandle<()> {
        thread::Builder::new()
            .name(String::from("Thread-ContextManager"))
            .spawn(move || {
                self.run();
            })
            .expect("Could not build a thread for Context Manager")
    }
}

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
//! The static execution adapter provides a way to execute transaction handlers directly.
//!
//! This module provides the `StaticExecutionAdapter`, an implementation of `ExecutionAdapter`
//! which execute transactions via `TransactionHandler` instances directly.
use std::sync::mpsc::{channel, Sender};
use std::thread;

use crate::context::manager::sync::ContextManager;
use crate::context::manager::ContextManagerError;
use crate::context::ContextId;
use crate::execution::adapter::{ExecutionAdapter, ExecutionAdapterError, ExecutionOperationError};
use crate::execution::{ExecutionRegistry, TransactionFamily};
use crate::handler::{ApplyError, ContextError, TransactionContext, TransactionHandler};
use crate::protocol::receipt::Event;
use crate::protocol::transaction::TransactionPair;
use crate::scheduler::{ExecutionTaskCompletionNotification, InvalidTransactionResult};

// A type declaration to make the use of this complicated type-bounded box easier to work with
type OnDoneCallback =
    Box<dyn Fn(Result<ExecutionTaskCompletionNotification, ExecutionAdapterError>) + Send>;

/// The StaticExecutionAdapter to wrap TransactionHandlers
///
/// This struct takes a series of transaction handlers which can be used to execution transactions.
/// These transactions are executed on a single background thread.
pub struct StaticExecutionAdapter {
    join_handle: thread::JoinHandle<bool>,
    sender: Sender<StaticAdapterCommand>,
}

impl StaticExecutionAdapter {
    /// Creates a new adapter, if possible.
    ///
    /// Creates a `StaticExecutionAdapter` wrapping the given `TransactionHandler` vector and a
    /// `ContextManager` instance. This adapter will dispatch transaction pairs to the appropriate
    /// handler, if found.
    ///
    /// # Errors
    ///
    /// `ExecutionAdapterError` is returned if the background thread cannot be created.
    pub fn new_adapter(
        handlers: Vec<Box<dyn TransactionHandler>>,
        context_manager: ContextManager,
    ) -> Result<Self, ExecutionAdapterError> {
        let (sender, receiver) = channel();
        let join_handle = thread::Builder::new()
            .name("StaticExecutionAdapter".into())
            .spawn(move || {
                while let Ok(cmd) = receiver.recv() {
                    match cmd {
                        StaticAdapterCommand::Execute(execute_cmd) => {
                            let (txn_pair, context_id, on_done) = *execute_cmd;
                            debug!("Executing {:?} in context {:?}", &txn_pair, &context_id);
                            execute_transaction(
                                &handlers,
                                txn_pair,
                                &context_manager,
                                context_id,
                                on_done,
                            );
                        }
                        StaticAdapterCommand::Start(mut execution_registry) => {
                            register_handlers(&handlers, &mut *execution_registry);
                        }
                        StaticAdapterCommand::Stop => {
                            break;
                        }
                    }
                }
                true
            })
            .map_err(|err| ExecutionAdapterError::GeneralExecutionError(Box::new(err)))?;

        Ok(StaticExecutionAdapter {
            join_handle,
            sender,
        })
    }
}

fn execute_transaction(
    handlers: &[Box<dyn TransactionHandler>],
    transaction_pair: TransactionPair,
    context_manager: &ContextManager,
    context_id: ContextId,
    on_done: OnDoneCallback,
) {
    let family = TransactionFamily::from_pair(&transaction_pair);
    match handlers.iter().find(|handler| {
        handler.family_name() == family.family_name()
            && handler
                .family_versions()
                .iter()
                .any(|v| v == family.family_version())
    }) {
        Some(handler) => {
            let mut static_context = StaticContext::new(context_manager, &context_id);

            match handler.apply(&transaction_pair, &mut static_context) {
                Ok(_) => on_done(Ok(ExecutionTaskCompletionNotification::Valid(
                    context_id,
                    transaction_pair.transaction().header_signature().to_owned(),
                ))),
                Err(ApplyError::InvalidTransaction(error_message)) => {
                    on_done(Ok(ExecutionTaskCompletionNotification::Invalid(
                        context_id,
                        InvalidTransactionResult {
                            transaction_id: transaction_pair
                                .transaction()
                                .header_signature()
                                .to_owned(),
                            error_message,
                            error_data: vec![],
                        },
                    )))
                }
                Err(err) => on_done(Err(ExecutionAdapterError::GeneralExecutionError(Box::new(
                    err,
                )))),
            }
        }
        None => on_done(Err(ExecutionAdapterError::RoutingError(Box::new(
            transaction_pair,
        )))),
    };
}

fn register_handlers(
    handlers: &[Box<dyn TransactionHandler>],
    execution_registry: &mut dyn ExecutionRegistry,
) {
    for handler in handlers {
        for version in handler.family_versions() {
            execution_registry.register_transaction_family(TransactionFamily::new(
                handler.family_name().to_owned(),
                version.clone(),
            ));
        }
    }
}

impl ExecutionAdapter for StaticExecutionAdapter {
    fn start(
        &mut self,
        execution_registry: Box<dyn ExecutionRegistry>,
    ) -> Result<(), ExecutionOperationError> {
        self.sender
            .send(StaticAdapterCommand::Start(execution_registry))
            .map_err(|err| {
                ExecutionOperationError::StartError(format!(
                    "Unable to start static execution adapter: {}",
                    err
                ))
            })
    }

    fn execute(
        &self,
        transaction_pair: TransactionPair,
        context_id: ContextId,
        on_done: OnDoneCallback,
    ) -> Result<(), ExecutionOperationError> {
        self.sender
            .send(StaticAdapterCommand::Execute(Box::new((
                transaction_pair,
                context_id,
                on_done,
            ))))
            .map_err(|err| {
                ExecutionOperationError::ExecuteError(format!(
                    "Unable to send transaction for static execution: {}",
                    err
                ))
            })
    }

    fn stop(self: Box<Self>) -> Result<(), ExecutionOperationError> {
        self.sender
            .send(StaticAdapterCommand::Stop)
            .map_err(|err| {
                ExecutionOperationError::StopError(format!("Unable to send stop command: {}", err))
            })?;

        self.join_handle.join().map_err(|_| {
            ExecutionOperationError::StopError("Unable to join internal thread.".into())
        })?;

        Ok(())
    }
}

enum StaticAdapterCommand {
    Start(Box<dyn ExecutionRegistry>),
    Stop,
    Execute(Box<(TransactionPair, ContextId, OnDoneCallback)>),
}

struct StaticContext<'a, 'b> {
    context_manager: &'a ContextManager,
    context_id: &'b ContextId,
}

impl<'a, 'b> StaticContext<'a, 'b> {
    fn new(context_manager: &'a ContextManager, context_id: &'b ContextId) -> Self {
        StaticContext {
            context_manager,
            context_id,
        }
    }
}

impl<'a, 'b> TransactionContext for StaticContext<'a, 'b> {
    fn get_state_entries(
        &self,
        addresses: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, ContextError> {
        self.context_manager
            .get(self.context_id, addresses)
            .map_err(ContextError::from)
    }

    fn set_state_entries(&self, entries: Vec<(String, Vec<u8>)>) -> Result<(), ContextError> {
        for (address, value) in entries.into_iter() {
            self.context_manager
                .set_state(self.context_id, address, value)?;
        }

        Ok(())
    }

    fn delete_state_entries(&self, addresses: &[String]) -> Result<Vec<String>, ContextError> {
        let mut results = vec![];
        for address in addresses.iter() {
            if self
                .context_manager
                .delete_state(self.context_id, address.as_str())?
                .is_some()
            {
                results.push(address.clone());
            }
        }
        Ok(results)
    }

    fn add_receipt_data(&self, data: Vec<u8>) -> Result<(), ContextError> {
        self.context_manager
            .add_data(self.context_id, data)
            .map_err(ContextError::from)
    }

    fn add_event(
        &self,
        event_type: String,
        attributes: Vec<(String, String)>,
        data: Vec<u8>,
    ) -> Result<(), ContextError> {
        self.context_manager
            .add_event(
                self.context_id,
                Event {
                    event_type,
                    attributes,
                    data,
                },
            )
            .map_err(ContextError::from)
    }
}

impl From<ContextManagerError> for ContextError {
    fn from(err: ContextManagerError) -> Self {
        // Error's should be addressed in the handler::error module.
        ContextError::SendError(Box::new(err))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::collections::HashMap;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use std::time;

    use cylinder::{secp256k1::Secp256k1Context, Context, Signer};

    use crate::context::ContextLifecycle;
    use crate::families::command::{make_command_transaction, CommandTransactionHandler};
    use crate::protocol::command::{
        AddEvent, AddReceiptData, BytesEntry, Command, DeleteState, GetState, ReturnInternalError,
        ReturnInvalid, SetState, Sleep, SleepType,
    };
    use crate::protocol::receipt::TransactionResult;
    use crate::scheduler::{ExecutionTaskCompletionNotification, InvalidTransactionResult};
    use crate::state::hashmap::HashMapState;

    fn create_bytes_entry(state_writes: Vec<(String, Vec<u8>)>) -> Vec<BytesEntry> {
        state_writes
            .into_iter()
            .map(|(k, v)| BytesEntry::new(k.to_string(), v.to_vec()))
            .collect()
    }

    /// Apply the static adapter with a simple transaction that sets a value successfully.
    #[test]
    fn apply_static_adapter_simple_set() {
        let registry = MockRegistry::default();

        let state = HashMapState::new();
        let state_id = HashMapState::state_id(&HashMap::new());

        let mut context_manager: ContextManager = ContextManager::new(Box::new(state));

        let handler = CommandTransactionHandler::new();

        let mut static_adapter =
            StaticExecutionAdapter::new_adapter(vec![Box::new(handler)], context_manager.clone())
                .expect("Could not create adapter");

        assert!(static_adapter.start(Box::new(registry.clone())).is_ok());

        // Create and execute a simple transaction.
        let txn_pair = make_command_transaction(
            &[Command::SetState(SetState::new(create_bytes_entry(vec![
                ("abc".into(), b"abc".to_vec()),
            ])))],
            &*new_signer(),
        );
        let txn_id = txn_pair.transaction().header_signature().into();
        let context_id = context_manager.create_context(&[], &state_id);

        let (send, recv) = std::sync::mpsc::channel();
        assert!(static_adapter
            .execute(
                txn_pair,
                context_id.clone(),
                Box::new(move |res| {
                    send.send(res).expect("Unable to send result");
                }),
            )
            .is_ok());
        let result = recv.recv().unwrap();

        assert_eq!(
            ExecutionTaskCompletionNotification::Valid(context_id.clone(), txn_id),
            result.unwrap()
        );
        assert_eq!(
            vec![("abc".to_owned(), b"abc".to_vec())],
            context_manager
                .get(&context_id, &["abc".to_owned()])
                .unwrap()
        );

        assert!(Box::new(static_adapter).stop().is_ok());
    }

    /// Apply the static adapter with a failing transaction which returns an invalid error.
    #[test]
    fn apply_static_adapter_invalid_txn() {
        let registry = MockRegistry::default();

        let state = HashMapState::new();
        let state_id = HashMapState::state_id(&HashMap::new());

        let mut context_manager: ContextManager = ContextManager::new(Box::new(state));

        let handler = CommandTransactionHandler::new();

        let mut static_adapter =
            StaticExecutionAdapter::new_adapter(vec![Box::new(handler)], context_manager.clone())
                .expect("Could not create adapter");

        assert!(static_adapter.start(Box::new(registry.clone())).is_ok());

        // Create and execute a failing transaction, resulting in an invalid error.
        let txn_pair = make_command_transaction(
            &[
                Command::GetState(GetState::new(vec!["abc".into()])),
                Command::ReturnInvalid(ReturnInvalid::new("Test Fail Succeeded".into())),
            ],
            &*new_signer(),
        );

        let txn_id = txn_pair.transaction().header_signature().to_owned();
        let context_id = context_manager.create_context(&[], &state_id);

        let (send, recv) = std::sync::mpsc::channel();
        assert!(static_adapter
            .execute(
                txn_pair,
                context_id.clone(),
                Box::new(move |res| {
                    send.send(res).expect("Unable to send result");
                }),
            )
            .is_ok());
        let result = recv.recv().unwrap();

        assert_eq!(
            ExecutionTaskCompletionNotification::Invalid(
                context_id,
                InvalidTransactionResult {
                    transaction_id: txn_id,
                    error_message: "Test Fail Succeeded".into(),
                    error_data: vec![],
                }
            ),
            result.unwrap()
        );

        assert!(Box::new(static_adapter).stop().is_ok());
    }

    /// Apply the static adapter with a failing transaction which returns an internal error.
    #[test]
    fn apply_static_adapter_internal_error() {
        let registry = MockRegistry::default();

        let state = HashMapState::new();
        let state_id = HashMapState::state_id(&HashMap::new());

        let mut context_manager: ContextManager = ContextManager::new(Box::new(state));

        let handler = CommandTransactionHandler::new();

        let mut static_adapter =
            StaticExecutionAdapter::new_adapter(vec![Box::new(handler)], context_manager.clone())
                .expect("Could not create adapter");

        assert!(static_adapter.start(Box::new(registry.clone())).is_ok());

        // Create and execute a failing transaction, resulting in an internal error.
        let txn_pair = make_command_transaction(
            &[
                Command::GetState(GetState::new(vec!["abc".into()])),
                Command::ReturnInternalError(ReturnInternalError::new(
                    "Test Internal Fail Succeeded".into(),
                )),
            ],
            &*new_signer(),
        );

        let context_id = context_manager.create_context(&[], &state_id);

        let (send, recv) = std::sync::mpsc::channel();
        assert!(static_adapter
            .execute(
                txn_pair,
                context_id.clone(),
                Box::new(move |res| {
                    send.send(res).expect("Unable to send result");
                }),
            )
            .is_ok());
        let result = recv.recv().unwrap();

        assert!(result.is_err());

        assert!(Box::new(static_adapter).stop().is_ok());
    }

    /// Apply the static adapter with a valid delete transaction.
    #[test]
    fn apply_static_adapter_valid_delete() {
        let registry = MockRegistry::default();

        let state = HashMapState::new();
        let state_id = HashMapState::state_id(&HashMap::new());

        let mut context_manager: ContextManager = ContextManager::new(Box::new(state));

        let handler = CommandTransactionHandler::new();

        let mut static_adapter =
            StaticExecutionAdapter::new_adapter(vec![Box::new(handler)], context_manager.clone())
                .expect("Could not create adapter");

        assert!(static_adapter.start(Box::new(registry.clone())).is_ok());

        // Create and execute a valid delete transaction.
        let txn_pair = make_command_transaction(
            &[
                Command::SetState(SetState::new(create_bytes_entry(vec![(
                    "abc".into(),
                    b"abc".to_vec(),
                )]))),
                Command::GetState(GetState::new(vec!["abc".into()])),
                Command::DeleteState(DeleteState::new(vec!["abc".into()])),
            ],
            &*new_signer(),
        );
        let txn_id = txn_pair.transaction().header_signature().to_owned();
        let context_id = context_manager.create_context(&[], &state_id);

        let (send, recv) = std::sync::mpsc::channel();
        assert!(static_adapter
            .execute(
                txn_pair,
                context_id.clone(),
                Box::new(move |res| {
                    send.send(res).expect("Unable to send result");
                }),
            )
            .is_ok());
        let result = recv.recv().unwrap();

        assert_eq!(
            ExecutionTaskCompletionNotification::Valid(context_id.clone(), txn_id),
            result.unwrap()
        );
        assert_eq!(
            context_manager
                .get(&context_id, &["abc".to_owned()])
                .unwrap(),
            vec![],
        );

        assert!(Box::new(static_adapter).stop().is_ok());
    }

    /// Apply the static adapter with a valid busy wait command.
    #[test]
    fn apply_static_adapter_busy_wait_sleep() {
        let registry = MockRegistry::default();

        let state = HashMapState::new();
        let state_id = HashMapState::state_id(&HashMap::new());

        let mut context_manager: ContextManager = ContextManager::new(Box::new(state));

        let handler = CommandTransactionHandler::new();

        let mut static_adapter =
            StaticExecutionAdapter::new_adapter(vec![Box::new(handler)], context_manager.clone())
                .expect("Could not create adapter");

        assert!(static_adapter.start(Box::new(registry.clone())).is_ok());

        // Create and execute a busy wait transaction.
        let txn_pair = make_command_transaction(
            &[Command::Sleep(Sleep::new(100, SleepType::BusyWait))],
            &*new_signer(),
        );
        let txn_id = txn_pair.transaction().header_signature().to_owned();
        let context_id = context_manager.create_context(&[], &state_id);

        let time_before_execution = time::Instant::now();

        let (send, recv) = std::sync::mpsc::channel();
        assert!(static_adapter
            .execute(
                txn_pair,
                context_id.clone(),
                Box::new(move |res| {
                    send.send(res).expect("Unable to send result");
                }),
            )
            .is_ok());
        let result = recv.recv().unwrap();

        let elapsed = time_before_execution.elapsed();
        assert!(elapsed.ge(&time::Duration::from_millis(100)));

        assert_eq!(
            ExecutionTaskCompletionNotification::Valid(context_id.clone(), txn_id),
            result.unwrap()
        );

        assert!(Box::new(static_adapter).stop().is_ok());
    }

    /// Apply the static adapter with a valid wait command.
    #[test]
    fn apply_static_adapter_wait_sleep() {
        let registry = MockRegistry::default();

        let state = HashMapState::new();
        let state_id = HashMapState::state_id(&HashMap::new());

        let mut context_manager: ContextManager = ContextManager::new(Box::new(state));

        let handler = CommandTransactionHandler::new();

        let mut static_adapter =
            StaticExecutionAdapter::new_adapter(vec![Box::new(handler)], context_manager.clone())
                .expect("Could not create adapter");

        assert!(static_adapter.start(Box::new(registry.clone())).is_ok());

        // Create and execute a sleep transaction.
        let txn_pair = make_command_transaction(
            &[Command::Sleep(Sleep::new(100, SleepType::Wait))],
            &*new_signer(),
        );
        let txn_id = txn_pair.transaction().header_signature().to_owned();
        let context_id = context_manager.create_context(&[], &state_id);

        let time_before_execution = time::Instant::now();

        let (send, recv) = std::sync::mpsc::channel();
        assert!(static_adapter
            .execute(
                txn_pair,
                context_id.clone(),
                Box::new(move |res| {
                    send.send(res).expect("Unable to send result");
                }),
            )
            .is_ok());
        let result = recv.recv().unwrap();

        let elapsed = time_before_execution.elapsed();
        assert!(elapsed.ge(&time::Duration::from_millis(100)));

        assert_eq!(
            ExecutionTaskCompletionNotification::Valid(context_id.clone(), txn_id),
            result.unwrap()
        );

        assert!(Box::new(static_adapter).stop().is_ok());
    }

    /// Apply the static adapter with a list of commands that will return early with an internal
    /// error.
    #[test]
    fn apply_static_adapter_early_internal_error() {
        let registry = MockRegistry::default();

        let state = HashMapState::new();
        let state_id = HashMapState::state_id(&HashMap::new());

        let mut context_manager: ContextManager = ContextManager::new(Box::new(state));

        let handler = CommandTransactionHandler::new();

        let mut static_adapter =
            StaticExecutionAdapter::new_adapter(vec![Box::new(handler)], context_manager.clone())
                .expect("Could not create adapter");

        assert!(static_adapter.start(Box::new(registry.clone())).is_ok());

        // Create and execute a Set transaction, followed by an Internal error. This will cause
        // the rest of the commands to be short-circuited.
        let txn_pair = make_command_transaction(
            &[
                Command::SetState(SetState::new(create_bytes_entry(vec![(
                    "abc".into(),
                    b"abc".to_vec(),
                )]))),
                Command::ReturnInternalError(ReturnInternalError::new(
                    "Return internal error between transactions".into(),
                )),
                Command::SetState(SetState::new(create_bytes_entry(vec![(
                    "def".into(),
                    b"def".to_vec(),
                )]))),
            ],
            &*new_signer(),
        );

        let context_id = context_manager.create_context(&[], &state_id);

        let (send, recv) = std::sync::mpsc::channel();
        assert!(static_adapter
            .execute(
                txn_pair,
                context_id.clone(),
                Box::new(move |res| {
                    send.send(res).expect("Unable to send result");
                }),
            )
            .is_ok());
        let result = recv.recv().unwrap();

        assert!(result.is_err());
        assert_eq!(
            vec![("abc".to_owned(), b"abc".to_vec())],
            context_manager
                .get(&context_id, &["abc".to_owned()])
                .unwrap(),
        );
        assert_eq!(
            context_manager
                .get(&context_id, &["def".to_owned()])
                .unwrap(),
            vec![],
        );

        assert!(Box::new(static_adapter).stop().is_ok());
    }

    /// Apply the static adapter with several commands to add Events.
    #[test]
    fn apply_static_adapter_add_events() {
        let registry = MockRegistry::default();

        let state = HashMapState::new();
        let state_id = HashMapState::state_id(&HashMap::new());

        let mut context_manager: ContextManager = ContextManager::new(Box::new(state));

        let handler = CommandTransactionHandler::new();

        let mut static_adapter =
            StaticExecutionAdapter::new_adapter(vec![Box::new(handler)], context_manager.clone())
                .expect("Could not create adapter");

        assert!(static_adapter.start(Box::new(registry.clone())).is_ok());

        // Create and execute an Add Event transaction.
        let txn_pair = make_command_transaction(
            &[
                Command::AddEvent(AddEvent::new(
                    "First event".to_string(),
                    create_bytes_entry(vec![
                        ("key1".to_string(), "value1".as_bytes().to_vec()),
                        ("key2".to_string(), "value2".as_bytes().to_vec()),
                    ]),
                    b"abc".to_vec(),
                )),
                Command::AddEvent(AddEvent::new(
                    "Second event".to_string(),
                    create_bytes_entry(vec![
                        ("key1".to_string(), "value1".as_bytes().to_vec()),
                        ("key2".to_string(), "value2".as_bytes().to_vec()),
                    ]),
                    b"def".to_vec(),
                )),
            ],
            &*new_signer(),
        );
        let txn_id = txn_pair.transaction().header_signature().to_owned();
        let context_id = context_manager.create_context(&[], &state_id);

        let (send, recv) = std::sync::mpsc::channel();
        assert!(static_adapter
            .execute(
                txn_pair,
                context_id.clone(),
                Box::new(move |res| {
                    send.send(res).expect("Unable to send result");
                }),
            )
            .is_ok());
        let result = recv.recv().unwrap();

        assert_eq!(
            ExecutionTaskCompletionNotification::Valid(context_id.clone(), txn_id.clone()),
            result.unwrap()
        );

        let txn_receipt = context_manager
            .get_transaction_receipt(&context_id, &txn_id)
            .unwrap();
        let events = match txn_receipt.transaction_result {
            TransactionResult::Valid { events, .. } => events,
            _ => panic!("transaction is invalid"),
        };
        let first_event = events.first().unwrap();
        assert_eq!(first_event.event_type, "First event".to_string());
        assert_eq!(
            first_event.attributes,
            vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string())
            ]
        );
        assert_eq!(first_event.data, b"abc".to_vec());

        let second_event = events.last().unwrap();
        assert_eq!(second_event.event_type, "Second event".to_string());
        assert_eq!(
            second_event.attributes,
            vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string())
            ]
        );
        assert_eq!(second_event.data, b"def".to_vec());

        assert!(Box::new(static_adapter).stop().is_ok());
    }

    /// Apply the static adapter with several commands to add Transaction Receipt data.
    #[test]
    fn apply_static_adapter_add_receipt_data() {
        let registry = MockRegistry::default();

        let state = HashMapState::new();
        let state_id = HashMapState::state_id(&HashMap::new());

        let mut context_manager: ContextManager = ContextManager::new(Box::new(state));

        let handler = CommandTransactionHandler::new();

        let mut static_adapter =
            StaticExecutionAdapter::new_adapter(vec![Box::new(handler)], context_manager.clone())
                .expect("Could not create adapter");

        assert!(static_adapter.start(Box::new(registry.clone())).is_ok());

        // Create and execute an Add Receipt Data transaction.
        let txn_pair = make_command_transaction(
            &[
                Command::AddReceiptData(AddReceiptData::new(b"abc".to_vec())),
                Command::AddReceiptData(AddReceiptData::new(b"def".to_vec())),
            ],
            &*new_signer(),
        );

        let txn_id = txn_pair.transaction().header_signature().to_owned();
        let context_id = context_manager.create_context(&[], &state_id);

        let (send, recv) = std::sync::mpsc::channel();
        assert!(static_adapter
            .execute(
                txn_pair,
                context_id.clone(),
                Box::new(move |res| {
                    send.send(res).expect("Unable to send result");
                }),
            )
            .is_ok());
        let result = recv.recv().unwrap();

        assert_eq!(
            ExecutionTaskCompletionNotification::Valid(context_id.clone(), txn_id.clone()),
            result.unwrap()
        );

        let txn_receipt = context_manager
            .get_transaction_receipt(&context_id, &txn_id)
            .unwrap();
        let receipt_data = match txn_receipt.transaction_result {
            TransactionResult::Valid { data, .. } => data,
            _ => panic!("transaction is invalid"),
        };
        assert_eq!(receipt_data.first().unwrap(), &b"abc".to_vec());
        assert_eq!(receipt_data.last().unwrap(), &b"def".to_vec());

        assert!(Box::new(static_adapter).stop().is_ok());
    }

    fn new_signer() -> Box<dyn Signer> {
        let context = Secp256k1Context::new();
        let key = context.new_random_private_key();
        context.new_signer(key)
    }

    #[derive(Clone, Default)]
    struct MockRegistry {
        registered: Arc<AtomicBool>,
    }

    impl ExecutionRegistry for MockRegistry {
        fn register_transaction_family(&mut self, _family: TransactionFamily) {
            self.registered.store(true, Ordering::Relaxed);
        }

        fn unregister_transaction_family(&mut self, _family: &TransactionFamily) {
            self.registered.store(false, Ordering::Relaxed);
        }
    }
}

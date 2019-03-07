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
use crate::execution::adapter::{ExecutionAdapter, ExecutionAdapterError};
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
                Ok(_) => on_done(Ok(ExecutionTaskCompletionNotification::Valid(context_id))),
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
    execution_registry: &mut ExecutionRegistry,
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
    fn start(&mut self, execution_registry: Box<dyn ExecutionRegistry>) {
        if let Err(err) = self
            .sender
            .send(StaticAdapterCommand::Start(execution_registry))
        {
            error!("Unable to submit start signal: {}", err);
        }
    }

    fn execute(
        &self,
        transaction_pair: TransactionPair,
        context_id: ContextId,
        on_done: OnDoneCallback,
    ) {
        if let Err(err) = self.sender.send(StaticAdapterCommand::Execute(Box::new((
            transaction_pair,
            context_id,
            on_done,
        )))) {
            error!("Unable to submit transaction for execution: {}", err);
        }
    }

    fn stop(self: Box<Self>) -> bool {
        if let Err(err) = self.sender.send(StaticAdapterCommand::Stop) {
            error!("Unable to signal stop to static execution adapter: {}", err);
            return false;
        }

        match self.join_handle.join() {
            Ok(stopped) => stopped,
            Err(_) => {
                error!("Unable to stop static execution adapter");
                false
            }
        }
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

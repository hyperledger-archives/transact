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

//! Implementation of core scheduler thread.

use crate::context::manager::ContextManagerError;
use crate::context::{ContextId, ContextLifecycle};
use crate::protocol::batch::BatchPair;
use crate::scheduler::BatchExecutionResult;
use crate::scheduler::ExecutionTask;
use crate::scheduler::ExecutionTaskCompletionNotification;
use crate::scheduler::TransactionExecutionResult;

use hex;
use std::error::Error;
use std::sync::mpsc::{Receiver, SendError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use super::shared::Shared;

/// An enum of messages which can be sent to the SchedulerCore via a
/// `Sender<CoreMessage>`.
pub enum CoreMessage {
    /// An indicator to the scheduler that a batch has been added.
    BatchAdded,

    /// An indicator that an execution task has been completed. If the
    /// notification is for a valid transaction, then the relevant data will be
    /// contained in its context; for an invalid transaction, the error
    /// information is within the notification itself.
    ExecutionResult(ExecutionTaskCompletionNotification),

    /// An indicator to the scheduler that the executor is ready to receive an
    /// ExecuteTask message.
    Next,

    /// An indicator to the `SchedulerCore` thread that it should exit its
    /// loop.
    Shutdown,
}

#[derive(Debug)]
enum CoreError {
    ExecutionSendError(Box<SendError<ExecutionTask>>),
    ContextManagerError(Box<ContextManagerError>),
}

impl std::error::Error for CoreError {
    fn description(&self) -> &str {
        match *self {
            CoreError::ExecutionSendError(ref err) => err.description(),
            CoreError::ContextManagerError(ref err) => err.description(),
        }
    }

    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            CoreError::ExecutionSendError(ref err) => Some(err),
            CoreError::ContextManagerError(ref err) => Some(err),
        }
    }
}

impl std::fmt::Display for CoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            CoreError::ExecutionSendError(ref err) => {
                write!(f, "ExecutionSendError: {}", err.description())
            }
            CoreError::ContextManagerError(ref err) => {
                write!(f, "ContextManagerError: {}", err.description())
            }
        }
    }
}

impl From<SendError<ExecutionTask>> for CoreError {
    fn from(error: SendError<ExecutionTask>) -> CoreError {
        CoreError::ExecutionSendError(Box::new(error))
    }
}

impl From<ContextManagerError> for CoreError {
    fn from(error: ContextManagerError) -> CoreError {
        CoreError::ContextManagerError(Box::new(error))
    }
}

pub struct SchedulerCore {
    /// The data shared between this core thread and the thread which owns
    /// `SerialScheduler`.
    shared_lock: Arc<Mutex<Shared>>,

    /// The receiver for all messages sent to the core thread.
    rx: Receiver<CoreMessage>,

    /// The sender to be used to send an ExecutionTask to the iterator after
    /// it requested one with CoreMessage::Next.
    execution_tx: Sender<ExecutionTask>,

    /// Indicates that next() has been called on the SchedulerExecutionInterface
    /// and is waiting for an ExecutionTask to be sent.
    next_ready: bool,

    /// The current batch which is being executed.
    current_batch: Option<BatchPair>,

    /// The interface for context creation and deletion.
    context_lifecycle: Box<ContextLifecycle>,

    /// The state root upon which transactions in this scheduler will be
    /// executed.
    state_id: String,

    /// The context from the previously run transaction.
    previous_context: Option<ContextId>,
}

impl SchedulerCore {
    pub fn new(
        shared_lock: Arc<Mutex<Shared>>,
        rx: Receiver<CoreMessage>,
        execution_tx: Sender<ExecutionTask>,
        context_lifecycle: Box<ContextLifecycle>,
        state_id: String,
    ) -> Self {
        SchedulerCore {
            shared_lock,
            rx,
            execution_tx,
            next_ready: false,
            current_batch: None,
            context_lifecycle,
            state_id,
            previous_context: None,
        }
    }

    fn try_schedule_next(&mut self) -> Result<(), CoreError> {
        if !self.next_ready {
            return Ok(());
        }

        let mut shared = self
            .shared_lock
            .lock()
            .expect("scheduler shared lock is poisoned");

        if let Some(pair) = shared.pop_unscheduled_batch() {
            if pair.batch().transactions().len() != 1 {
                unimplemented!("can't handle more than one transaction per batch");
            }

            let transaction = match pair.batch().transactions()[0].clone().into_pair() {
                Ok(transaction) => transaction,
                Err(err) => {
                    // An error can occur here if the header of the
                    // transaction cannot be deserialized. If this occurs,
                    // we panic as handling this is not currently implemented.
                    unimplemented!("cannot handle ill-formed transaction: {}", err)
                }
            };

            let context_id = match self.previous_context {
                Some(previous_context_id) => self
                    .context_lifecycle
                    .create_context(&[previous_context_id], &self.state_id),
                None => self.context_lifecycle.create_context(&[], &self.state_id),
            };

            self.current_batch = Some(pair);
            self.execution_tx
                .send(ExecutionTask::new(transaction, context_id))?
        }

        Ok(())
    }

    fn run(&mut self) -> Result<(), CoreError> {
        loop {
            match self.rx.recv() {
                Ok(CoreMessage::BatchAdded) => {
                    self.try_schedule_next()?;
                }
                Ok(CoreMessage::ExecutionResult(task_notification)) => {
                    let batch = match self.current_batch.take() {
                        Some(batch) => batch,
                        None => {
                            panic!("received execution result but no current batch is executing")
                        }
                    };

                    let results = match task_notification {
                        ExecutionTaskCompletionNotification::Valid(context_id) => {
                            self.previous_context = Some(context_id);
                            vec![TransactionExecutionResult::Valid(
                                self.context_lifecycle.get_transaction_receipt(
                                    &context_id,
                                    &hex::encode(&batch.header().transaction_ids()[0]),
                                )?,
                            )]
                        }
                        ExecutionTaskCompletionNotification::Invalid(_context_id, result) => {
                            vec![TransactionExecutionResult::Invalid(result)]
                        }
                    };

                    let batch_result = BatchExecutionResult { batch, results };

                    let shared = self
                        .shared_lock
                        .lock()
                        .expect("scheduler shared lock is poisoned");
                    match shared.result_callback() {
                        Some(callback) => {
                            callback(Some(batch_result));
                        }
                        None => {
                            warn!(
                                "dropped batch execution result: {}",
                                batch_result.batch.batch().header_signature()
                            );
                        }
                    }
                }
                Ok(CoreMessage::Next) => {
                    self.next_ready = true;
                    self.try_schedule_next()?;
                }
                Ok(CoreMessage::Shutdown) => {
                    break;
                }
                Err(err) => {
                    // This is expected if the other side shuts down
                    // before this end. However, it would be more
                    // elegant to gracefully handle it by sending a
                    // close message across.
                    error!("Thread-SerialScheduler recv error: {}", err);
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn start(mut self) -> std::thread::JoinHandle<()> {
        thread::Builder::new()
            .name(String::from("Thread-SerialScheduler"))
            .spawn(move || {
                if let Err(err) = self.run() {
                    error!("scheduler thread ended due to error: {}", err);
                }
            })
            .expect("could not build a thread for the scheduler")
    }
}

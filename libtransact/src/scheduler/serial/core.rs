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
use crate::protocol::transaction::Transaction;
use crate::scheduler::BatchExecutionResult;
use crate::scheduler::ExecutionTask;
use crate::scheduler::ExecutionTaskCompletionNotification;
use crate::scheduler::InvalidTransactionResult;
use crate::scheduler::SchedulerError;
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
    ExecutionSend(Box<SendError<ExecutionTask>>),
    ContextManager(Box<ContextManagerError>),
    Internal(String),
}

impl std::error::Error for CoreError {
    fn description(&self) -> &str {
        match *self {
            CoreError::ExecutionSend(ref err) => err.description(),
            CoreError::ContextManager(ref err) => err.description(),
            CoreError::Internal(ref err) => err,
        }
    }

    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            CoreError::ExecutionSend(ref err) => Some(err),
            CoreError::ContextManager(ref err) => Some(err),
            CoreError::Internal(_) => None,
        }
    }
}

impl std::fmt::Display for CoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            CoreError::ExecutionSend(ref err) => write!(
                f,
                "failed to send transaction to executor: {}",
                err.description()
            ),
            CoreError::ContextManager(ref err) => {
                write!(f, "call to ContextManager failed: {}", err.description())
            }
            CoreError::Internal(ref err) => write!(f, "internal error occurred: {}", err),
        }
    }
}

impl From<SendError<ExecutionTask>> for CoreError {
    fn from(error: SendError<ExecutionTask>) -> CoreError {
        CoreError::ExecutionSend(Box::new(error))
    }
}

impl From<ContextManagerError> for CoreError {
    fn from(error: ContextManagerError) -> CoreError {
        CoreError::ContextManager(Box::new(error))
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

    /// The ID of the current transaction which is being executed (from the current batch).
    current_txn: Option<String>,

    /// A queue of the current batch's transactions that have not been exeucted yet.
    txn_queue: Vec<Transaction>,

    /// The results of the current batch's transactions that have already been executed.
    txn_results: Vec<TransactionExecutionResult>,

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
            current_txn: None,
            txn_queue: vec![],
            txn_results: vec![],
            context_lifecycle,
            state_id,
            previous_context: None,
        }
    }

    fn try_schedule_next(&mut self) -> Result<(), CoreError> {
        if !self.next_ready {
            return Ok(());
        }

        if self.current_txn.is_some() {
            return Ok(());
        }

        if self.current_batch.is_none() {
            let mut shared = self
                .shared_lock
                .lock()
                .expect("scheduler shared lock is poisoned");

            if let Some(unscheduled_batch) = shared.pop_unscheduled_batch() {
                self.txn_queue = unscheduled_batch.batch().transactions().to_vec();
                self.current_batch = Some(unscheduled_batch);
            } else {
                return Ok(());
            }
        }

        let transaction = self.txn_queue.pop().ok_or_else(|| {
            CoreError::Internal(format!(
                "no transactions left in current batch ({})",
                self.current_batch
                    .as_ref()
                    .map(|pair| pair.batch().header_signature())
                    .unwrap_or("")
            ))
        })?;
        let transaction_id = transaction.header_signature().into();
        let transaction_pair = match transaction.into_pair() {
            Ok(pair) => pair,
            Err(err) => {
                self.invalidate_current_batch(InvalidTransactionResult {
                    transaction_id,
                    error_message: format!("ill-formed transaction: {}", err),
                    error_data: vec![],
                })?;
                self.send_batch_result()?;
                return Ok(());
            }
        };

        let context_id = match self.previous_context {
            Some(previous_context_id) => self
                .context_lifecycle
                .create_context(&[previous_context_id], &self.state_id),
            None => self.context_lifecycle.create_context(&[], &self.state_id),
        };

        self.current_txn = Some(transaction_pair.transaction().header_signature().into());
        self.execution_tx
            .send(ExecutionTask::new(transaction_pair, context_id))?;
        self.next_ready = false;

        Ok(())
    }

    fn invalidate_current_batch(
        &mut self,
        invalid_result: InvalidTransactionResult,
    ) -> Result<(), CoreError> {
        let current_batch_id = self
            .current_batch
            .as_ref()
            .ok_or_else(|| {
                CoreError::Internal(
                    "attemping to invalidate current batch but no current batch exists".into(),
                )
            })?
            .batch()
            .header_signature();

        // Invalidate all previously executed transactions in the batch
        self.txn_results = self
            .txn_results
            .iter()
            .map(|result| match result {
                TransactionExecutionResult::Valid(receipt) => {
                    TransactionExecutionResult::Invalid(InvalidTransactionResult {
                        transaction_id: receipt.transaction_id.clone(),
                        error_message: format!(
                            "containing batch ({}) is invalid",
                            current_batch_id,
                        ),
                        error_data: vec![],
                    })
                }
                TransactionExecutionResult::Invalid(_) => {
                    // When an invalid transaction is encountered, the scheduler should fail-fast
                    // and invalidate the whole batch immediately; if an invalid result is in the
                    // previously executed transaction results, this did not happen.
                    panic!("should not already have an invalid result");
                }
            })
            .collect();

        self.txn_results
            .push(TransactionExecutionResult::Invalid(invalid_result));

        // Invalidate all unexecuted transactions in the batch
        self.txn_results.append(
            &mut self
                .txn_queue
                .drain(..)
                .map(|txn| {
                    TransactionExecutionResult::Invalid(InvalidTransactionResult {
                        transaction_id: txn.header_signature().into(),
                        error_message: format!(
                            "containing batch ({}) is invalid",
                            current_batch_id
                        ),
                        error_data: vec![],
                    })
                })
                .collect(),
        );

        Ok(())
    }

    fn send_batch_result(&mut self) -> Result<(), CoreError> {
        let batch = self.current_batch.take().ok_or_else(|| {
            CoreError::Internal(
                "attempting to send batch result but no current batch is executing".into(),
            )
        })?;

        let mut results = vec![];
        std::mem::swap(&mut results, &mut self.txn_results);

        let batch_result = BatchExecutionResult { batch, results };

        let shared = self
            .shared_lock
            .lock()
            .expect("scheduler shared lock is poisoned");
        shared.result_callback()(Some(batch_result));

        Ok(())
    }

    fn send_scheduler_error(&mut self, error: SchedulerError) {
        let shared = self
            .shared_lock
            .lock()
            .expect("scheduler shared lock is poisoned");
        shared.error_callback()(error);
    }

    fn run(&mut self) -> Result<(), CoreError> {
        loop {
            match self.rx.recv() {
                Ok(CoreMessage::BatchAdded) => {
                    self.try_schedule_next()?;
                }
                Ok(CoreMessage::ExecutionResult(task_notification)) => {
                    let current_txn_id = self.current_txn.as_ref().ok_or_else(|| {
                        CoreError::Internal(
                            "received execution result but no current transaction is executing"
                                .into(),
                        )
                    })?;
                    match task_notification {
                        ExecutionTaskCompletionNotification::Valid(context_id, transaction_id) => {
                            if &transaction_id != current_txn_id {
                                self.send_scheduler_error(SchedulerError::UnexpectedNotification(
                                    transaction_id,
                                ));
                                continue;
                            }
                            self.current_txn = None;
                            self.previous_context = Some(context_id);
                            self.txn_results.push(TransactionExecutionResult::Valid(
                                self.context_lifecycle.get_transaction_receipt(
                                    &context_id,
                                    &hex::encode(transaction_id),
                                )?,
                            ));
                        }
                        ExecutionTaskCompletionNotification::Invalid(_context_id, result) => {
                            if &result.transaction_id != current_txn_id {
                                self.send_scheduler_error(SchedulerError::UnexpectedNotification(
                                    result.transaction_id,
                                ));
                                continue;
                            }
                            self.current_txn = None;
                            self.invalidate_current_batch(result)?;
                        }
                    };

                    if self.txn_queue.is_empty() {
                        self.send_batch_result()?;
                    }

                    self.try_schedule_next()?;
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

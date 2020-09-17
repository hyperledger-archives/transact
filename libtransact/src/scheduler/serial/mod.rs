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

//! A `Scheduler` which schedules transaction for execution one at time.

mod core;
mod execution;
mod shared;

use crate::context::ContextLifecycle;
use crate::protocol::batch::BatchPair;

use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

use super::{
    BatchExecutionResult, ExecutionTask, ExecutionTaskCompletionNotifier, Scheduler,
    SchedulerError, SchedulerFactory,
};

// If the shared lock is poisoned, report an internal error since the scheduler cannot recover.
impl From<std::sync::PoisonError<std::sync::MutexGuard<'_, shared::Shared>>> for SchedulerError {
    fn from(
        error: std::sync::PoisonError<std::sync::MutexGuard<'_, shared::Shared>>,
    ) -> SchedulerError {
        SchedulerError::Internal(format!("scheduler shared lock is poisoned: {}", error))
    }
}

// If the core `Receiver` disconnects, report an internal error since the scheduler can't operate
// without the core thread.
impl From<std::sync::mpsc::SendError<core::CoreMessage>> for SchedulerError {
    fn from(error: std::sync::mpsc::SendError<core::CoreMessage>) -> SchedulerError {
        SchedulerError::Internal(format!("scheduler's core thread disconnected: {}", error))
    }
}

/// A `Scheduler` implementation which schedules transactions for execution
/// one at a time.
pub struct SerialScheduler {
    shared_lock: Arc<Mutex<shared::Shared>>,
    core_tx: Sender<core::CoreMessage>,
    task_iterator: Option<Box<dyn Iterator<Item = ExecutionTask> + Send>>,
}

impl SerialScheduler {
    /// Returns a newly created `SerialScheduler`.
    pub fn new(
        context_lifecycle: Box<dyn ContextLifecycle>,
        state_id: String,
    ) -> Result<SerialScheduler, SchedulerError> {
        let (execution_tx, execution_rx) = mpsc::channel();
        let (core_tx, core_rx) = mpsc::channel();

        let shared_lock = Arc::new(Mutex::new(shared::Shared::new()));

        // Start the thread to accept and process CoreMessage messages
        core::SchedulerCore::new(
            shared_lock.clone(),
            core_rx,
            execution_tx,
            context_lifecycle,
            state_id,
        )
        .start()?;

        Ok(SerialScheduler {
            shared_lock,
            core_tx: core_tx.clone(),
            task_iterator: Some(Box::new(execution::SerialExecutionTaskIterator::new(
                core_tx,
                execution_rx,
            ))),
        })
    }
}

impl Scheduler for SerialScheduler {
    fn set_result_callback(
        &mut self,
        callback: Box<dyn Fn(Option<BatchExecutionResult>) + Send>,
    ) -> Result<(), SchedulerError> {
        self.shared_lock.lock()?.set_result_callback(callback);
        Ok(())
    }

    fn set_error_callback(
        &mut self,
        callback: Box<dyn Fn(SchedulerError) + Send>,
    ) -> Result<(), SchedulerError> {
        self.shared_lock.lock()?.set_error_callback(callback);
        Ok(())
    }

    fn add_batch(&mut self, batch: BatchPair) -> Result<(), SchedulerError> {
        let mut shared = self.shared_lock.lock()?;

        if shared.finalized() {
            return Err(SchedulerError::SchedulerFinalized);
        }

        if shared.batch_already_queued(&batch) {
            return Err(SchedulerError::DuplicateBatch(
                batch.batch().header_signature().into(),
            ));
        }

        shared.add_unscheduled_batch(batch);

        // Notify the core that a batch has been added. Note that the batch is
        // not sent across the channel because the batch has already been added
        // to the unscheduled queue above, where we hold a lock; adding a batch
        // must be exclusive with finalize.
        self.core_tx.send(core::CoreMessage::BatchAdded)?;

        Ok(())
    }

    fn cancel(&mut self) -> Result<Vec<BatchPair>, SchedulerError> {
        let mut unscheduled_batches = self.shared_lock.lock()?.drain_unscheduled_batches();

        // Notify the core thread to cancel and wait for it to return an aborted batch. If the
        // scheduler was already finalized, it may have already shutdown, which could cause message
        // sending/receiving to fail. This is normal behavior, so it's ignored. If the scheduler is
        // not finalized, something went wrong.
        let (sender, receiver) = mpsc::channel();
        if self
            .core_tx
            .send(core::CoreMessage::Cancelled(sender))
            .is_err()
            && !self.shared_lock.lock()?.finalized()
        {
            return Err(SchedulerError::Internal(
                "scheduler's core thread disconnected before finalization".into(),
            ));
        }
        let aborted_batch = match receiver.recv() {
            Ok(batch) => batch,
            Err(_) => {
                if self.shared_lock.lock()?.finalized() {
                    None
                } else {
                    return Err(SchedulerError::Internal(
                        "scheduler's core thread did not return aborted batch".into(),
                    ));
                }
            }
        };

        // If there was an aborted batch, add it to the front of the list of unscheduled batches
        if let Some(batch) = aborted_batch {
            let mut new_list = vec![batch];
            new_list.append(&mut unscheduled_batches);
            unscheduled_batches = new_list;
        }

        Ok(unscheduled_batches)
    }

    fn finalize(&mut self) -> Result<(), SchedulerError> {
        self.shared_lock.lock()?.set_finalized(true);
        self.core_tx.send(core::CoreMessage::Finalized)?;
        Ok(())
    }

    fn take_task_iterator(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = ExecutionTask> + Send>, SchedulerError> {
        self.task_iterator
            .take()
            .ok_or(SchedulerError::NoTaskIterator)
    }

    fn new_notifier(&mut self) -> Result<Box<dyn ExecutionTaskCompletionNotifier>, SchedulerError> {
        Ok(Box::new(
            execution::SerialExecutionTaskCompletionNotifier::new(self.core_tx.clone()),
        ))
    }
}

/// Factory for creating `SerialScheduler`s
pub struct SerialSchedulerFactory {
    context_lifecycle: Box<dyn ContextLifecycle>,
}

impl SerialSchedulerFactory {
    /// Creates a new `SerialSchedulerFactory` that uses the given `context_lifecycle`
    pub fn new(context_lifecycle: Box<dyn ContextLifecycle>) -> Self {
        Self { context_lifecycle }
    }
}

impl SchedulerFactory for SerialSchedulerFactory {
    fn create_scheduler(&self, state_id: String) -> Result<Box<dyn Scheduler>, SchedulerError> {
        SerialScheduler::new(self.context_lifecycle.clone(), state_id)
            .map(|scheduler| Box::new(scheduler) as Box<dyn Scheduler>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::tests::*;
    use crate::scheduler::ExecutionTaskCompletionNotification;

    // General Scheduler tests

    /// In addition to the basic functionality verified by `test_scheduler_add_batch`, this test
    /// verifies that the SerialScheduler adds the batch to its unscheduled batches queue.
    #[test]
    fn test_serial_scheduler_add_batch() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let mut scheduler =
            SerialScheduler::new(context_lifecycle, state_id).expect("Failed to create scheduler");

        let batch = test_scheduler_add_batch(&mut scheduler);

        assert!(scheduler
            .shared_lock
            .lock()
            .expect("shared lock is poisoned")
            .batch_already_queued(&batch));

        scheduler.cancel().expect("Failed to cancel");
        scheduler.finalize().expect("Failed to finalize");
    }

    /// In addition to the basic functionality verified by `test_scheduler_cancel`, this test
    /// verifies that the SerialScheduler drains all batches from its unscheduled batches queue.
    #[test]
    fn test_serial_scheduler_cancel() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let mut scheduler =
            SerialScheduler::new(context_lifecycle, state_id).expect("Failed to create scheduler");

        test_scheduler_cancel(&mut scheduler);

        assert!(scheduler
            .shared_lock
            .lock()
            .expect("shared lock is poisoned")
            .unscheduled_batches_is_empty());

        scheduler.cancel().expect("Failed to cancel");
        scheduler.finalize().expect("Failed to finalize");
    }

    /// In addition to the basic functionality verified by `test_scheduler_finalize`, this test
    /// verifies that the SerialScheduler properly updates its internal state to finalized.
    #[test]
    fn test_serial_scheduler_finalize() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let mut scheduler =
            SerialScheduler::new(context_lifecycle, state_id).expect("Failed to create scheduler");

        test_scheduler_finalize(&mut scheduler);

        assert!(scheduler
            .shared_lock
            .lock()
            .expect("shared lock is poisoned")
            .finalized());

        // Scheduler was finalized with no batches, so it has already shutdown
    }

    /// Tests that the serial scheduler can process a batch with a single transaction.
    #[test]
    pub fn test_serial_scheduler_flow_with_one_transaction() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let mut scheduler =
            SerialScheduler::new(context_lifecycle, state_id).expect("Failed to create scheduler");
        test_scheduler_flow_with_one_transaction(&mut scheduler);

        scheduler.cancel().expect("Failed to cancel");
        scheduler.finalize().expect("Failed to finalize");
    }

    /// Tests that a finalized serial scheduler can process two batches with a single transaction
    /// and return None when finished
    #[test]
    pub fn test_serial_scheduler_flow_with_two_batches() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let mut scheduler =
            SerialScheduler::new(context_lifecycle, state_id).expect("Failed to create scheduler");
        test_scheduler_flow_with_two_batches(&mut scheduler);

        // Scheduler was finalized and all batches/transactions have "executed", so it has already
        // shutdown
    }

    /// Tests that the serial scheduler can process a batch with multiple transactions.
    #[test]
    pub fn test_serial_scheduler_flow_with_multiple_transactions() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let mut scheduler =
            SerialScheduler::new(context_lifecycle, state_id).expect("Failed to create scheduler");
        test_scheduler_flow_with_multiple_transactions(&mut scheduler);

        scheduler.cancel().expect("Failed to cancel");
        scheduler.finalize().expect("Failed to finalize");
    }

    /// Tests that the serial scheduler invalidates the whole batch when one of its transactions is
    /// invalid.
    #[test]
    pub fn test_serial_scheduler_invalid_transaction_invalidates_batch() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let mut scheduler =
            SerialScheduler::new(context_lifecycle, state_id).expect("Failed to create scheduler");
        test_scheduler_invalid_transaction_invalidates_batch(&mut scheduler);

        scheduler.cancel().expect("Failed to cancel");
        scheduler.finalize().expect("Failed to finalize");
    }

    // SerialScheduler-specific tests

    /// Verifies that the serial scheduler can be cancelled immediately after finalization when
    /// there are no unscheduled batches. This ensures that the scheduler is resillient to the
    /// aborted batch sender being dropped in the case where the internal thread has shutdown
    /// normally.
    #[test]
    fn finalize_then_cancel() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let mut scheduler =
            SerialScheduler::new(context_lifecycle, state_id).expect("Failed to create scheduler");

        scheduler.finalize().expect("Failed to finalize");
        scheduler.cancel().expect("Failed to cancel");
    }

    /// Verifies that the serial scheduler can be cancelled after some time has passed since
    /// finalization when there are no unscheduled batches. This ensures that the scheduler is
    /// resillient to the internal receiver being dropped in the case where the internal thread has
    /// shutdown normally.
    #[test]
    fn finalize_then_cancel_after_wait() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let mut scheduler =
            SerialScheduler::new(context_lifecycle, state_id).expect("Failed to create scheduler");

        scheduler.finalize().expect("Failed to finalize");
        std::thread::sleep(std::time::Duration::from_secs(1));
        scheduler.cancel().expect("Failed to cancel");
    }

    /// This test verifies that the SerialScheduler executes transactions strictly in order, and
    /// does not return the next execution task until the previous one is completed.
    #[test]
    fn test_serial_scheduler_ordering() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let mut scheduler =
            SerialScheduler::new(context_lifecycle, state_id).expect("Failed to create scheduler");

        let transactions = mock_transactions(10);
        let batch = mock_batch(transactions.clone());
        scheduler
            .add_batch(batch.clone())
            .expect("Failed to add batch");
        scheduler.finalize().expect("Failed to finalize");

        let mut task_iterator = scheduler
            .take_task_iterator()
            .expect("Failed to get task iterator");
        let notifier = scheduler
            .new_notifier()
            .expect("Failed to get new notifier");

        let mut transaction_ids = transactions.into_iter();

        // Get the first task, but take some time to execute it in a background thread; meanwhile,
        // wait for the next task. A channel is used to verify that the next task isn't returned
        // until the result for the first is received by the scheduler.
        let (tx, rx) = mpsc::channel();
        let first_task_notifier = notifier.clone();
        let first_task_txn_id = task_iterator
            .next()
            .expect("Failed to get 1st task")
            .pair()
            .transaction()
            .header_signature()
            .to_string();
        assert_eq!(
            transaction_ids
                .next()
                .expect("Failed to get next transaction")
                .header_signature(),
            &first_task_txn_id,
        );
        std::thread::Builder::new()
            .name("Thread-test_serial_scheduler_ordering".into())
            .spawn(move || {
                std::thread::sleep(std::time::Duration::from_secs(1));
                first_task_notifier.notify(ExecutionTaskCompletionNotification::Valid(
                    mock_context_id(),
                    first_task_txn_id,
                ));
                // This send must occur before the next task is returned.
                tx.send(()).expect("Failed to send");
            })
            .expect("Failed to spawn thread");

        let second_task_txn_id = task_iterator
            .next()
            .expect("Failed to get 2nd task")
            .pair()
            .transaction()
            .header_signature()
            .to_string();
        assert_eq!(
            transaction_ids
                .next()
                .expect("Failed to get next transaction")
                .header_signature(),
            &second_task_txn_id,
        );
        // If the signal was never sent, this task is being returned before the
        // previous result was sent.
        rx.try_recv()
            .expect("Returned next task before previous completed");
        notifier.notify(ExecutionTaskCompletionNotification::Valid(
            mock_context_id(),
            second_task_txn_id,
        ));

        // Process the rest of the execution tasks and verify the order
        loop {
            match task_iterator.next() {
                Some(task) => {
                    let txn_id = task.pair().transaction().header_signature().to_string();
                    assert_eq!(
                        transaction_ids
                            .next()
                            .expect("Failed to get next transaction")
                            .header_signature(),
                        &txn_id,
                    );
                    notifier.notify(ExecutionTaskCompletionNotification::Valid(
                        mock_context_id(),
                        txn_id,
                    ));
                }
                None => break,
            }
        }

        // Scheduler was finalized and all batches/transactions have "executed", so it has already
        // shutdown
    }
}

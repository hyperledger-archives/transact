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
use crate::scheduler::BatchExecutionResult;
use crate::scheduler::ExecutionTask;
use crate::scheduler::ExecutionTaskCompletionNotifier;
use crate::scheduler::Scheduler;

use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

/// A `Scheduler` implementation which schedules transactions for execution
/// one at a time.
pub struct SerialScheduler {
    shared_lock: Arc<Mutex<shared::Shared>>,
    core_handle: Option<std::thread::JoinHandle<()>>,
    core_tx: Sender<core::CoreMessage>,
    task_iterator: Option<Box<Iterator<Item = ExecutionTask> + Send>>,
}

impl SerialScheduler {
    /// Returns a newly created `SerialScheduler`.
    pub fn new(context_lifecycle: Box<ContextLifecycle>, state_id: String) -> SerialScheduler {
        let (execution_tx, execution_rx) = mpsc::channel();
        let (core_tx, core_rx) = mpsc::channel();

        let shared_lock = Arc::new(Mutex::new(shared::Shared::new()));

        // Start the thread to accept and process CoreMessage messages
        let core_handle = core::SchedulerCore::new(
            shared_lock.clone(),
            core_rx,
            execution_tx,
            context_lifecycle,
            state_id,
        )
        .start();

        SerialScheduler {
            shared_lock,
            core_handle: Some(core_handle),
            core_tx: core_tx.clone(),
            task_iterator: Some(Box::new(execution::SerialExecutionTaskIterator::new(
                core_tx,
                execution_rx,
            ))),
        }
    }
}

impl Scheduler for SerialScheduler {
    fn set_result_callback(&mut self, callback: Box<Fn(Option<BatchExecutionResult>) + Send>) {
        let mut shared = self
            .shared_lock
            .lock()
            .expect("scheduler shared lock is poisoned");
        shared.set_result_callback(callback);
    }

    fn add_batch(&mut self, batch: BatchPair) {
        let mut shared = self
            .shared_lock
            .lock()
            .expect("scheduler shared lock is poisoned");

        if shared.finalized() {
            panic!("add_batch called after scheduler finalized");
        }

        shared.add_unscheduled_batch(batch);

        // Notify the core that a batch has been added. Note that the batch is
        // not sent across the channel because the batch has already been added
        // to the unscheduled queue above, where we hold a lock; adding a batch
        // must be exclusive with finalize.
        self.core_tx
            .send(core::CoreMessage::BatchAdded)
            .expect("failed to send to the scheduler thread");
    }

    fn cancel(&mut self) -> Vec<BatchPair> {
        let mut shared = self
            .shared_lock
            .lock()
            .expect("scheduler shared lock is poisoned");
        shared.drain_unscheduled_batches()
    }

    fn finalize(&mut self) {
        let mut shared = self
            .shared_lock
            .lock()
            .expect("scheduler shared lock is poisoned");
        shared.set_finalized(true);
    }

    fn take_task_iterator(&mut self) -> Box<dyn Iterator<Item = ExecutionTask> + Send> {
        match self.task_iterator.take() {
            Some(taken) => taken,
            None => {
                panic!("task iterator can not be taken more than once");
            }
        }
    }

    fn new_notifier(&mut self) -> Box<dyn ExecutionTaskCompletionNotifier> {
        Box::new(execution::SerialExecutionTaskCompletionNotifier::new(
            self.core_tx.clone(),
        ))
    }
}

impl Drop for SerialScheduler {
    fn drop(&mut self) {
        match self.core_tx.send(core::CoreMessage::Shutdown) {
            Ok(_) => {
                if let Some(join_handle) = self.core_handle.take() {
                    join_handle.join().expect("failed to join scheduler thread");
                }
            }
            Err(err) => {
                error!(
                    "failed to send to send to scheduler thread during drop: {}",
                    err
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::manager::ContextManagerError;
    use crate::context::{ContextId, ContextLifecycle};
    use crate::protocol::receipt::TransactionReceipt;
    use crate::scheduler::tests::*;

    struct MockContextLifecycle {}

    impl MockContextLifecycle {
        fn new() -> Self {
            MockContextLifecycle {}
        }
    }

    impl ContextLifecycle for MockContextLifecycle {
        fn create_context(
            &mut self,
            _dependent_contexts: &[ContextId],
            _state_id: &str,
        ) -> ContextId {
            [
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x01,
            ]
        }

        fn get_transaction_receipt(
            &self,
            _context_id: &ContextId,
            _transaction_id: &str,
        ) -> Result<TransactionReceipt, ContextManagerError> {
            unimplemented!()
        }

        fn drop_context(&mut self, _context_id: ContextId) {}
    }

    /// This test will hang if join() fails within the scheduler.
    #[test]
    fn test_scheduler_thread_cleanup() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let _ = SerialScheduler::new(context_lifecycle, state_id);
    }

    #[test]
    fn test_serial_scheduler() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let mut scheduler = SerialScheduler::new(context_lifecycle, state_id);
        test_scheduler(&mut scheduler);
    }

    #[test]
    fn test_serial_scheduler_cancel() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let mut scheduler = SerialScheduler::new(context_lifecycle, state_id);
        test_scheduler_cancel(&mut scheduler);
    }

    #[test]
    pub fn test_serial_scheduler_flow_with_one_transaction() {
        let state_id = String::from("state0");
        let context_lifecycle = Box::new(MockContextLifecycle::new());
        let mut scheduler = SerialScheduler::new(context_lifecycle, state_id);
        test_scheduler_flow_with_one_transaction(&mut scheduler);
    }
}

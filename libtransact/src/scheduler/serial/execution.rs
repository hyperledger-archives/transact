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

//! Implementation of the components used for interfacing with the component
//! reponsible for the execution of transactions (usually the Executor).

use crate::scheduler::ExecutionTask;
use crate::scheduler::ExecutionTaskCompletionNotification;
use crate::scheduler::ExecutionTaskCompletionNotifier;

use std::sync::mpsc::{Receiver, Sender};

use super::core::CoreMessage;

pub struct SerialExecutionTaskIterator {
    tx: Sender<CoreMessage>,
    rx: Receiver<Option<ExecutionTask>>,
    is_complete: bool,
}

impl SerialExecutionTaskIterator {
    pub fn new(tx: Sender<CoreMessage>, rx: Receiver<Option<ExecutionTask>>) -> Self {
        SerialExecutionTaskIterator {
            tx,
            rx,
            is_complete: false,
        }
    }
}

impl Iterator for SerialExecutionTaskIterator {
    type Item = ExecutionTask;

    /// Return the next execution task which is available to be executed.
    fn next(&mut self) -> Option<ExecutionTask> {
        if self.is_complete {
            debug!(
                "Execution task iterator already returned `None`; `next` should not be called again"
            );
            return None;
        }

        // Send a message to the scheduler requesting the next task be sent.
        match self.tx.send(CoreMessage::Next) {
            Ok(_) => match self.rx.recv() {
                Ok(task) => {
                    self.is_complete = task.is_none();
                    task
                }
                Err(_) => {
                    error!(
                        "Failed to receive next execution task; scheduler shutdown unexpectedly"
                    );
                    self.is_complete = true;
                    None
                }
            },
            Err(_) => {
                trace!("Scheduler core message receiver dropped; checking if it shutdown properly");
                match self.rx.recv() {
                    Ok(Some(_)) => error!(
                        "Scheduler sent unexpected execution task before shutting down unexpectedly"
                    ),
                    // If `None` was sent, the scheduler had no more tasks so a shutdown is expected
                    Ok(None) => {}
                    _ => error!(
                        "Failed to request next execution task; scheduler shutdown unexpectedly"
                    ),
                }
                self.is_complete = true;
                None
            }
        }
    }
}

#[derive(Clone)]
pub struct SerialExecutionTaskCompletionNotifier {
    tx: Sender<CoreMessage>,
}

impl SerialExecutionTaskCompletionNotifier {
    pub fn new(tx: Sender<CoreMessage>) -> Self {
        SerialExecutionTaskCompletionNotifier { tx }
    }
}

impl ExecutionTaskCompletionNotifier for SerialExecutionTaskCompletionNotifier {
    fn notify(&self, notification: ExecutionTaskCompletionNotification) {
        self.tx
            .send(CoreMessage::ExecutionResult(notification))
            .unwrap_or_else(|err| error!("failed to send notification to core: {}", err));
    }

    fn clone_box(&self) -> Box<dyn ExecutionTaskCompletionNotifier> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{mpsc::channel, Arc, Mutex};

    use cylinder::{secp256k1::Secp256k1Context, Context, Signer};
    use log::{set_boxed_logger, set_max_level, Level, LevelFilter, Log, Metadata, Record};
    use rusty_fork::rusty_fork_test;

    use crate::context::ContextId;
    use crate::protocol::transaction::{HashMethod, TransactionBuilder};

    // This macro runs each of the tests in a separate process, which is necessary because the
    // logger is per-process. If we ran the tests normally (which is accomplished with threads), the
    // loggers would interfere with each other.
    rusty_fork_test! {
        /// Verifies that the task iterator works properly under normal conditions
        ///
        /// 1. Initialize the logger
        /// 2. Initialize the channels used by the iterator
        /// 3. Create the iterator and attempt to get two tasks in a new thread
        /// 4. Simulate receiving the requests, send one task, and send a `None` to signal no more tasks
        /// 5. Join the thread and verify the tasks were returned by the iterator
        /// 6. Verify that no errors were logged
        #[test]
        fn task_iterator_successful() {
            let logger = init_logger();

            let (core_tx, core_rx) = channel();
            let (task_tx, task_rx) = channel();

            let join_handle = std::thread::spawn(move || {
                let mut iter = SerialExecutionTaskIterator::new(core_tx, task_rx);
                (iter.next(), iter.next())
            });

            recv_next(&core_rx);
            task_tx
                .send(Some(mock_execution_task()))
                .expect("Failed to send execution task");
            recv_next(&core_rx);
            task_tx.send(None).expect("Failed to send `None`");

            let (task1, task2) = join_handle.join().expect("Iterator thread panicked");
            assert!(task1.is_some());
            assert!(task2.is_none());

            assert!(!logger.has_err());
        }

        /// Verifies that the task iterator makes a debug log and returns `None` if `next` is called
        /// after a `None` result has already been returned.
        ///
        /// 1. Initialize the logger
        /// 2. Initialize the channels used by the iterator
        /// 3. Create the iterator and attempt to get three tasks in a new thread
        /// 4. Simulate receiving two of the requests, send one task, and send a `None` to signal no
        ///    more tasks
        /// 5. Verify that a third request was not sent, because the iterator should have detected
        ///    that `next` was called after a `None` result was received
        /// 6. Join the thread and verify the correct tasks were returned by the iterator
        /// 7. Verify that a debug log was made
        #[test]
        fn task_iterator_multiple_nones() {
            let logger = init_logger();

            let (core_tx, core_rx) = channel();
            let (task_tx, task_rx) = channel();

            let join_handle = std::thread::spawn(move || {
                let mut iter = SerialExecutionTaskIterator::new(core_tx, task_rx);
                (iter.next(), iter.next(), iter.next())
            });

            recv_next(&core_rx);
            task_tx
                .send(Some(mock_execution_task()))
                .expect("Failed to send execution task");
            recv_next(&core_rx);
            task_tx.send(None).expect("Failed to send `None`");

            core_rx.try_recv().expect_err("Got an unexpected task request");

            let (task1, task2, task3) = join_handle.join().expect("Iterator thread panicked");
            assert!(task1.is_some());
            assert!(task2.is_none());
            assert!(task3.is_none());

            assert!(logger.has_debug());
        }

        /// Verifies that the task iterator returns `None` without logging an error if the next task
        /// request fails to send but the scheduler sent a `None` result on shutdown.
        ///
        /// 1. Initialize the logger
        /// 2. Initialize the channels used by the iterator but drop the core receiver immediately
        /// 3. Create the iterator and attempt to get a task in a new thread
        /// 4. Send a `None` to the task receiver to simulate proper scheduler thread shutdown
        /// 5. Join the thread and verify the `None` was returned by the iterator
        /// 7. Verify that no error was logged
        #[test]
        fn task_iterator_send_failed_but_shutdown_properly() {
            let logger = init_logger();

            let (core_tx, _) = channel();
            let (task_tx, task_rx) = channel();

            let join_handle = std::thread::spawn(move || {
                SerialExecutionTaskIterator::new(core_tx, task_rx).next()
            });

            task_tx.send(None).expect("Failed to send `None`");

            let task = join_handle.join().expect("Iterator thread panicked");
            assert!(task.is_none());

            assert!(!logger.has_err());
        }

        /// Verifies that the task iterator returns `None` and logs an error if the next task
        /// request fails but an execution task is still received.
        ///
        /// 1. Initialize the logger
        /// 2. Initialize the channels used by the iterator but drop the core receiver immediately
        /// 3. Create the iterator and attempt to get a task in a new thread
        /// 4. Send a task to the receiver to simulate an unexpected task
        /// 5. Join the thread and verify `None` was returned by the iterator
        /// 7. Verify that an error was logged
        #[test]
        fn task_iterator_send_failed_with_unexpected_task() {
            let logger = init_logger();

            let (core_tx, _) = channel();
            let (task_tx, task_rx) = channel();

            let join_handle = std::thread::spawn(move || {
                SerialExecutionTaskIterator::new(core_tx, task_rx).next()
            });

            task_tx.send(Some(mock_execution_task())).expect("Failed to send task");

            let task = join_handle.join().expect("Iterator thread panicked");
            assert!(task.is_none());

            assert!(logger.has_err());
        }

        /// Verifies that the task iterator returns `None` and logs an error if the next task
        /// request fails and `None` task was never received.
        ///
        /// 1. Initialize the logger
        /// 2. Initialize the channels used by the iterator but drop the core receiver and the task
        ///    sender immediately
        /// 3. Create the iterator and attempt to get a task in a new thread
        /// 4. Join the thread and verify `None` was returned by the iterator
        /// 5. Verify that an error was logged
        #[test]
        fn task_iterator_send_failed_no_notification() {
            let logger = init_logger();

            let (core_tx, _) = channel();
            let (_, task_rx) = channel();

            let join_handle = std::thread::spawn(move || {
                SerialExecutionTaskIterator::new(core_tx, task_rx).next()
            });

            let task = join_handle.join().expect("Iterator thread panicked");
            assert!(task.is_none());

            assert!(logger.has_err());
        }

        /// Verifies that the task iterator returns `None` and logs an error if the next task
        /// request succeeds but receiving the response fails.
        ///
        /// 1. Initialize the logger
        /// 2. Initialize the channels used by the iterator but drop the task sender immediately
        /// 3. Create the iterator and attempt to get a task in a new thread
        /// 4. Join the thread and verify `None` was returned by the iterator
        /// 5. Verify that an error was logged
        #[test]
        fn task_iterator_send_successful_but_receive_failed() {
            let logger = init_logger();

            let (core_tx, _core_rx) = channel();
            let (_, task_rx) = channel();

            let join_handle = std::thread::spawn(move || {
                SerialExecutionTaskIterator::new(core_tx, task_rx).next()
            });

            let task = join_handle.join().expect("Iterator thread panicked");
            assert!(task.is_none());

            assert!(logger.has_err());
        }
    }

    fn recv_next(core_rx: &Receiver<CoreMessage>) {
        match core_rx.recv() {
            Ok(CoreMessage::Next) => {}
            res => panic!("Expected `Ok(CoreMessage::Next)`, got {:?} instead", res),
        }
    }

    fn mock_execution_task() -> ExecutionTask {
        ExecutionTask {
            pair: TransactionBuilder::new()
                .with_family_name("test".into())
                .with_family_version("0.1".into())
                .with_inputs(vec![])
                .with_outputs(vec![])
                .with_payload_hash_method(HashMethod::SHA512)
                .with_payload(vec![])
                .build_pair(&*new_signer())
                .expect("Failed to build txn pair"),
            context_id: ContextId::default(),
        }
    }

    fn new_signer() -> Box<dyn Signer> {
        let context = Secp256k1Context::new();
        let key = context.new_random_private_key();
        context.new_signer(key)
    }

    fn init_logger() -> MockLogger {
        let logger = MockLogger::default();
        set_boxed_logger(Box::new(logger.clone())).expect("Failed to set logger");
        set_max_level(LevelFilter::Debug);
        logger
    }

    #[derive(Clone, Default)]
    struct MockLogger {
        log_levels: Arc<Mutex<Vec<Level>>>,
    }

    impl MockLogger {
        /// Determines whether or not an error message was logged
        pub fn has_err(&self) -> bool {
            self.log_levels
                .lock()
                .expect("Failed to get log_levels lock")
                .iter()
                .any(|level| level == &Level::Error)
        }

        /// Determines whether or not a debug message was logged
        pub fn has_debug(&self) -> bool {
            self.log_levels
                .lock()
                .expect("Failed to get log_levels lock")
                .iter()
                .any(|level| level == &Level::Debug)
        }
    }

    impl Log for MockLogger {
        fn enabled(&self, _metadata: &Metadata) -> bool {
            true
        }

        fn log(&self, record: &Record) {
            self.log_levels
                .lock()
                .expect("Failed to get log_levels lock")
                .push(record.level());
        }

        fn flush(&self) {}
    }
}

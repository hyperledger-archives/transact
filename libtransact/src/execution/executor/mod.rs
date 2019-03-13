/*
 * Copyright 2019 Bitwise IO, Inc.
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

mod internal;
mod reader;

use internal::ExecutorThread;
use reader::IteratorAdapter;

use crate::execution::adapter::ExecutionAdapter;
use crate::scheduler::ExecutionTask;
use crate::scheduler::ExecutionTaskCompletionNotifier;
use log::debug;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};

pub struct Executor {
    schedulers: Arc<Mutex<HashMap<usize, IteratorAdapter>>>,
    executor_thread: ExecutorThread,
}

impl Executor {
    pub fn execute(
        &self,
        task_iterator: Box<Iterator<Item = ExecutionTask> + Send>,
        notifier: Box<ExecutionTaskCompletionNotifier>,
    ) -> Result<(), ExecutorError> {
        if let Some(sender) = self.executor_thread.sender() {
            let index = self
                .schedulers
                .lock()
                .expect("The iterator adapters map lock is poisoned")
                .keys()
                .max()
                .cloned()
                .unwrap_or(0);

            let mut iterator_adapter = IteratorAdapter::new(index);

            let schedulers = Arc::clone(&self.schedulers);

            let done_callback = Box::new(move |index| {
                debug!(
                    "Callback called removing iterator adapter {} for SchedulerExecutionInterface",
                    index
                );

                schedulers
                    .lock()
                    .expect("The IteratorAdapter mutex is poisoned")
                    .remove(&index);
            });

            iterator_adapter
                .start(task_iterator, notifier, sender, done_callback)
                .map_err(|err| {
                    ExecutorError::ResourcesUnavailable(err.description().to_string())
                })?;

            debug!("Execute called, creating execution adapter {}", index);

            let mut schedulers = self
                .schedulers
                .lock()
                .expect("The iterator adapter map lock is poisoned");

            schedulers.insert(index, iterator_adapter);

            Ok(())
        } else {
            Err(ExecutorError::NotStarted)
        }
    }

    pub fn start(&mut self) -> Result<(), ExecutorError> {
        self.executor_thread.start().map_err(|_| {
            ExecutorError::AlreadyStarted("The Executor has already had start called.".to_string())
        })
    }

    pub fn stop(self) {
        for sched in self
            .schedulers
            .lock()
            .expect("The IteratorAdapter mutex is poisoned")
            .drain()
        {
            sched.1.stop();
        }
        self.executor_thread.stop();
    }

    pub fn new(execution_adapters: Vec<Box<ExecutionAdapter>>) -> Self {
        Executor {
            schedulers: Arc::new(Mutex::new(HashMap::new())),
            executor_thread: ExecutorThread::new(execution_adapters),
        }
    }
}

#[derive(Debug)]
pub enum ExecutorError {
    // The Executor has not been started, and so calling `execute` will return an error.
    NotStarted,
    // The Executor has had start called more than once.
    AlreadyStarted(String),

    ResourcesUnavailable(String),
}

impl std::error::Error for ExecutorError {}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ExecutorError::NotStarted => f.write_str("Executor not started"),
            ExecutorError::AlreadyStarted(ref msg) => {
                write!(f, "Executor already started: {}", msg)
            }
            ExecutorError::ResourcesUnavailable(ref msg) => {
                write!(f, "Resource Unavailable: {}", msg)
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::execution::adapter::test_adapter::TestExecutionAdapter;
    use crate::protocol::transaction::{HashMethod, TransactionBuilder, TransactionPair};
    use crate::scheduler::ExecutionTask;
    use crate::scheduler::ExecutionTaskCompletionNotification;
    use crate::scheduler::ExecutionTaskCompletionNotifier;
    use crate::signing::{hash::HashSigner, Signer};
    use std::collections::VecDeque;
    use std::time::Duration;

    static FAMILY_NAME1: &str = "test1";
    static FAMILY_NAME2: &str = "test2";
    static FAMILY_VERSION: &str = "1.0";
    static KEY1: &str = "111111111111111111111111111111111111111111111111111111111111111111";
    static KEY2: &str = "222222222222222222222222222222222222222222222222222222222222222222";
    static KEY3: &str = "333333333333333333333333333333333333333333333333333333333333333333";
    static KEY4: &str = "444444444444444444444444444444444444444444444444444444444444444444";
    static KEY5: &str = "555555555555555555555555555555555555555555555555555555555555555555";
    static KEY6: &str = "666666666666666666666666666666666666666666666666666666666666666666";
    static KEY7: &str = "777777777777777777777777777777777777777777777777777777777777777777";
    static NONCE: &str = "f9kdzz";
    static BYTES2: [u8; 4] = [0x05, 0x06, 0x07, 0x08];

    static NUMBER_OF_TRANSACTIONS: usize = 20;

    #[test]
    fn test_executor() {
        let test_execution_adapter1 = TestExecutionAdapter::new();

        let adapter1 = test_execution_adapter1.clone();

        let test_execution_adapter2 = TestExecutionAdapter::new();

        let adapter2 = test_execution_adapter2.clone();

        let mut executor = Executor::new(vec![
            Box::new(test_execution_adapter1),
            Box::new(test_execution_adapter2),
        ]);

        executor.start().expect("Executor did not correctly start");

        let iterator1 = MockTaskExecutionIterator::new();
        let notifier1 = MockExecutionTaskCompletionNotifier::new();

        let iterator2 = MockTaskExecutionIterator::new();
        let notifier2 = MockExecutionTaskCompletionNotifier::new();

        executor
            .execute(Box::new(iterator1), Box::new(notifier1.clone()))
            .expect("Start has been called so the executor can execute");

        executor
            .execute(Box::new(iterator2), Box::new(notifier2.clone()))
            .expect("Start has been called so the executor can execute");

        adapter1.register("test1", "1.0");
        adapter2.register("test2", "1.0");

        std::thread::sleep(Duration::from_millis(200));

        assert_eq!(
            notifier1.num_results(),
            NUMBER_OF_TRANSACTIONS,
            "All transactions for schedule 1 received a result"
        );

        assert_eq!(
            notifier2.num_results(),
            NUMBER_OF_TRANSACTIONS,
            "All transactions for schedule 2 received a result"
        );
    }

    fn create_txn(signer: &Signer, family_name: &str) -> TransactionPair {
        TransactionBuilder::new()
            .with_batcher_public_key(hex::decode(KEY1).unwrap())
            .with_dependencies(vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap()])
            .with_family_name(family_name.to_string())
            .with_family_version(FAMILY_VERSION.to_string())
            .with_inputs(vec![
                hex::decode(KEY4).unwrap(),
                hex::decode(&KEY5[0..4]).unwrap(),
            ])
            .with_nonce(NONCE.to_string().into_bytes())
            .with_outputs(vec![
                hex::decode(KEY6).unwrap(),
                hex::decode(&KEY7[0..4]).unwrap(),
            ])
            .with_payload_hash_method(HashMethod::SHA512)
            .with_payload(BYTES2.to_vec())
            .build_pair(signer)
            .expect("The TransactionBuilder was not given the correct items")
    }

    struct MockTaskExecutionIterator {
        tasks: VecDeque<ExecutionTask>,
    }

    impl MockTaskExecutionIterator {
        fn new() -> Self {
            let signer = HashSigner::new();
            let context_id = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

            let family_name = |i| {
                if i % 2 == 0 {
                    FAMILY_NAME1
                } else {
                    FAMILY_NAME2
                }
            };

            MockTaskExecutionIterator {
                tasks: (0..NUMBER_OF_TRANSACTIONS)
                    .map(move |i| create_txn(&signer, family_name(i)))
                    .map(move |txn_pair| ExecutionTask::new(txn_pair, context_id.clone()))
                    .collect(),
            }
        }
    }

    impl Iterator for MockTaskExecutionIterator {
        type Item = ExecutionTask;

        fn next(&mut self) -> Option<ExecutionTask> {
            self.tasks.pop_front()
        }
    }

    #[derive(Clone)]
    struct MockExecutionTaskCompletionNotifier {
        results: Arc<Mutex<Vec<ExecutionTaskCompletionNotification>>>,
    }

    impl MockExecutionTaskCompletionNotifier {
        fn new() -> Self {
            MockExecutionTaskCompletionNotifier {
                results: Arc::new(Mutex::new(vec![])),
            }
        }

        fn num_results(&self) -> usize {
            self.results
                .lock()
                .expect("The MockTaskExecutionIterator lock is poisoned")
                .len()
        }
    }

    impl ExecutionTaskCompletionNotifier for MockExecutionTaskCompletionNotifier {
        fn notify(&self, notification: ExecutionTaskCompletionNotification) {
            self.results
                .lock()
                .expect("The MockScheduler lock is poisoned")
                .push(notification);
        }
    }
}

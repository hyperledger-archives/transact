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

use crate::execution::adapter::ExecutionAdapter;
use crate::execution::executer_internal::{
    ExecuterThread, RegistrationExecutionEvent, RegistrationExecutionEventSender,
};
use crate::scheduler::SchedulePair;
use log::debug;
use log::warn;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::channel,
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};

/// The `IteratorAdapter` sends all of the `Item`s from an `Iterator` along a single channel.
///
/// In the normal course of an executer there will be many `IteratorAdaptor`s, one for each `Scheduler`.
struct IteratorAdapter {
    id: usize,
    threads: Option<(JoinHandle<()>, JoinHandle<()>)>,
    stop: Arc<AtomicBool>,
}

impl IteratorAdapter {
    fn new(id: usize) -> Self {
        IteratorAdapter {
            id,
            threads: None,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    fn start(
        &mut self,
        schedule: Box<SchedulePair>,
        internal: RegistrationExecutionEventSender,
        done_callback: Box<FnMut(usize) + Send>,
    ) -> Result<(), std::io::Error> {
        let stop = Arc::clone(&self.stop);

        let mut done_callback = done_callback;

        if self.threads.is_none() {
            let (sender, receiver) = channel();

            let it = schedule.get_schedule_iterator();

            let join_handle = thread::Builder::new()
                .name(format!("iterator_adapter_{}", self.id))
                .spawn(move || {
                    for execution_task in it {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }

                        let execution_event = (sender.clone(), execution_task);
                        let event =
                            RegistrationExecutionEvent::Execution(Box::new(execution_event));

                        if let Err(err) = internal.send(event) {
                            warn!("During sending on the internal executer channel: {}", err)
                        }
                    }
                })?;

            let stop = Arc::clone(&self.stop);
            let id = self.id;

            let join_handle_receive = thread::Builder::new()
                .name(format!("iterator_adapter_receive_thread_{}", self.id))
                .spawn(move || loop {
                    while let Ok(execution_result) = receiver.recv() {
                        schedule.add_execution_result(execution_result);

                        if stop.load(Ordering::Relaxed) {
                            done_callback(id);
                            break;
                        }
                    }
                })?;

            self.threads = Some((join_handle, join_handle_receive));
        }
        Ok(())
    }

    fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some((send, receive)) = self.threads {
            Self::shutdown(send);
            Self::shutdown(receive);
        }
    }

    fn shutdown(join_handle: JoinHandle<()>) {
        if let Err(err) = join_handle.join() {
            warn!("Error joining with IteratorAdapter thread: {:?}", err);
        }
    }
}

pub struct Executer {
    schedulers: Arc<Mutex<HashMap<usize, IteratorAdapter>>>,
    executer_thread: ExecuterThread,
}

impl Executer {
    pub fn execute(&self, schedule: Box<SchedulePair>) -> Result<(), ExecuterError> {
        if let Some(sender) = self.executer_thread.sender() {
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
                    "Callback called removing iterator adapter {} for SchedulePair",
                    index
                );

                schedulers
                    .lock()
                    .expect("The IteratorAdapter mutex is poisoned")
                    .remove(&index);
            });

            iterator_adapter
                .start(schedule, sender, done_callback)
                .map_err(|err| {
                    ExecuterError::ResourcesUnavailable(err.description().to_string())
                })?;

            debug!("Execute called, creating execution adapter {}", index);

            let mut schedulers = self
                .schedulers
                .lock()
                .expect("The iterator adapter map lock is poisoned");

            schedulers.insert(index, iterator_adapter);

            Ok(())
        } else {
            Err(ExecuterError::NotStarted)
        }
    }

    pub fn start(&mut self) -> Result<(), ExecuterError> {
        self.executer_thread.start().map_err(|_| {
            ExecuterError::AlreadyStarted("The Executer has already had start called.".to_string())
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
        self.executer_thread.stop();
    }

    pub fn new(execution_adapters: Vec<Box<ExecutionAdapter>>) -> Self {
        Executer {
            schedulers: Arc::new(Mutex::new(HashMap::new())),
            executer_thread: ExecuterThread::new(execution_adapters),
        }
    }
}

#[derive(Debug)]
pub enum ExecuterError {
    // The Executer has not been started, and so calling `execute` will return an error.
    NotStarted,
    // The Executer has had start called more than once.
    AlreadyStarted(String),

    ResourcesUnavailable(String),
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::execution::adapter::test_adapter::TestExecutionAdapter;
    use crate::execution::adapter::ExecutionResult;
    use crate::scheduler::ExecutionTask;
    use crate::signing::{hash::HashSigner, Signer};
    use crate::transaction::{HashMethod, TransactionBuilder, TransactionPair};
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
    fn test_executer() {
        let test_execution_adapter1 = TestExecutionAdapter::new();

        let adapter1 = test_execution_adapter1.clone();

        let test_execution_adapter2 = TestExecutionAdapter::new();

        let adapter2 = test_execution_adapter2.clone();

        let mut executer = Executer::new(vec![
            Box::new(test_execution_adapter1),
            Box::new(test_execution_adapter2),
        ]);

        executer.start().expect("Executer did not correctly start");

        let schedule1 = MockSchedule::new();
        let schedule1_results = schedule1.clone();

        let schedule2 = MockSchedule::new();
        let schedule2_results = schedule2.clone();

        executer
            .execute(Box::new(schedule1))
            .expect("Start has been called so the executer can execute");

        executer
            .execute(Box::new(schedule2))
            .expect("Start has been called so the executer can execute");

        adapter1.register("test1", "1.0");
        adapter2.register("test2", "1.0");

        std::thread::sleep(Duration::from_millis(200));

        assert_eq!(
            schedule1_results.num_results(),
            NUMBER_OF_TRANSACTIONS,
            "All transactions for schedule 1 received a result"
        );

        assert_eq!(
            schedule2_results.num_results(),
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

    fn create_iterator() -> impl Iterator<Item = ExecutionTask> {
        let signer = HashSigner::new();
        let context_id = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

        let family_name = |i| {
            if i % 2 == 0 {
                FAMILY_NAME1
            } else {
                FAMILY_NAME2
            }
        };

        (0..NUMBER_OF_TRANSACTIONS)
            .map(move |i| create_txn(&signer, family_name(i)))
            .map(move |txn_pair| ExecutionTask::new(txn_pair, context_id.clone()))
    }

    #[derive(Clone)]
    struct MockSchedule {
        results: Arc<Mutex<Vec<ExecutionResult>>>,
    }

    impl MockSchedule {
        fn new() -> Self {
            MockSchedule {
                results: Arc::new(Mutex::new(vec![])),
            }
        }

        fn num_results(&self) -> usize {
            self.results
                .lock()
                .expect("The MockScheduler lock is poisoned")
                .len()
        }
    }

    impl SchedulePair for MockSchedule {
        fn get_schedule_iterator(&self) -> Box<Iterator<Item = ExecutionTask> + Send> {
            Box::new(create_iterator())
        }

        fn add_execution_result(&self, execution_result: ExecutionResult) {
            self.results
                .lock()
                .expect("The MockScheduler lock is poisoned")
                .push(execution_result);
        }
    }
}

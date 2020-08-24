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

//! A `Scheduler` which runs multiple sub-schedulers. The primary purpose of the `MultiScheduler`
//! is for testing; it enables running multiple schedulers in parallel to verify that they all
//! produce the same results for a given workload.

mod core;
mod shared;

use crate::protocol::batch::BatchPair;
use crate::scheduler::{
    BatchExecutionResult, ExecutionTask, ExecutionTaskCompletionNotifier, Scheduler,
    SchedulerError, SchedulerFactory,
};

use std::cell::RefCell;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

// If the shared lock is poisoned, report an internal error since the scheduler cannot recover.
impl From<std::sync::PoisonError<std::sync::MutexGuard<'_, shared::MultiSchedulerShared>>>
    for SchedulerError
{
    fn from(
        error: std::sync::PoisonError<std::sync::MutexGuard<'_, shared::MultiSchedulerShared>>,
    ) -> SchedulerError {
        SchedulerError::Internal(format!("scheduler shared lock is poisoned: {}", error))
    }
}

// If the core `Receiver` disconnects, report an internal error since the scheduler can't operate
// without the core thread.
impl From<std::sync::mpsc::SendError<core::MultiSchedulerCoreMessage>> for SchedulerError {
    fn from(error: std::sync::mpsc::SendError<core::MultiSchedulerCoreMessage>) -> SchedulerError {
        SchedulerError::Internal(format!("scheduler's core thread disconnected: {}", error))
    }
}

/// The MultiScheduler will send the task iterators and notifiers of its sub-schedulers to the
/// struct that implements this trait.
pub trait SubSchedulerHandler: Send {
    /// Gives the task iterator and notifier of a sub-scheduler to the sub-scheduler handler; the
    /// sub-scheduler handler will get tasks directly from the sub-scheduler and send task
    /// execution reults back to the sub-scheduler.
    fn pass_scheduler(
        &mut self,
        task_iterator: Box<dyn Iterator<Item = ExecutionTask> + Send>,
        notifier: Box<dyn ExecutionTaskCompletionNotifier>,
    ) -> Result<(), String>;
}

/// A `Scheduler` implementation which runs multiple sub-schedulers.
pub struct MultiScheduler {
    shared_lock: Arc<Mutex<shared::MultiSchedulerShared>>,
}

impl MultiScheduler {
    /// Returns a newly created `MultiScheduler` that runs the specified sub-schedulers.
    pub fn new(
        mut schedulers: Vec<Box<dyn Scheduler>>,
        sub_scheduler_handler: &mut dyn SubSchedulerHandler,
    ) -> Result<MultiScheduler, SchedulerError> {
        let (core_tx, core_rx) = mpsc::channel();

        for (i, scheduler) in schedulers.iter_mut().enumerate() {
            // All sub-schedulers will callback to the MultiScheduler; these callbacks will
            // record the index of the calling sub-scheduler
            let tx = core_tx.clone();
            scheduler
                .set_result_callback(Box::new(move |result| {
                    tx.send(core::MultiSchedulerCoreMessage::BatchResult(i, result))
                        .unwrap_or_else(|err| {
                            error!(
                                "scheduler {} failed to send result to MultiScheduler: {}",
                                i, err
                            )
                        });
                }))
                .map_err(|err| {
                    SchedulerError::Internal(format!(
                        "failed to set result callback for sub-scheduler {}: {}",
                        i, err
                    ))
                })?;
            let tx = core_tx.clone();
            scheduler
                .set_error_callback(Box::new(move |err| {
                    tx.send(core::MultiSchedulerCoreMessage::SubSchedulerError(i, err))
                        .unwrap_or_else(|err| {
                            error!(
                                "scheduler {} failed to send error to MultiScheduler: {}",
                                i, err
                            )
                        });
                }))
                .map_err(|err| {
                    SchedulerError::Internal(format!(
                        "failed to set error callback for sub-scheduler {}: {}",
                        i, err
                    ))
                })?;
            // Each sub-scheduler except for the first is sent to the SubSchedulerHandler to run in
            // parallel; the first sub-scheduler's task iterator and notifier will be returned by
            // the multi-scheduler's take_task_iterator and new_notifier methods.
            if i > 0 {
                sub_scheduler_handler
                    .pass_scheduler(
                        scheduler.take_task_iterator().map_err(|err| {
                            SchedulerError::Internal(format!(
                                "failed to take task iterator from sub-scheudler {}: {}",
                                i, err
                            ))
                        })?,
                        scheduler.new_notifier().map_err(|err| {
                            SchedulerError::Internal(format!(
                                "failed to get new notifier from sub-scheduler {}: {}",
                                i, err
                            ))
                        })?,
                    )
                    .map_err(|err| {
                        SchedulerError::Internal(format!(
                            "failed to pass sub-scheduler {} to handler: {}",
                            i, err,
                        ))
                    })?;
            }
        }

        let shared_lock = Arc::new(Mutex::new(shared::MultiSchedulerShared::new(schedulers)));

        core::MultiSchedulerCore::new(shared_lock.clone(), core_rx).start()?;

        Ok(MultiScheduler { shared_lock })
    }
}

impl Scheduler for MultiScheduler {
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
        if shared.batch_already_pending(&batch) {
            return Err(SchedulerError::DuplicateBatch(
                batch.batch().header_signature().into(),
            ));
        }
        shared.add_batch(batch)
    }

    fn cancel(&mut self) -> Result<Vec<BatchPair>, SchedulerError> {
        self.shared_lock.lock()?.cancel()
    }

    fn finalize(&mut self) -> Result<(), SchedulerError> {
        self.shared_lock.lock()?.finalize()
    }

    fn take_task_iterator(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = ExecutionTask> + Send>, SchedulerError> {
        // The MultiScheduler passes all sub-schedulers' task iterators directly to the
        // SubSchedulerHandler except for the first sub-scheduler's, which it returns here.
        self.shared_lock.lock()?.schedulers_mut()[0].take_task_iterator()
    }

    fn new_notifier(&mut self) -> Result<Box<dyn ExecutionTaskCompletionNotifier>, SchedulerError> {
        // The MultiScheduler passes all sub-schedulers' notifiers directly to the
        // SubSchedulerHandler except for the first sub-scheduler's, which it returns here.
        self.shared_lock.lock()?.schedulers_mut()[0].new_notifier()
    }
}

/// Factory for creating `MultiScheduler`s
pub struct MultiSchedulerFactory {
    sub_scheduler_factories: Vec<(Box<dyn SchedulerFactory>, usize)>,
    sub_scheduler_handler: RefCell<Box<dyn SubSchedulerHandler>>,
}

impl MultiSchedulerFactory {
    /// Creates a new `MultiSchedulerFactory`
    ///
    /// # Arguments
    ///
    /// `sub_scheduler_factories` - List of factories for creating subschedulers, along with number
    /// of schedulers to create from each factory
    /// `sub_scheduler_handler` - The handler that will execute transactions for the sub-schedulers
    pub fn new(
        sub_scheduler_factories: Vec<(Box<dyn SchedulerFactory>, usize)>,
        sub_scheduler_handler: RefCell<Box<dyn SubSchedulerHandler>>,
    ) -> Self {
        Self {
            sub_scheduler_factories,
            sub_scheduler_handler,
        }
    }
}

impl SchedulerFactory for MultiSchedulerFactory {
    fn create_scheduler(&self, state_id: String) -> Result<Box<dyn Scheduler>, SchedulerError> {
        let schedulers = self
            .sub_scheduler_factories
            .iter()
            .flat_map(|(factory, num)| {
                let state_id = state_id.clone();
                (0..*num).map(move |_| factory.create_scheduler(state_id.clone()))
            })
            .collect::<Result<_, _>>()?;
        let mut sub_scheduler_handler =
            self.sub_scheduler_handler.try_borrow_mut().map_err(|_| {
                SchedulerError::Internal(
                    "Sub-scheduler handler cell is already mutably borrowed".into(),
                )
            })?;
        MultiScheduler::new(schedulers, &mut **sub_scheduler_handler)
            .map(|scheduler| Box::new(scheduler) as Box<dyn Scheduler>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::tests::*;
    use crate::scheduler::{ExecutionTaskCompletionNotification, ExecutionTaskCompletionNotifier};

    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::MutexGuard;

    #[derive(Clone)]
    struct MockSubScheduler {
        received_batches: Arc<Mutex<Vec<BatchPair>>>,
        finalized: Arc<AtomicBool>,
        callback: Arc<Mutex<Box<dyn Fn(Option<BatchExecutionResult>) + Send>>>,
        results: Vec<Option<BatchExecutionResult>>,
    }

    impl MockSubScheduler {
        fn new(results: Vec<Option<BatchExecutionResult>>) -> Self {
            MockSubScheduler {
                received_batches: Arc::new(Mutex::new(vec![])),
                finalized: Arc::new(AtomicBool::new(false)),
                callback: Arc::new(Mutex::new(Box::new(|_| {
                    panic!("callback not set for subscheduler")
                }))),
                results,
            }
        }

        fn received_batches(&self) -> MutexGuard<Vec<BatchPair>> {
            self.received_batches
                .lock()
                .expect("received batches lock poisoned")
        }

        fn finalized(&self) -> bool {
            self.finalized.load(Ordering::Relaxed)
        }
    }

    impl Scheduler for MockSubScheduler {
        fn set_result_callback(
            &mut self,
            callback: Box<dyn Fn(Option<BatchExecutionResult>) + Send>,
        ) -> Result<(), SchedulerError> {
            self.callback = Arc::new(Mutex::new(callback));
            Ok(())
        }

        fn set_error_callback(
            &mut self,
            _callback: Box<dyn Fn(SchedulerError) + Send>,
        ) -> Result<(), SchedulerError> {
            Ok(())
        }

        fn add_batch(&mut self, batch: BatchPair) -> Result<(), SchedulerError> {
            self.received_batches
                .lock()
                .expect("received batches lock poisoned")
                .push(batch);
            Ok(())
        }

        fn cancel(&mut self) -> Result<Vec<BatchPair>, SchedulerError> {
            self.received_batches
                .lock()
                .expect("received batches lock poisoned")
                .clear();
            Ok(vec![])
        }

        fn finalize(&mut self) -> Result<(), SchedulerError> {
            self.finalized.store(true, Ordering::Relaxed);
            if self.results.is_empty() {
                self.callback.lock().expect("callback lock poisoned")(None);
            }
            Ok(())
        }

        fn take_task_iterator(
            &mut self,
        ) -> Result<Box<dyn Iterator<Item = ExecutionTask> + Send>, SchedulerError> {
            // This isn't used; the test will tell the SubSchedulerHandler when to send a
            // notifcation to this sub-scheduler
            Ok(Box::new(std::iter::empty()))
        }

        fn new_notifier(
            &mut self,
        ) -> Result<Box<dyn ExecutionTaskCompletionNotifier>, SchedulerError> {
            // The MockSubSchedulerNotifier just send sthe MockSubScheduler's predefined results to
            // the MockSubScheduler's callback
            #[derive(Clone)]
            struct MockSubSchedulerNotifier {
                results: RefCell<VecDeque<Option<BatchExecutionResult>>>,
                callback: Arc<Mutex<Box<dyn Fn(Option<BatchExecutionResult>) + Send>>>,
            }
            impl ExecutionTaskCompletionNotifier for MockSubSchedulerNotifier {
                fn notify(&self, _notification: ExecutionTaskCompletionNotification) {
                    let next_result = match self.results.borrow_mut().pop_front() {
                        Some(res) => res,
                        None => {
                            warn!("subscheduler has no more results");
                            return;
                        }
                    };
                    self.callback.lock().expect("callback lock poisoned")(next_result)
                }
                fn clone_box(&self) -> Box<dyn ExecutionTaskCompletionNotifier> {
                    Box::new(self.clone())
                }
            }
            Ok(Box::new(MockSubSchedulerNotifier {
                results: RefCell::new(self.results.drain(..).collect()),
                callback: Arc::clone(&self.callback),
            }))
        }
    }

    struct MockSubSchedulerHandler {
        notifiers: Vec<Box<dyn ExecutionTaskCompletionNotifier>>,
    }

    impl MockSubSchedulerHandler {
        fn new() -> Self {
            MockSubSchedulerHandler { notifiers: vec![] }
        }

        fn next(&self) {
            let mut i = 0;
            for notifier in &self.notifiers {
                i = i + 1;
                // This result doesn't matter, just need a notification; the MockSubSchedulers have
                // pre-generated results that they will use.
                notifier.notify(ExecutionTaskCompletionNotification::Valid(
                    mock_context_id(),
                    "".into(),
                ))
            }
        }
    }

    impl SubSchedulerHandler for MockSubSchedulerHandler {
        fn pass_scheduler(
            &mut self,
            _task_iterator: Box<dyn Iterator<Item = ExecutionTask> + Send>,
            notifier: Box<dyn ExecutionTaskCompletionNotifier>,
        ) -> Result<(), String> {
            self.notifiers.push(notifier);
            Ok(())
        }
    }

    /// The caller may want to keep the mock sub-schedulers to check their properties after the
    /// multi-scheduler performs certain actions; the mock sub-schedulers also need to be cast as
    /// generic Schedulers for the MultiScheduler.
    fn clone_mocksubschedulers_into_multischeduler(
        sub_schedulers: &Vec<Box<MockSubScheduler>>,
    ) -> MultiScheduler {
        let sub_schedulers = sub_schedulers
            .iter()
            .map(|sub_scheduler| sub_scheduler.clone() as Box<dyn Scheduler>)
            .collect();
        MultiScheduler::new(sub_schedulers, &mut MockSubSchedulerHandler::new())
            .expect("Failed to create scheduler")
    }

    // General Scheduler tests

    /// In addition to the basic functionality verified by `test_scheduler_add_batch`, this test
    /// verifies that the MultiScheduler adds the batch to all sub-schedulers and creates a pending
    /// result for the batch.
    #[test]
    pub fn test_multi_scheduler_add_batch() {
        let sub_schedulers: Vec<_> = (0..3)
            .map(|_| Box::new(MockSubScheduler::new(vec![])))
            .collect();
        let mut multi_scheduler = clone_mocksubschedulers_into_multischeduler(&sub_schedulers);

        let batch = test_scheduler_add_batch(&mut multi_scheduler);

        for sub_scheduler in sub_schedulers {
            assert!(sub_scheduler.received_batches().contains(&batch));
        }
        assert!(multi_scheduler
            .shared_lock
            .lock()
            .expect("shared lock is poisoned")
            .batch_already_pending(&batch));

        multi_scheduler.cancel().expect("Failed to cancel");
        multi_scheduler.finalize().expect("Failed to finalize");
    }

    /// In addition to the basic functionality verified by `test_scheduler_cancel`, this test
    /// verifies that the MultiScheduler cancels all sub-schedulers and drains its pending batches.
    #[test]
    fn test_multi_scheduler_cancel() {
        let sub_schedulers: Vec<_> = (0..3)
            .map(|_| Box::new(MockSubScheduler::new(vec![])))
            .collect();
        let mut multi_scheduler = clone_mocksubschedulers_into_multischeduler(&sub_schedulers);

        test_scheduler_cancel(&mut multi_scheduler);

        for sub_scheduler in sub_schedulers {
            assert!(sub_scheduler.received_batches().is_empty());
        }
        assert!(multi_scheduler
            .shared_lock
            .lock()
            .expect("shared lock is poisoned")
            .pending_results()
            .is_empty());

        multi_scheduler.cancel().expect("Failed to cancel");
        multi_scheduler.finalize().expect("Failed to finalize");
    }

    /// In addition to the basic functionality verified by `test_scheduler_finalize`, this test
    /// verifies that the MultiScheduler finalizes all sub-schedulers and updates its internal
    /// state to finalized.
    #[test]
    fn test_multi_scheduler_finalize() {
        let sub_schedulers: Vec<_> = (0..3)
            .map(|_| Box::new(MockSubScheduler::new(vec![])))
            .collect();
        let mut multi_scheduler = clone_mocksubschedulers_into_multischeduler(&sub_schedulers);

        test_scheduler_finalize(&mut multi_scheduler);

        for sub_scheduler in sub_schedulers {
            assert!(sub_scheduler.finalized());
        }
        assert!(multi_scheduler
            .shared_lock
            .lock()
            .expect("shared lock is poisoned")
            .finalized());

        multi_scheduler.cancel().expect("Failed to cancel");
        multi_scheduler.finalize().expect("Failed to finalize");
    }

    // MultiScheduler-specific tests

    /// This test verifies that when all sub-schedulers report that they are done, but one or more
    /// sub-scheduler(s) did not return a result for a batch, the MultiScheduler returns an error
    #[test]
    fn test_done_incorrectly() {
        let batch = mock_batch_with_num_txns(1);
        let valid_receipt = valid_receipt_from_batch(batch.clone());

        // The first sub-scheduler doens't have a result for the batch
        let sub_schedulers = vec![
            Box::new(MockSubScheduler::new(vec![valid_receipt.clone()])) as Box<dyn Scheduler>,
            Box::new(MockSubScheduler::new(vec![valid_receipt.clone()])) as Box<dyn Scheduler>,
            Box::new(MockSubScheduler::new(vec![])) as Box<dyn Scheduler>,
        ];
        let mut sub_scheduler_handler = MockSubSchedulerHandler::new();
        let mut multi_scheduler = MultiScheduler::new(sub_schedulers, &mut sub_scheduler_handler)
            .expect("Failed to create scheduler");
        sub_scheduler_handler
            .pass_scheduler(
                multi_scheduler
                    .take_task_iterator()
                    .expect("Failed to take task iterator"),
                multi_scheduler
                    .new_notifier()
                    .expect("Failed to get new notifier"),
            )
            .expect("Failed to pass first scheduler to handler");
        multi_scheduler
            .add_batch(batch.clone())
            .expect("Failed to add batch");

        // Use a channel to pass the err to this test
        let (tx, rx) = mpsc::channel();
        multi_scheduler
            .set_error_callback(Box::new(move |err| {
                tx.send(err).expect("Failed to send error");
            }))
            .expect("Failed to set error callback");

        sub_scheduler_handler.next();
        multi_scheduler.finalize().expect("Failed to finalize");

        match rx.recv().expect("Failed to receive error") {
            SchedulerError::Internal(err_str) => {
                assert!(err_str.contains(batch.batch().header_signature()));
            }
            e => panic!("Wrong error type received: {:?}", e),
        }

        // Scheduler has been finalized and all sub-schedulers have completed, so scheduler has
        // already shutdown
    }

    /// This test verifies that the MultiScheduler properly returns results (valid and invalid)
    /// when all sub-schedulers have notified it of the same result for a batch, and that the
    /// MultiScheduler retuns an error using the error callback if the sub-schedulers do not all
    /// agree on the same result for a batch.
    #[test]
    pub fn test_multi_scheduler_result_handling() {
        let batches = mock_batches_with_one_transaction(3);

        // First batch is valid for all schedulers, second batch is invalid for all schedulers,
        // third batch has a different result for one of the schedulers
        let valid_receipt_batch_0 = valid_receipt_from_batch(batches[0].clone());
        let invalid_receipt_batch_1 = invalid_receipt_from_batch(batches[1].clone());
        let valid_receipt_batch_2 = valid_receipt_from_batch(batches[2].clone());
        let invalid_receipt_batch_2 = invalid_receipt_from_batch(batches[2].clone());
        let sub_schedulers = vec![
            Box::new(MockSubScheduler::new(vec![
                valid_receipt_batch_0.clone(),
                invalid_receipt_batch_1.clone(),
                invalid_receipt_batch_2.clone(),
            ])) as Box<dyn Scheduler>,
            Box::new(MockSubScheduler::new(vec![
                valid_receipt_batch_0.clone(),
                invalid_receipt_batch_1.clone(),
                valid_receipt_batch_2.clone(),
            ])) as Box<dyn Scheduler>,
            Box::new(MockSubScheduler::new(vec![
                valid_receipt_batch_0.clone(),
                invalid_receipt_batch_1.clone(),
                valid_receipt_batch_2.clone(),
            ])) as Box<dyn Scheduler>,
        ];

        let mut sub_scheduler_handler = MockSubSchedulerHandler::new();
        let mut multi_scheduler = MultiScheduler::new(sub_schedulers, &mut sub_scheduler_handler)
            .expect("Failed to create scheduler");
        sub_scheduler_handler
            .pass_scheduler(
                multi_scheduler
                    .take_task_iterator()
                    .expect("Failed to take task iterator"),
                multi_scheduler
                    .new_notifier()
                    .expect("Failed to get new notifier"),
            )
            .expect("Failed to pass first scheduler to handler");
        for batch in &batches {
            multi_scheduler
                .add_batch(batch.clone())
                .expect("Failed to add batch");
        }

        // Use channels to pass the callbacks to this test
        let (result_tx, result_rx) = mpsc::channel();
        multi_scheduler
            .set_result_callback(Box::new(move |result| {
                // The scheduler will be finalized/cancelled later, at which point the scheduler
                // will send a `None` result, but the receiver may already have been dropped at that
                // point.
                if result.is_some() {
                    result_tx.send(result).expect("Failed to send result");
                }
            }))
            .expect("Failed to set result callback");
        let (error_tx, error_rx) = mpsc::channel();
        multi_scheduler
            .set_error_callback(Box::new(move |err| {
                error_tx.send(err).expect("Failed to send error");
            }))
            .expect("Failed to set error callback");

        // Tell the sub-scheduler handler to notify the sub-schedulers for each batch and verify
        // the results
        sub_scheduler_handler.next();
        let result = result_rx.recv().expect("Failed to receive 1st result");
        assert_eq!(result, valid_receipt_batch_0);

        sub_scheduler_handler.next();
        let result = result_rx.recv().expect("Failed to receive 2nd result");
        assert_eq!(result, invalid_receipt_batch_1);

        sub_scheduler_handler.next();
        match error_rx.recv().expect("Failed to receive error") {
            SchedulerError::Internal(err_str) => {
                assert!(err_str.contains(batches[2].batch().header_signature()))
            }
            e => panic!("Wrong error type received: {:?}", e),
        }

        multi_scheduler.cancel().expect("Failed to cancel");
        multi_scheduler.finalize().expect("Failed to finalize");
    }
}

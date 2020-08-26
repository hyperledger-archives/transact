/*
 * Copyright 2019 Bitwise IO, Inc.
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

//! Batch scheduling with transaction execution APIs
//!
//! A `Scheduler` is used to execute one or more `Batch` objects, receiving
//! a `BatchExecutionResult` as a result of the execution of a `Batch`.  A `Batch` contains one or
//! more `Transaction` objects; each `Transaction` within a `Batch` is executed and then
//! consolidated to generate a `BatchExecutionResult`.
//!
//! In order for a `Scheduler` to execute batches, its associated `SchedulerExecutionInterface`
//! must be consumed by a component responsible for iterating over the `Transaction`s and providing
//! `ExecutionTaskCompletionNotification`s back to the `Scheduler` via the
//! `SchedulerExecutionInterface`.

pub mod multi;
pub mod parallel;
pub mod serial;

use std::error::Error;

use crate::context::ContextId;
use crate::protocol::batch::BatchPair;
use crate::protocol::receipt::{TransactionReceipt, TransactionResult};
use crate::protocol::transaction::TransactionPair;

/// A transation and associated information required to execute it.
pub struct ExecutionTask {
    pair: TransactionPair,
    context_id: ContextId,
}

impl ExecutionTask {
    /// Create a new `ExecutionPair`.
    pub fn new(pair: TransactionPair, context_id: ContextId) -> Self {
        ExecutionTask { pair, context_id }
    }

    /// The transaction to be executed.
    pub fn pair(&self) -> &TransactionPair {
        &self.pair
    }

    /// The identifier of the context to be used when accessing state.
    pub fn context_id(&self) -> &ContextId {
        &self.context_id
    }

    /// Decompose into its components.
    pub fn take(self) -> (TransactionPair, ContextId) {
        (self.pair, self.context_id)
    }
}

/// Result of executing a batch.
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct BatchExecutionResult {
    /// The `BatchPair` which was executed.
    pub batch: BatchPair,

    /// The receipts for each transaction in the batch.
    pub receipts: Vec<TransactionReceipt>,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct InvalidTransactionResult {
    /// Transaction identifier.
    pub transaction_id: String,

    /// Human-readable reason explaining why the transaction was invalid.
    pub error_message: String,

    /// Transaction-specific error data which can be interpreted by clients
    /// familiar with this transaction's family.
    pub error_data: Vec<u8>,
}

impl Into<TransactionReceipt> for InvalidTransactionResult {
    fn into(self) -> TransactionReceipt {
        TransactionReceipt {
            transaction_id: self.transaction_id,
            transaction_result: TransactionResult::Invalid {
                error_message: self.error_message,
                error_data: self.error_data,
            },
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ExecutionTaskCompletionNotification {
    /// The transation was invalid.
    Invalid(ContextId, InvalidTransactionResult),

    /// The transation was valid (String is transaction ID).
    Valid(ContextId, String),
}

#[derive(Clone, Debug)]
pub enum SchedulerError {
    /// The scheduler's `add_batch` method was called with a batch that the scheduler already has
    /// pending or in progress; the contained `String` is the batch ID.
    DuplicateBatch(String),
    /// An internal error occurred that the scheduler could not recover from.
    Internal(String),
    /// A scheduler only has one task iterator, so its `take_task_iterator` method can only be
    /// called once.
    NoTaskIterator,
    /// The scheduler's `add_batch` method was called, but the scheduler was already finalized
    SchedulerFinalized,
    /// An `ExecutionTaskCompletionNotification` was received for a transaction that the scheduler
    /// was not expecting; the contained `String` is the transaction ID.
    UnexpectedNotification(String),
}

impl Error for SchedulerError {}

impl std::fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            SchedulerError::DuplicateBatch(ref batch_id) => {
                write!(f, "duplicate batch added to scheduler: {}", batch_id)
            }
            SchedulerError::Internal(ref err) => {
                write!(f, "scheduler encountered an internal error: {}", err)
            }
            SchedulerError::NoTaskIterator => write!(f, "task iterator already taken"),
            SchedulerError::SchedulerFinalized => write!(f, "batch added to finalized scheduler"),
            SchedulerError::UnexpectedNotification(ref txn_id) => write!(
                f,
                "scheduler received an unexpected notification: {}",
                txn_id
            ),
        }
    }
}

/// Schedules batches and transactions and returns execution results.
///
/// # Cleanup
///
/// Implementations of the `Scheduler` trait may need to perform some cleanup before they are
/// dropped (shutting down background threads, for example). If required, implementors of this trait
/// should perform cleanup when the scheduler is finalized and all batch results have been sent
/// using the result callback. This will happen for a finalized scheduler when all scheduled batches
/// are executed, or when the finalized scheduler is cancelled.
///
/// This ensures that the scheduler returns all batches to the caller before cleaning itself up.
/// Consumers of this trait must ensure that the scheduler is finalized and all batch results have
/// been received (by cancelling and/or waiting for a `None` result on the callback) before dropping
/// the scheduler, otherwise system resources may be leaked.
pub trait Scheduler: Send {
    /// Sets a callback to receive results from processing batches. The order
    /// the results are received is not guarenteed to be the same order as the
    /// batches were added with `add_batch`. If callback is called with None,
    /// all batch results have been sent (only used when the scheduler has been
    /// finalized and no more batches will be added).
    fn set_result_callback(
        &mut self,
        callback: Box<dyn Fn(Option<BatchExecutionResult>) + Send>,
    ) -> Result<(), SchedulerError>;

    /// Sets a callback to receive any errors encountered by the Scheduler that are not related to
    /// a specific batch.
    fn set_error_callback(
        &mut self,
        callback: Box<dyn Fn(SchedulerError) + Send>,
    ) -> Result<(), SchedulerError>;

    /// Adds a BatchPair to the scheduler.
    fn add_batch(&mut self, batch: BatchPair) -> Result<(), SchedulerError>;

    /// Drops any unexecuted batches from this scheduler and immediately aborts any batches that are
    /// currently executing. If already finalized, the scheduler should indicate that no more batch
    /// results will be sent by passing a `None` result to the callback.
    ///
    /// Returns a `Vec` of the unscheduled and aborted `BatchPair`s.
    fn cancel(&mut self) -> Result<Vec<BatchPair>, SchedulerError>;

    /// Finalizes the scheduler, which will disable the ability to add more
    /// batches. After this is called, `add_batch()` will be return a
    /// FinalizedSchedulerError.
    fn finalize(&mut self) -> Result<(), SchedulerError>;

    /// Returns an iterator that returns transactions to be executed.
    fn take_task_iterator(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = ExecutionTask> + Send>, SchedulerError>;

    /// Returns a newly allocated ExecutionTaskCompletionNotifier which allows
    /// sending a notification to the scheduler that indicates the task has
    /// been executed.
    fn new_notifier(&mut self) -> Result<Box<dyn ExecutionTaskCompletionNotifier>, SchedulerError>;
}

/// Creates new schedulers
pub trait SchedulerFactory: Send {
    /// Returns a new scheduler with the given state ID
    fn create_scheduler(&self, state_id: String) -> Result<Box<dyn Scheduler>, SchedulerError>;
}

/// Allows sending a notification to the scheduler that execution of a task
/// has completed.
pub trait ExecutionTaskCompletionNotifier: Send {
    /// Sends a notification to the scheduler.
    fn notify(&self, notification: ExecutionTaskCompletionNotification);

    fn clone_box(&self) -> Box<dyn ExecutionTaskCompletionNotifier>;
}

impl Clone for Box<dyn ExecutionTaskCompletionNotifier> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

fn default_result_callback(batch_result: Option<BatchExecutionResult>) {
    warn!(
        "No result callback set; dropping batch execution result: {}",
        match batch_result {
            Some(ref result) => result.batch.batch().header_signature(),
            None => "None",
        }
    );
}

fn default_error_callback(error: SchedulerError) {
    error!("No error callback set; SchedulerError: {}", error);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::manager::ContextManagerError;
    use crate::context::ContextLifecycle;
    use crate::protocol::batch::BatchBuilder;
    use crate::protocol::receipt::TransactionReceiptBuilder;
    use crate::protocol::transaction::{HashMethod, Transaction, TransactionBuilder};

    use std::sync::mpsc;

    use cylinder::{secp256k1::Secp256k1Context, Context, Signer};

    pub fn mock_transactions(num: u8) -> Vec<Transaction> {
        let signer = new_signer();
        (0..num)
            .map(|i| {
                TransactionBuilder::new()
                    .with_family_name("mock".into())
                    .with_family_version("0.1".into())
                    .with_inputs(vec![])
                    .with_outputs(vec![])
                    .with_nonce(vec![i])
                    .with_payload(vec![])
                    .with_payload_hash_method(HashMethod::SHA512)
                    .build(&*signer)
                    .expect("Failed to build transaction")
            })
            .collect()
    }

    pub fn mock_batch(transactions: Vec<Transaction>) -> BatchPair {
        let signer = new_signer();
        BatchBuilder::new()
            .with_transactions(transactions)
            .build_pair(&*signer)
            .expect("Failed to build batch pair")
    }

    pub fn mock_batch_with_num_txns(num: u8) -> BatchPair {
        mock_batch(mock_transactions(num))
    }

    pub fn mock_batches_with_one_transaction(num_batches: u8) -> Vec<BatchPair> {
        mock_transactions(num_batches)
            .into_iter()
            .map(|txn| mock_batch(vec![txn]))
            .collect()
    }

    fn new_signer() -> Box<dyn Signer> {
        let context = Secp256k1Context::new();
        let key = context.new_random_private_key();
        context.new_signer(key)
    }

    pub fn valid_receipt_from_batch(batch: BatchPair) -> Option<BatchExecutionResult> {
        let receipts = batch
            .batch()
            .transactions()
            .iter()
            .map(|txn| TransactionReceipt {
                transaction_id: txn.header_signature().into(),
                transaction_result: TransactionResult::Valid {
                    state_changes: vec![],
                    events: vec![],
                    data: vec![],
                },
            })
            .collect();
        Some(BatchExecutionResult { batch, receipts })
    }

    pub fn invalid_receipt_from_batch(batch: BatchPair) -> Option<BatchExecutionResult> {
        let receipts = batch
            .batch()
            .transactions()
            .iter()
            .map(|txn| TransactionReceipt {
                transaction_id: txn.header_signature().into(),
                transaction_result: TransactionResult::Invalid {
                    error_message: String::new(),
                    error_data: vec![],
                },
            })
            .collect();
        Some(BatchExecutionResult { batch, receipts })
    }

    pub fn mock_context_id() -> ContextId {
        [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ]
    }

    #[derive(Clone)]
    pub struct MockContextLifecycle {}

    impl MockContextLifecycle {
        pub fn new() -> Self {
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
            transaction_id: &str,
        ) -> Result<TransactionReceipt, ContextManagerError> {
            TransactionReceiptBuilder::new()
                .valid()
                .with_transaction_id(transaction_id.into())
                .build()
                .map_err(|err| ContextManagerError::from(err))
        }

        fn drop_context(&mut self, _context_id: ContextId) {}

        fn clone_box(&self) -> Box<dyn ContextLifecycle> {
            Box::new(self.clone())
        }
    }

    /// Attempt to add a batch to the scheduler; attempt to add the batch again and verify that a
    /// `DuplicateBatch` error is returned. Return the batch so the calling test can verify other
    /// expected behavior.
    pub fn test_scheduler_add_batch(scheduler: &mut dyn Scheduler) -> BatchPair {
        let batch = mock_batch_with_num_txns(1);
        scheduler
            .add_batch(batch.clone())
            .expect("Failed to add batch");
        match scheduler.add_batch(batch.clone()) {
            Err(SchedulerError::DuplicateBatch(batch_id)) => {
                assert_eq!(batch_id, batch.batch().header_signature())
            }
            res => panic!("Did not get DuplicateBatch; got {:?}", res),
        }
        batch
    }

    /// Add two batches to the scheduler, then attempt to cancel it twice; verify that it properly
    /// drains and returns the pending batches.
    pub fn test_scheduler_cancel(scheduler: &mut dyn Scheduler) {
        let batches = mock_batches_with_one_transaction(2);

        scheduler
            .add_batch(batches[0].clone())
            .expect("Failed to add 1st batch");
        scheduler
            .add_batch(batches[1].clone())
            .expect("Failed to add 2nd batch");

        for batch in scheduler.cancel().expect("Failed 1st cancel") {
            assert!(batches.contains(&batch));
        }
        assert!(scheduler.cancel().expect("Failed 2nd cancel").is_empty());
    }

    /// Finalize the scheduler, then verify:
    /// 1. A `None` result is sent by the scheduler, indicating that it is finalized and has
    ///    processed all batches
    /// 2. When attempting to add a batch, a `SchedulerFinalized` error is returned
    /// 3. The scheduler's execution task iterator returns None when next() is called
    pub fn test_scheduler_finalize(scheduler: &mut dyn Scheduler) {
        // Use a channel to pass the result to this test
        let (tx, rx) = mpsc::channel();
        scheduler
            .set_result_callback(Box::new(move |result| {
                tx.send(result).expect("Failed to send result");
            }))
            .expect("Failed to set result callback");
        let mut task_iterator = scheduler
            .take_task_iterator()
            .expect("Failed to get task iterator");

        scheduler.finalize().expect("Failed to finalize");

        assert!(rx.recv().expect("Failed to receive result").is_none());

        match scheduler.add_batch(mock_batch_with_num_txns(1)) {
            Err(SchedulerError::SchedulerFinalized) => (),
            res => panic!("Did not get SchedulerFinalized; got {:?}", res),
        }

        assert!(task_iterator.next().is_none());
    }

    /// Tests a simple scheduler workflow of processing a single transaction; this tests the
    /// scheduler's task iterator, notifier, and result callback functionality.
    ///
    /// For the purposes of this test, we simply return an invalid transaction
    /// as we are not testing the actual execution of the transaction but
    /// rather the flow of getting a result after adding the batch.
    pub fn test_scheduler_flow_with_one_transaction(scheduler: &mut dyn Scheduler) {
        // Use a channel to pass the result to this test
        let (tx, rx) = mpsc::channel();
        scheduler
            .set_result_callback(Box::new(move |result| {
                // The scheduler should be finalized/cancelled by the test that calls this method;
                // when that happens, the scheduler will send a `None` result, but the receiver will
                // already have been dropped.
                if result.is_some() {
                    tx.send(result).expect("Failed to send result");
                }
            }))
            .expect("Failed to set result callback");

        // Add batch to scheduler
        let batch = mock_batch_with_num_txns(1);
        scheduler
            .add_batch(batch.clone())
            .expect("Failed to add batch");

        // Simulate retrieving the execution task, executing it, and sending the notification
        let mut task_iterator = scheduler
            .take_task_iterator()
            .expect("Failed to get task iterator");
        let notifier = scheduler
            .new_notifier()
            .expect("Failed to get new notifier");
        notifier.notify(ExecutionTaskCompletionNotification::Invalid(
            mock_context_id(),
            InvalidTransactionResult {
                transaction_id: task_iterator
                    .next()
                    .expect("Failed to get task")
                    .pair()
                    .transaction()
                    .header_signature()
                    .into(),
                error_message: String::new(),
                error_data: vec![],
            },
        ));

        // Verify that the correct result is returned
        let result = rx.recv().expect("Failed to receive result");
        assert_eq!(result, invalid_receipt_from_batch(batch));
    }

    /// Tests a simple scheduler workflow of processing two batches; this tests the
    /// scheduler's task iterator, notifier, and result callback functionality.
    ///
    /// For the purposes of this test, we simply return an invalid transactions
    /// as we are not testing the actual execution of the transactions but
    /// rather the flow of getting a result after adding the batch.
    ///
    /// Also verifies that a None is sent to the result callback, once all batches are handled
    /// if the scheduler is finalized
    pub fn test_scheduler_flow_with_two_batches(scheduler: &mut dyn Scheduler) {
        // Use a channel to pass the result to this test
        let (tx, rx) = mpsc::channel();
        scheduler
            .set_result_callback(Box::new(move |result| {
                tx.send(result).expect("Failed to send result");
            }))
            .expect("Failed to set result callback");

        // Add batches to scheduler
        let batches = mock_batches_with_one_transaction(2);
        for batch in batches.iter() {
            scheduler
                .add_batch(batch.clone())
                .expect("Failed to add batch");
        }

        // finalize scheduler so a None will be returned when the batches complete
        scheduler.finalize().expect("Unable to finalize scheduler");

        // Simulate retrieving the execution task, executing it, and sending the notification
        let mut task_iterator = scheduler
            .take_task_iterator()
            .expect("Failed to get task iterator");
        let notifier = scheduler
            .new_notifier()
            .expect("Failed to get new notifier");

        // set first batch to invalid
        notifier.notify(ExecutionTaskCompletionNotification::Invalid(
            mock_context_id(),
            InvalidTransactionResult {
                transaction_id: task_iterator
                    .next()
                    .expect("Failed to get task")
                    .pair()
                    .transaction()
                    .header_signature()
                    .into(),
                error_message: String::new(),
                error_data: vec![],
            },
        ));

        // set second batch to invalid
        notifier.notify(ExecutionTaskCompletionNotification::Invalid(
            mock_context_id(),
            InvalidTransactionResult {
                transaction_id: task_iterator
                    .next()
                    .expect("Failed to get task")
                    .pair()
                    .transaction()
                    .header_signature()
                    .into(),
                error_message: String::new(),
                error_data: vec![],
            },
        ));

        // Verify that the correct result is returned for each batch
        for batch in batches {
            let result = rx.recv().expect("Failed to receive result");
            assert_eq!(result, invalid_receipt_from_batch(batch));
        }
        // Verify that None is returned since all batches have been completed
        let result = rx.recv().expect("Failed to receive result");
        assert_eq!(result, None);
    }

    /// Tests a simple scheduler workflow of processing a single batch with three transactions.
    pub fn test_scheduler_flow_with_multiple_transactions(scheduler: &mut dyn Scheduler) {
        // Use a channel to pass the result to this test
        let (tx, rx) = mpsc::channel();
        scheduler
            .set_result_callback(Box::new(move |result| {
                // The scheduler should be finalized/cancelled by the test that calls this method;
                // when that happens, the scheduler will send a `None` result, but the receiver will
                // already have been dropped.
                if result.is_some() {
                    tx.send(result).expect("Failed to send result");
                }
            }))
            .expect("Failed to set result callback");

        // Add batch to scheduler
        let original_batch = mock_batch_with_num_txns(3);
        scheduler
            .add_batch(original_batch.clone())
            .expect("Failed to add batch");

        // Simulate retrieving the execution task, executing it, and sending the notification
        let mut task_iterator = scheduler
            .take_task_iterator()
            .expect("Failed to get task iterator");
        let notifier = scheduler
            .new_notifier()
            .expect("Failed to get new notifier");
        notifier.notify(ExecutionTaskCompletionNotification::Valid(
            mock_context_id(),
            task_iterator
                .next()
                .expect("Failed to get task")
                .pair()
                .transaction()
                .header_signature()
                .into(),
        ));
        notifier.notify(ExecutionTaskCompletionNotification::Valid(
            mock_context_id(),
            task_iterator
                .next()
                .expect("Failed to get task")
                .pair()
                .transaction()
                .header_signature()
                .into(),
        ));
        notifier.notify(ExecutionTaskCompletionNotification::Valid(
            mock_context_id(),
            task_iterator
                .next()
                .expect("Failed to get task")
                .pair()
                .transaction()
                .header_signature()
                .into(),
        ));

        // Verify that the correct result is returned; can't just compare result itself, since the
        // order of transactions in the result is unknown.
        let BatchExecutionResult { batch, receipts } = rx
            .recv()
            .expect("Failed to receive result")
            .expect("Got None result");
        assert_eq!(batch, original_batch);

        let original_batch_txn_ids = original_batch
            .batch()
            .transactions()
            .iter()
            .map(|txn| txn.header_signature())
            .collect::<Vec<_>>()
            .sort_unstable();
        let receipt_txn_ids = receipts
            .iter()
            .map(|receipt| match &receipt.transaction_result {
                TransactionResult::Valid { .. } => &receipt.transaction_id,
                res => panic!("Did not get valid receipt; got {:?}", res),
            })
            .collect::<Vec<_>>()
            .sort_unstable();
        assert_eq!(original_batch_txn_ids, receipt_txn_ids);
    }

    /// Process a batch with multiple transactions, one of which is invalid; verify that the
    /// scheduler invalidates the entire batch and returns the appropriate result.
    pub fn test_scheduler_invalid_transaction_invalidates_batch(scheduler: &mut dyn Scheduler) {
        // Use a channel to pass the result to this test
        let (tx, rx) = mpsc::channel();
        scheduler
            .set_result_callback(Box::new(move |result| {
                // The scheduler should be finalized/cancelled by the test that calls this method;
                // when that happens, the scheduler will send a `None` result, but the receiver will
                // already have been dropped.
                if result.is_some() {
                    tx.send(result).expect("Failed to send result");
                }
            }))
            .expect("Failed to set result callback");

        // Add batch with 3 transactions to scheduler
        let original_batch = mock_batch_with_num_txns(3);
        scheduler
            .add_batch(original_batch.clone())
            .expect("Failed to add batch");

        // Simulate retrieving the execution tasks, executing them, and sending the notifications.
        let mut task_iterator = scheduler
            .take_task_iterator()
            .expect("Failed to get task iterator");
        let notifier = scheduler
            .new_notifier()
            .expect("Failed to get new notifier");

        notifier.notify(ExecutionTaskCompletionNotification::Valid(
            mock_context_id(),
            task_iterator
                .next()
                .expect("Failed to get task")
                .pair()
                .transaction()
                .header_signature()
                .into(),
        ));
        notifier.notify(ExecutionTaskCompletionNotification::Invalid(
            mock_context_id(),
            InvalidTransactionResult {
                transaction_id: task_iterator
                    .next()
                    .expect("Failed to get task")
                    .pair()
                    .transaction()
                    .header_signature()
                    .into(),
                error_message: String::new(),
                error_data: vec![],
            },
        ));
        // Don't actually get the 3rd task; the scheduler should have invalidated the whole batch
        // and sent the result already, so the 3rd transaction won't be in the iterator.

        let BatchExecutionResult { batch, receipts } = rx
            .recv()
            .expect("Failed to receive result")
            .expect("Got None result");
        assert_eq!(batch, original_batch);

        let original_batch_txn_ids = original_batch
            .batch()
            .transactions()
            .iter()
            .map(|txn| txn.header_signature())
            .collect::<Vec<_>>()
            .sort_unstable();
        let receipt_txn_ids = receipts
            .iter()
            .map(|receipt| match &receipt.transaction_result {
                TransactionResult::Invalid { .. } => &receipt.transaction_id,
                res => panic!("Did not get invalid receipt; got {:?}", res),
            })
            .collect::<Vec<_>>()
            .sort_unstable();
        assert_eq!(original_batch_txn_ids, receipt_txn_ids);
    }
}

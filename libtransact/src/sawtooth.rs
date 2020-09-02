// Copyright 2019 Cargill Incorporated
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Sawtooth Compatibility Layer
//!
//! This module provides a compatibility layer for use with [Hyperledger
//! Sawtooth](https://sawtooth.hyperledger.org) transaction families.  It provides adapters to
//! allow the use of existing Sawtooth transaction families, implemented with the [Rust
//! SDK](https://crates.io/crates/sawtooth-sdk), in an application built with Transact.
//!
//! Note, to use this module, the Transact library must have the `"sawtooth-compat"` feature
//! enabled.

use sawtooth_sdk::messages::processor::TpProcessRequest;
use sawtooth_sdk::messages::transaction::TransactionHeader as SawtoothTxnHeader;
use sawtooth_sdk::processor::handler::{
    ApplyError as SawtoothApplyError, ContextError as SawtoothContextError,
    TransactionContext as SawtoothContext, TransactionHandler as SawtoothTransactionHandler,
};

use crate::handler::{ApplyError, ContextError, TransactionContext, TransactionHandler};
use crate::protocol::transaction::{TransactionHeader, TransactionPair};

/// Adapts a Sawtooth Transaction Handler to a Transact TransactionHandler.
///
/// This adapter allows Sawtooth SDK TransactionHandler implementations to be used with the
/// Transact static execution adapters.  Existing Sawtooth transaction families and smart contract
/// engines can then be compiled in to an application using Transact.
///
/// For example, the Sawtooth transaction handler for the [XO Transaction
/// Family](https://sawtooth.hyperledger.org/docs/core/releases/latest/transaction_family_specifications/xo_transaction_family.html)
/// can be adapted as follows:
///
///     # use sawtooth_xo::handler::XoTransactionHandler;
///     # use transact::context::manager::sync::ContextManager;
///     # use transact::database::btree::BTreeDatabase;
///     # use transact::execution::adapter::static_adapter::StaticExecutionAdapter;
///     # use transact::sawtooth::SawtoothToTransactHandlerAdapter;
///     # use transact::state::merkle::{self, MerkleRadixTree, MerkleState};
///     #
///     # let db = Box::new(BTreeDatabase::new(&merkle::INDEXES));
///     # let context_manager = ContextManager::new(Box::new(MerkleState::new(db.clone())));
///     let execution_adapter = StaticExecutionAdapter::new_adapter(
///         vec![Box::new(SawtoothToTransactHandlerAdapter::new(
///             XoTransactionHandler::new(),
///         ))],
///         context_manager,
///     );
///     # let _ignore = execution_adapter;
pub struct SawtoothToTransactHandlerAdapter<H: SawtoothTransactionHandler + Send> {
    family_name: String,
    family_versions: Vec<String>,
    handler: H,
}

impl<H: SawtoothTransactionHandler + Send> SawtoothToTransactHandlerAdapter<H> {
    /// Constructs a new Sawtooth to Transact handler adapter.
    pub fn new(handler: H) -> Self {
        SawtoothToTransactHandlerAdapter {
            family_name: handler.family_name(),
            family_versions: handler.family_versions(),
            handler,
        }
    }
}

impl<H: SawtoothTransactionHandler + Send> TransactionHandler
    for SawtoothToTransactHandlerAdapter<H>
{
    fn family_name(&self) -> &str {
        &self.family_name
    }

    fn family_versions(&self) -> &[String] {
        &self.family_versions
    }

    fn apply(
        &self,
        transaction_pair: &TransactionPair,
        context: &mut dyn TransactionContext,
    ) -> Result<(), ApplyError> {
        let request = txn_pair_to_process_request(transaction_pair);
        let mut context_adapter = TransactToSawtoothContextAdapter::new(context);
        self.handler
            .apply(&request, &mut context_adapter)
            .map_err(|err| match err {
                SawtoothApplyError::InvalidTransaction(error_message) => {
                    ApplyError::InvalidTransaction(error_message)
                }
                SawtoothApplyError::InternalError(error_message) => {
                    ApplyError::InternalError(error_message)
                }
            })
    }
}

struct TransactToSawtoothContextAdapter<'a> {
    transact_context: &'a dyn TransactionContext,
}

impl<'a> TransactToSawtoothContextAdapter<'a> {
    fn new(transact_context: &'a dyn TransactionContext) -> Self {
        TransactToSawtoothContextAdapter { transact_context }
    }
}

impl<'a> SawtoothContext for TransactToSawtoothContextAdapter<'a> {
    fn get_state_entry(&self, address: &str) -> Result<Option<Vec<u8>>, SawtoothContextError> {
        let results = self
            .transact_context
            .get_state_entries(&[address.to_owned()])
            .map_err(to_context_error)?;

        // take the first item, if it exists
        Ok(results.into_iter().next().map(|(_, v)| v))
    }

    fn get_state_entries(
        &self,
        addresses: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, SawtoothContextError> {
        self.transact_context
            .get_state_entries(addresses)
            .map_err(to_context_error)
    }

    fn set_state_entry(&self, address: String, data: Vec<u8>) -> Result<(), SawtoothContextError> {
        self.set_state_entries(vec![(address, data)])
    }

    fn set_state_entries(
        &self,
        entries: Vec<(String, Vec<u8>)>,
    ) -> Result<(), SawtoothContextError> {
        self.transact_context
            .set_state_entries(entries)
            .map_err(to_context_error)
    }

    fn delete_state_entry(&self, address: &str) -> Result<Option<String>, SawtoothContextError> {
        Ok(self
            .delete_state_entries(&[address.to_owned()])?
            .into_iter()
            .next())
    }

    fn delete_state_entries(
        &self,
        addresses: &[String],
    ) -> Result<Vec<String>, SawtoothContextError> {
        self.transact_context
            .delete_state_entries(addresses)
            .map_err(to_context_error)
    }

    fn add_receipt_data(&self, data: &[u8]) -> Result<(), SawtoothContextError> {
        self.transact_context
            .add_receipt_data(data.to_vec())
            .map_err(to_context_error)
    }

    fn add_event(
        &self,
        event_type: String,
        attributes: Vec<(String, String)>,
        data: &[u8],
    ) -> Result<(), SawtoothContextError> {
        self.transact_context
            .add_event(event_type, attributes, data.to_vec())
            .map_err(to_context_error)
    }
}

fn txn_pair_to_process_request(transaction_pair: &TransactionPair) -> TpProcessRequest {
    let mut process_request = TpProcessRequest::new();

    let header = as_sawtooth_header(transaction_pair.header());
    process_request.set_header(header);

    let txn = transaction_pair.transaction();
    process_request.set_payload(txn.payload().to_vec());
    process_request.set_signature(txn.header_signature().to_owned());

    process_request
}

fn as_sawtooth_header(header: &TransactionHeader) -> SawtoothTxnHeader {
    let mut sawtooth_header = SawtoothTxnHeader::new();

    sawtooth_header.set_family_name(header.family_name().to_owned());
    sawtooth_header.set_family_version(header.family_version().to_owned());
    sawtooth_header.set_signer_public_key(hex::encode(&header.signer_public_key()));
    sawtooth_header.set_batcher_public_key(hex::encode(&header.batcher_public_key()));
    sawtooth_header.set_dependencies(header.dependencies().iter().map(hex::encode).collect());
    sawtooth_header.set_inputs(header.inputs().iter().map(hex::encode).collect());
    sawtooth_header.set_outputs(header.outputs().iter().map(hex::encode).collect());
    sawtooth_header.set_nonce(hex::encode(&header.nonce()));

    sawtooth_header
}

fn to_context_error(err: ContextError) -> SawtoothContextError {
    SawtoothContextError::ReceiveError(Box::new(err))
}

#[cfg(test)]
mod xo_compat_test {
    use std::panic;

    use cylinder::{secp256k1::Secp256k1Context, Context, Signer};
    use sawtooth_xo::handler::XoTransactionHandler;
    use sha2::{Digest, Sha512};

    use crate::context::manager::sync::ContextManager;
    use crate::database::{btree::BTreeDatabase, Database};
    use crate::execution::{
        adapter::static_adapter::StaticExecutionAdapter,
        executor::{ExecutionTaskSubmitter, Executor},
    };
    use crate::protocol::{
        batch::{BatchBuilder, BatchPair},
        receipt::{StateChange, TransactionResult},
        transaction::{HashMethod, TransactionBuilder},
    };
    use crate::scheduler::{serial::SerialScheduler, BatchExecutionResult, Scheduler};
    use crate::state::merkle::{self, MerkleRadixTree, MerkleState};

    use super::*;

    /// Test that the compatibility handler executes a create game transaction.
    ///
    /// #. Configure an executor with the XoTransactionHandler
    /// #. Create a scheduler and add a single transaction to create an XO game
    /// #. Wait until the result is returned
    /// #. Verify that the result is a) valid and b) has the appropriate state changes
    #[test]
    fn execute_create_xo_game() {
        let db = Box::new(BTreeDatabase::new(&merkle::INDEXES));
        let context_manager = ContextManager::new(Box::new(MerkleState::new(db.clone())));

        let mut executor = create_executor(&context_manager);
        executor.start().expect("Start should not have failed");

        let task_submitter = executor
            .execution_task_submitter()
            .expect("Unable to get task submitter form started executor");

        let signer = new_signer();

        let batch_pair = create_batch(&*signer, "my_game", "my_game,create,");

        let state_root = initial_db_root(&*db);

        let mut scheduler = SerialScheduler::new(Box::new(context_manager), state_root.clone())
            .expect("Failed to create scheduler");

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        scheduler
            .set_result_callback(Box::new(move |batch_result| {
                result_tx
                    .send(batch_result)
                    .expect("Unable to send batch result")
            }))
            .expect("Failed to set result callback");

        scheduler
            .add_batch(batch_pair)
            .expect("Failed to add batch");
        scheduler.finalize().expect("Failed to finalize scheduler");

        run_schedule(&task_submitter, &mut scheduler);

        let batch_result = result_rx
            .recv()
            .expect("Unable to receive result from executor")
            .expect("Should not have received None from the executor");

        assert_state_changes(
            vec![StateChange::Set {
                key: calculate_game_address("my_game"),
                value: "my_game,---------,P1-NEXT,,".as_bytes().to_vec(),
            }],
            batch_result,
        );

        executor.stop();
    }

    ///
    /// Test that the compatibility handler executes multiple transactions and returns the correct
    /// receipts with the expected state changes.
    ///
    /// #. Configure an executor with the XoTransactionHandler
    /// #. Create a scheduler and add transactions to create an XO game and take a space
    /// #. Wait until the result is returned
    /// #. Verify that the result is a) valid and b) has the appropriate state changes
    #[test]
    fn execute_multiple_xo_transactions() {
        let db = Box::new(BTreeDatabase::new(&merkle::INDEXES));
        let context_manager = ContextManager::new(Box::new(MerkleState::new(db.clone())));

        let mut executor = create_executor(&context_manager);
        executor.start().expect("Start should not have failed");

        let task_submitter = executor
            .execution_task_submitter()
            .expect("Unable to get task submitter form started executor");

        let signer = new_signer();

        let create_batch_pair = create_batch(&*signer, "my_game", "my_game,create,");
        let take_batch_pair = create_batch(&*signer, "my_game", "my_game,take,1");

        let state_root = initial_db_root(&*db);

        let mut scheduler = SerialScheduler::new(Box::new(context_manager), state_root.clone())
            .expect("Failed to create scheduler");

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        scheduler
            .set_result_callback(Box::new(move |batch_result| {
                result_tx
                    .send(batch_result)
                    .expect("Unable to send batch result")
            }))
            .expect("Failed to set result callback");

        scheduler
            .add_batch(create_batch_pair)
            .expect("Failed to add 1st batch");
        scheduler
            .add_batch(take_batch_pair)
            .expect("Failed to add 2nd batch");
        scheduler.finalize().expect("Failed to finalize scheduler");

        run_schedule(&task_submitter, &mut scheduler);

        let create_batch_result = result_rx
            .recv()
            .expect("Unable to receive result from executor")
            .expect("Should not have received None from the executor");

        let take_batch_result = result_rx
            .recv()
            .expect("Unable to receive result from executor")
            .expect("Should not have received None from the executor");

        assert_state_changes(
            vec![StateChange::Set {
                key: calculate_game_address("my_game"),
                value: "my_game,---------,P1-NEXT,,".as_bytes().to_vec(),
            }],
            create_batch_result,
        );

        assert_state_changes(
            vec![StateChange::Set {
                key: calculate_game_address("my_game"),
                value: format!(
                    "my_game,X--------,P2-NEXT,{},",
                    signer
                        .public_key()
                        .expect("Failed to get public key")
                        .as_hex(),
                )
                .into_bytes(),
            }],
            take_batch_result,
        );

        executor.stop();
    }

    fn assert_state_changes(
        expected_state_changes: Vec<StateChange>,
        batch_result: BatchExecutionResult,
    ) {
        assert_eq!(1, batch_result.receipts.len());

        let mut batch_result = batch_result;

        let receipt = batch_result
            .receipts
            .pop()
            .expect("Length 1, but no first element");
        match receipt.transaction_result {
            TransactionResult::Valid { state_changes, .. } => {
                assert_eq!(expected_state_changes, state_changes)
            }
            TransactionResult::Invalid { .. } => panic!("transaction failed"),
        }
    }

    fn create_batch(signer: &dyn Signer, game_name: &str, payload: &str) -> BatchPair {
        let game_address = calculate_game_address(game_name);
        let txn_pair = TransactionBuilder::new()
            .with_family_name("xo".to_string())
            .with_family_version("1.0".to_string())
            .with_inputs(vec![hex::decode(&game_address).unwrap()])
            .with_nonce("test_nonce".as_bytes().to_vec())
            .with_outputs(vec![hex::decode(&game_address).unwrap()])
            .with_payload_hash_method(HashMethod::SHA512)
            .with_payload(payload.as_bytes().to_vec())
            .build_pair(signer)
            .expect("The TransactionBuilder was not given the correct items");

        BatchBuilder::new()
            .with_transactions(vec![txn_pair.take().0])
            .build_pair(signer)
            .expect("Unable to build batch a pair")
    }

    fn create_executor(context_manager: &ContextManager) -> Executor {
        Executor::new(vec![Box::new(
            StaticExecutionAdapter::new_adapter(
                vec![Box::new(SawtoothToTransactHandlerAdapter::new(
                    XoTransactionHandler::new(),
                ))],
                context_manager.clone(),
            )
            .expect("Unable to create static execution adapter"),
        )])
    }

    fn run_schedule(task_submitter: &ExecutionTaskSubmitter, scheduler: &mut dyn Scheduler) {
        let task_iterator = scheduler
            .take_task_iterator()
            .expect("Failed to take task iterator");
        task_submitter
            .submit(
                task_iterator,
                scheduler.new_notifier().expect("Failed to get notifier"),
            )
            .expect("Failed to execute schedule");
    }

    fn calculate_game_address<S: AsRef<[u8]>>(name: S) -> String {
        let mut sha = Sha512::default();
        sha.input(name);
        "5b7349".to_owned() + &hex::encode(&sha.result())[..64]
    }

    fn initial_db_root(db: &dyn Database) -> String {
        let merkle_db =
            MerkleRadixTree::new(db.clone_box(), None).expect("Cannot initialize merkle database");

        merkle_db.get_merkle_root()
    }

    fn new_signer() -> Box<dyn Signer> {
        let context = Secp256k1Context::new();
        let key = context.new_random_private_key();
        context.new_signer(key)
    }
}

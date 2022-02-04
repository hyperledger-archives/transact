/*
 * Copyright 2021 Cargill Incorporated
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
 * ------------------------------------------------------------------------------
 */

//! Implementations of the `BatchWorkload` and `TransactionWorkload` traits for the command family.

mod command_iter;

use cylinder::Signer;

use crate::error::InvalidStateError;
use crate::protocol::{
    batch::{BatchBuilder, BatchPair},
    command::{Command, CommandPayload},
    sabre::payload::ExecuteContractActionBuilder,
    transaction::TransactionPair,
};
use crate::protos::IntoBytes;
use crate::workload::{BatchWorkload, ExpectedBatchResult, TransactionWorkload};

#[cfg(feature = "family-command-transaction-builder")]
use super::transaction_builder::CommandTransactionBuilder as CmdTransactionBuilder;

pub use crate::families::command::workload::command_iter::CommandGeneratingIter;

/// A transaction workload that generates signed `command` transactions.
pub struct CommandTransactionWorkload {
    generator: CommandGeneratingIter,
    signer: Box<dyn Signer>,
}

impl CommandTransactionWorkload {
    /// Create a new [CommandTransactionWorkload]
    ///
    /// # Arguments
    ///
    /// * `generator` - An iterator that generates `command`s from the command family
    /// * `signer` - Used to sign the generated transactions
    pub fn new(generator: CommandGeneratingIter, signer: Box<dyn Signer>) -> Self {
        Self { generator, signer }
    }
}

/// An implementation of the `TransactionWorkload` trait for command family.
impl TransactionWorkload for CommandTransactionWorkload {
    /// Create a new signed `command` transaction. Returns the `TransactionPair` and the expected
    /// result after the transaction is executed.
    fn next_transaction(
        &mut self,
    ) -> Result<(TransactionPair, Option<ExpectedBatchResult>), InvalidStateError> {
        let (command, address) = self
            .generator
            .next()
            .ok_or_else(|| InvalidStateError::with_message("No command available".to_string()))?;

        let command_payload = CommandPayload::new(vec![command.clone()]);

        let payload_bytes = command_payload
            .into_bytes()
            .expect("Unable to get bytes from Command Payload");

        let expected_batch_result = match &command {
            Command::ReturnInvalid(_) => Some(ExpectedBatchResult::Invalid),
            _ => Some(ExpectedBatchResult::Valid),
        };

        let addresses = match command {
            Command::SetState(set_state) => set_state
                .state_writes()
                .iter()
                .map(|b| String::from(b.key()))
                .collect::<Vec<String>>(),
            Command::DeleteState(delete_state) => delete_state.state_keys().to_vec(),
            Command::GetState(get_state) => get_state.state_keys().to_vec(),
            _ => vec![address],
        };

        let txn = ExecuteContractActionBuilder::new()
            .with_name(String::from("command"))
            .with_version(String::from("1.0"))
            .with_inputs(addresses.clone())
            .with_outputs(addresses)
            .with_payload(payload_bytes)
            .into_payload_builder()
            .map_err(|err| {
                InvalidStateError::with_message(format!(
                    "Unable to convert execute action into sabre payload: {}",
                    err
                ))
            })?
            .into_transaction_builder()
            .map_err(|err| {
                InvalidStateError::with_message(format!(
                    "Unable to convert execute payload into transaction: {}",
                    err
                ))
            })?
            .build_pair(&*self.signer)
            .map_err(|err| {
                InvalidStateError::with_message(format!(
                    "Failed to build transaction pair: {}",
                    err
                ))
            })?;

        Ok((txn, expected_batch_result))
    }
}

/// A batch workload that generates signed batches that contain `command` transactions.
pub struct CommandBatchWorkload {
    transaction_workload: CommandTransactionWorkload,
    signer: Box<dyn Signer>,
}

impl CommandBatchWorkload {
    /// Create a new [CommandBatchWorkload]
    ///
    /// # Arguments
    ///
    /// * `transaction_workload` - A [CommandTransactionWorkload] that generates command
    ///   transactions
    /// * `signer` - Used to sign the generated batches
    pub fn new(transaction_workload: CommandTransactionWorkload, signer: Box<dyn Signer>) -> Self {
        Self {
            transaction_workload,
            signer,
        }
    }
}

/// An implementation of the `BatchWorkload` trait for command family.
impl BatchWorkload for CommandBatchWorkload {
    /// Create a new signed `command` batch. Returns the `BatchPair` and the expected result after
    /// the batch is submitted.
    fn next_batch(
        &mut self,
    ) -> Result<(BatchPair, Option<ExpectedBatchResult>), InvalidStateError> {
        let (txn, result) = self.transaction_workload.next_transaction()?;
        Ok((
            BatchBuilder::new()
                .with_transactions(vec![txn.take().0])
                .build_pair(&*self.signer)
                .map_err(|err| {
                    InvalidStateError::with_message(format!("Failed to build batch pair: {}", err))
                })?,
            result,
        ))
    }
}

#[cfg(feature = "family-command-transaction-builder")]
#[deprecated(
    since = "0.4.1",
    note = "Please use families::command::CommandTransactionBuilder"
)]
pub type CommandTransactionBuilder = CmdTransactionBuilder;

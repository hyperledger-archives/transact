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

pub mod playlist;

use cylinder::Signer;

use crate::protocol::{
    batch::{BatchBuilder, BatchPair},
    command::{Command, CommandPayload},
    sabre::ExecuteContractActionBuilder,
    transaction::{HashMethod, TransactionBuilder, TransactionPair},
};
use crate::protos::{FromProto, IntoBytes};
use crate::workload::{error::WorkloadError, BatchWorkload, TransactionWorkload};

use self::playlist::CommandGeneratingIter;

pub struct CommandTransactionWorkload {
    generator: CommandGeneratingIter,
    signer: Box<dyn Signer>,
}

impl CommandTransactionWorkload {
    pub fn new(generator: CommandGeneratingIter, signer: Box<dyn Signer>) -> Self {
        Self { generator, signer }
    }
}

impl TransactionWorkload for CommandTransactionWorkload {
    fn next_transaction(&mut self) -> Result<TransactionPair, WorkloadError> {
        let (command_proto, address) = self
            .generator
            .next()
            .ok_or_else(|| WorkloadError::InvalidState("No command available".to_string()))?;

        let command = Command::from_proto(command_proto).map_err(|_| {
            WorkloadError::InvalidState("Unable to convert from command proto".to_string())
        })?;

        let command_payload = CommandPayload::new(vec![command.clone()]);

        let payload_bytes = command_payload
            .into_bytes()
            .expect("Unable to get bytes from Command Payload");

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
                WorkloadError::InvalidState(format!(
                    "Unable to convert execute action into sabre payload: {}",
                    err
                ))
            })?
            .into_transaction_builder()
            .map_err(|err| {
                WorkloadError::InvalidState(format!(
                    "Unable to convert execute payload into transaction: {}",
                    err
                ))
            })?
            .build_pair(&*self.signer)?;

        Ok(txn)
    }
}

pub struct CommandBatchWorkload {
    transaction_workload: CommandTransactionWorkload,
    signer: Box<dyn Signer>,
}

impl CommandBatchWorkload {
    pub fn new(transaction_workload: CommandTransactionWorkload, signer: Box<dyn Signer>) -> Self {
        Self {
            transaction_workload,
            signer,
        }
    }
}

impl BatchWorkload for CommandBatchWorkload {
    fn next_batch(&mut self) -> Result<BatchPair, WorkloadError> {
        Ok(BatchBuilder::new()
            .with_transactions(vec![self.transaction_workload.next_transaction()?.take().0])
            .build_pair(&*self.signer)?)
    }
}

pub fn make_command_transaction(commands: &[Command], signer: &dyn Signer) -> TransactionPair {
    let command_payload = CommandPayload::new(commands.to_vec());
    TransactionBuilder::new()
        .with_batcher_public_key(vec![0u8, 0u8, 0u8, 0u8])
        .with_family_name(String::from("command"))
        .with_family_version(String::from("1"))
        .with_inputs(
            commands
                .iter()
                .map(|cmd| match cmd {
                    Command::SetState(set_state) => Some(
                        set_state
                            .state_writes()
                            .iter()
                            .flat_map(|b| b.key().as_bytes().to_vec())
                            .collect(),
                    ),
                    Command::DeleteState(delete_state) => Some(
                        delete_state
                            .state_keys()
                            .to_vec()
                            .iter()
                            .flat_map(|k| k.as_bytes().to_vec())
                            .collect(),
                    ),
                    _ => None,
                })
                .filter(Option::is_some)
                .flatten()
                .collect(),
        )
        .with_outputs(
            commands
                .iter()
                .map(|cmd| match cmd {
                    Command::SetState(set_state) => Some(
                        set_state
                            .state_writes()
                            .iter()
                            .flat_map(|b| b.key().as_bytes().to_vec())
                            .collect(),
                    ),
                    Command::DeleteState(delete_state) => Some(
                        delete_state
                            .state_keys()
                            .to_vec()
                            .iter()
                            .flat_map(|k| k.as_bytes().to_vec())
                            .collect(),
                    ),
                    _ => None,
                })
                .filter(Option::is_some)
                .flatten()
                .collect(),
        )
        .with_payload_hash_method(HashMethod::Sha512)
        .with_payload(
            command_payload
                .into_bytes()
                .expect("Unable to get bytes from Command Payload"),
        )
        .build_pair(signer)
        .unwrap()
}

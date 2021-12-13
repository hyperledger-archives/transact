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

use cylinder::Signer;

use crate::error::InvalidStateError;
use crate::protocol::{
    command::{Command, CommandPayload},
    transaction::{HashMethod, TransactionBuilder, TransactionPair},
};
use crate::protos::IntoBytes;

/// Builds a `command` tansaction with the list of commands stored in `commands`
#[derive(Default)]
pub struct CommandTransactionBuilder {
    commands: Option<Vec<Command>>,
}

impl CommandTransactionBuilder {
    /// Create a new [CommandTransactionBuilder]
    pub fn new() -> Self {
        CommandTransactionBuilder::default()
    }

    /// Set the `commands` field of the [CommandTransactionBuilder]
    ///
    /// # Arguments
    ///
    /// * `commands` - The list of commands that will in the final transaction
    pub fn with_commands(mut self, commands: Vec<Command>) -> Self {
        self.commands = Some(commands);
        self
    }

    /// Create a `TransactionBuilder` with the list of commands stored in the `commands` field
    pub fn into_transaction_builder(self) -> Result<TransactionBuilder, InvalidStateError> {
        let commands_vec = self.commands.ok_or_else(|| {
            InvalidStateError::with_message("'commands' field is required".to_string())
        })?;

        let commands = commands_vec.as_slice();

        let command_payload = CommandPayload::new(commands.to_vec())
            .into_bytes()
            .map_err(|_| {
                InvalidStateError::with_message(
                    "Failed to convert command payload to bytes".to_string(),
                )
            })?;

        Ok(TransactionBuilder::new()
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
            .with_payload(command_payload))
    }

    /// Create a `TransactionPair` signed by the given signer
    pub fn build_pair(self, signer: &dyn Signer) -> Result<TransactionPair, InvalidStateError> {
        self.into_transaction_builder()?
            .build_pair(signer)
            .map_err(|_| {
                InvalidStateError::with_message("Failed build transaction pair".to_string())
            })
    }
}

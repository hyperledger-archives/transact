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
//! A basic Command Transaction Family
use std::error::Error;

use crate::handler::{ApplyError, TransactionContext, TransactionHandler};
use crate::protocol::transaction::{HashMethod, TransactionBuilder, TransactionPair};
use crate::signing::hash::HashSigner;

const COMMAND_FAMILY_NAME: &str = "command";
const COMMAND_VERSION: &str = "0.1";

#[derive(Default)]
pub struct CommandTransactionHandler {
    versions: Vec<String>,
}

impl CommandTransactionHandler {
    pub fn new() -> Self {
        CommandTransactionHandler {
            versions: vec![COMMAND_VERSION.to_owned()],
        }
    }
}

impl TransactionHandler for CommandTransactionHandler {
    fn family_name(&self) -> &str {
        COMMAND_FAMILY_NAME
    }

    fn family_versions(&self) -> &[String] {
        &self.versions
    }

    fn apply(
        &self,
        transaction_pair: &TransactionPair,
        context: &mut dyn TransactionContext,
    ) -> Result<(), ApplyError> {
        let commands = parse_commands(transaction_pair.transaction().payload())
            .map_err(|err| ApplyError::InvalidTransaction(err.to_string()))?;

        for command in commands.into_iter() {
            match command {
                Command::Set { address, value } => {
                    context.set_state_entry(address, value)?;
                }
                Command::Delete { address } => {
                    context.delete_state_entry(&address)?;
                }
                Command::Get { address } => {
                    context.get_state_entry(&address)?;
                }
                Command::Fail { error_msg } => {
                    return Err(ApplyError::InvalidTransaction(error_msg));
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum Command {
    Set { address: String, value: Vec<u8> },
    Delete { address: String },
    Get { address: String },
    Fail { error_msg: String },
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Command::Set {
                ref address,
                ref value,
            } => write!(f, "set,{},{}", address, hex::encode(value)),
            Command::Get { ref address } => write!(f, "get,{}", address),
            Command::Delete { ref address } => write!(f, "del,{}", address),
            Command::Fail { ref error_msg } => write!(f, "fail,{}", error_msg),
        }
    }
}

pub fn make_command_transaction(commands: &[Command]) -> TransactionPair {
    let signer = HashSigner::new();
    TransactionBuilder::new()
        .with_batcher_public_key(vec![0u8, 0u8, 0u8, 0u8])
        .with_family_name(COMMAND_FAMILY_NAME.to_owned())
        .with_family_version(COMMAND_VERSION.to_owned())
        .with_inputs(
            commands
                .iter()
                .map(|cmd| match cmd {
                    Command::Set { address, .. } | Command::Delete { address } => {
                        Some(address.as_bytes().to_vec())
                    }
                    _ => None,
                })
                .filter(Option::is_some)
                .map(Option::unwrap)
                .collect(),
        )
        .with_outputs(
            commands
                .iter()
                .map(|cmd| match cmd {
                    Command::Set { address, .. } | Command::Delete { address } => {
                        Some(address.as_bytes().to_vec())
                    }
                    _ => None,
                })
                .filter(Option::is_some)
                .map(Option::unwrap)
                .collect(),
        )
        .with_payload_hash_method(HashMethod::SHA512)
        .with_payload(
            commands
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("|")
                .into_bytes(),
        )
        .build_pair(&signer)
        .unwrap()
}

fn parse_commands(payload: &[u8]) -> Result<Vec<Command>, ParseCommandError> {
    std::str::from_utf8(payload)
        .map_err(|err| ParseCommandError(format!("Payload not valid utf8 bytes: {}", err)))?
        .split("|")
        .map(|s| s.parse())
        .collect::<Result<Vec<Command>, ParseCommandError>>()
}

impl std::str::FromStr for Command {
    type Err = ParseCommandError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut command_parts = s.split(',');

        let command = command_parts.next().map(|s| s.to_lowercase());
        match command {
            Some(ref set) if set == "set" => {
                let address = command_parts
                    .next()
                    .map(|s| s.to_owned())
                    .ok_or_else(|| ParseCommandError("Cannot set without address".into()))?;

                Ok(Command::Set {
                    address,
                    value: command_parts
                        .next()
                        .ok_or_else(|| ParseCommandError("Cannot set without a value".into()))
                        .and_then(|v| {
                            if v.is_empty() {
                                Ok(vec![])
                            } else {
                                hex::decode(v).map_err(|err| {
                                    ParseCommandError(format!("Invalid hex: {}", err))
                                })
                            }
                        })?,
                })
            }
            Some(ref del) if del == "del" => {
                let address = command_parts
                    .next()
                    .map(|s| s.to_owned())
                    .ok_or_else(|| ParseCommandError("Cannot delete without address".into()))?;

                Ok(Command::Delete { address })
            }
            Some(ref get) if get == "get" => {
                let address = command_parts
                    .next()
                    .map(|s| s.to_owned())
                    .ok_or_else(|| ParseCommandError("Cannot get without address".into()))?;

                Ok(Command::Get { address })
            }
            Some(ref fail) if fail == "fail" => {
                let error_msg = command_parts
                    .next()
                    .map(|s| s.to_owned())
                    .unwrap_or("".to_owned());

                Ok(Command::Fail { error_msg })
            }
            Some(cmd) => Err(ParseCommandError(format!("No command {} supported", cmd))),
            None => Err(ParseCommandError("No command included".into())),
        }
    }
}

#[derive(Debug)]
pub struct ParseCommandError(String);

impl Error for ParseCommandError {}

impl std::fmt::Display for ParseCommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Unable to parse command: {}", self.0)
    }
}

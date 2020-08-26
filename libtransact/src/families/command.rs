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

use std::{thread, time};

use cylinder::Signer;

use crate::handler::{ApplyError, TransactionContext, TransactionHandler};
use crate::protocol;
use crate::protocol::command::{Command, CommandPayload, SleepType};
use crate::protocol::transaction::{HashMethod, TransactionBuilder, TransactionPair};
use crate::protos::{FromBytes, IntoBytes};

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
        let command_payload = CommandPayload::from_bytes(transaction_pair.transaction().payload())
            .expect("Unable to parse CommandPayload");

        for command in command_payload.commands().iter() {
            match command {
                Command::SetState(set_state) => {
                    context.set_state_entries(
                        set_state
                            .state_writes()
                            .to_vec()
                            .iter()
                            .map(|b| (b.key().to_string(), b.value().to_vec()))
                            .collect(),
                    )?;
                }
                Command::DeleteState(delete_state) => {
                    context.delete_state_entries(delete_state.state_keys())?;
                }
                Command::GetState(get_state) => {
                    context.get_state_entries(get_state.state_keys())?;
                }
                Command::AddEvent(add_event) => {
                    context.add_event(
                        add_event.event_type().to_string(),
                        add_event
                            .attributes()
                            .to_vec()
                            .iter()
                            .map(|b| {
                                (
                                    b.key().to_string(),
                                    String::from_utf8(b.value().to_vec())
                                        .expect("Unable to get BytesEntry value"),
                                )
                            })
                            .collect(),
                        add_event.data().to_vec(),
                    )?;
                }
                Command::AddReceiptData(add_receipt_data) => {
                    context.add_receipt_data(add_receipt_data.receipt_data().to_vec())?;
                }
                Command::Sleep(sleep_payload) => {
                    sleep(
                        sleep_payload.sleep_type().clone(),
                        *sleep_payload.duration_millis(),
                    );
                }
                Command::ReturnInternalError(internal_err) => {
                    return Err(ApplyError::InternalError(
                        internal_err.error_message().to_string(),
                    ));
                }
                Command::ReturnInvalid(invalid_err) => {
                    return Err(ApplyError::InvalidTransaction(
                        invalid_err.error_message().to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}

pub fn make_command_transaction(commands: &[Command], signer: &dyn Signer) -> TransactionPair {
    let command_payload = protocol::command::CommandPayload::new(commands.to_vec());
    TransactionBuilder::new()
        .with_batcher_public_key(vec![0u8, 0u8, 0u8, 0u8])
        .with_family_name(COMMAND_FAMILY_NAME.to_owned())
        .with_family_version(COMMAND_VERSION.to_owned())
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
                .map(Option::unwrap)
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
                .map(Option::unwrap)
                .collect(),
        )
        .with_payload_hash_method(HashMethod::SHA512)
        .with_payload(
            command_payload
                .into_bytes()
                .expect("Unable to get bytes from Command Payload"),
        )
        .build_pair(signer)
        .unwrap()
}

fn sleep(sleep_type: SleepType, duration: u32) {
    let duration_millis = time::Duration::from_millis(duration.into());
    match sleep_type {
        SleepType::Wait => {
            thread::sleep(duration_millis);
        }
        SleepType::BusyWait => {
            let now = time::Instant::now();
            loop {
                if now.elapsed() >= duration_millis {
                    break;
                }
            }
        }
    }
}

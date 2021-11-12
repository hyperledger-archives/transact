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
 * -----------------------------------------------------------------------------
 */

//! Defines a command family implementation of `TransactionHandler`.
//!
//! The following are the commands that can be sent to the [`CommandTransactionHandler`], with a
//! brief explanation of what they do:
//!
//! GetState - Read state entries
//! SetState - Write state entries
//! DeleteState - Delete state entries
//! AddEvent - Add an event to the transaction receipt
//! AddReceiptData - Add an event to the receipt
//! Sleep - Sleep for a specified duration
//! ReturnInvalid - "Return" InvalidTransaction
//! ReturnInternalError - "Return" InternalError

use std::{thread, time};

use crate::handler::{ApplyError, TransactionContext, TransactionHandler};
use crate::protocol::command::{Command, CommandPayload, SleepType};
use crate::protocol::transaction::TransactionPair;
use crate::protos::FromBytes;

const COMMAND_FAMILY_NAME: &str = "command";
const COMMAND_NAMESPACE: &str = "06abbc";
const COMMAND_VERSION: &str = "1";

/// A transaction handler for the command family.
#[derive(Default)]
pub struct CommandTransactionHandler {
    family_name: String,
    versions: Vec<String>,
    namespaces: Vec<String>,
}

impl CommandTransactionHandler {
    pub fn new() -> Self {
        CommandTransactionHandler {
            family_name: COMMAND_FAMILY_NAME.to_string(),
            versions: vec![COMMAND_VERSION.to_owned()],
            namespaces: vec![COMMAND_NAMESPACE.to_owned()],
        }
    }

    /// Returns the command transaction family namespace.
    pub fn namespaces(&self) -> Vec<String> {
        self.namespaces.clone()
    }
}

impl TransactionHandler for CommandTransactionHandler {
    /// Returns the name of the command transaction family.
    fn family_name(&self) -> &str {
        &self.family_name
    }

    /// Returns the list of versions that the `CommandTransactionHandler` can process.
    fn family_versions(&self) -> &[String] {
        &self.versions
    }

    /// Defines the business logic for the command transaction family. This function is called by
    /// the transaction processor when a `TpProcessRequest` is received.
    ///
    /// # Arguments
    ///
    /// * `transaction_pair` - The command `TransactionPair` to be processed
    /// * `context` - The transaction context which provides access to reading and writing from
    ///               state, as well as well as appending data to the receipt.
    fn apply(
        &self,
        transaction_pair: &TransactionPair,
        context: &mut dyn TransactionContext,
    ) -> Result<(), ApplyError> {
        let command_payload = CommandPayload::from_bytes(transaction_pair.transaction().payload())
            .map_err(|_| {
                ApplyError::InvalidTransaction("Unable to parse CommandPayload".to_string())
            })?;

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

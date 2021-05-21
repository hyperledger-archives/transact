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

use crate::protocol;
use crate::protocol::command::Command;
use crate::protocol::transaction::HashMethod;
use crate::protocol::transaction::{TransactionBuilder, TransactionPair};
use crate::protos::IntoBytes;

pub fn make_command_transaction(commands: &[Command], signer: &dyn Signer) -> TransactionPair {
    let command_payload = protocol::command::CommandPayload::new(commands.to_vec());
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
        .with_payload_hash_method(HashMethod::Sha512)
        .with_payload(
            command_payload
                .into_bytes()
                .expect("Unable to get bytes from Command Payload"),
        )
        .build_pair(signer)
        .unwrap()
}

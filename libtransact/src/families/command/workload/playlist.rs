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

//! Tools for generating command family transactions
use protobuf::RepeatedField;
use rand::prelude::*;
use sha2::{Digest, Sha512};

use crate::protos::command;
use crate::protos::command::{Command, Command_CommandType};

pub struct CommandGeneratingIter {
    rng: StdRng,
    addresses: Vec<String>,
    set_addresses: Vec<String>,
}

impl CommandGeneratingIter {
    pub fn new(seed: u64) -> Self {
        CommandGeneratingIter {
            rng: SeedableRng::seed_from_u64(seed),
            addresses: make_command_workload_addresses(),
            set_addresses: Vec::new(),
        }
    }

    pub fn add_set_address(&mut self, address: String) {
        if !self.set_addresses.contains(&address) {
            self.set_addresses.push(address);
        }
    }

    pub fn remove_set_address(&mut self, index: usize) {
        self.set_addresses.remove(index);
    }
}

impl Iterator for CommandGeneratingIter {
    type Item = (Command, String);

    fn next(&mut self) -> Option<Self::Item> {
        let mut command = Command::new();

        // If no addresses are currently set, generate a number between 2 and 6
        // to ensure `delete_state` will not be chosen
        let rand = if self.set_addresses.is_empty() {
            self.rng.gen_range(2, 6)
        } else {
            self.rng.gen_range(2, 7)
        };

        let command_type = match rand {
            2 => Command_CommandType::SET_STATE,
            3 => Command_CommandType::GET_STATE,
            4 => Command_CommandType::ADD_EVENT,
            5 => Command_CommandType::RETURN_INVALID,
            6 => Command_CommandType::DELETE_STATE,
            _ => panic!("Should not have generated outside of [2, 7)"),
        };

        command.set_command_type(command_type);

        match command_type {
            Command_CommandType::SET_STATE => {
                let (set_state, address) = make_set_state_command(&mut self.rng, &self.addresses);
                command.set_set_state(set_state);
                // add address to list of set addresses
                self.add_set_address(address.clone());
                Some((command, address))
            }
            Command_CommandType::GET_STATE => {
                let (get_state, address) = make_get_state_command(&mut self.rng, &self.addresses);
                command.set_get_state(get_state);
                Some((command, address))
            }
            Command_CommandType::ADD_EVENT => {
                let (add_event, address) = make_add_event_command(&mut self.rng, &self.addresses);
                command.set_add_event(add_event);
                Some((command, address))
            }
            Command_CommandType::RETURN_INVALID => {
                let (return_invalid, address) =
                    make_return_invalid_command(&mut self.rng, &self.addresses);
                command.set_return_invalid(return_invalid);
                Some((command, address))
            }
            Command_CommandType::DELETE_STATE => {
                // get a state address that has been set
                let address_index = self.rng.gen_range(0, self.set_addresses.len());
                let address = String::from(&self.set_addresses[address_index]);
                // remove the address from the list of set addresses
                self.remove_set_address(address_index);

                let delete_state = make_delete_state_command(address.clone());
                command.set_delete_state(delete_state);
                Some((command, address))
            }
            _ => panic!("Should not have generated outside of [2, 7)"),
        }
    }
}

fn make_set_state_command(rng: &mut StdRng, addresses: &[String]) -> (command::SetState, String) {
    let mut bytes_entry = command::BytesEntry::new();

    let address = &addresses[rng.gen_range(0, addresses.len())];

    bytes_entry.set_key(address.to_string());
    bytes_entry.set_value(rng.gen_range(0, 1000).to_string().as_bytes().to_vec());

    let mut set_state = command::SetState::new();
    set_state.set_state_writes(RepeatedField::from_vec(vec![bytes_entry]));

    (set_state, address.to_string())
}

fn make_get_state_command(rng: &mut StdRng, addresses: &[String]) -> (command::GetState, String) {
    let address = &addresses[rng.gen_range(0, addresses.len())];

    let state_keys = vec![address.to_string()];

    let mut get_state = command::GetState::new();
    get_state.set_state_keys(RepeatedField::from_vec(state_keys));

    (get_state, address.to_string())
}

fn make_add_event_command(rng: &mut StdRng, addresses: &[String]) -> (command::AddEvent, String) {
    let address = &addresses[rng.gen_range(0, addresses.len())];
    let mut bytes_entry = command::BytesEntry::new();

    bytes_entry.set_key("key".to_string());
    bytes_entry.set_value(rng.gen_range(0, 1000).to_string().as_bytes().to_vec());

    let mut add_event = command::AddEvent::new();

    add_event.set_event_type("event_type".to_string());
    add_event.set_attributes(RepeatedField::from_vec(vec![bytes_entry]));
    add_event.set_data(rng.gen_range(0, 1000).to_string().as_bytes().to_vec());

    (add_event, address.to_string())
}

fn make_return_invalid_command(
    rng: &mut StdRng,
    addresses: &[String],
) -> (command::ReturnInvalid, String) {
    let address = &addresses[rng.gen_range(0, addresses.len())];

    let mut return_invalid = command::ReturnInvalid::new();
    return_invalid.set_error_message("'return_invalid' command mock error message".to_string());

    (return_invalid, address.to_string())
}

fn make_delete_state_command(address: String) -> command::DeleteState {
    let state_keys = vec![address];

    let mut delete_state = command::DeleteState::new();
    delete_state.set_state_keys(RepeatedField::from_vec(state_keys));

    delete_state
}

fn make_command_workload_addresses() -> Vec<String> {
    let mut addresses = Vec::new();

    // Create 100 addresses
    for i in 0..100 {
        let mut sha = Sha512::new();
        sha.update(format!("address{}", i).as_bytes());
        let hash = &mut sha.finalize();

        let hex = bytes_to_hex_str(hash);

        // Using the precomputed Sha512 hash of "command"
        addresses.push(String::from("06abbc") + &hex[0..64]);
    }
    addresses
}

pub fn bytes_to_hex_str(b: &[u8]) -> String {
    b.iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join("")
}

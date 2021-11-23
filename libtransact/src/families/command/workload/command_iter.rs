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
use rand::distributions::Standard;
use rand::prelude::*;
use sha2::{Digest, Sha512};

use crate::protocol::command;

/// An iterator that generates `command`s
pub struct CommandGeneratingIter {
    rng: StdRng,
    addresses: Vec<String>,
    set_addresses: Vec<String>,
}

impl CommandGeneratingIter {
    /// Create a new [CommandGeneratingIter]
    ///
    /// # Arguments
    ///
    /// * `seed` - Used to create a new `SeedableRng`
    pub fn new(seed: u64) -> Self {
        CommandGeneratingIter {
            rng: SeedableRng::seed_from_u64(seed),
            addresses: make_command_workload_addresses(),
            set_addresses: Vec::new(),
        }
    }

    /// Add an address to `set_addresses`, this is a list of all addresses currently set
    ///
    /// # Arguments
    ///
    /// * `address` - The address to add to the `set_addresses` list
    pub fn add_set_address(&mut self, address: String) {
        if !self.set_addresses.contains(&address) {
            self.set_addresses.push(address);
        }
    }

    /// Remove an address from `set_addresses`, this is a list of all addresses currently set
    ///
    /// # Arguments
    ///
    /// * `address` - The address to remove from the `set_addresses` list
    pub fn remove_set_address(&mut self, index: usize) {
        self.set_addresses.remove(index);
    }
}

impl Iterator for CommandGeneratingIter {
    type Item = (command::Command, String);

    fn next(&mut self) -> Option<Self::Item> {
        let mut command_type: CommandType = self.rng.gen();
        // Delete state must have an existing state to delete in order to be run. If no addresses
        // are currently set and `CommandType::DeleteState` is generated, generate a new command
        if self.set_addresses.is_empty() {
            while command_type == CommandType::DeleteState {
                command_type = self.rng.gen();
            }
        }

        match command_type {
            CommandType::SetState => {
                let (set_state, address) = make_set_state_command(&mut self.rng, &self.addresses);
                let command = command::Command::SetState(set_state);
                // add address to list of set addresses
                self.add_set_address(address.clone());
                Some((command, address))
            }
            CommandType::GetState => {
                let (get_state, address) = make_get_state_command(&mut self.rng, &self.addresses);
                let command = command::Command::GetState(get_state);
                Some((command, address))
            }
            CommandType::AddEvent => {
                let (add_event, address) = make_add_event_command(&mut self.rng, &self.addresses);
                let command = command::Command::AddEvent(add_event);
                Some((command, address))
            }
            CommandType::ReturnInvalid => {
                let (return_invalid, address) =
                    make_return_invalid_command(&mut self.rng, &self.addresses);
                let command = command::Command::ReturnInvalid(return_invalid);
                Some((command, address))
            }
            CommandType::DeleteState => {
                // get a state address that has been set
                let address_index = self.rng.gen_range(0..self.set_addresses.len());
                let address = String::from(&self.set_addresses[address_index]);
                // remove the address from the list of set addresses
                self.remove_set_address(address_index);

                let delete_state = make_delete_state_command(address.clone());
                let command = command::Command::DeleteState(delete_state);
                Some((command, address))
            }
        }
    }
}

fn make_set_state_command(rng: &mut StdRng, addresses: &[String]) -> (command::SetState, String) {
    let address = &addresses[rng.gen_range(0..addresses.len())];

    let bytes_entry = command::BytesEntry::new(
        address.to_string(),
        rng.gen_range(0..1000).to_string().as_bytes().to_vec(),
    );

    (
        command::SetState::new(vec![bytes_entry]),
        address.to_string(),
    )
}

fn make_get_state_command(rng: &mut StdRng, addresses: &[String]) -> (command::GetState, String) {
    let address = &addresses[rng.gen_range(0..addresses.len())];

    (
        command::GetState::new(vec![address.to_string()]),
        address.to_string(),
    )
}

fn make_add_event_command(rng: &mut StdRng, addresses: &[String]) -> (command::AddEvent, String) {
    let address = &addresses[rng.gen_range(0..addresses.len())];

    let bytes_entry = command::BytesEntry::new(
        "key".to_string(),
        rng.gen_range(0..1000).to_string().as_bytes().to_vec(),
    );

    (
        command::AddEvent::new(
            "event_type".to_string(),
            vec![bytes_entry],
            rng.gen_range(0..1000).to_string().as_bytes().to_vec(),
        ),
        address.to_string(),
    )
}

fn make_return_invalid_command(
    rng: &mut StdRng,
    addresses: &[String],
) -> (command::ReturnInvalid, String) {
    let address = &addresses[rng.gen_range(0..addresses.len())];

    (
        command::ReturnInvalid::new("'return_invalid' command mock error message".to_string()),
        address.to_string(),
    )
}

fn make_delete_state_command(address: String) -> command::DeleteState {
    command::DeleteState::new(vec![address])
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

fn bytes_to_hex_str(b: &[u8]) -> String {
    b.iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join("")
}

#[derive(Debug, PartialEq)]
enum CommandType {
    SetState,
    GetState,
    AddEvent,
    ReturnInvalid,
    DeleteState,
}

impl Distribution<CommandType> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> CommandType {
        match rng.gen_range(2..7) {
            2 => CommandType::SetState,
            3 => CommandType::GetState,
            4 => CommandType::AddEvent,
            5 => CommandType::ReturnInvalid,
            _ => CommandType::DeleteState,
        }
    }
}

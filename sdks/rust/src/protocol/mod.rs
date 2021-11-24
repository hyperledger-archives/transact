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

pub mod payload;
pub mod state;

use std::error::Error;

use sha2::{Digest, Sha512};

pub const SABRE_PROTOCOL_VERSION: &str = "1";

pub const ADMINISTRATORS_SETTING_KEY: &str = "sawtooth.swa.administrators";

pub const ADMINISTRATORS_SETTING_ADDRESS: &str =
    "000000a87cb5eafdcca6a814e4add97c4b517d3c530c2f44b31d18e3b0c44298fc1c14";
pub const NAMESPACE_REGISTRY_ADDRESS_PREFIX: &str = "00ec00";
pub const CONTRACT_REGISTRY_ADDRESS_PREFIX: &str = "00ec01";
pub const CONTRACT_ADDRESS_PREFIX: &str = "00ec02";
pub const SMART_PERMISSION_ADDRESS_PREFIX: &str = "00ec03";
pub const AGENT_ADDRESS_PREFIX: &str = "cad11d00";
pub const ORG_ADDRESS_PREFIX: &str = "cad11d01";

pub const ADMINISTRATORS_SETTING_ADDRESS_BYTES: &[u8] = &[
    0, 0, 0, 168, 124, 181, 234, 253, 204, 166, 168, 20, 228, 173, 217, 124, 75, 81, 125, 60, 83,
    12, 47, 68, 179, 29, 24, 227, 176, 196, 66, 152, 252, 28, 20,
];
pub const NAMESPACE_REGISTRY_ADDRESS_PREFIX_BYTES: &[u8] = &[0, 236, 0];
pub const CONTRACT_REGISTRY_ADDRESS_PREFIX_BYTES: &[u8] = &[0, 236, 1];
pub const CONTRACT_ADDRESS_PREFIX_BYTES: &[u8] = &[0, 236, 2];
pub const SMART_PERMISSION_ADDRESS_PREFIX_BYTES: &[u8] = &[0, 236, 3];
pub const AGENT_ADDRESS_PREFIX_BYTES: &[u8] = &[202, 209, 29, 0];
pub const ORG_ADDRESS_PREFIX_BYTES: &[u8] = &[202, 209, 29, 1];

/// Compute a state address for a given namespace registry.
///
/// # Arguments
///
/// * `namespace` - the address prefix for this namespace
pub fn compute_namespace_registry_address(namespace: &str) -> Result<Vec<u8>, AddressingError> {
    let prefix = match namespace.get(..6) {
        Some(x) => x,
        None => {
            return Err(AddressingError::InvalidInput(format!(
                "namespace '{}' is less than 6 characters long",
                namespace,
            )));
        }
    };
    let hash = Sha512::digest(prefix.as_bytes());
    Ok([NAMESPACE_REGISTRY_ADDRESS_PREFIX_BYTES, &hash[..32]].concat())
}

/// Compute a state address for a given contract registry.
///
/// # Arguments
///
/// * `name` - the name of the contract registry
pub fn compute_contract_registry_address(name: &str) -> Result<Vec<u8>, AddressingError> {
    let hash = Sha512::digest(name.as_bytes());
    Ok([CONTRACT_REGISTRY_ADDRESS_PREFIX_BYTES, &hash[..32]].concat())
}

/// Compute a state address for a given contract.
///
/// # Arguments
///
/// * `name` - the name of the contract
/// * `version` - the version of the contract
pub fn compute_contract_address(name: &str, version: &str) -> Result<Vec<u8>, AddressingError> {
    let s = String::from(name) + "," + version;
    let hash = Sha512::digest(s.as_bytes());
    Ok([CONTRACT_ADDRESS_PREFIX_BYTES, &hash[..32]].concat())
}

/// Compute a state address for a given agent name.
///
/// # Arguments
///
/// * `name` - the agent's name
pub fn compute_agent_address(name: &[u8]) -> Result<Vec<u8>, AddressingError> {
    let hash = Sha512::digest(name);
    Ok([AGENT_ADDRESS_PREFIX_BYTES, &hash[..31]].concat())
}

/// Compute a state address for a given organization id.
///
/// # Arguments
///
/// * `id` - the organization's id
pub fn compute_org_address(id: &str) -> Result<Vec<u8>, AddressingError> {
    let hash = Sha512::digest(id.as_bytes());
    Ok([ORG_ADDRESS_PREFIX_BYTES, &hash[..31]].concat())
}

#[derive(Debug)]
pub enum AddressingError {
    InvalidInput(String),
}

impl Error for AddressingError {}

impl std::fmt::Display for AddressingError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            AddressingError::InvalidInput(msg) => write!(f, "addressing input is invalid: {}", msg),
        }
    }
}

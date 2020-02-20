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
pub mod pike;
pub mod state;

use std::error::Error;

use sha2::{Digest, Sha512};

pub const ADMINISTRATORS_SETTING_ADDRESS: &str =
    "000000a87cb5eafdcca6a814e4add97c4b517d3c530c2f44b31d18e3b0c44298fc1c14";
pub const ADMINISTRATORS_SETTING_KEY: &str = "sawtooth.swa.administrators";

pub const NAMESPACE_REGISTRY_ADDRESS_PREFIX: &str = "00ec00";
pub const CONTRACT_REGISTRY_ADDRESS_PREFIX: &str = "00ec01";
pub const CONTRACT_ADDRESS_PREFIX: &str = "00ec02";
pub const SMART_PERMISSION_ADDRESS_PREFIX: &str = "00ec03";
pub const AGENT_ADDRESS_PREFIX: &str = "cad11d00";
pub const ORG_ADDRESS_PREFIX: &str = "cad11d01";

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
    let hash = sha512_hash(prefix.as_bytes());
    Ok([&parse_hex(NAMESPACE_REGISTRY_ADDRESS_PREFIX)?, &hash[..32]].concat())
}

/// Compute a state address for a given contract registry.
///
/// # Arguments
///
/// * `name` - the name of the contract registry
pub fn compute_contract_registry_address(name: &str) -> Result<Vec<u8>, AddressingError> {
    let hash = sha512_hash(name.as_bytes());
    Ok([&parse_hex(CONTRACT_REGISTRY_ADDRESS_PREFIX)?, &hash[..32]].concat())
}

/// Compute a state address for a given contract.
///
/// # Arguments
///
/// * `name` - the name of the contract
/// * `version` - the version of the contract
pub fn compute_contract_address(name: &str, version: &str) -> Result<Vec<u8>, AddressingError> {
    let s = String::from(name) + "," + version;
    let hash = sha512_hash(s.as_bytes());
    Ok([&parse_hex(CONTRACT_ADDRESS_PREFIX)?, &hash[..32]].concat())
}

/// Compute a state address for a given smart permission.
///
/// # Arguments
///
/// * `org_id` - the organization's id
/// * `name` - smart permission name
pub fn compute_smart_permission_address(
    org_id: &str,
    name: &str,
) -> Result<Vec<u8>, AddressingError> {
    let org_id_hash = sha512_hash(org_id.as_bytes());
    let name_hash = sha512_hash(name.as_bytes());
    Ok([
        &parse_hex(SMART_PERMISSION_ADDRESS_PREFIX)?,
        &org_id_hash[..3],
        &name_hash[..29],
    ]
    .concat())
}

/// Compute a state address for a given agent name.
///
/// # Arguments
///
/// * `name` - the agent's name
pub fn compute_agent_address(name: &[u8]) -> Result<Vec<u8>, AddressingError> {
    let hash = sha512_hash(name);
    Ok([&parse_hex(AGENT_ADDRESS_PREFIX)?, &hash[..31]].concat())
}

/// Compute a state address for a given organization id.
///
/// # Arguments
///
/// * `id` - the organization's id
pub fn compute_org_address(id: &str) -> Result<Vec<u8>, AddressingError> {
    let hash = sha512_hash(id.as_bytes());
    Ok([&parse_hex(ORG_ADDRESS_PREFIX)?, &hash[..31]].concat())
}

fn sha512_hash(bytes: &[u8]) -> Vec<u8> {
    let mut hasher = Sha512::new();
    hasher.input(bytes);
    hasher.result().to_vec()
}

/// Convert a hex string to bytes.
fn parse_hex(hex: &str) -> Result<Vec<u8>, AddressingError> {
    if hex.len() % 2 != 0 {
        return Err(AddressingError::InvalidInput(format!(
            "hex string has odd number of digits: {}",
            hex
        )));
    }

    let mut res = vec![];
    for i in (0..hex.len()).step_by(2) {
        res.push(u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| {
            AddressingError::InvalidInput(format!("string contains invalid hex: {}", hex))
        })?);
    }

    Ok(res)
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

// Copyright 2018 Cargill Incorporated
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

use crypto::digest::Digest;
use crypto::sha2::Sha512;
use crypto::sha2::Sha256;

use sawtooth_sdk::processor::handler::ApplyError;

/// The namespace registry prefix for global state (00ec00)
const NAMESPACE_REGISTRY_PREFIX: &'static str = "00ec00";

/// The contract registry prefix for global state (00ec01)
const CONTRACT_REGISTRY_PREFIX: &'static str = "00ec01";

/// The contract prefix for global state (00ec02)
const CONTRACT_PREFIX: &'static str = "00ec02";

/// The smart permission prefix for global state (00ec03)
const SMART_PERMISSION_PREFIX: &'static str = "00ec03";

const PIKE_AGENT_PREFIX: &'static str = "cad11d00";

const PIKE_ORG_PREFIX: &'static str = "cad11d01";

const SETTING_PREFIX: &'static str = "000000";

pub fn hash(to_hash: &str, num: usize) -> Result<String, ApplyError> {
    let mut sha = Sha512::new();
    sha.input_str(to_hash);
    let temp = sha.result_str().to_string();
    let hash = match temp.get(..num) {
        Some(x) => x,
        None => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Cannot hash {} to Sha512 and return String with len {}",
                to_hash, num
            )))
        }
    };
    Ok(hash.into())
}

/// Returns a hex string representation of the supplied bytes
///
/// # Arguments
///
/// * `b` - input bytes
fn bytes_to_hex_str(b: &[u8]) -> String {
    b.iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join("")
}

pub fn hash_256(to_hash: &str, num: usize) -> Result<String, ApplyError> {
    let mut sha = Sha256::new();
    sha.input_str(to_hash);
    let temp = sha.result_str().to_string();
    let hash = match temp.get(..num) {
        Some(x) => x,
        None => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Cannot hash {} to Sha256 and return String with len {}",
                to_hash, num
            )))
        }
    };
    Ok(hash.into())
}

pub fn make_contract_address(name: &str, version: &str) -> Result<String, ApplyError> {
    Ok(CONTRACT_PREFIX.to_string() + &hash(&(name.to_string() + "," + version), 64)?)
}

pub fn make_contract_registry_address(name: &str) -> Result<String, ApplyError> {
    Ok(CONTRACT_REGISTRY_PREFIX.to_string() + &hash(name, 64)?)
}

pub fn make_namespace_registry_address(namespace: &str) -> Result<String, ApplyError> {
    let prefix = match namespace.get(..6) {
        Some(x) => x,
        None => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Namespace must be at least 6 characters long: {}",
                namespace
            )))
        }
    };
    Ok(NAMESPACE_REGISTRY_PREFIX.to_string() + &hash(prefix, 64)?)
}

pub fn get_sawtooth_admins_address() -> Result<String, ApplyError> {
    Ok(
        SETTING_PREFIX.to_string() + &hash_256("sawtooth", 16)? + &hash_256("swa", 16)?
            + &hash_256("administrators", 16)? + &hash_256("", 16)?,
    )
}

/// Returns a state address for a smart permission
///
/// # Arguments
///
/// * `name` - smart permission name
/// * `org_id - ID of the organization that owns the smart permission`
pub fn compute_smart_permission_address(org_id: &str, name: &str) -> String {
    let mut sha_org_id = Sha512::new();
    sha_org_id.input(org_id.as_bytes());

    let mut sha_name = Sha512::new();
    sha_name.input(name.as_bytes());

    String::from(SMART_PERMISSION_PREFIX)
        + &sha_org_id.result_str()[..6].to_string()
        + &sha_name.result_str()[..58].to_string()
}

/// Returns a state address for a given agent name
///
/// # Arguments
///
/// * `name` - the agent's name
pub fn compute_agent_address(public_key: &str) -> String {
    let hash: &mut [u8] = &mut [0; 64];

    let mut sha = Sha512::new();
    sha.input(public_key.as_bytes());
    sha.result(hash);

    String::from(PIKE_AGENT_PREFIX) + &bytes_to_hex_str(hash)[..62]
}

/// Returns a state address for a given organization id
///
/// # Arguments
///
/// * `id` - the organization's id
pub fn compute_org_address(id: &str) -> String {
    let hash: &mut [u8] = &mut [0; 64];

    let mut sha = Sha512::new();
    sha.input(id.as_bytes());
    sha.result(hash);

    String::from(PIKE_ORG_PREFIX) + &bytes_to_hex_str(hash)[..62]
}

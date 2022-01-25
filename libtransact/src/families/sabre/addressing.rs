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

use sha2::{Digest, Sha512};

use crate::handler::ApplyError;

/// The namespace registry prefix for global state (00ec00)
const NAMESPACE_REGISTRY_PREFIX: &str = "00ec00";

/// The contract registry prefix for global state (00ec01)
const CONTRACT_REGISTRY_PREFIX: &str = "00ec01";

/// The contract prefix for global state (00ec02)
const CONTRACT_PREFIX: &str = "00ec02";

pub fn hash(to_hash: &str, num: usize) -> Result<String, ApplyError> {
    let temp = Sha512::digest(to_hash)
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    let hash = match temp.get(..num) {
        Some(x) => x,
        None => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Cannot hash {} to Sha512 and return String with len {}",
                to_hash, num
            )));
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
            )));
        }
    };
    Ok(NAMESPACE_REGISTRY_PREFIX.to_string() + &hash(prefix, 64)?)
}

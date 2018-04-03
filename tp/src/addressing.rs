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

/// The namespace registry prefix for global state (00ec00)
const NAMESPACE_REGISTRY_PREFIX: &'static str = "00ec00";

/// The contract registry prefix for global state (00ec01)
const CONTRACT_REGISTRY_PREFIX: &'static str = "00ec01";

/// The contract prefix for global state (00ec02)
const CONTRACT_PREFIX: &'static str = "00ec02";

const SETTING_PREFIX: &'static str = "000000";

pub fn hash(to_hash: &str, num: usize) -> String {
    let mut sha = Sha512::new();
    sha.input_str(to_hash);
    let temp = sha.result_str().to_string();
    let hash = match temp.get(..num) {
        Some(x) => x,
        None => "",
    };
    hash.to_string()
}

pub fn hash_256(to_hash: &str, num: usize) -> String {
    let mut sha = Sha256::new();
    sha.input_str(to_hash);
    let temp = sha.result_str().to_string();
    let hash = match temp.get(..num) {
        Some(x) => x,
        None => "",
    };
    hash.to_string()
}

pub fn make_contract_address(name: &str, version: &str) -> String {
    CONTRACT_PREFIX.to_string() + &hash(&(name.to_string() + "," + version), 64)
}

pub fn make_contract_registry_address(name: &str) -> String {
    CONTRACT_REGISTRY_PREFIX.to_string() + &hash(name, 64)
}

pub fn make_namespace_registry_address(namespace: &str) -> String {
    let prefix = match namespace.get(..6) {
        Some(x) => x,
        None => "",
    };
    NAMESPACE_REGISTRY_PREFIX.to_string() + &hash(prefix, 64)
}

pub fn get_sawtooth_admins_address() -> String {
    SETTING_PREFIX.to_string() + &hash_256("sawtooth", 16) + &hash_256("swa", 16)
        + &hash_256("administrators", 16) + &hash_256("", 16)
}

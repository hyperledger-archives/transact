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

/// The namespace registry prefix for global state (00ec00)
const NAMESPACE_REGISTRY_PREFIX: &'static str = "00ec00";

/// The contract registry prefix for global state (00ec01)
const CONTRACT_REGISTRY_PREFIX: &'static str = "00ec01";

/// The contract prefix for global state (00ec02)
const CONTRACT_PREFIX: &'static str = "00ec02";

pub fn get_namspace_prefix(namespace: &str) -> String {
    let mut sha = Sha512::new();
    sha.input_str(namespace);
    sha.result_str()[..6].to_string()
}

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

pub fn make_contract_address(name: &str, version: &str) -> String {
    get_namspace_prefix(&CONTRACT_PREFIX) + &hash(&(name.to_string() + "," + version), 64)
}

pub fn make_contract_registry_address(name: &str) -> String {
    get_namspace_prefix(&CONTRACT_REGISTRY_PREFIX) + &hash(name, 64)
}

pub fn make_namespace_registry_address(namespace: &str) -> String {
    get_namspace_prefix(&NAMESPACE_REGISTRY_PREFIX) + &hash(namespace, 64)
}

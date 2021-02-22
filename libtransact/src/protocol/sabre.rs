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

//! Native protocol for building an ExecuteContract sabre payload

use std::error::Error as StdError;

use protobuf::Message;
use protobuf::RepeatedField;
use sha2::{Digest, Sha512};

use crate::protocol::transaction::{HashMethod, TransactionBuilder};
use crate::protos;
use crate::protos::{
    FromBytes, FromNative, FromProto, IntoBytes, IntoNative, IntoProto, ProtoConversionError,
};

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

/// Native implementation for SabrePayload_Action
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    ExecuteContract(ExecuteContractAction),
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Action::ExecuteContract(_) => write!(f, "Action: Execute Contract"),
        }
    }
}

impl From<ExecuteContractAction> for Action {
    fn from(action: ExecuteContractAction) -> Self {
        Action::ExecuteContract(action)
    }
}

#[derive(Debug)]
pub enum ActionBuildError {
    MissingField(String),
}

impl StdError for ActionBuildError {}

impl std::fmt::Display for ActionBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Self::MissingField(ref s) => write!(f, "missing field: {}", s),
        }
    }
}

/// Native implementation for ExecuteContractAction
#[derive(Default, Debug, Clone, PartialEq)]
pub struct ExecuteContractAction {
    name: String,
    version: String,
    inputs: Vec<String>,
    outputs: Vec<String>,
    payload: Vec<u8>,
}

impl ExecuteContractAction {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn inputs(&self) -> &[String] {
        &self.inputs
    }

    pub fn outputs(&self) -> &[String] {
        &self.outputs
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl FromProto<protos::sabre::ExecuteContractAction> for ExecuteContractAction {
    fn from_proto(
        proto: protos::sabre::ExecuteContractAction,
    ) -> Result<Self, ProtoConversionError> {
        Ok(ExecuteContractAction {
            name: proto.get_name().to_string(),
            version: proto.get_version().to_string(),
            inputs: proto.get_inputs().to_vec(),
            outputs: proto.get_outputs().to_vec(),
            payload: proto.get_payload().to_vec(),
        })
    }
}

impl FromNative<ExecuteContractAction> for protos::sabre::ExecuteContractAction {
    fn from_native(
        execute_contract_action: ExecuteContractAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::sabre::ExecuteContractAction::new();
        proto.set_name(execute_contract_action.name().to_string());
        proto.set_version(execute_contract_action.version().to_string());
        proto.set_inputs(RepeatedField::from_vec(
            execute_contract_action.inputs().to_vec(),
        ));
        proto.set_outputs(RepeatedField::from_vec(
            execute_contract_action.outputs().to_vec(),
        ));
        proto.set_payload(execute_contract_action.payload().to_vec());
        Ok(proto)
    }
}

impl FromBytes<ExecuteContractAction> for ExecuteContractAction {
    fn from_bytes(bytes: &[u8]) -> Result<ExecuteContractAction, ProtoConversionError> {
        let proto: protos::sabre::ExecuteContractAction = Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get ExecuteContractAction from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for ExecuteContractAction {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from ExecuteContractAction".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::sabre::ExecuteContractAction> for ExecuteContractAction {}
impl IntoNative<ExecuteContractAction> for protos::sabre::ExecuteContractAction {}

/// Builder used to create a ExecuteContractAction
#[derive(Default, Clone)]
pub struct ExecuteContractActionBuilder {
    name: Option<String>,
    version: Option<String>,
    inputs: Vec<String>,
    outputs: Vec<String>,
    payload: Vec<u8>,
}

impl ExecuteContractActionBuilder {
    pub fn new() -> Self {
        ExecuteContractActionBuilder::default()
    }

    pub fn with_name(mut self, name: String) -> ExecuteContractActionBuilder {
        self.name = Some(name);
        self
    }

    pub fn with_version(mut self, version: String) -> ExecuteContractActionBuilder {
        self.version = Some(version);
        self
    }

    pub fn with_inputs(mut self, inputs: Vec<String>) -> ExecuteContractActionBuilder {
        self.inputs = inputs;
        self
    }

    pub fn with_outputs(mut self, outputs: Vec<String>) -> ExecuteContractActionBuilder {
        self.outputs = outputs;
        self
    }

    pub fn with_payload(mut self, payload: Vec<u8>) -> ExecuteContractActionBuilder {
        self.payload = payload;
        self
    }

    pub fn build(self) -> Result<ExecuteContractAction, ActionBuildError> {
        let name = self.name.ok_or_else(|| {
            ActionBuildError::MissingField("'name' field is required".to_string())
        })?;

        let version = self.version.ok_or_else(|| {
            ActionBuildError::MissingField("'version' field is required".to_string())
        })?;

        let inputs = self.inputs;
        let outputs = self.outputs;

        let payload = {
            if self.payload.is_empty() {
                return Err(ActionBuildError::MissingField(
                    "'payloads' field is required".to_string(),
                ));
            } else {
                self.payload
            }
        };

        Ok(ExecuteContractAction {
            name,
            version,
            inputs,
            outputs,
            payload,
        })
    }

    pub fn into_payload_builder(self) -> Result<SabrePayloadBuilder, ActionBuildError> {
        self.build()
            .map(|action| SabrePayloadBuilder::new().with_action(Action::from(action)))
    }
}

/// Native implementation for SabrePayload
#[derive(Debug, Clone, PartialEq)]
pub struct SabrePayload {
    action: Action,
}

impl SabrePayload {
    pub fn action(&self) -> &Action {
        &self.action
    }
}

impl FromProto<protos::sabre::SabrePayload> for SabrePayload {
    fn from_proto(proto: protos::sabre::SabrePayload) -> Result<Self, ProtoConversionError> {
        let action = match proto.get_action() {
            protos::sabre::SabrePayload_Action::EXECUTE_CONTRACT => {
                ExecuteContractAction::from_proto(proto.get_execute_contract().clone())?.into()
            }
            protos::sabre::SabrePayload_Action::ACTION_UNSET => {
                return Err(ProtoConversionError::InvalidTypeError(
                    "Cannot convert SabrePayload_Action with type unset".to_string(),
                ));
            }
            _ => {
                return Err(ProtoConversionError::InvalidTypeError(format!(
                    "Action: {:?} not supported",
                    {}
                )));
            }
        };

        Ok(SabrePayload { action })
    }
}

impl FromNative<SabrePayload> for protos::sabre::SabrePayload {
    fn from_native(native: SabrePayload) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::sabre::SabrePayload::new();

        match native.action() {
            Action::ExecuteContract(payload) => {
                proto.set_action(protos::sabre::SabrePayload_Action::EXECUTE_CONTRACT);
                proto.set_execute_contract(payload.clone().into_proto()?);
            }
        }

        Ok(proto)
    }
}

impl FromBytes<SabrePayload> for SabrePayload {
    fn from_bytes(bytes: &[u8]) -> Result<SabrePayload, ProtoConversionError> {
        let proto: protos::sabre::SabrePayload =
            Message::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get SabrePayload from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for SabrePayload {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from SabrePayload".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::sabre::SabrePayload> for SabrePayload {}
impl IntoNative<SabrePayload> for protos::sabre::SabrePayload {}

#[derive(Debug)]
pub enum SabrePayloadBuildError {
    AddressingError(String),
    InvalidAction(String),
    MissingField(String),
    ProtoConversionError(String),
    SigningError(String),
}

impl StdError for SabrePayloadBuildError {
    fn description(&self) -> &str {
        match *self {
            SabrePayloadBuildError::AddressingError(ref msg) => msg,
            SabrePayloadBuildError::InvalidAction(ref msg) => msg,
            SabrePayloadBuildError::MissingField(ref msg) => msg,
            SabrePayloadBuildError::ProtoConversionError(ref msg) => msg,
            SabrePayloadBuildError::SigningError(ref msg) => msg,
        }
    }
}

impl std::fmt::Display for SabrePayloadBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            SabrePayloadBuildError::AddressingError(ref s) => write!(f, "AddressingError: {}", s),
            SabrePayloadBuildError::InvalidAction(ref s) => write!(f, "InvalidAction: {}", s),
            SabrePayloadBuildError::MissingField(ref s) => write!(f, "MissingField: {}", s),
            SabrePayloadBuildError::ProtoConversionError(ref s) => {
                write!(f, "ProtoConversionError: {}", s)
            }
            SabrePayloadBuildError::SigningError(ref s) => write!(f, "SigningError: {}", s),
        }
    }
}

impl From<AddressingError> for SabrePayloadBuildError {
    fn from(err: AddressingError) -> Self {
        Self::AddressingError(err.to_string())
    }
}

/// Builder used to create SabrePayload
#[derive(Default, Clone)]
pub struct SabrePayloadBuilder {
    action: Option<Action>,
}

impl SabrePayloadBuilder {
    pub fn new() -> Self {
        SabrePayloadBuilder::default()
    }

    pub fn with_action(mut self, action: Action) -> SabrePayloadBuilder {
        self.action = Some(action);
        self
    }

    pub fn build(self) -> Result<SabrePayload, SabrePayloadBuildError> {
        let action = self.action.ok_or_else(|| {
            SabrePayloadBuildError::MissingField("'action' field is required".to_string())
        })?;

        Ok(SabrePayload { action })
    }

    /// Convert the `SabrePayloadBuilder` into a `TransactionBuilder`, filling in all required
    /// fields.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_transaction_builder(self) -> Result<TransactionBuilder, SabrePayloadBuildError> {
        let payload = self.build()?;

        let (input_addresses, output_addresses) = match payload.action() {
            Action::ExecuteContract(ExecuteContractAction {
                name,
                version,
                inputs,
                outputs,
                ..
            }) => {
                let addresses = vec![
                    compute_contract_registry_address(&name)?,
                    compute_contract_address(&name, &version)?,
                ];

                let mut input_addresses = addresses.clone();
                for input in inputs {
                    let namespace = match input.get(..6) {
                        Some(namespace) => namespace,
                        None => {
                            return Err(SabrePayloadBuildError::InvalidAction(format!(
                                "invalid input: '{}' is less than 6 characters long",
                                input,
                            )));
                        }
                    };
                    input_addresses.push(compute_namespace_registry_address(namespace)?);
                    input_addresses.push(parse_hex(&input)?);
                }

                let mut output_addresses = addresses;
                for output in outputs {
                    let namespace = match output.get(..6) {
                        Some(namespace) => namespace,
                        None => {
                            return Err(SabrePayloadBuildError::InvalidAction(format!(
                                "invalid output: '{}' is less than 6 characters long",
                                output,
                            )));
                        }
                    };
                    output_addresses.push(compute_namespace_registry_address(namespace)?);
                    output_addresses.push(parse_hex(&output)?);
                }

                (input_addresses, output_addresses)
            }
        };

        let payload_bytes = payload.into_bytes().map_err(|err| {
            SabrePayloadBuildError::ProtoConversionError(format!(
                "failed to serialize SabrePayload as bytes: {}",
                err
            ))
        })?;

        Ok(TransactionBuilder::new()
            .with_family_name("sabre".into())
            .with_family_version(SABRE_PROTOCOL_VERSION.into())
            .with_inputs(input_addresses)
            .with_outputs(output_addresses)
            .with_payload_hash_method(HashMethod::SHA512)
            .with_payload(payload_bytes))
    }
}

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

// Compute a state address for a given namespace registry.
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
    Ok([NAMESPACE_REGISTRY_ADDRESS_PREFIX_BYTES, &hash[..32]].concat())
}

/// Compute a state address for a given contract registry.
///
/// # Arguments
///
/// * `name` - the name of the contract registry
pub fn compute_contract_registry_address(name: &str) -> Result<Vec<u8>, AddressingError> {
    let hash = sha512_hash(name.as_bytes());
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
    let hash = sha512_hash(s.as_bytes());
    Ok([CONTRACT_ADDRESS_PREFIX_BYTES, &hash[..32]].concat())
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
        SMART_PERMISSION_ADDRESS_PREFIX_BYTES,
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
    Ok([AGENT_ADDRESS_PREFIX_BYTES, &hash[..31]].concat())
}

/// Compute a state address for a given organization id.
///
/// # Arguments
///
/// * `id` - the organization's id
pub fn compute_org_address(id: &str) -> Result<Vec<u8>, AddressingError> {
    let hash = sha512_hash(id.as_bytes());
    Ok([ORG_ADDRESS_PREFIX_BYTES, &hash[..31]].concat())
}

fn sha512_hash(bytes: &[u8]) -> Vec<u8> {
    let mut hasher = Sha512::new();
    hasher.input(bytes);
    hasher.result().to_vec()
}

#[derive(Debug)]
pub enum AddressingError {
    InvalidInput(String),
}

impl StdError for AddressingError {}

impl std::fmt::Display for AddressingError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            AddressingError::InvalidInput(msg) => write!(f, "addressing input is invalid: {}", msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // check that a execute contract action is built correctly
    fn check_execute_contract_action() {
        let builder = ExecuteContractActionBuilder::new();
        let action = builder
            .with_name("TestContract".to_string())
            .with_version("0.1".to_string())
            .with_inputs(vec!["test".to_string(), "input".to_string()])
            .with_outputs(vec!["test".to_string(), "output".to_string()])
            .with_payload(b"test_payload".to_vec())
            .build()
            .unwrap();

        assert_eq!(action.name(), "TestContract");
        assert_eq!(action.version(), "0.1");
        assert_eq!(action.inputs(), ["test".to_string(), "input".to_string()]);
        assert_eq!(action.outputs(), ["test".to_string(), "output".to_string()]);
        assert_eq!(action.payload(), b"test_payload");
    }

    #[test]
    // check that a execute contract can be converted to bytes and back
    fn check_execute_contract_action_bytes() {
        let builder = ExecuteContractActionBuilder::new();
        let original = builder
            .with_name("TestContract".to_string())
            .with_version("0.1".to_string())
            .with_inputs(vec!["test".to_string(), "input".to_string()])
            .with_outputs(vec!["test".to_string(), "output".to_string()])
            .with_payload(b"test_payload".to_vec())
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let execute = ExecuteContractAction::from_bytes(&bytes).unwrap();
        assert_eq!(execute, original);
    }
}

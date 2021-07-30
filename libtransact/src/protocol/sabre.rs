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
    CreateContract(CreateContractAction),
    ExecuteContract(ExecuteContractAction),
    CreateContractRegistry(CreateContractRegistryAction),
    CreateNamespaceRegistry(CreateNamespaceRegistryAction),
    CreateNamespaceRegistryPermission(CreateNamespaceRegistryPermissionAction),
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Action::CreateContract(_) => write!(f, "Action: Create Contract"),
            Action::ExecuteContract(_) => write!(f, "Action: Execute Contract"),
            Action::CreateContractRegistry(_) => write!(f, "Action: Create Contract Registry"),
            Action::CreateNamespaceRegistry(_) => write!(f, "Action: Create Namespace Registry"),
            Action::CreateNamespaceRegistryPermission(_) => {
                write!(f, "Create Namespace Registry Permission")
            }
        }
    }
}

impl From<CreateContractAction> for Action {
    fn from(action: CreateContractAction) -> Self {
        Action::CreateContract(action)
    }
}

impl From<ExecuteContractAction> for Action {
    fn from(action: ExecuteContractAction) -> Self {
        Action::ExecuteContract(action)
    }
}

impl From<CreateContractRegistryAction> for Action {
    fn from(action: CreateContractRegistryAction) -> Self {
        Action::CreateContractRegistry(action)
    }
}

impl From<CreateNamespaceRegistryAction> for Action {
    fn from(action: CreateNamespaceRegistryAction) -> Self {
        Action::CreateNamespaceRegistry(action)
    }
}

impl From<CreateNamespaceRegistryPermissionAction> for Action {
    fn from(action: CreateNamespaceRegistryPermissionAction) -> Self {
        Action::CreateNamespaceRegistryPermission(action)
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

/// Native implementation for CreateContractAction
#[derive(Default, Debug, Clone, PartialEq)]
pub struct CreateContractAction {
    name: String,
    version: String,
    inputs: Vec<String>,
    outputs: Vec<String>,
    contract: Vec<u8>,
}

impl CreateContractAction {
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

    pub fn contract(&self) -> &[u8] {
        &self.contract
    }
}

impl FromProto<protos::sabre::CreateContractAction> for CreateContractAction {
    fn from_proto(
        proto: protos::sabre::CreateContractAction,
    ) -> Result<Self, ProtoConversionError> {
        Ok(CreateContractAction {
            name: proto.get_name().to_string(),
            version: proto.get_version().to_string(),
            inputs: proto.get_inputs().to_vec(),
            outputs: proto.get_outputs().to_vec(),
            contract: proto.get_contract().to_vec(),
        })
    }
}

impl FromNative<CreateContractAction> for protos::sabre::CreateContractAction {
    fn from_native(
        create_contract_action: CreateContractAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::sabre::CreateContractAction::new();
        proto.set_name(create_contract_action.name().to_string());
        proto.set_version(create_contract_action.version().to_string());
        proto.set_inputs(RepeatedField::from_vec(
            create_contract_action.inputs().to_vec(),
        ));
        proto.set_outputs(RepeatedField::from_vec(
            create_contract_action.outputs().to_vec(),
        ));
        proto.set_contract(create_contract_action.contract().to_vec());
        Ok(proto)
    }
}

impl IntoProto<protos::sabre::CreateContractAction> for CreateContractAction {}
impl IntoNative<CreateContractAction> for protos::sabre::CreateContractAction {}

/// Builder used to create a CreateContractAction
#[derive(Default, Clone)]
pub struct CreateContractActionBuilder {
    name: Option<String>,
    version: Option<String>,
    inputs: Vec<String>,
    outputs: Vec<String>,
    contract: Vec<u8>,
}

impl CreateContractActionBuilder {
    pub fn new() -> Self {
        CreateContractActionBuilder::default()
    }

    pub fn with_name(mut self, name: String) -> CreateContractActionBuilder {
        self.name = Some(name);
        self
    }

    pub fn with_version(mut self, version: String) -> CreateContractActionBuilder {
        self.version = Some(version);
        self
    }

    pub fn with_inputs(mut self, inputs: Vec<String>) -> CreateContractActionBuilder {
        self.inputs = inputs;
        self
    }

    pub fn with_outputs(mut self, outputs: Vec<String>) -> CreateContractActionBuilder {
        self.outputs = outputs;
        self
    }

    pub fn with_contract(mut self, contract: Vec<u8>) -> CreateContractActionBuilder {
        self.contract = contract;
        self
    }

    pub fn build(self) -> Result<CreateContractAction, ActionBuildError> {
        let name = self.name.ok_or_else(|| {
            ActionBuildError::MissingField("'name' field is required".to_string())
        })?;

        let version = self.version.ok_or_else(|| {
            ActionBuildError::MissingField("'version' field is required".to_string())
        })?;

        let inputs = self.inputs;
        let outputs = self.outputs;

        let contract = {
            if self.contract.is_empty() {
                return Err(ActionBuildError::MissingField(
                    "'contract' field is required".to_string(),
                ));
            } else {
                self.contract
            }
        };

        Ok(CreateContractAction {
            name,
            version,
            inputs,
            outputs,
            contract,
        })
    }

    pub fn into_payload_builder(self) -> Result<SabrePayloadBuilder, ActionBuildError> {
        self.build()
            .map(|action| SabrePayloadBuilder::new().with_action(Action::from(action)))
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

/// Native implementation for CreateContractRegistryAction
#[derive(Default, Debug, Clone, PartialEq)]
pub struct CreateContractRegistryAction {
    name: String,
    owners: Vec<String>,
}

impl CreateContractRegistryAction {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn owners(&self) -> &[String] {
        &self.owners
    }
}

impl FromProto<protos::sabre::CreateContractRegistryAction> for CreateContractRegistryAction {
    fn from_proto(
        proto: protos::sabre::CreateContractRegistryAction,
    ) -> Result<Self, ProtoConversionError> {
        Ok(CreateContractRegistryAction {
            name: proto.get_name().to_string(),
            owners: proto.get_owners().to_vec(),
        })
    }
}

impl FromNative<CreateContractRegistryAction> for protos::sabre::CreateContractRegistryAction {
    fn from_native(
        create_contract_registry_action: CreateContractRegistryAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::sabre::CreateContractRegistryAction::new();
        proto.set_name(create_contract_registry_action.name().to_string());
        proto.set_owners(RepeatedField::from_vec(
            create_contract_registry_action.owners().to_vec(),
        ));
        Ok(proto)
    }
}

impl IntoProto<protos::sabre::CreateContractRegistryAction> for CreateContractRegistryAction {}
impl IntoNative<CreateContractRegistryAction> for protos::sabre::CreateContractRegistryAction {}

/// Builder used to create a CreateContractRegistryAction
#[derive(Default, Clone)]
pub struct CreateContractRegistryActionBuilder {
    name: Option<String>,
    owners: Vec<String>,
}

impl CreateContractRegistryActionBuilder {
    pub fn new() -> Self {
        CreateContractRegistryActionBuilder::default()
    }

    pub fn with_name(mut self, name: String) -> CreateContractRegistryActionBuilder {
        self.name = Some(name);
        self
    }

    pub fn with_owners(mut self, owners: Vec<String>) -> CreateContractRegistryActionBuilder {
        self.owners = owners;
        self
    }

    pub fn build(self) -> Result<CreateContractRegistryAction, ActionBuildError> {
        let name = self.name.ok_or_else(|| {
            ActionBuildError::MissingField("'name' field is required".to_string())
        })?;

        let owners = {
            if self.owners.is_empty() {
                return Err(ActionBuildError::MissingField(
                    "'owners' field is required".to_string(),
                ));
            } else {
                self.owners
            }
        };

        Ok(CreateContractRegistryAction { name, owners })
    }

    pub fn into_payload_builder(self) -> Result<SabrePayloadBuilder, ActionBuildError> {
        self.build()
            .map(|action| SabrePayloadBuilder::new().with_action(Action::from(action)))
    }
}

/// Native implementation for CreateNamespaceRegistryAction
#[derive(Default, Debug, Clone, PartialEq)]
pub struct CreateNamespaceRegistryAction {
    namespace: String,
    owners: Vec<String>,
}

impl CreateNamespaceRegistryAction {
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn owners(&self) -> &[String] {
        &self.owners
    }
}

impl FromProto<protos::sabre::CreateNamespaceRegistryAction> for CreateNamespaceRegistryAction {
    fn from_proto(
        proto: protos::sabre::CreateNamespaceRegistryAction,
    ) -> Result<Self, ProtoConversionError> {
        Ok(CreateNamespaceRegistryAction {
            namespace: proto.get_namespace().to_string(),
            owners: proto.get_owners().to_vec(),
        })
    }
}

impl FromNative<CreateNamespaceRegistryAction> for protos::sabre::CreateNamespaceRegistryAction {
    fn from_native(
        create_namespace_registry_action: CreateNamespaceRegistryAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::sabre::CreateNamespaceRegistryAction::new();
        proto.set_namespace(create_namespace_registry_action.namespace().to_string());
        proto.set_owners(RepeatedField::from_vec(
            create_namespace_registry_action.owners().to_vec(),
        ));
        Ok(proto)
    }
}

impl IntoProto<protos::sabre::CreateNamespaceRegistryAction> for CreateNamespaceRegistryAction {}
impl IntoNative<CreateNamespaceRegistryAction> for protos::sabre::CreateNamespaceRegistryAction {}

/// Builder used to create a CreateNamespaceRegistryAction
#[derive(Default, Clone)]
pub struct CreateNamespaceRegistryActionBuilder {
    namespace: Option<String>,
    owners: Vec<String>,
}

impl CreateNamespaceRegistryActionBuilder {
    pub fn new() -> Self {
        CreateNamespaceRegistryActionBuilder::default()
    }

    pub fn with_namespace(mut self, namespace: String) -> CreateNamespaceRegistryActionBuilder {
        self.namespace = Some(namespace);
        self
    }

    pub fn with_owners(mut self, owners: Vec<String>) -> CreateNamespaceRegistryActionBuilder {
        self.owners = owners;
        self
    }

    pub fn build(self) -> Result<CreateNamespaceRegistryAction, ActionBuildError> {
        let namespace = self.namespace.ok_or_else(|| {
            ActionBuildError::MissingField("'namespace' field is required".to_string())
        })?;

        let owners = {
            if self.owners.is_empty() {
                return Err(ActionBuildError::MissingField(
                    "'owners' field is required".to_string(),
                ));
            } else {
                self.owners
            }
        };

        Ok(CreateNamespaceRegistryAction { namespace, owners })
    }

    pub fn into_payload_builder(self) -> Result<SabrePayloadBuilder, ActionBuildError> {
        self.build()
            .map(|action| SabrePayloadBuilder::new().with_action(Action::from(action)))
    }
}

/// Native implementation for CreateNamespaceRegistryPermissionAction
#[derive(Default, Debug, Clone, PartialEq)]
pub struct CreateNamespaceRegistryPermissionAction {
    namespace: String,
    contract_name: String,
    read: bool,
    write: bool,
}

impl CreateNamespaceRegistryPermissionAction {
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn contract_name(&self) -> &str {
        &self.contract_name
    }

    pub fn read(&self) -> bool {
        self.read
    }

    pub fn write(&self) -> bool {
        self.write
    }
}

impl FromProto<protos::sabre::CreateNamespaceRegistryPermissionAction>
    for CreateNamespaceRegistryPermissionAction
{
    fn from_proto(
        proto: protos::sabre::CreateNamespaceRegistryPermissionAction,
    ) -> Result<Self, ProtoConversionError> {
        Ok(CreateNamespaceRegistryPermissionAction {
            namespace: proto.get_namespace().to_string(),
            contract_name: proto.get_contract_name().to_string(),
            read: proto.get_read(),
            write: proto.get_write(),
        })
    }
}

impl FromNative<CreateNamespaceRegistryPermissionAction>
    for protos::sabre::CreateNamespaceRegistryPermissionAction
{
    fn from_native(
        create_namespace_permission_action: CreateNamespaceRegistryPermissionAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::sabre::CreateNamespaceRegistryPermissionAction::new();
        proto.set_namespace(create_namespace_permission_action.namespace().to_string());
        proto.set_contract_name(
            create_namespace_permission_action
                .contract_name()
                .to_string(),
        );
        proto.set_read(create_namespace_permission_action.read());
        proto.set_write(create_namespace_permission_action.write());
        Ok(proto)
    }
}

impl IntoProto<protos::sabre::CreateNamespaceRegistryPermissionAction>
    for CreateNamespaceRegistryPermissionAction
{
}
impl IntoNative<CreateNamespaceRegistryPermissionAction>
    for protos::sabre::CreateNamespaceRegistryPermissionAction
{
}

/// Builder used to create CreateNamespaceRegistryPermissionAction
#[derive(Default, Clone)]
pub struct CreateNamespaceRegistryPermissionActionBuilder {
    namespace: Option<String>,
    contract_name: Option<String>,
    read: Option<bool>,
    write: Option<bool>,
}

impl CreateNamespaceRegistryPermissionActionBuilder {
    pub fn new() -> Self {
        CreateNamespaceRegistryPermissionActionBuilder::default()
    }

    pub fn with_namespace(
        mut self,
        namespace: String,
    ) -> CreateNamespaceRegistryPermissionActionBuilder {
        self.namespace = Some(namespace);
        self
    }

    pub fn with_contract_name(
        mut self,
        contract_name: String,
    ) -> CreateNamespaceRegistryPermissionActionBuilder {
        self.contract_name = Some(contract_name);
        self
    }

    pub fn with_read(mut self, read: bool) -> CreateNamespaceRegistryPermissionActionBuilder {
        self.read = Some(read);
        self
    }

    pub fn with_write(mut self, write: bool) -> CreateNamespaceRegistryPermissionActionBuilder {
        self.write = Some(write);
        self
    }

    pub fn build(self) -> Result<CreateNamespaceRegistryPermissionAction, ActionBuildError> {
        let namespace = self.namespace.ok_or_else(|| {
            ActionBuildError::MissingField("'namespace' field is required".to_string())
        })?;

        let contract_name = self.contract_name.ok_or_else(|| {
            ActionBuildError::MissingField("'contract_name' field is required".to_string())
        })?;

        let read = self.read.unwrap_or_default();
        let write = self.write.unwrap_or_default();

        Ok(CreateNamespaceRegistryPermissionAction {
            namespace,
            contract_name,
            read,
            write,
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
            Action::CreateContract(payload) => {
                proto.set_action(protos::sabre::SabrePayload_Action::CREATE_CONTRACT);
                proto.set_create_contract(payload.clone().into_proto()?);
            }
            Action::ExecuteContract(payload) => {
                proto.set_action(protos::sabre::SabrePayload_Action::EXECUTE_CONTRACT);
                proto.set_execute_contract(payload.clone().into_proto()?);
            }
            Action::CreateContractRegistry(payload) => {
                proto.set_action(protos::sabre::SabrePayload_Action::CREATE_CONTRACT_REGISTRY);
                proto.set_create_contract_registry(payload.clone().into_proto()?);
            }
            Action::CreateNamespaceRegistry(payload) => {
                proto.set_action(protos::sabre::SabrePayload_Action::CREATE_NAMESPACE_REGISTRY);
                proto.set_create_namespace_registry(payload.clone().into_proto()?);
            }
            Action::CreateNamespaceRegistryPermission(payload) => {
                proto.set_action(
                    protos::sabre::SabrePayload_Action::CREATE_NAMESPACE_REGISTRY_PERMISSION,
                );
                proto.set_create_namespace_registry_permission(payload.clone().into_proto()?);
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
            Action::CreateContract(CreateContractAction { name, version, .. }) => {
                let addresses = vec![
                    compute_contract_registry_address(name)?,
                    compute_contract_address(name, version)?,
                ];
                (addresses.clone(), addresses)
            }
            Action::ExecuteContract(ExecuteContractAction {
                name,
                version,
                inputs,
                outputs,
                ..
            }) => {
                let addresses = vec![
                    compute_contract_registry_address(name)?,
                    compute_contract_address(name, version)?,
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
                    input_addresses.push(parse_hex(input)?);
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
                    output_addresses.push(parse_hex(output)?);
                }

                (input_addresses, output_addresses)
            }
            Action::CreateContractRegistry(CreateContractRegistryAction { name, .. }) => {
                let addresses = vec![
                    compute_contract_registry_address(name)?,
                    ADMINISTRATORS_SETTING_ADDRESS_BYTES.to_vec(),
                ];
                (addresses.clone(), addresses)
            }
            Action::CreateNamespaceRegistry(CreateNamespaceRegistryAction {
                namespace, ..
            })
            | Action::CreateNamespaceRegistryPermission(
                CreateNamespaceRegistryPermissionAction { namespace, .. },
            ) => {
                let addresses = vec![
                    compute_namespace_registry_address(namespace)?,
                    ADMINISTRATORS_SETTING_ADDRESS_BYTES.to_vec(),
                ];
                (addresses.clone(), addresses)
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
            .with_payload_hash_method(HashMethod::Sha512)
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
    hasher.update(bytes);
    hasher.finalize().to_vec()
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

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
#[cfg(not(target_arch = "wasm32"))]
use cylinder::Signer;
use protobuf::Message;
use protobuf::RepeatedField;
#[cfg(not(target_arch = "wasm32"))]
use transact::protocol::transaction::{HashMethod, TransactionBuilder};

use std::error::Error as StdError;

use crate::protos;
use crate::protos::{
    FromBytes, FromNative, FromProto, IntoBytes, IntoNative, IntoProto, ProtoConversionError,
};

use super::AddressingError;
#[cfg(not(target_arch = "wasm32"))]
use super::{
    compute_agent_address, compute_contract_address, compute_contract_registry_address,
    compute_namespace_registry_address, compute_org_address, compute_smart_permission_address,
    ADMINISTRATORS_SETTING_ADDRESS_BYTES, SABRE_PROTOCOL_VERSION,
};

/// Native implementation for SabrePayload_Action
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    CreateContract(CreateContractAction),
    DeleteContract(DeleteContractAction),
    ExecuteContract(ExecuteContractAction),
    CreateContractRegistry(CreateContractRegistryAction),
    DeleteContractRegistry(DeleteContractRegistryAction),
    UpdateContractRegistryOwners(UpdateContractRegistryOwnersAction),
    CreateNamespaceRegistry(CreateNamespaceRegistryAction),
    DeleteNamespaceRegistry(DeleteNamespaceRegistryAction),
    UpdateNamespaceRegistryOwners(UpdateNamespaceRegistryOwnersAction),
    CreateNamespaceRegistryPermission(CreateNamespaceRegistryPermissionAction),
    DeleteNamespaceRegistryPermission(DeleteNamespaceRegistryPermissionAction),
    CreateSmartPermission(CreateSmartPermissionAction),
    UpdateSmartPermission(UpdateSmartPermissionAction),
    DeleteSmartPermission(DeleteSmartPermissionAction),
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Action::CreateContract(_) => write!(f, "Action: Create Contract"),
            Action::DeleteContract(_) => write!(f, "Action: Delete Contract"),
            Action::ExecuteContract(_) => write!(f, "Action: Execute Contract"),
            Action::CreateContractRegistry(_) => write!(f, "Action: Create Contract Registry"),
            Action::DeleteContractRegistry(_) => write!(f, "Action: Delete Contract Registry"),
            Action::UpdateContractRegistryOwners(_) => {
                write!(f, "Action: Update Contract Registry Owners")
            }
            Action::CreateNamespaceRegistry(_) => write!(f, "Action: Create Namespace Registry"),
            Action::DeleteNamespaceRegistry(_) => write!(f, "Action: Delete Namespace Registry"),
            Action::UpdateNamespaceRegistryOwners(_) => {
                write!(f, "Action: Update Namespace Registry Owners")
            }
            Action::CreateNamespaceRegistryPermission(_) => {
                write!(f, "Create Namespace Registry Permission")
            }
            Action::DeleteNamespaceRegistryPermission(_) => {
                write!(f, "Delete Namespace Registry Permission")
            }
            Action::CreateSmartPermission(_) => write!(f, "Create smart permission"),
            Action::UpdateSmartPermission(_) => write!(f, "Update smart permission"),
            Action::DeleteSmartPermission(_) => write!(f, "Delete smart permission"),
        }
    }
}

impl From<CreateContractAction> for Action {
    fn from(action: CreateContractAction) -> Self {
        Action::CreateContract(action)
    }
}

impl From<DeleteContractAction> for Action {
    fn from(action: DeleteContractAction) -> Self {
        Action::DeleteContract(action)
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

impl From<DeleteContractRegistryAction> for Action {
    fn from(action: DeleteContractRegistryAction) -> Self {
        Action::DeleteContractRegistry(action)
    }
}

impl From<UpdateContractRegistryOwnersAction> for Action {
    fn from(action: UpdateContractRegistryOwnersAction) -> Self {
        Action::UpdateContractRegistryOwners(action)
    }
}

impl From<CreateNamespaceRegistryAction> for Action {
    fn from(action: CreateNamespaceRegistryAction) -> Self {
        Action::CreateNamespaceRegistry(action)
    }
}

impl From<DeleteNamespaceRegistryAction> for Action {
    fn from(action: DeleteNamespaceRegistryAction) -> Self {
        Action::DeleteNamespaceRegistry(action)
    }
}

impl From<UpdateNamespaceRegistryOwnersAction> for Action {
    fn from(action: UpdateNamespaceRegistryOwnersAction) -> Self {
        Action::UpdateNamespaceRegistryOwners(action)
    }
}

impl From<CreateNamespaceRegistryPermissionAction> for Action {
    fn from(action: CreateNamespaceRegistryPermissionAction) -> Self {
        Action::CreateNamespaceRegistryPermission(action)
    }
}

impl From<DeleteNamespaceRegistryPermissionAction> for Action {
    fn from(action: DeleteNamespaceRegistryPermissionAction) -> Self {
        Action::DeleteNamespaceRegistryPermission(action)
    }
}

impl From<CreateSmartPermissionAction> for Action {
    fn from(action: CreateSmartPermissionAction) -> Self {
        Action::CreateSmartPermission(action)
    }
}

impl From<UpdateSmartPermissionAction> for Action {
    fn from(action: UpdateSmartPermissionAction) -> Self {
        Action::UpdateSmartPermission(action)
    }
}

impl From<DeleteSmartPermissionAction> for Action {
    fn from(action: DeleteSmartPermissionAction) -> Self {
        Action::DeleteSmartPermission(action)
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

impl FromProto<protos::payload::CreateContractAction> for CreateContractAction {
    fn from_proto(
        proto: protos::payload::CreateContractAction,
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

impl FromNative<CreateContractAction> for protos::payload::CreateContractAction {
    fn from_native(
        create_contract_action: CreateContractAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::payload::CreateContractAction::new();
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

impl FromBytes<CreateContractAction> for CreateContractAction {
    fn from_bytes(bytes: &[u8]) -> Result<CreateContractAction, ProtoConversionError> {
        let proto: protos::payload::CreateContractAction = Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get CreateContractAction from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for CreateContractAction {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from CreateContractAction".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::payload::CreateContractAction> for CreateContractAction {}
impl IntoNative<CreateContractAction> for protos::payload::CreateContractAction {}

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

/// Native implementation for DeleteContractAction
#[derive(Default, Debug, Clone, PartialEq)]
pub struct DeleteContractAction {
    name: String,
    version: String,
}

impl DeleteContractAction {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> &str {
        &self.version
    }
}

impl FromProto<protos::payload::DeleteContractAction> for DeleteContractAction {
    fn from_proto(
        proto: protos::payload::DeleteContractAction,
    ) -> Result<Self, ProtoConversionError> {
        Ok(DeleteContractAction {
            name: proto.get_name().to_string(),
            version: proto.get_version().to_string(),
        })
    }
}

impl FromNative<DeleteContractAction> for protos::payload::DeleteContractAction {
    fn from_native(
        delete_contract_action: DeleteContractAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::payload::DeleteContractAction::new();
        proto.set_name(delete_contract_action.name().to_string());
        proto.set_version(delete_contract_action.version().to_string());
        Ok(proto)
    }
}

impl FromBytes<DeleteContractAction> for DeleteContractAction {
    fn from_bytes(bytes: &[u8]) -> Result<DeleteContractAction, ProtoConversionError> {
        let proto: protos::payload::DeleteContractAction = Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get DeleteContractAction from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for DeleteContractAction {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from DeleteContractAction".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::payload::DeleteContractAction> for DeleteContractAction {}
impl IntoNative<DeleteContractAction> for protos::payload::DeleteContractAction {}

/// Builder used to create a DeleteContractAction
#[derive(Default, Clone)]
pub struct DeleteContractActionBuilder {
    name: Option<String>,
    version: Option<String>,
}

impl DeleteContractActionBuilder {
    pub fn new() -> Self {
        DeleteContractActionBuilder::default()
    }

    pub fn with_name(mut self, name: String) -> DeleteContractActionBuilder {
        self.name = Some(name);
        self
    }

    pub fn with_version(mut self, version: String) -> DeleteContractActionBuilder {
        self.version = Some(version);
        self
    }

    pub fn build(self) -> Result<DeleteContractAction, ActionBuildError> {
        let name = self.name.ok_or_else(|| {
            ActionBuildError::MissingField("'name' field is required".to_string())
        })?;

        let version = self.version.ok_or_else(|| {
            ActionBuildError::MissingField("'version' field is required".to_string())
        })?;

        Ok(DeleteContractAction { name, version })
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

impl FromProto<protos::payload::ExecuteContractAction> for ExecuteContractAction {
    fn from_proto(
        proto: protos::payload::ExecuteContractAction,
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

impl FromNative<ExecuteContractAction> for protos::payload::ExecuteContractAction {
    fn from_native(
        execute_contract_action: ExecuteContractAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::payload::ExecuteContractAction::new();
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
        let proto: protos::payload::ExecuteContractAction = Message::parse_from_bytes(bytes)
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

impl IntoProto<protos::payload::ExecuteContractAction> for ExecuteContractAction {}
impl IntoNative<ExecuteContractAction> for protos::payload::ExecuteContractAction {}

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

impl FromProto<protos::payload::CreateContractRegistryAction> for CreateContractRegistryAction {
    fn from_proto(
        proto: protos::payload::CreateContractRegistryAction,
    ) -> Result<Self, ProtoConversionError> {
        Ok(CreateContractRegistryAction {
            name: proto.get_name().to_string(),
            owners: proto.get_owners().to_vec(),
        })
    }
}

impl FromNative<CreateContractRegistryAction> for protos::payload::CreateContractRegistryAction {
    fn from_native(
        create_contract_registry_action: CreateContractRegistryAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::payload::CreateContractRegistryAction::new();
        proto.set_name(create_contract_registry_action.name().to_string());
        proto.set_owners(RepeatedField::from_vec(
            create_contract_registry_action.owners().to_vec(),
        ));
        Ok(proto)
    }
}

impl FromBytes<CreateContractRegistryAction> for CreateContractRegistryAction {
    fn from_bytes(bytes: &[u8]) -> Result<CreateContractRegistryAction, ProtoConversionError> {
        let proto: protos::payload::CreateContractRegistryAction = Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get CreateContractRegistryAction from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for CreateContractRegistryAction {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from CreateContractRegistryAction".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::payload::CreateContractRegistryAction> for CreateContractRegistryAction {}
impl IntoNative<CreateContractRegistryAction> for protos::payload::CreateContractRegistryAction {}

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

/// Native implementation for DeleteContractRegistryAction
#[derive(Default, Debug, Clone, PartialEq)]
pub struct DeleteContractRegistryAction {
    name: String,
}

impl DeleteContractRegistryAction {
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl FromProto<protos::payload::DeleteContractRegistryAction> for DeleteContractRegistryAction {
    fn from_proto(
        proto: protos::payload::DeleteContractRegistryAction,
    ) -> Result<Self, ProtoConversionError> {
        Ok(DeleteContractRegistryAction {
            name: proto.get_name().to_string(),
        })
    }
}

impl FromNative<DeleteContractRegistryAction> for protos::payload::DeleteContractRegistryAction {
    fn from_native(
        delete_contract_registry_action: DeleteContractRegistryAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::payload::DeleteContractRegistryAction::new();
        proto.set_name(delete_contract_registry_action.name().to_string());
        Ok(proto)
    }
}

impl FromBytes<DeleteContractRegistryAction> for DeleteContractRegistryAction {
    fn from_bytes(bytes: &[u8]) -> Result<DeleteContractRegistryAction, ProtoConversionError> {
        let proto: protos::payload::DeleteContractRegistryAction = Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get DeleteContractRegistryAction from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for DeleteContractRegistryAction {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from DeleteContractRegistryAction".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::payload::DeleteContractRegistryAction> for DeleteContractRegistryAction {}
impl IntoNative<DeleteContractRegistryAction> for protos::payload::DeleteContractRegistryAction {}

/// Builder used to create a DeleteContractRegistryAction
#[derive(Default, Clone)]
pub struct DeleteContractRegistryActionBuilder {
    name: Option<String>,
}

impl DeleteContractRegistryActionBuilder {
    pub fn new() -> Self {
        DeleteContractRegistryActionBuilder::default()
    }

    pub fn with_name(mut self, name: String) -> DeleteContractRegistryActionBuilder {
        self.name = Some(name);
        self
    }

    pub fn build(self) -> Result<DeleteContractRegistryAction, ActionBuildError> {
        let name = self.name.ok_or_else(|| {
            ActionBuildError::MissingField("'name' field is required".to_string())
        })?;

        Ok(DeleteContractRegistryAction { name })
    }

    pub fn into_payload_builder(self) -> Result<SabrePayloadBuilder, ActionBuildError> {
        self.build()
            .map(|action| SabrePayloadBuilder::new().with_action(Action::from(action)))
    }
}

/// Native implementation for UpdateContractRegistryOwnersAction
#[derive(Default, Debug, Clone, PartialEq)]
pub struct UpdateContractRegistryOwnersAction {
    name: String,
    owners: Vec<String>,
}

impl UpdateContractRegistryOwnersAction {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn owners(&self) -> &[String] {
        &self.owners
    }
}

impl FromProto<protos::payload::UpdateContractRegistryOwnersAction>
    for UpdateContractRegistryOwnersAction
{
    fn from_proto(
        proto: protos::payload::UpdateContractRegistryOwnersAction,
    ) -> Result<Self, ProtoConversionError> {
        Ok(UpdateContractRegistryOwnersAction {
            name: proto.get_name().to_string(),
            owners: proto.get_owners().to_vec(),
        })
    }
}

impl FromNative<UpdateContractRegistryOwnersAction>
    for protos::payload::UpdateContractRegistryOwnersAction
{
    fn from_native(
        update_contract_registry_action: UpdateContractRegistryOwnersAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::payload::UpdateContractRegistryOwnersAction::new();
        proto.set_name(update_contract_registry_action.name().to_string());
        proto.set_owners(RepeatedField::from_vec(
            update_contract_registry_action.owners().to_vec(),
        ));
        Ok(proto)
    }
}

impl FromBytes<UpdateContractRegistryOwnersAction> for UpdateContractRegistryOwnersAction {
    fn from_bytes(
        bytes: &[u8],
    ) -> Result<UpdateContractRegistryOwnersAction, ProtoConversionError> {
        let proto: protos::payload::UpdateContractRegistryOwnersAction =
            Message::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get UpdateContractRegistryOwnersAction from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for UpdateContractRegistryOwnersAction {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from UpdateContractRegistryOwnersAction".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::payload::UpdateContractRegistryOwnersAction>
    for UpdateContractRegistryOwnersAction
{
}
impl IntoNative<UpdateContractRegistryOwnersAction>
    for protos::payload::UpdateContractRegistryOwnersAction
{
}

/// Builder used to create a UpdateContractRegistryOwnersAction
#[derive(Default, Clone)]
pub struct UpdateContractRegistryOwnersActionBuilder {
    name: Option<String>,
    owners: Vec<String>,
}

impl UpdateContractRegistryOwnersActionBuilder {
    pub fn new() -> Self {
        UpdateContractRegistryOwnersActionBuilder::default()
    }

    pub fn with_name(mut self, name: String) -> UpdateContractRegistryOwnersActionBuilder {
        self.name = Some(name);
        self
    }

    pub fn with_owners(mut self, owners: Vec<String>) -> UpdateContractRegistryOwnersActionBuilder {
        self.owners = owners;
        self
    }

    pub fn build(self) -> Result<UpdateContractRegistryOwnersAction, ActionBuildError> {
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

        Ok(UpdateContractRegistryOwnersAction { name, owners })
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

impl FromProto<protos::payload::CreateNamespaceRegistryAction> for CreateNamespaceRegistryAction {
    fn from_proto(
        proto: protos::payload::CreateNamespaceRegistryAction,
    ) -> Result<Self, ProtoConversionError> {
        Ok(CreateNamespaceRegistryAction {
            namespace: proto.get_namespace().to_string(),
            owners: proto.get_owners().to_vec(),
        })
    }
}

impl FromNative<CreateNamespaceRegistryAction> for protos::payload::CreateNamespaceRegistryAction {
    fn from_native(
        create_namespace_registry_action: CreateNamespaceRegistryAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::payload::CreateNamespaceRegistryAction::new();
        proto.set_namespace(create_namespace_registry_action.namespace().to_string());
        proto.set_owners(RepeatedField::from_vec(
            create_namespace_registry_action.owners().to_vec(),
        ));
        Ok(proto)
    }
}

impl FromBytes<CreateNamespaceRegistryAction> for CreateNamespaceRegistryAction {
    fn from_bytes(bytes: &[u8]) -> Result<CreateNamespaceRegistryAction, ProtoConversionError> {
        let proto: protos::payload::CreateNamespaceRegistryAction =
            Message::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get CreateNamespaceRegistryAction from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for CreateNamespaceRegistryAction {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from CreateNamespaceRegistryAction".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::payload::CreateNamespaceRegistryAction> for CreateNamespaceRegistryAction {}
impl IntoNative<CreateNamespaceRegistryAction> for protos::payload::CreateNamespaceRegistryAction {}

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

/// Native implementation for DeleteNamespaceRegistryAction
#[derive(Default, Debug, Clone, PartialEq)]
pub struct DeleteNamespaceRegistryAction {
    namespace: String,
}

impl DeleteNamespaceRegistryAction {
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}
impl FromProto<protos::payload::DeleteNamespaceRegistryAction> for DeleteNamespaceRegistryAction {
    fn from_proto(
        proto: protos::payload::DeleteNamespaceRegistryAction,
    ) -> Result<Self, ProtoConversionError> {
        Ok(DeleteNamespaceRegistryAction {
            namespace: proto.get_namespace().to_string(),
        })
    }
}

impl FromNative<DeleteNamespaceRegistryAction> for protos::payload::DeleteNamespaceRegistryAction {
    fn from_native(
        delete_namespace_registry_action: DeleteNamespaceRegistryAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::payload::DeleteNamespaceRegistryAction::new();
        proto.set_namespace(delete_namespace_registry_action.namespace().to_string());
        Ok(proto)
    }
}

impl FromBytes<DeleteNamespaceRegistryAction> for DeleteNamespaceRegistryAction {
    fn from_bytes(bytes: &[u8]) -> Result<DeleteNamespaceRegistryAction, ProtoConversionError> {
        let proto: protos::payload::DeleteNamespaceRegistryAction =
            Message::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get DeleteNamespaceRegistryAction from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for DeleteNamespaceRegistryAction {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from DeleteNamespaceRegistryAction".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::payload::DeleteNamespaceRegistryAction> for DeleteNamespaceRegistryAction {}
impl IntoNative<DeleteNamespaceRegistryAction> for protos::payload::DeleteNamespaceRegistryAction {}

/// Builder used to create a DeleteNamespaceRegistryAction
#[derive(Default, Clone)]
pub struct DeleteNamespaceRegistryActionBuilder {
    namespace: Option<String>,
}

impl DeleteNamespaceRegistryActionBuilder {
    pub fn new() -> Self {
        DeleteNamespaceRegistryActionBuilder::default()
    }

    pub fn with_namespace(mut self, namespace: String) -> DeleteNamespaceRegistryActionBuilder {
        self.namespace = Some(namespace);
        self
    }

    pub fn build(self) -> Result<DeleteNamespaceRegistryAction, ActionBuildError> {
        let namespace = self.namespace.ok_or_else(|| {
            ActionBuildError::MissingField("'namespace' field is required".to_string())
        })?;

        Ok(DeleteNamespaceRegistryAction { namespace })
    }

    pub fn into_payload_builder(self) -> Result<SabrePayloadBuilder, ActionBuildError> {
        self.build()
            .map(|action| SabrePayloadBuilder::new().with_action(Action::from(action)))
    }
}

/// Native implementation for UpdateNamespaceRegistryOwnersAction
#[derive(Default, Debug, Clone, PartialEq)]
pub struct UpdateNamespaceRegistryOwnersAction {
    namespace: String,
    owners: Vec<String>,
}

impl UpdateNamespaceRegistryOwnersAction {
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn owners(&self) -> &[String] {
        &self.owners
    }
}

impl FromProto<protos::payload::UpdateNamespaceRegistryOwnersAction>
    for UpdateNamespaceRegistryOwnersAction
{
    fn from_proto(
        proto: protos::payload::UpdateNamespaceRegistryOwnersAction,
    ) -> Result<Self, ProtoConversionError> {
        Ok(UpdateNamespaceRegistryOwnersAction {
            namespace: proto.get_namespace().to_string(),
            owners: proto.get_owners().to_vec(),
        })
    }
}

impl FromNative<UpdateNamespaceRegistryOwnersAction>
    for protos::payload::UpdateNamespaceRegistryOwnersAction
{
    fn from_native(
        update_namespace_registry_action: UpdateNamespaceRegistryOwnersAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::payload::UpdateNamespaceRegistryOwnersAction::new();
        proto.set_namespace(update_namespace_registry_action.namespace().to_string());
        proto.set_owners(RepeatedField::from_vec(
            update_namespace_registry_action.owners().to_vec(),
        ));
        Ok(proto)
    }
}

impl FromBytes<UpdateNamespaceRegistryOwnersAction> for UpdateNamespaceRegistryOwnersAction {
    fn from_bytes(
        bytes: &[u8],
    ) -> Result<UpdateNamespaceRegistryOwnersAction, ProtoConversionError> {
        let proto: protos::payload::UpdateNamespaceRegistryOwnersAction =
            Message::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get UpdateNamespaceRegistryOwnersAction from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for UpdateNamespaceRegistryOwnersAction {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from UpdateNamespaceRegistryOwnersAction".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::payload::UpdateNamespaceRegistryOwnersAction>
    for UpdateNamespaceRegistryOwnersAction
{
}
impl IntoNative<UpdateNamespaceRegistryOwnersAction>
    for protos::payload::UpdateNamespaceRegistryOwnersAction
{
}

/// Builder used to create UpdateNamespaceRegistryOwnersAction
#[derive(Default, Clone)]
pub struct UpdateNamespaceRegistryOwnersActionBuilder {
    namespace: Option<String>,
    owners: Vec<String>,
}

impl UpdateNamespaceRegistryOwnersActionBuilder {
    pub fn new() -> Self {
        UpdateNamespaceRegistryOwnersActionBuilder::default()
    }

    pub fn with_namespace(
        mut self,
        namespace: String,
    ) -> UpdateNamespaceRegistryOwnersActionBuilder {
        self.namespace = Some(namespace);
        self
    }

    pub fn with_owners(
        mut self,
        owners: Vec<String>,
    ) -> UpdateNamespaceRegistryOwnersActionBuilder {
        self.owners = owners;
        self
    }

    pub fn build(self) -> Result<UpdateNamespaceRegistryOwnersAction, ActionBuildError> {
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

        Ok(UpdateNamespaceRegistryOwnersAction { namespace, owners })
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

impl FromProto<protos::payload::CreateNamespaceRegistryPermissionAction>
    for CreateNamespaceRegistryPermissionAction
{
    fn from_proto(
        proto: protos::payload::CreateNamespaceRegistryPermissionAction,
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
    for protos::payload::CreateNamespaceRegistryPermissionAction
{
    fn from_native(
        create_namespace_permission_action: CreateNamespaceRegistryPermissionAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::payload::CreateNamespaceRegistryPermissionAction::new();
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

impl FromBytes<CreateNamespaceRegistryPermissionAction>
    for CreateNamespaceRegistryPermissionAction
{
    fn from_bytes(
        bytes: &[u8],
    ) -> Result<CreateNamespaceRegistryPermissionAction, ProtoConversionError> {
        let proto: protos::payload::CreateNamespaceRegistryPermissionAction =
            Message::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get CreateNamespaceRegistryPermissionAction from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for CreateNamespaceRegistryPermissionAction {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from CreateNamespaceRegistryPermissionAction".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::payload::CreateNamespaceRegistryPermissionAction>
    for CreateNamespaceRegistryPermissionAction
{
}
impl IntoNative<CreateNamespaceRegistryPermissionAction>
    for protos::payload::CreateNamespaceRegistryPermissionAction
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

/// Native implementation for DeleteNamespaceRegistryPermissionAction
#[derive(Default, Debug, Clone, PartialEq)]
pub struct DeleteNamespaceRegistryPermissionAction {
    namespace: String,
    contract_name: String,
}

impl DeleteNamespaceRegistryPermissionAction {
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn contract_name(&self) -> &str {
        &self.contract_name
    }
}

impl FromProto<protos::payload::DeleteNamespaceRegistryPermissionAction>
    for DeleteNamespaceRegistryPermissionAction
{
    fn from_proto(
        proto: protos::payload::DeleteNamespaceRegistryPermissionAction,
    ) -> Result<Self, ProtoConversionError> {
        Ok(DeleteNamespaceRegistryPermissionAction {
            namespace: proto.get_namespace().to_string(),
            contract_name: proto.get_contract_name().to_string(),
        })
    }
}

impl FromNative<DeleteNamespaceRegistryPermissionAction>
    for protos::payload::DeleteNamespaceRegistryPermissionAction
{
    fn from_native(
        delete_namespace_permission_action: DeleteNamespaceRegistryPermissionAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::payload::DeleteNamespaceRegistryPermissionAction::new();
        proto.set_namespace(delete_namespace_permission_action.namespace().to_string());
        proto.set_contract_name(
            delete_namespace_permission_action
                .contract_name()
                .to_string(),
        );
        Ok(proto)
    }
}

impl FromBytes<DeleteNamespaceRegistryPermissionAction>
    for DeleteNamespaceRegistryPermissionAction
{
    fn from_bytes(
        bytes: &[u8],
    ) -> Result<DeleteNamespaceRegistryPermissionAction, ProtoConversionError> {
        let proto: protos::payload::DeleteNamespaceRegistryPermissionAction =
            Message::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get DeleteNamespaceRegistryPermissionAction from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for DeleteNamespaceRegistryPermissionAction {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from DeleteNamespaceRegistryPermissionAction".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::payload::DeleteNamespaceRegistryPermissionAction>
    for DeleteNamespaceRegistryPermissionAction
{
}
impl IntoNative<DeleteNamespaceRegistryPermissionAction>
    for protos::payload::DeleteNamespaceRegistryPermissionAction
{
}

/// Builder used to create DeleteNamespaceRegistryPermissionAction
#[derive(Default, Clone)]
pub struct DeleteNamespaceRegistryPermissionActionBuilder {
    namespace: Option<String>,
    contract_name: Option<String>,
}

impl DeleteNamespaceRegistryPermissionActionBuilder {
    pub fn new() -> Self {
        DeleteNamespaceRegistryPermissionActionBuilder::default()
    }

    pub fn with_namespace(
        mut self,
        namespace: String,
    ) -> DeleteNamespaceRegistryPermissionActionBuilder {
        self.namespace = Some(namespace);
        self
    }

    pub fn with_contract_name(
        mut self,
        contract_name: String,
    ) -> DeleteNamespaceRegistryPermissionActionBuilder {
        self.contract_name = Some(contract_name);
        self
    }

    pub fn build(self) -> Result<DeleteNamespaceRegistryPermissionAction, ActionBuildError> {
        let namespace = self.namespace.ok_or_else(|| {
            ActionBuildError::MissingField("'namespace' field is required".to_string())
        })?;

        let contract_name = self.contract_name.ok_or_else(|| {
            ActionBuildError::MissingField("'contract_name' field is required".to_string())
        })?;

        Ok(DeleteNamespaceRegistryPermissionAction {
            namespace,
            contract_name,
        })
    }

    pub fn into_payload_builder(self) -> Result<SabrePayloadBuilder, ActionBuildError> {
        self.build()
            .map(|action| SabrePayloadBuilder::new().with_action(Action::from(action)))
    }
}

/// Native implementation for CreateSmartPermissionAction
#[derive(Default, Debug, Clone, PartialEq)]
pub struct CreateSmartPermissionAction {
    name: String,
    org_id: String,
    function: Vec<u8>,
}

impl CreateSmartPermissionAction {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn org_id(&self) -> &str {
        &self.org_id
    }

    pub fn function(&self) -> &[u8] {
        &self.function
    }
}

impl FromProto<protos::payload::CreateSmartPermissionAction> for CreateSmartPermissionAction {
    fn from_proto(
        proto: protos::payload::CreateSmartPermissionAction,
    ) -> Result<Self, ProtoConversionError> {
        Ok(CreateSmartPermissionAction {
            name: proto.get_name().to_string(),
            org_id: proto.get_org_id().to_string(),
            function: proto.get_function().to_vec(),
        })
    }
}

impl FromNative<CreateSmartPermissionAction> for protos::payload::CreateSmartPermissionAction {
    fn from_native(
        create_smart_permission_action: CreateSmartPermissionAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::payload::CreateSmartPermissionAction::new();
        proto.set_name(create_smart_permission_action.name().to_string());
        proto.set_org_id(create_smart_permission_action.org_id().to_string());
        proto.set_function(create_smart_permission_action.function().to_vec());
        Ok(proto)
    }
}

impl FromBytes<CreateSmartPermissionAction> for CreateSmartPermissionAction {
    fn from_bytes(bytes: &[u8]) -> Result<CreateSmartPermissionAction, ProtoConversionError> {
        let proto: protos::payload::CreateSmartPermissionAction = Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get CreateSmartPermissionAction from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for CreateSmartPermissionAction {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from CreateSmartPermissionAction".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::payload::CreateSmartPermissionAction> for CreateSmartPermissionAction {}
impl IntoNative<CreateSmartPermissionAction> for protos::payload::CreateSmartPermissionAction {}

/// Builder used to create CreateSmartPermissionAction
#[derive(Default, Clone)]
pub struct CreateSmartPermissionActionBuilder {
    name: Option<String>,
    org_id: Option<String>,
    function: Vec<u8>,
}

impl CreateSmartPermissionActionBuilder {
    pub fn new() -> Self {
        CreateSmartPermissionActionBuilder::default()
    }

    pub fn with_name(mut self, name: String) -> CreateSmartPermissionActionBuilder {
        self.name = Some(name);
        self
    }

    pub fn with_org_id(mut self, org_id: String) -> CreateSmartPermissionActionBuilder {
        self.org_id = Some(org_id);
        self
    }

    pub fn with_function(mut self, function: Vec<u8>) -> CreateSmartPermissionActionBuilder {
        self.function = function;
        self
    }

    pub fn build(self) -> Result<CreateSmartPermissionAction, ActionBuildError> {
        let name = self.name.ok_or_else(|| {
            ActionBuildError::MissingField("'name' field is required".to_string())
        })?;

        let org_id = self.org_id.ok_or_else(|| {
            ActionBuildError::MissingField("'org_id' field is required".to_string())
        })?;

        let function = {
            if self.function.is_empty() {
                return Err(ActionBuildError::MissingField(
                    "'function' field is required".to_string(),
                ));
            } else {
                self.function
            }
        };

        Ok(CreateSmartPermissionAction {
            name,
            org_id,
            function,
        })
    }

    pub fn into_payload_builder(self) -> Result<SabrePayloadBuilder, ActionBuildError> {
        self.build()
            .map(|action| SabrePayloadBuilder::new().with_action(Action::from(action)))
    }
}

/// Native implementation for UpdateSmartPermissionAction
#[derive(Default, Debug, Clone, PartialEq)]
pub struct UpdateSmartPermissionAction {
    name: String,
    org_id: String,
    function: Vec<u8>,
}

impl UpdateSmartPermissionAction {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn org_id(&self) -> &str {
        &self.org_id
    }

    pub fn function(&self) -> &[u8] {
        &self.function
    }
}

impl FromProto<protos::payload::UpdateSmartPermissionAction> for UpdateSmartPermissionAction {
    fn from_proto(
        proto: protos::payload::UpdateSmartPermissionAction,
    ) -> Result<Self, ProtoConversionError> {
        Ok(UpdateSmartPermissionAction {
            name: proto.get_name().to_string(),
            org_id: proto.get_org_id().to_string(),
            function: proto.get_function().to_vec(),
        })
    }
}

impl FromNative<UpdateSmartPermissionAction> for protos::payload::UpdateSmartPermissionAction {
    fn from_native(
        update_smart_permission_action: UpdateSmartPermissionAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::payload::UpdateSmartPermissionAction::new();
        proto.set_name(update_smart_permission_action.name().to_string());
        proto.set_org_id(update_smart_permission_action.org_id().to_string());
        proto.set_function(update_smart_permission_action.function().to_vec());
        Ok(proto)
    }
}

impl FromBytes<UpdateSmartPermissionAction> for UpdateSmartPermissionAction {
    fn from_bytes(bytes: &[u8]) -> Result<UpdateSmartPermissionAction, ProtoConversionError> {
        let proto: protos::payload::UpdateSmartPermissionAction = Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get UpdateSmartPermissionAction from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for UpdateSmartPermissionAction {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from UpdateSmartPermissionAction".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::payload::UpdateSmartPermissionAction> for UpdateSmartPermissionAction {}
impl IntoNative<UpdateSmartPermissionAction> for protos::payload::UpdateSmartPermissionAction {}

/// Builder used to create UpdateSmartPermissionAction
#[derive(Default, Clone)]
pub struct UpdateSmartPermissionActionBuilder {
    name: Option<String>,
    org_id: Option<String>,
    function: Vec<u8>,
}

impl UpdateSmartPermissionActionBuilder {
    pub fn new() -> Self {
        UpdateSmartPermissionActionBuilder::default()
    }

    pub fn with_name(mut self, name: String) -> UpdateSmartPermissionActionBuilder {
        self.name = Some(name);
        self
    }

    pub fn with_org_id(mut self, org_id: String) -> UpdateSmartPermissionActionBuilder {
        self.org_id = Some(org_id);
        self
    }

    pub fn with_function(mut self, function: Vec<u8>) -> UpdateSmartPermissionActionBuilder {
        self.function = function;
        self
    }

    pub fn build(self) -> Result<UpdateSmartPermissionAction, ActionBuildError> {
        let name = self.name.ok_or_else(|| {
            ActionBuildError::MissingField("'name' field is required".to_string())
        })?;

        let org_id = self.org_id.ok_or_else(|| {
            ActionBuildError::MissingField("'org_id' field is required".to_string())
        })?;

        let function = {
            if self.function.is_empty() {
                return Err(ActionBuildError::MissingField(
                    "'function' field is required".to_string(),
                ));
            } else {
                self.function
            }
        };

        Ok(UpdateSmartPermissionAction {
            name,
            org_id,
            function,
        })
    }

    pub fn into_payload_builder(self) -> Result<SabrePayloadBuilder, ActionBuildError> {
        self.build()
            .map(|action| SabrePayloadBuilder::new().with_action(Action::from(action)))
    }
}

/// Native implementation for DeleteSmartPermissionAction
#[derive(Default, Debug, Clone, PartialEq)]
pub struct DeleteSmartPermissionAction {
    name: String,
    org_id: String,
}

impl DeleteSmartPermissionAction {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn org_id(&self) -> &str {
        &self.org_id
    }
}

impl FromProto<protos::payload::DeleteSmartPermissionAction> for DeleteSmartPermissionAction {
    fn from_proto(
        proto: protos::payload::DeleteSmartPermissionAction,
    ) -> Result<Self, ProtoConversionError> {
        Ok(DeleteSmartPermissionAction {
            name: proto.get_name().to_string(),
            org_id: proto.get_org_id().to_string(),
        })
    }
}

impl FromNative<DeleteSmartPermissionAction> for protos::payload::DeleteSmartPermissionAction {
    fn from_native(
        delete_smart_permission_action: DeleteSmartPermissionAction,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::payload::DeleteSmartPermissionAction::new();
        proto.set_name(delete_smart_permission_action.name().to_string());
        proto.set_org_id(delete_smart_permission_action.org_id().to_string());
        Ok(proto)
    }
}

impl FromBytes<DeleteSmartPermissionAction> for DeleteSmartPermissionAction {
    fn from_bytes(bytes: &[u8]) -> Result<DeleteSmartPermissionAction, ProtoConversionError> {
        let proto: protos::payload::DeleteSmartPermissionAction = Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get DeleteSmartPermissionAction from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for DeleteSmartPermissionAction {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from DeleteSmartPermissionAction".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::payload::DeleteSmartPermissionAction> for DeleteSmartPermissionAction {}
impl IntoNative<DeleteSmartPermissionAction> for protos::payload::DeleteSmartPermissionAction {}

/// Builder used to create DeleteSmartPermissionAction
#[derive(Default, Clone)]
pub struct DeleteSmartPermissionActionBuilder {
    name: Option<String>,
    org_id: Option<String>,
}

impl DeleteSmartPermissionActionBuilder {
    pub fn new() -> Self {
        DeleteSmartPermissionActionBuilder::default()
    }

    pub fn with_name(mut self, name: String) -> DeleteSmartPermissionActionBuilder {
        self.name = Some(name);
        self
    }

    pub fn with_org_id(mut self, org_id: String) -> DeleteSmartPermissionActionBuilder {
        self.org_id = Some(org_id);
        self
    }

    pub fn build(self) -> Result<DeleteSmartPermissionAction, ActionBuildError> {
        let name = self.name.ok_or_else(|| {
            ActionBuildError::MissingField("'name' field is required".to_string())
        })?;

        let org_id = self.org_id.ok_or_else(|| {
            ActionBuildError::MissingField("'org_id' field is required".to_string())
        })?;

        Ok(DeleteSmartPermissionAction { name, org_id })
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

impl FromProto<protos::payload::SabrePayload> for SabrePayload {
    fn from_proto(proto: protos::payload::SabrePayload) -> Result<Self, ProtoConversionError> {
        let action = match proto.get_action() {
            protos::payload::SabrePayload_Action::CREATE_CONTRACT => {
                CreateContractAction::from_proto(proto.get_create_contract().clone())?.into()
            }
            protos::payload::SabrePayload_Action::DELETE_CONTRACT => {
                DeleteContractAction::from_proto(proto.get_delete_contract().clone())?.into()
            }
            protos::payload::SabrePayload_Action::EXECUTE_CONTRACT => {
                ExecuteContractAction::from_proto(proto.get_execute_contract().clone())?.into()
            }
            protos::payload::SabrePayload_Action::CREATE_CONTRACT_REGISTRY => {
                CreateContractRegistryAction::from_proto(
                    proto.get_create_contract_registry().clone(),
                )?
                .into()
            }
            protos::payload::SabrePayload_Action::DELETE_CONTRACT_REGISTRY => {
                DeleteContractRegistryAction::from_proto(
                    proto.get_delete_contract_registry().clone(),
                )?
                .into()
            }
            protos::payload::SabrePayload_Action::UPDATE_CONTRACT_REGISTRY_OWNERS => {
                UpdateContractRegistryOwnersAction::from_proto(
                    proto.get_update_contract_registry_owners().clone(),
                )?
                .into()
            }
            protos::payload::SabrePayload_Action::CREATE_NAMESPACE_REGISTRY => {
                CreateNamespaceRegistryAction::from_proto(
                    proto.get_create_namespace_registry().clone(),
                )?
                .into()
            }
            protos::payload::SabrePayload_Action::DELETE_NAMESPACE_REGISTRY => {
                DeleteNamespaceRegistryAction::from_proto(
                    proto.get_delete_namespace_registry().clone(),
                )?
                .into()
            }
            protos::payload::SabrePayload_Action::UPDATE_NAMESPACE_REGISTRY_OWNERS => {
                UpdateNamespaceRegistryOwnersAction::from_proto(
                    proto.get_update_namespace_registry_owners().clone(),
                )?
                .into()
            }
            protos::payload::SabrePayload_Action::CREATE_NAMESPACE_REGISTRY_PERMISSION => {
                CreateNamespaceRegistryPermissionAction::from_proto(
                    proto.get_create_namespace_registry_permission().clone(),
                )?
                .into()
            }
            protos::payload::SabrePayload_Action::DELETE_NAMESPACE_REGISTRY_PERMISSION => {
                DeleteNamespaceRegistryPermissionAction::from_proto(
                    proto.get_delete_namespace_registry_permission().clone(),
                )?
                .into()
            }
            protos::payload::SabrePayload_Action::CREATE_SMART_PERMISSION => {
                CreateSmartPermissionAction::from_proto(
                    proto.get_create_smart_permission().clone(),
                )?
                .into()
            }
            protos::payload::SabrePayload_Action::UPDATE_SMART_PERMISSION => {
                UpdateSmartPermissionAction::from_proto(
                    proto.get_update_smart_permission().clone(),
                )?
                .into()
            }
            protos::payload::SabrePayload_Action::DELETE_SMART_PERMISSION => {
                DeleteSmartPermissionAction::from_proto(
                    proto.get_delete_smart_permission().clone(),
                )?
                .into()
            }
            protos::payload::SabrePayload_Action::ACTION_UNSET => {
                return Err(ProtoConversionError::InvalidTypeError(
                    "Cannot convert SabrePayload_Action with type unset.".to_string(),
                ));
            }
        };

        Ok(SabrePayload { action })
    }
}

impl FromNative<SabrePayload> for protos::payload::SabrePayload {
    fn from_native(native: SabrePayload) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::payload::SabrePayload::new();

        match native.action() {
            Action::CreateContract(payload) => {
                proto.set_action(protos::payload::SabrePayload_Action::CREATE_CONTRACT);
                proto.set_create_contract(payload.clone().into_proto()?);
            }
            Action::DeleteContract(payload) => {
                proto.set_action(protos::payload::SabrePayload_Action::DELETE_CONTRACT);
                proto.set_delete_contract(payload.clone().into_proto()?);
            }
            Action::ExecuteContract(payload) => {
                proto.set_action(protos::payload::SabrePayload_Action::EXECUTE_CONTRACT);
                proto.set_execute_contract(payload.clone().into_proto()?);
            }
            Action::CreateContractRegistry(payload) => {
                proto.set_action(protos::payload::SabrePayload_Action::CREATE_CONTRACT_REGISTRY);
                proto.set_create_contract_registry(payload.clone().into_proto()?);
            }
            Action::DeleteContractRegistry(payload) => {
                proto.set_action(protos::payload::SabrePayload_Action::DELETE_CONTRACT_REGISTRY);
                proto.set_delete_contract_registry(payload.clone().into_proto()?);
            }
            Action::UpdateContractRegistryOwners(payload) => {
                proto.set_action(
                    protos::payload::SabrePayload_Action::UPDATE_CONTRACT_REGISTRY_OWNERS,
                );
                proto.set_update_contract_registry_owners(payload.clone().into_proto()?);
            }
            Action::CreateNamespaceRegistry(payload) => {
                proto.set_action(protos::payload::SabrePayload_Action::CREATE_NAMESPACE_REGISTRY);
                proto.set_create_namespace_registry(payload.clone().into_proto()?);
            }
            Action::DeleteNamespaceRegistry(payload) => {
                proto.set_action(protos::payload::SabrePayload_Action::DELETE_NAMESPACE_REGISTRY);
                proto.set_delete_namespace_registry(payload.clone().into_proto()?);
            }
            Action::UpdateNamespaceRegistryOwners(payload) => {
                proto.set_action(
                    protos::payload::SabrePayload_Action::UPDATE_NAMESPACE_REGISTRY_OWNERS,
                );
                proto.set_update_namespace_registry_owners(payload.clone().into_proto()?);
            }
            Action::CreateNamespaceRegistryPermission(payload) => {
                proto.set_action(
                    protos::payload::SabrePayload_Action::CREATE_NAMESPACE_REGISTRY_PERMISSION,
                );
                proto.set_create_namespace_registry_permission(payload.clone().into_proto()?);
            }
            Action::DeleteNamespaceRegistryPermission(payload) => {
                proto.set_action(
                    protos::payload::SabrePayload_Action::DELETE_NAMESPACE_REGISTRY_PERMISSION,
                );
                proto.set_delete_namespace_registry_permission(payload.clone().into_proto()?);
            }

            Action::CreateSmartPermission(payload) => {
                proto.set_action(protos::payload::SabrePayload_Action::CREATE_SMART_PERMISSION);
                proto.set_create_smart_permission(payload.clone().into_proto()?);
            }
            Action::UpdateSmartPermission(payload) => {
                proto.set_action(protos::payload::SabrePayload_Action::UPDATE_SMART_PERMISSION);
                proto.set_update_smart_permission(payload.clone().into_proto()?);
            }
            Action::DeleteSmartPermission(payload) => {
                proto.set_action(protos::payload::SabrePayload_Action::DELETE_SMART_PERMISSION);
                proto.set_delete_smart_permission(payload.clone().into_proto()?);
            }
        }

        Ok(proto)
    }
}

impl FromBytes<SabrePayload> for SabrePayload {
    fn from_bytes(bytes: &[u8]) -> Result<SabrePayload, ProtoConversionError> {
        let proto: protos::payload::SabrePayload =
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

impl IntoProto<protos::payload::SabrePayload> for SabrePayload {}
impl IntoNative<SabrePayload> for protos::payload::SabrePayload {}

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
    ///
    /// NOTE: a signer is required because some input/output addresses of smart permission payloads
    /// are calculated using the signer's public key.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_transaction_builder(
        self,
        signer: &dyn Signer,
    ) -> Result<TransactionBuilder, SabrePayloadBuildError> {
        let payload = self.build()?;

        let (input_addresses, output_addresses) = match payload.action() {
            Action::CreateContract(CreateContractAction { name, version, .. })
            | Action::DeleteContract(DeleteContractAction { name, version, .. }) => {
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
            Action::CreateContractRegistry(CreateContractRegistryAction { name, .. })
            | Action::DeleteContractRegistry(DeleteContractRegistryAction { name, .. })
            | Action::UpdateContractRegistryOwners(UpdateContractRegistryOwnersAction {
                name,
                ..
            }) => {
                let addresses = vec![
                    compute_contract_registry_address(name)?,
                    ADMINISTRATORS_SETTING_ADDRESS_BYTES.to_vec(),
                ];
                (addresses.clone(), addresses)
            }
            Action::CreateNamespaceRegistry(CreateNamespaceRegistryAction {
                namespace, ..
            })
            | Action::DeleteNamespaceRegistry(DeleteNamespaceRegistryAction {
                namespace, ..
            })
            | Action::UpdateNamespaceRegistryOwners(UpdateNamespaceRegistryOwnersAction {
                namespace,
                ..
            })
            | Action::CreateNamespaceRegistryPermission(
                CreateNamespaceRegistryPermissionAction { namespace, .. },
            )
            | Action::DeleteNamespaceRegistryPermission(
                DeleteNamespaceRegistryPermissionAction { namespace, .. },
            ) => {
                let addresses = vec![
                    compute_namespace_registry_address(namespace)?,
                    ADMINISTRATORS_SETTING_ADDRESS_BYTES.to_vec(),
                ];
                (addresses.clone(), addresses)
            }
            Action::CreateSmartPermission(CreateSmartPermissionAction { org_id, name, .. })
            | Action::UpdateSmartPermission(UpdateSmartPermissionAction { org_id, name, .. })
            | Action::DeleteSmartPermission(DeleteSmartPermissionAction { org_id, name, .. }) => {
                let public_key = signer.public_key().map_err(|err| {
                    SabrePayloadBuildError::SigningError(format!(
                        "Unable to get public key from signer: {}",
                        err
                    ))
                })?;
                let addresses = vec![
                    compute_smart_permission_address(org_id, name)?,
                    // This converts the public key to a hex string and gets the raw bytes of that
                    // string; this is required because it is how the agent addresses is calculated
                    // by the Sabre transaction processor.
                    compute_agent_address(public_key.as_hex().as_bytes())?,
                    compute_org_address(org_id)?,
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

#[cfg(test)]
mod tests {
    use super::*;

    use cylinder::{secp256k1::Secp256k1Context, Context, Signer};

    #[test]
    // check that a create contract action is built correctly
    fn check_create_contract_action() {
        let builder = CreateContractActionBuilder::new();
        let action = builder
            .with_name("TestContract".to_string())
            .with_version("0.1".to_string())
            .with_inputs(vec!["test".to_string(), "input".to_string()])
            .with_outputs(vec!["test".to_string(), "output".to_string()])
            .with_contract(b"test".to_vec())
            .build()
            .unwrap();

        assert_eq!(action.name(), "TestContract");
        assert_eq!(action.version(), "0.1");
        assert_eq!(action.inputs(), ["test".to_string(), "input".to_string()]);
        assert_eq!(action.outputs(), ["test".to_string(), "output".to_string()]);
        assert_eq!(action.contract(), b"test");
    }

    #[test]
    // check that a create contract can be converted to bytes and back
    fn check_create_contract_bytes() {
        let builder = CreateContractActionBuilder::new();
        let original = builder
            .with_name("TestContract".to_string())
            .with_version("0.1".to_string())
            .with_inputs(vec!["test".to_string(), "input".to_string()])
            .with_outputs(vec!["test".to_string(), "output".to_string()])
            .with_contract(b"test".to_vec())
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let create = CreateContractAction::from_bytes(&bytes).unwrap();
        assert_eq!(create, original);
    }

    #[test]
    // check that a delete create action is built correctly
    fn check_delete_contract_action() {
        let builder = DeleteContractActionBuilder::new();
        let action = builder
            .with_name("TestContract".to_string())
            .with_version("0.1".to_string())
            .build()
            .unwrap();

        assert_eq!(action.name(), "TestContract");
        assert_eq!(action.version(), "0.1");
    }

    #[test]
    // check that a delete contract can be converted to bytes and back
    fn check_delete_contract_action_bytes() {
        let builder = DeleteContractActionBuilder::new();
        let original = builder
            .with_name("TestContract".to_string())
            .with_version("0.1".to_string())
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let create = DeleteContractAction::from_bytes(&bytes).unwrap();
        assert_eq!(create, original);
    }

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

    #[test]
    // check that a create contract registry action is built correctly
    fn check_create_contract_registry_action() {
        let builder = CreateContractRegistryActionBuilder::new();
        let action = builder
            .with_name("TestContract".to_string())
            .with_owners(vec!["test".to_string(), "owner".to_string()])
            .build()
            .unwrap();

        assert_eq!(action.name(), "TestContract");
        assert_eq!(action.owners(), ["test".to_string(), "owner".to_string()]);
    }

    #[test]
    // check that a create contract registry can be converted to bytes and back
    fn check_create_contract_registry_action_bytes() {
        let builder = CreateContractRegistryActionBuilder::new();
        let original = builder
            .with_name("TestContract".to_string())
            .with_owners(vec!["test".to_string(), "owner".to_string()])
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let create = CreateContractRegistryAction::from_bytes(&bytes).unwrap();
        assert_eq!(create, original);
    }

    #[test]
    // check that a delete contract registry action is built correctly
    fn check_delete_contract_registry_action() {
        let builder = DeleteContractRegistryActionBuilder::new();
        let action = builder
            .with_name("TestContract".to_string())
            .build()
            .unwrap();

        assert_eq!(action.name(), "TestContract");
    }

    #[test]
    // check that a delete contract can be converted to bytes and back
    fn check_delete_contract_registry_action_bytes() {
        let builder = DeleteContractRegistryActionBuilder::new();
        let original = builder
            .with_name("TestContract".to_string())
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let delete = DeleteContractRegistryAction::from_bytes(&bytes).unwrap();
        assert_eq!(delete, original);
    }

    #[test]
    // check that a update contract registry owners action is built correctly
    fn check_update_contract_registry_owners_action() {
        let builder = UpdateContractRegistryOwnersActionBuilder::new();
        let action = builder
            .with_name("TestContract".to_string())
            .with_owners(vec!["test".to_string(), "owner".to_string()])
            .build()
            .unwrap();

        assert_eq!(action.name(), "TestContract");
        assert_eq!(action.owners(), ["test".to_string(), "owner".to_string()]);
    }

    #[test]
    // check that a update contract registry owners can be converted to bytes and back
    fn check_update_contract_registry_owners_action_bytes() {
        let builder = UpdateContractRegistryOwnersActionBuilder::new();
        let original = builder
            .with_name("TestContract".to_string())
            .with_owners(vec!["test".to_string(), "owner".to_string()])
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let update = UpdateContractRegistryOwnersAction::from_bytes(&bytes).unwrap();
        assert_eq!(update, original);
    }

    #[test]
    // check that a create namespace registry action is built correctly
    fn check_create_namespace_registry_action() {
        let builder = CreateNamespaceRegistryActionBuilder::new();
        let action = builder
            .with_namespace("TestNamespace".to_string())
            .with_owners(vec!["test".to_string(), "owner".to_string()])
            .build()
            .unwrap();

        assert_eq!(action.namespace(), "TestNamespace");
        assert_eq!(action.owners(), ["test".to_string(), "owner".to_string()]);
    }

    #[test]
    // check that a create namespace registry can be converted to bytes and back
    fn check_create_namespace_registry_action_bytes() {
        let builder = CreateNamespaceRegistryActionBuilder::new();
        let original = builder
            .with_namespace("TestNamespace".to_string())
            .with_owners(vec!["test".to_string(), "owner".to_string()])
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let create = CreateNamespaceRegistryAction::from_bytes(&bytes).unwrap();
        assert_eq!(create, original);
    }

    #[test]
    // check that a delete namespace registry action is built correctly
    fn check_delete_namespace_registry_action() {
        let builder = DeleteNamespaceRegistryActionBuilder::new();
        let action = builder
            .with_namespace("TestNamespace".to_string())
            .build()
            .unwrap();

        assert_eq!(action.namespace(), "TestNamespace");
    }

    #[test]
    // check that a delete namespace registry can be converted to bytes and back
    fn check_delete_namespace_registry_action_bytes() {
        let builder = DeleteNamespaceRegistryActionBuilder::new();
        let original = builder
            .with_namespace("TestNamespace".to_string())
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let delete = DeleteNamespaceRegistryAction::from_bytes(&bytes).unwrap();
        assert_eq!(delete, original);
    }

    #[test]
    // check that a update namespace registry owners action is built correctly
    fn check_update_namespace_registry_owners_action() {
        let builder = UpdateNamespaceRegistryOwnersActionBuilder::new();
        let action = builder
            .with_namespace("TestNamespace".to_string())
            .with_owners(vec!["test".to_string(), "owner".to_string()])
            .build()
            .unwrap();

        assert_eq!(action.namespace(), "TestNamespace");
        assert_eq!(action.owners(), ["test".to_string(), "owner".to_string()]);
    }

    #[test]
    // check that a update namespace registry owners can be converted to bytes and back
    fn check_update_namespace_registry_owners_action_bytes() {
        let builder = UpdateNamespaceRegistryOwnersActionBuilder::new();
        let original = builder
            .with_namespace("TestNamespace".to_string())
            .with_owners(vec!["test".to_string(), "owner".to_string()])
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let update = UpdateNamespaceRegistryOwnersAction::from_bytes(&bytes).unwrap();
        assert_eq!(update, original);
    }

    #[test]
    // check that a create namespace registry permission action is built correctly
    fn check_create_namespace_registry_permission_action() {
        let builder = CreateNamespaceRegistryPermissionActionBuilder::new();
        let action = builder
            .with_namespace("TestNamespace".to_string())
            .with_contract_name("TestContract".to_string())
            .with_read(true)
            .with_write(true)
            .build()
            .unwrap();

        assert_eq!(action.namespace(), "TestNamespace");
        assert_eq!(action.contract_name(), "TestContract");
        assert_eq!(action.read(), true);
        assert_eq!(action.write(), true);
    }

    #[test]
    // check that a create namespace registry permission can be converted to bytes and back
    fn check_create_namespace_registry_permission_action_bytes() {
        let builder = CreateNamespaceRegistryPermissionActionBuilder::new();
        let original = builder
            .with_namespace("TestNamespace".to_string())
            .with_contract_name("TestContract".to_string())
            .with_read(true)
            .with_write(true)
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let create = CreateNamespaceRegistryPermissionAction::from_bytes(&bytes).unwrap();
        assert_eq!(create, original);
    }

    #[test]
    // check that a delete namespace registry permission action is built correctly
    fn check_delete_namespace_registry_permission_action() {
        let builder = DeleteNamespaceRegistryPermissionActionBuilder::new();
        let action = builder
            .with_namespace("TestNamespace".to_string())
            .with_contract_name("TestContract".to_string())
            .build()
            .unwrap();

        assert_eq!(action.namespace(), "TestNamespace");
        assert_eq!(action.contract_name(), "TestContract");
    }

    #[test]
    // check that a delete namespace registry permission can be converted to bytes and back
    fn check_delete_namespace_registry_permission_action_bytes() {
        let builder = DeleteNamespaceRegistryPermissionActionBuilder::new();
        let original = builder
            .with_namespace("TestNamespace".to_string())
            .with_contract_name("TestContract".to_string())
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let create = DeleteNamespaceRegistryPermissionAction::from_bytes(&bytes).unwrap();
        assert_eq!(create, original);
    }

    #[test]
    // check that a create smart permsion action is built correctly
    fn check_create_smart_permission_action() {
        let builder = CreateSmartPermissionActionBuilder::new();
        let action = builder
            .with_name("SmartPermission".to_string())
            .with_org_id("org_id".to_string())
            .with_function(b"test".to_vec())
            .build()
            .unwrap();

        assert_eq!(action.name(), "SmartPermission");
        assert_eq!(action.org_id(), "org_id");
        assert_eq!(action.function(), b"test");
    }

    #[test]
    // check that create smart permission can be converted to bytes and back
    fn check_create_smart_permission_action_bytes() {
        let builder = CreateSmartPermissionActionBuilder::new();
        let original = builder
            .with_name("SmartPermission".to_string())
            .with_org_id("org_id".to_string())
            .with_function(b"test".to_vec())
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let create = CreateSmartPermissionAction::from_bytes(&bytes).unwrap();
        assert_eq!(create, original);
    }

    #[test]
    // check that a update smart permission action is built correctly
    fn check_update_smart_permission_action() {
        let builder = UpdateSmartPermissionActionBuilder::new();
        let action = builder
            .with_name("SmartPermission".to_string())
            .with_org_id("org_id".to_string())
            .with_function(b"test".to_vec())
            .build()
            .unwrap();

        assert_eq!(action.name(), "SmartPermission");
        assert_eq!(action.org_id(), "org_id");
        assert_eq!(action.function(), b"test");
    }

    #[test]
    // check that a update smart permission can be converted to bytes and back
    fn check_update_smart_permission_action_bytes() {
        let builder = UpdateSmartPermissionActionBuilder::new();
        let original = builder
            .with_name("SmartPermission".to_string())
            .with_org_id("org_id".to_string())
            .with_function(b"test".to_vec())
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let create = UpdateSmartPermissionAction::from_bytes(&bytes).unwrap();
        assert_eq!(create, original);
    }

    #[test]
    // check that a delete smart permission action is built correctly
    fn check_delete_smart_permission_action() {
        let builder = DeleteSmartPermissionActionBuilder::new();
        let action = builder
            .with_name("SmartPermission".to_string())
            .with_org_id("org_id".to_string())
            .build()
            .unwrap();

        assert_eq!(action.name(), "SmartPermission");
        assert_eq!(action.org_id(), "org_id");
    }

    #[test]
    // check that a delete smart permission can be converted to bytes and back
    fn check_delete_smart_permission_action_bytes() {
        let builder = DeleteSmartPermissionActionBuilder::new();
        let original = builder
            .with_name("SmartPermission".to_string())
            .with_org_id("org_id".to_string())
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let create = DeleteSmartPermissionAction::from_bytes(&bytes).unwrap();
        assert_eq!(create, original);
    }

    #[test]
    // check that a sabre payload with execute action is built correctly
    fn check_payload() {
        let builder = ExecuteContractActionBuilder::new();
        let action = builder
            .with_name("TestContract".to_string())
            .with_version("0.1".to_string())
            .with_inputs(vec!["test".to_string(), "input".to_string()])
            .with_outputs(vec!["test".to_string(), "output".to_string()])
            .with_payload(b"test_payload".to_vec())
            .build()
            .unwrap();

        let builder = SabrePayloadBuilder::new();
        let payload = builder.with_action(action.clone().into()).build().unwrap();

        assert_eq!(payload.action, Action::from(action));
    }

    #[test]
    // check that a sabre payload can be converted to bytes and back
    fn check_payload_bytes() {
        let builder = ExecuteContractActionBuilder::new();
        let action = builder
            .with_name("TestContract".to_string())
            .with_version("0.1".to_string())
            .with_inputs(vec!["test".to_string(), "input".to_string()])
            .with_outputs(vec!["test".to_string(), "output".to_string()])
            .with_payload(b"test_payload".to_vec())
            .build()
            .unwrap();

        let builder = SabrePayloadBuilder::new();
        let original = builder.with_action(action.into()).build().unwrap();
        let bytes = original.clone().into_bytes().unwrap();

        let payload = SabrePayload::from_bytes(&bytes).unwrap();
        assert_eq!(payload, original);
    }

    #[test]
    // check that a create contract can be converted -> sabre payload builder -> transaction
    // builder -> transaction
    fn create_contract_into_transaction() {
        let signer = new_signer();

        let txn_pair = CreateContractActionBuilder::new()
            .with_name("TestContract".to_string())
            .with_version("0.1".to_string())
            .with_inputs(vec!["test".to_string(), "input".to_string()])
            .with_outputs(vec!["test".to_string(), "output".to_string()])
            .with_contract(b"test".to_vec())
            .into_payload_builder()
            .expect("failed to convert to payload builder")
            .into_transaction_builder(&*signer)
            .expect("failed to convert to transaction builder")
            .build_pair(&*signer)
            .expect("failed to build transaction pair");

        let txn_header = txn_pair.header();

        assert_eq!(txn_header.family_name(), "sabre");
        assert_eq!(
            txn_header.family_version(),
            SABRE_PROTOCOL_VERSION.to_string()
        );
        assert_eq!(txn_header.payload_hash_method(), &HashMethod::SHA512);
    }

    #[test]
    // check that a delete contract can be converted -> sabre payload builder -> transaction
    // builder -> transaction
    fn delete_contract_into_transaction() {
        let signer = new_signer();

        let txn_pair = DeleteContractActionBuilder::new()
            .with_name("TestContract".to_string())
            .with_version("0.1".to_string())
            .into_payload_builder()
            .expect("failed to convert to payload builder")
            .into_transaction_builder(&*signer)
            .expect("failed to convert to transaction builder")
            .build_pair(&*signer)
            .expect("failed to build transaction pair");

        let txn_header = txn_pair.header();

        assert_eq!(txn_header.family_name(), "sabre");
        assert_eq!(
            txn_header.family_version(),
            SABRE_PROTOCOL_VERSION.to_string()
        );
        assert_eq!(txn_header.payload_hash_method(), &HashMethod::SHA512);
    }

    #[test]
    // check that an execute contract can be converted -> sabre payload builder -> transaction
    // builder -> transaction
    fn execute_contract_into_transaction() {
        let signer = new_signer();

        let txn_pair = ExecuteContractActionBuilder::new()
            .with_name("TestContract".to_string())
            .with_version("0.1".to_string())
            .with_inputs(vec!["abcdef".to_string(), "012345".to_string()])
            .with_outputs(vec!["abcdef".to_string(), "678910".to_string()])
            .with_payload(b"test_payload".to_vec())
            .into_payload_builder()
            .expect("failed to convert to payload builder")
            .into_transaction_builder(&*signer)
            .expect("failed to convert to transaction builder")
            .build_pair(&*signer)
            .expect("failed to build transaction pair");

        let txn_header = txn_pair.header();

        assert_eq!(txn_header.family_name(), "sabre");
        assert_eq!(
            txn_header.family_version(),
            SABRE_PROTOCOL_VERSION.to_string()
        );
        assert_eq!(txn_header.payload_hash_method(), &HashMethod::SHA512);
    }

    #[test]
    // check that a create contract registry can be converted -> sabre payload builder ->
    // transaction builder -> transaction
    fn create_contract_registry_into_transaction() {
        let signer = new_signer();

        let txn_pair = CreateContractRegistryActionBuilder::new()
            .with_name("TestContract".to_string())
            .with_owners(vec!["test".to_string(), "owner".to_string()])
            .into_payload_builder()
            .expect("failed to convert to payload builder")
            .into_transaction_builder(&*signer)
            .expect("failed to convert to transaction builder")
            .build_pair(&*signer)
            .expect("failed to build transaction pair");

        let txn_header = txn_pair.header();

        assert_eq!(txn_header.family_name(), "sabre");
        assert_eq!(
            txn_header.family_version(),
            SABRE_PROTOCOL_VERSION.to_string()
        );
        assert_eq!(txn_header.payload_hash_method(), &HashMethod::SHA512);
    }

    #[test]
    // check that a delete contract registry can be converted -> sabre payload builder ->
    // transaction builder -> transaction
    fn delete_contract_registry_into_transaction() {
        let signer = new_signer();

        let txn_pair = DeleteContractRegistryActionBuilder::new()
            .with_name("TestContract".to_string())
            .into_payload_builder()
            .expect("failed to convert to payload builder")
            .into_transaction_builder(&*signer)
            .expect("failed to convert to transaction builder")
            .build_pair(&*signer)
            .expect("failed to build transaction pair");

        let txn_header = txn_pair.header();

        assert_eq!(txn_header.family_name(), "sabre");
        assert_eq!(
            txn_header.family_version(),
            SABRE_PROTOCOL_VERSION.to_string()
        );
        assert_eq!(txn_header.payload_hash_method(), &HashMethod::SHA512);
    }

    #[test]
    // check that an update contract registry owners can be converted -> sabre payload builder ->
    // transaction builder -> transaction
    fn update_contract_registry_owners_into_transaction() {
        let signer = new_signer();

        let txn_pair = UpdateContractRegistryOwnersActionBuilder::new()
            .with_name("TestContract".to_string())
            .with_owners(vec!["test".to_string(), "owner".to_string()])
            .into_payload_builder()
            .expect("failed to convert to payload builder")
            .into_transaction_builder(&*signer)
            .expect("failed to convert to transaction builder")
            .build_pair(&*signer)
            .expect("failed to build transaction pair");

        let txn_header = txn_pair.header();

        assert_eq!(txn_header.family_name(), "sabre");
        assert_eq!(
            txn_header.family_version(),
            SABRE_PROTOCOL_VERSION.to_string()
        );
        assert_eq!(txn_header.payload_hash_method(), &HashMethod::SHA512);
    }

    #[test]
    // check that a create namespace registry can be converted -> sabre payload builder ->
    // transaction builder -> transaction
    fn create_namespace_registry_into_transaction() {
        let signer = new_signer();

        let txn_pair = CreateNamespaceRegistryActionBuilder::new()
            .with_namespace("TestNamespace".to_string())
            .with_owners(vec!["test".to_string(), "owner".to_string()])
            .into_payload_builder()
            .expect("failed to convert to payload builder")
            .into_transaction_builder(&*signer)
            .expect("failed to convert to transaction builder")
            .build_pair(&*signer)
            .expect("failed to build transaction pair");

        let txn_header = txn_pair.header();

        assert_eq!(txn_header.family_name(), "sabre");
        assert_eq!(
            txn_header.family_version(),
            SABRE_PROTOCOL_VERSION.to_string()
        );
        assert_eq!(txn_header.payload_hash_method(), &HashMethod::SHA512);
    }

    #[test]
    // check that a delete namespace registry can be converted -> sabre payload builder ->
    // transaction builder -> transaction
    fn delete_namespace_registry_into_transaction() {
        let signer = new_signer();

        let txn_pair = DeleteNamespaceRegistryActionBuilder::new()
            .with_namespace("TestNamespace".to_string())
            .into_payload_builder()
            .expect("failed to convert to payload builder")
            .into_transaction_builder(&*signer)
            .expect("failed to convert to transaction builder")
            .build_pair(&*signer)
            .expect("failed to build transaction pair");

        let txn_header = txn_pair.header();

        assert_eq!(txn_header.family_name(), "sabre");
        assert_eq!(
            txn_header.family_version(),
            SABRE_PROTOCOL_VERSION.to_string()
        );
        assert_eq!(txn_header.payload_hash_method(), &HashMethod::SHA512);
    }

    #[test]
    // check that a update namespace registry owners can be converted -> sabre payload builder ->
    // transaction builder -> transaction
    fn update_namespace_registry_owners_into_transaction() {
        let signer = new_signer();

        let txn_pair = UpdateNamespaceRegistryOwnersActionBuilder::new()
            .with_namespace("TestNamespace".to_string())
            .with_owners(vec!["test".to_string(), "owner".to_string()])
            .into_payload_builder()
            .expect("failed to convert to payload builder")
            .into_transaction_builder(&*signer)
            .expect("failed to convert to transaction builder")
            .build_pair(&*signer)
            .expect("failed to build transaction pair");

        let txn_header = txn_pair.header();

        assert_eq!(txn_header.family_name(), "sabre");
        assert_eq!(
            txn_header.family_version(),
            SABRE_PROTOCOL_VERSION.to_string()
        );
        assert_eq!(txn_header.payload_hash_method(), &HashMethod::SHA512);
    }

    #[test]
    // check that a create namespace registry permission can be converted -> sabre payload builder
    // -> transaction builder -> transaction
    fn create_namespace_registry_permission_into_transaction() {
        let signer = new_signer();

        let txn_pair = CreateNamespaceRegistryPermissionActionBuilder::new()
            .with_namespace("TestNamespace".to_string())
            .with_contract_name("TestContract".to_string())
            .with_read(true)
            .with_write(true)
            .into_payload_builder()
            .expect("failed to convert to payload builder")
            .into_transaction_builder(&*signer)
            .expect("failed to convert to transaction builder")
            .build_pair(&*signer)
            .expect("failed to build transaction pair");

        let txn_header = txn_pair.header();

        assert_eq!(txn_header.family_name(), "sabre");
        assert_eq!(
            txn_header.family_version(),
            SABRE_PROTOCOL_VERSION.to_string()
        );
        assert_eq!(txn_header.payload_hash_method(), &HashMethod::SHA512);
    }

    #[test]
    // check that a delete namespace registry permission can be converted -> sabre payload builder
    // -> transaction builder -> transaction
    fn delete_namespace_registry_permission_into_transaction() {
        let signer = new_signer();

        let txn_pair = DeleteNamespaceRegistryPermissionActionBuilder::new()
            .with_namespace("TestNamespace".to_string())
            .with_contract_name("TestContract".to_string())
            .into_payload_builder()
            .expect("failed to convert to payload builder")
            .into_transaction_builder(&*signer)
            .expect("failed to convert to transaction builder")
            .build_pair(&*signer)
            .expect("failed to build transaction pair");

        let txn_header = txn_pair.header();

        assert_eq!(txn_header.family_name(), "sabre");
        assert_eq!(
            txn_header.family_version(),
            SABRE_PROTOCOL_VERSION.to_string()
        );
        assert_eq!(txn_header.payload_hash_method(), &HashMethod::SHA512);
    }

    #[test]
    // check that a create smart permission can be converted -> sabre payload builder ->
    // transaction builder -> transaction
    fn create_smart_permission_into_transaction() {
        let signer = new_signer();

        let txn_pair = CreateSmartPermissionActionBuilder::new()
            .with_name("SmartPermission".to_string())
            .with_org_id("org_id".to_string())
            .with_function(b"test".to_vec())
            .into_payload_builder()
            .expect("failed to convert to payload builder")
            .into_transaction_builder(&*signer)
            .expect("failed to convert to transaction builder")
            .build_pair(&*signer)
            .expect("failed to build transaction pair");

        let txn_header = txn_pair.header();

        assert_eq!(txn_header.family_name(), "sabre");
        assert_eq!(
            txn_header.family_version(),
            SABRE_PROTOCOL_VERSION.to_string()
        );
        assert_eq!(txn_header.payload_hash_method(), &HashMethod::SHA512);
    }

    #[test]
    // check that a update smart permission can be converted -> sabre payload builder ->
    // transaction builder -> transaction
    fn update_smart_permission_into_transaction() {
        let signer = new_signer();

        let txn_pair = UpdateSmartPermissionActionBuilder::new()
            .with_name("SmartPermission".to_string())
            .with_org_id("org_id".to_string())
            .with_function(b"test".to_vec())
            .into_payload_builder()
            .expect("failed to convert to payload builder")
            .into_transaction_builder(&*signer)
            .expect("failed to convert to transaction builder")
            .build_pair(&*signer)
            .expect("failed to build transaction pair");

        let txn_header = txn_pair.header();

        assert_eq!(txn_header.family_name(), "sabre");
        assert_eq!(
            txn_header.family_version(),
            SABRE_PROTOCOL_VERSION.to_string()
        );
        assert_eq!(txn_header.payload_hash_method(), &HashMethod::SHA512);
    }

    #[test]
    // check that a delete smart permission can be converted -> sabre payload builder ->
    // transaction builder -> transaction
    fn delete_smart_permission_into_transaction() {
        let signer = new_signer();

        let txn_pair = DeleteSmartPermissionActionBuilder::new()
            .with_name("SmartPermission".to_string())
            .with_org_id("org_id".to_string())
            .into_payload_builder()
            .expect("failed to convert to payload builder")
            .into_transaction_builder(&*signer)
            .expect("failed to convert to transaction builder")
            .build_pair(&*signer)
            .expect("failed to build transaction pair");

        let txn_header = txn_pair.header();

        assert_eq!(txn_header.family_name(), "sabre");
        assert_eq!(
            txn_header.family_version(),
            SABRE_PROTOCOL_VERSION.to_string()
        );
        assert_eq!(txn_header.payload_hash_method(), &HashMethod::SHA512);
    }

    fn new_signer() -> Box<dyn Signer> {
        let context = Secp256k1Context::new();
        let key = context.new_random_private_key();
        context.new_signer(key)
    }
}

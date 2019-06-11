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

use protobuf::Message;
use protobuf::RepeatedField;

use std::error::Error as StdError;

use crate::protos;
use crate::protos::{
    FromBytes, FromNative, FromProto, IntoBytes, IntoNative, IntoProto, ProtoConversionError,
};

/// Native implementation for Version
#[derive(Default, Debug, Clone, PartialEq)]
pub struct Version {
    version: String,
    contract_sha512: String,
    creator: String,
}

impl Version {
    pub fn version(&self) -> &String {
        &self.version
    }

    pub fn contract_sha512(&self) -> &String {
        &self.contract_sha512
    }

    pub fn creator(&self) -> &String {
        &self.creator
    }

    pub fn into_builder(self) -> VersionBuilder {
        VersionBuilder::new()
            .with_version(self.version)
            .with_contract_sha512(self.contract_sha512)
            .with_creator(self.creator)
    }
}

impl FromProto<protos::contract_registry::ContractRegistry_Version> for Version {
    fn from_proto(
        proto: protos::contract_registry::ContractRegistry_Version,
    ) -> Result<Self, ProtoConversionError> {
        Ok(Version {
            version: proto.get_version().to_string(),
            contract_sha512: proto.get_contract_sha512().to_string(),
            creator: proto.get_creator().to_string(),
        })
    }
}

impl FromNative<Version> for protos::contract_registry::ContractRegistry_Version {
    fn from_native(native: Version) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::contract_registry::ContractRegistry_Version::new();

        proto.set_version(native.version().to_string());
        proto.set_contract_sha512(native.contract_sha512().to_string());
        proto.set_creator(native.creator().to_string());

        Ok(proto)
    }
}

impl IntoProto<protos::contract_registry::ContractRegistry_Version> for Version {}
impl IntoNative<Version> for protos::contract_registry::ContractRegistry_Version {}

#[derive(Debug)]
pub enum VersionBuildError {
    MissingField(String),
}

impl StdError for VersionBuildError {
    fn description(&self) -> &str {
        match *self {
            VersionBuildError::MissingField(ref msg) => msg,
        }
    }
}

impl std::fmt::Display for VersionBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            VersionBuildError::MissingField(ref s) => write!(f, "MissingField: {}", s),
        }
    }
}

/// Builder used to create a Version
#[derive(Default, Clone)]
pub struct VersionBuilder {
    version: Option<String>,
    contract_sha512: Option<String>,
    creator: Option<String>,
}

impl VersionBuilder {
    pub fn new() -> Self {
        VersionBuilder::default()
    }

    pub fn with_version(mut self, version: String) -> VersionBuilder {
        self.version = Some(version);
        self
    }

    pub fn with_contract_sha512(mut self, contract_sha512: String) -> VersionBuilder {
        self.contract_sha512 = Some(contract_sha512);
        self
    }

    pub fn with_creator(mut self, creator: String) -> VersionBuilder {
        self.creator = Some(creator);
        self
    }

    pub fn build(self) -> Result<Version, VersionBuildError> {
        let version = self.version.ok_or_else(|| {
            VersionBuildError::MissingField("'versions' field is required".to_string())
        })?;

        let contract_sha512 = self.contract_sha512.ok_or_else(|| {
            VersionBuildError::MissingField("'contract_sha512' field is required".to_string())
        })?;

        let creator = self.creator.ok_or_else(|| {
            VersionBuildError::MissingField("'creator' field is required".to_string())
        })?;

        Ok(Version {
            version,
            contract_sha512,
            creator,
        })
    }
}

/// Native implementation for ContractRegistry
#[derive(Default, Debug, Clone, PartialEq)]
pub struct ContractRegistry {
    name: String,
    versions: Vec<Version>,
    owners: Vec<String>,
}

impl ContractRegistry {
    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn versions(&self) -> &[Version] {
        &self.versions
    }

    pub fn owners(&self) -> &[String] {
        &self.owners
    }

    pub fn into_builder(self) -> ContractRegistryBuilder {
        ContractRegistryBuilder::new()
            .with_name(self.name)
            .with_versions(self.versions)
            .with_owners(self.owners)
    }
}

impl FromProto<protos::contract_registry::ContractRegistry> for ContractRegistry {
    fn from_proto(
        proto: protos::contract_registry::ContractRegistry,
    ) -> Result<Self, ProtoConversionError> {
        Ok(ContractRegistry {
            name: proto.get_name().to_string(),
            versions: proto
                .get_versions()
                .to_vec()
                .into_iter()
                .map(Version::from_proto)
                .collect::<Result<Vec<Version>, ProtoConversionError>>()?,
            owners: proto.get_owners().to_vec(),
        })
    }
}

impl FromNative<ContractRegistry> for protos::contract_registry::ContractRegistry {
    fn from_native(contract_registry: ContractRegistry) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::contract_registry::ContractRegistry::new();
        proto.set_name(contract_registry.name().to_string());
        proto.set_versions(RepeatedField::from_vec(
            contract_registry
                .versions()
                .to_vec()
                .into_iter()
                .map(Version::into_proto)
                .collect::<Result<
                    Vec<protos::contract_registry::ContractRegistry_Version>,
                    ProtoConversionError,
                >>()?,
        ));
        proto.set_owners(RepeatedField::from_vec(contract_registry.owners().to_vec()));

        Ok(proto)
    }
}

impl FromBytes<ContractRegistry> for ContractRegistry {
    fn from_bytes(bytes: &[u8]) -> Result<ContractRegistry, ProtoConversionError> {
        let proto: protos::contract_registry::ContractRegistry = protobuf::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get ContractRegistry from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for ContractRegistry {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from ContractRegistry".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::contract_registry::ContractRegistry> for ContractRegistry {}
impl IntoNative<ContractRegistry> for protos::contract_registry::ContractRegistry {}

#[derive(Debug)]
pub enum ContractRegistryBuildError {
    MissingField(String),
}

impl StdError for ContractRegistryBuildError {
    fn description(&self) -> &str {
        match *self {
            ContractRegistryBuildError::MissingField(ref msg) => msg,
        }
    }
}

impl std::fmt::Display for ContractRegistryBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ContractRegistryBuildError::MissingField(ref s) => write!(f, "MissingField: {}", s),
        }
    }
}

/// Builder used to create a ContractRegistry
#[derive(Default, Clone)]
pub struct ContractRegistryBuilder {
    name: Option<String>,
    versions: Vec<Version>,
    owners: Vec<String>,
}

impl ContractRegistryBuilder {
    pub fn new() -> Self {
        ContractRegistryBuilder::default()
    }

    pub fn with_name(mut self, name: String) -> ContractRegistryBuilder {
        self.name = Some(name);
        self
    }

    pub fn with_versions(mut self, versions: Vec<Version>) -> ContractRegistryBuilder {
        self.versions = versions;
        self
    }

    pub fn with_owners(mut self, owners: Vec<String>) -> ContractRegistryBuilder {
        self.owners = owners;
        self
    }

    pub fn build(self) -> Result<ContractRegistry, ContractRegistryBuildError> {
        let name = self.name.ok_or_else(|| {
            ContractRegistryBuildError::MissingField("'name' field is required".to_string())
        })?;

        let versions = self.versions;

        let owners = {
            if !self.owners.is_empty() {
                self.owners
            } else {
                return Err(ContractRegistryBuildError::MissingField(
                    "'owners' field is required".to_string(),
                ));
            }
        };

        Ok(ContractRegistry {
            name,
            versions,
            owners,
        })
    }
}

/// Native implementation for ContractRegistryList
#[derive(Default, Debug, Clone, PartialEq)]
pub struct ContractRegistryList {
    registries: Vec<ContractRegistry>,
}

impl ContractRegistryList {
    pub fn registries(&self) -> &[ContractRegistry] {
        &self.registries
    }
}

impl FromProto<protos::contract_registry::ContractRegistryList> for ContractRegistryList {
    fn from_proto(
        proto: protos::contract_registry::ContractRegistryList,
    ) -> Result<Self, ProtoConversionError> {
        Ok(ContractRegistryList {
            registries: proto
                .get_registries()
                .to_vec()
                .into_iter()
                .map(ContractRegistry::from_proto)
                .collect::<Result<Vec<ContractRegistry>, ProtoConversionError>>()?,
        })
    }
}

impl FromNative<ContractRegistryList> for protos::contract_registry::ContractRegistryList {
    fn from_native(
        contract_registry_list: ContractRegistryList,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::contract_registry::ContractRegistryList::new();
        proto.set_registries(
            RepeatedField::from_vec(
                contract_registry_list
                    .registries()
                    .to_vec()
                    .into_iter()
                    .map(ContractRegistry::into_proto)
                    .collect::<Result<
                        Vec<protos::contract_registry::ContractRegistry>,
                        ProtoConversionError,
                    >>()?,
            ),
        );

        Ok(proto)
    }
}

impl FromBytes<ContractRegistryList> for ContractRegistryList {
    fn from_bytes(bytes: &[u8]) -> Result<ContractRegistryList, ProtoConversionError> {
        let proto: protos::contract_registry::ContractRegistryList =
            protobuf::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get ContractRegistryList from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for ContractRegistryList {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from ContractRegistryList".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::contract_registry::ContractRegistryList> for ContractRegistryList {}
impl IntoNative<ContractRegistryList> for protos::contract_registry::ContractRegistryList {}

#[derive(Debug)]
pub enum ContractRegistryListBuildError {
    MissingField(String),
}

impl StdError for ContractRegistryListBuildError {
    fn description(&self) -> &str {
        match *self {
            ContractRegistryListBuildError::MissingField(ref msg) => msg,
        }
    }
}

impl std::fmt::Display for ContractRegistryListBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ContractRegistryListBuildError::MissingField(ref s) => write!(f, "MissingField: {}", s),
        }
    }
}

/// Builder used to create a ContractRegistryList
#[derive(Default, Clone)]
pub struct ContractRegistryListBuilder {
    registries: Vec<ContractRegistry>,
}

impl ContractRegistryListBuilder {
    pub fn new() -> Self {
        ContractRegistryListBuilder::default()
    }

    pub fn with_registries(
        mut self,
        registries: Vec<ContractRegistry>,
    ) -> ContractRegistryListBuilder {
        self.registries = registries;
        self
    }

    pub fn build(self) -> Result<ContractRegistryList, ContractRegistryListBuildError> {
        let registries = self.registries;

        Ok(ContractRegistryList { registries })
    }
}

/// Native implementation for Permission
#[derive(Default, Debug, Clone, PartialEq)]
pub struct Permission {
    contract_name: String,
    read: bool,
    write: bool,
}

impl Permission {
    pub fn contract_name(&self) -> &String {
        &self.contract_name
    }

    pub fn read(&self) -> bool {
        self.read
    }

    pub fn write(&self) -> bool {
        self.write
    }

    pub fn into_builder(self) -> PermissionBuilder {
        PermissionBuilder::new()
            .with_contract_name(self.contract_name)
            .with_read(self.read)
            .with_write(self.write)
    }
}

impl FromProto<protos::namespace_registry::NamespaceRegistry_Permission> for Permission {
    fn from_proto(
        proto: protos::namespace_registry::NamespaceRegistry_Permission,
    ) -> Result<Self, ProtoConversionError> {
        Ok(Permission {
            contract_name: proto.get_contract_name().to_string(),
            read: proto.get_read(),
            write: proto.get_write(),
        })
    }
}

impl FromNative<Permission> for protos::namespace_registry::NamespaceRegistry_Permission {
    fn from_native(native: Permission) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::namespace_registry::NamespaceRegistry_Permission::new();

        proto.set_contract_name(native.contract_name().to_string());
        proto.set_read(native.read());
        proto.set_write(native.write());

        Ok(proto)
    }
}

impl IntoProto<protos::namespace_registry::NamespaceRegistry_Permission> for Permission {}
impl IntoNative<Permission> for protos::namespace_registry::NamespaceRegistry_Permission {}

#[derive(Debug)]
pub enum PermissionBuildError {
    MissingField(String),
}

impl StdError for PermissionBuildError {
    fn description(&self) -> &str {
        match *self {
            PermissionBuildError::MissingField(ref msg) => msg,
        }
    }
}

impl std::fmt::Display for PermissionBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            PermissionBuildError::MissingField(ref s) => write!(f, "MissingField: {}", s),
        }
    }
}

/// Builder used to create a Permission
#[derive(Default, Clone)]
pub struct PermissionBuilder {
    contract_name: Option<String>,
    read: Option<bool>,
    write: Option<bool>,
}

impl PermissionBuilder {
    pub fn new() -> Self {
        PermissionBuilder::default()
    }

    pub fn with_contract_name(mut self, contract_name: String) -> PermissionBuilder {
        self.contract_name = Some(contract_name);
        self
    }

    pub fn with_read(mut self, read: bool) -> PermissionBuilder {
        self.read = Some(read);
        self
    }

    pub fn with_write(mut self, write: bool) -> PermissionBuilder {
        self.write = Some(write);
        self
    }

    pub fn build(self) -> Result<Permission, PermissionBuildError> {
        let contract_name = self.contract_name.ok_or_else(|| {
            PermissionBuildError::MissingField("'contract_name' field is required".to_string())
        })?;

        let read = self.read.unwrap_or_default();

        let write = self.write.unwrap_or_default();

        Ok(Permission {
            contract_name,
            read,
            write,
        })
    }
}

/// Native implementation for NamespaceRegistry
#[derive(Default, Debug, Clone, PartialEq)]
pub struct NamespaceRegistry {
    namespace: String,
    owners: Vec<String>,
    permissions: Vec<Permission>,
}

impl NamespaceRegistry {
    pub fn namespace(&self) -> &String {
        &self.namespace
    }

    pub fn owners(&self) -> &[String] {
        &self.owners
    }

    pub fn permissions(&self) -> &[Permission] {
        &self.permissions
    }

    pub fn into_builder(self) -> NamespaceRegistryBuilder {
        NamespaceRegistryBuilder::new()
            .with_namespace(self.namespace)
            .with_owners(self.owners)
            .with_permissions(self.permissions)
    }
}

impl FromProto<protos::namespace_registry::NamespaceRegistry> for NamespaceRegistry {
    fn from_proto(
        proto: protos::namespace_registry::NamespaceRegistry,
    ) -> Result<Self, ProtoConversionError> {
        Ok(NamespaceRegistry {
            namespace: proto.get_namespace().to_string(),
            owners: proto.get_owners().to_vec(),
            permissions: proto
                .get_permissions()
                .to_vec()
                .into_iter()
                .map(Permission::from_proto)
                .collect::<Result<Vec<Permission>, ProtoConversionError>>()?,
        })
    }
}

impl FromNative<NamespaceRegistry> for protos::namespace_registry::NamespaceRegistry {
    fn from_native(native: NamespaceRegistry) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::namespace_registry::NamespaceRegistry::new();
        proto.set_namespace(native.namespace().to_string());
        proto.set_owners(RepeatedField::from_vec(native.owners().to_vec()));
        proto.set_permissions(RepeatedField::from_vec(
            native
                .permissions()
                .to_vec()
                .into_iter()
                .map(Permission::into_proto)
                .collect::<Result<
                    Vec<protos::namespace_registry::NamespaceRegistry_Permission>,
                    ProtoConversionError,
                >>()?,
        ));

        Ok(proto)
    }
}

impl FromBytes<NamespaceRegistry> for NamespaceRegistry {
    fn from_bytes(bytes: &[u8]) -> Result<NamespaceRegistry, ProtoConversionError> {
        let proto: protos::namespace_registry::NamespaceRegistry =
            protobuf::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get NamespaceRegistry from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for NamespaceRegistry {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from NamespaceRegistry".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::namespace_registry::NamespaceRegistry> for NamespaceRegistry {}
impl IntoNative<NamespaceRegistry> for protos::namespace_registry::NamespaceRegistry {}

#[derive(Debug)]
pub enum NamespaceRegistryBuildError {
    MissingField(String),
}

impl StdError for NamespaceRegistryBuildError {
    fn description(&self) -> &str {
        match *self {
            NamespaceRegistryBuildError::MissingField(ref msg) => msg,
        }
    }
}

impl std::fmt::Display for NamespaceRegistryBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            NamespaceRegistryBuildError::MissingField(ref s) => write!(f, "MissingField: {}", s),
        }
    }
}

/// Builder used to create a NamespaceRegistry
#[derive(Default, Clone)]
pub struct NamespaceRegistryBuilder {
    namespace: Option<String>,
    owners: Vec<String>,
    permissions: Vec<Permission>,
}

impl NamespaceRegistryBuilder {
    pub fn new() -> Self {
        NamespaceRegistryBuilder::default()
    }

    pub fn with_namespace(mut self, namespace: String) -> NamespaceRegistryBuilder {
        self.namespace = Some(namespace);
        self
    }

    pub fn with_owners(mut self, owners: Vec<String>) -> NamespaceRegistryBuilder {
        self.owners = owners;
        self
    }

    pub fn with_permissions(mut self, permissions: Vec<Permission>) -> NamespaceRegistryBuilder {
        self.permissions = permissions;
        self
    }

    pub fn build(self) -> Result<NamespaceRegistry, NamespaceRegistryBuildError> {
        let namespace = self.namespace.ok_or_else(|| {
            NamespaceRegistryBuildError::MissingField("'namespace' field is required".to_string())
        })?;

        let owners = {
            if !self.owners.is_empty() {
                self.owners
            } else {
                return Err(NamespaceRegistryBuildError::MissingField(
                    "'owners' field is required".to_string(),
                ));
            }
        };

        let permissions = self.permissions;

        Ok(NamespaceRegistry {
            namespace,
            owners,
            permissions,
        })
    }
}

// Native implementation for NamespaceRegistryList
#[derive(Default, Debug, Clone, PartialEq)]
pub struct NamespaceRegistryList {
    registries: Vec<NamespaceRegistry>,
}

impl NamespaceRegistryList {
    pub fn registries(&self) -> &[NamespaceRegistry] {
        &self.registries
    }
}

impl FromProto<protos::namespace_registry::NamespaceRegistryList> for NamespaceRegistryList {
    fn from_proto(
        proto: protos::namespace_registry::NamespaceRegistryList,
    ) -> Result<Self, ProtoConversionError> {
        Ok(NamespaceRegistryList {
            registries: proto
                .get_registries()
                .to_vec()
                .into_iter()
                .map(NamespaceRegistry::from_proto)
                .collect::<Result<Vec<NamespaceRegistry>, ProtoConversionError>>()?,
        })
    }
}

impl FromNative<NamespaceRegistryList> for protos::namespace_registry::NamespaceRegistryList {
    fn from_native(
        namespace_registry_list: NamespaceRegistryList,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::namespace_registry::NamespaceRegistryList::new();
        proto.set_registries(
            RepeatedField::from_vec(
                namespace_registry_list
                    .registries()
                    .to_vec()
                    .into_iter()
                    .map(NamespaceRegistry::into_proto)
                    .collect::<Result<
                        Vec<protos::namespace_registry::NamespaceRegistry>,
                        ProtoConversionError,
                    >>()?,
            ),
        );

        Ok(proto)
    }
}

impl FromBytes<NamespaceRegistryList> for NamespaceRegistryList {
    fn from_bytes(bytes: &[u8]) -> Result<NamespaceRegistryList, ProtoConversionError> {
        let proto: protos::namespace_registry::NamespaceRegistryList =
            protobuf::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get NamespaceRegistryList from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for NamespaceRegistryList {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from NamespaceRegistryList".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::namespace_registry::NamespaceRegistryList> for NamespaceRegistryList {}
impl IntoNative<NamespaceRegistryList> for protos::namespace_registry::NamespaceRegistryList {}

#[derive(Debug)]
pub enum NamespaceRegistryListBuildError {
    MissingField(String),
}

impl StdError for NamespaceRegistryListBuildError {
    fn description(&self) -> &str {
        match *self {
            NamespaceRegistryListBuildError::MissingField(ref msg) => msg,
        }
    }
}

impl std::fmt::Display for NamespaceRegistryListBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            NamespaceRegistryListBuildError::MissingField(ref s) => {
                write!(f, "MissingField: {}", s)
            }
        }
    }
}

/// Builder used to create a NamespaceRegistryList
#[derive(Default, Clone)]
pub struct NamespaceRegistryListBuilder {
    registries: Vec<NamespaceRegistry>,
}

impl NamespaceRegistryListBuilder {
    pub fn new() -> Self {
        NamespaceRegistryListBuilder::default()
    }

    pub fn with_registries(
        mut self,
        registries: Vec<NamespaceRegistry>,
    ) -> NamespaceRegistryListBuilder {
        self.registries = registries;
        self
    }

    pub fn build(self) -> Result<NamespaceRegistryList, NamespaceRegistryListBuildError> {
        let registries = self.registries;

        Ok(NamespaceRegistryList { registries })
    }
}

/// Native implementation for Contract
#[derive(Default, Debug, Clone, PartialEq)]
pub struct Contract {
    name: String,
    version: String,
    inputs: Vec<String>,
    outputs: Vec<String>,
    creator: String,
    contract: Vec<u8>,
}

impl Contract {
    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn version(&self) -> &String {
        &self.version
    }

    pub fn inputs(&self) -> &[String] {
        &self.inputs
    }

    pub fn outputs(&self) -> &[String] {
        &self.outputs
    }

    pub fn creator(&self) -> &String {
        &self.creator
    }

    pub fn contract(&self) -> &[u8] {
        &self.contract
    }

    pub fn into_builder(self) -> ContractBuilder {
        ContractBuilder::new()
            .with_name(self.name)
            .with_version(self.version)
            .with_inputs(self.inputs)
            .with_outputs(self.outputs)
            .with_creator(self.creator)
            .with_contract(self.contract)
    }
}

impl FromProto<protos::contract::Contract> for Contract {
    fn from_proto(proto: protos::contract::Contract) -> Result<Self, ProtoConversionError> {
        Ok(Contract {
            name: proto.get_name().to_string(),
            version: proto.get_version().to_string(),
            inputs: proto.get_inputs().to_vec(),
            outputs: proto.get_outputs().to_vec(),
            creator: proto.get_creator().to_string(),
            contract: proto.get_contract().to_vec(),
        })
    }
}

impl FromNative<Contract> for protos::contract::Contract {
    fn from_native(contract: Contract) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::contract::Contract::new();

        proto.set_name(contract.name().to_string());
        proto.set_version(contract.version().to_string());
        proto.set_inputs(RepeatedField::from_vec(contract.inputs().to_vec()));
        proto.set_outputs(RepeatedField::from_vec(contract.outputs().to_vec()));
        proto.set_creator(contract.creator().to_string());
        proto.set_contract(contract.contract().to_vec());

        Ok(proto)
    }
}

impl FromBytes<Contract> for Contract {
    fn from_bytes(bytes: &[u8]) -> Result<Contract, ProtoConversionError> {
        let proto: protos::contract::Contract =
            protobuf::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get Contract from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for Contract {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from Contract".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::contract::Contract> for Contract {}
impl IntoNative<Contract> for protos::contract::Contract {}

#[derive(Debug)]
pub enum ContractBuildError {
    MissingField(String),
}

impl StdError for ContractBuildError {
    fn description(&self) -> &str {
        match *self {
            ContractBuildError::MissingField(ref msg) => msg,
        }
    }
}

impl std::fmt::Display for ContractBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ContractBuildError::MissingField(ref s) => write!(f, "MissingField: {}", s),
        }
    }
}

/// Builder used to create a Contract
#[derive(Default, Clone)]
pub struct ContractBuilder {
    name: Option<String>,
    version: Option<String>,
    inputs: Vec<String>,
    outputs: Vec<String>,
    creator: Option<String>,
    contract: Vec<u8>,
}

impl ContractBuilder {
    pub fn new() -> Self {
        ContractBuilder::default()
    }

    pub fn with_name(mut self, name: String) -> ContractBuilder {
        self.name = Some(name);
        self
    }

    pub fn with_version(mut self, version: String) -> ContractBuilder {
        self.version = Some(version);
        self
    }

    pub fn with_inputs(mut self, inputs: Vec<String>) -> ContractBuilder {
        self.inputs = inputs;
        self
    }

    pub fn with_outputs(mut self, outputs: Vec<String>) -> ContractBuilder {
        self.outputs = outputs;
        self
    }

    pub fn with_creator(mut self, creator: String) -> ContractBuilder {
        self.creator = Some(creator);
        self
    }

    pub fn with_contract(mut self, contract: Vec<u8>) -> ContractBuilder {
        self.contract = contract;
        self
    }

    pub fn build(self) -> Result<Contract, ContractBuildError> {
        let name = self.name.ok_or_else(|| {
            ContractBuildError::MissingField("'name' field is required".to_string())
        })?;

        let version = self.version.ok_or_else(|| {
            ContractBuildError::MissingField("'version' field is required".to_string())
        })?;

        let creator = self.creator.ok_or_else(|| {
            ContractBuildError::MissingField("'version' field is required".to_string())
        })?;

        let inputs = {
            if !self.inputs.is_empty() {
                self.inputs
            } else {
                return Err(ContractBuildError::MissingField(
                    "'inputs' field is required".to_string(),
                ));
            }
        };

        let outputs = {
            if !self.outputs.is_empty() {
                self.outputs
            } else {
                return Err(ContractBuildError::MissingField(
                    "'outputs' field is required".to_string(),
                ));
            }
        };

        let contract = {
            if !self.contract.is_empty() {
                self.contract
            } else {
                return Err(ContractBuildError::MissingField(
                    "'contract' field is required".to_string(),
                ));
            }
        };

        Ok(Contract {
            name,
            version,
            inputs,
            outputs,
            creator,
            contract,
        })
    }
}

// Native implementation for ContractList
#[derive(Default, Debug, Clone, PartialEq)]
pub struct ContractList {
    contracts: Vec<Contract>,
}

impl ContractList {
    pub fn contracts(&self) -> &[Contract] {
        &self.contracts
    }
}

impl FromProto<protos::contract::ContractList> for ContractList {
    fn from_proto(proto: protos::contract::ContractList) -> Result<Self, ProtoConversionError> {
        Ok(ContractList {
            contracts: proto
                .get_contracts()
                .to_vec()
                .into_iter()
                .map(Contract::from_proto)
                .collect::<Result<Vec<Contract>, ProtoConversionError>>()?,
        })
    }
}

impl FromNative<ContractList> for protos::contract::ContractList {
    fn from_native(contract_list: ContractList) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::contract::ContractList::new();
        proto.set_contracts(RepeatedField::from_vec(
            contract_list
                .contracts()
                .to_vec()
                .into_iter()
                .map(Contract::into_proto)
                .collect::<Result<Vec<protos::contract::Contract>, ProtoConversionError>>()?,
        ));

        Ok(proto)
    }
}

impl FromBytes<ContractList> for ContractList {
    fn from_bytes(bytes: &[u8]) -> Result<ContractList, ProtoConversionError> {
        let proto: protos::contract::ContractList =
            protobuf::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get ContractList from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for ContractList {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from ContractList".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::contract::ContractList> for ContractList {}
impl IntoNative<ContractList> for protos::contract::ContractList {}

#[derive(Debug)]
pub enum ContractListBuildError {
    MissingField(String),
}

impl StdError for ContractListBuildError {
    fn description(&self) -> &str {
        match *self {
            ContractListBuildError::MissingField(ref msg) => msg,
        }
    }
}

impl std::fmt::Display for ContractListBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ContractListBuildError::MissingField(ref s) => write!(f, "MissingField: {}", s),
        }
    }
}

/// Builder used to create a ContractList
#[derive(Default, Clone)]
pub struct ContractListBuilder {
    contracts: Vec<Contract>,
}

impl ContractListBuilder {
    pub fn new() -> Self {
        ContractListBuilder::default()
    }

    pub fn with_contracts(mut self, contracts: Vec<Contract>) -> ContractListBuilder {
        self.contracts = contracts;
        self
    }

    pub fn build(self) -> Result<ContractList, ContractListBuildError> {
        let contracts = self.contracts;

        Ok(ContractList { contracts })
    }
}

/// Native implementation for SmartPermission
#[derive(Default, Debug, Clone, PartialEq)]
pub struct SmartPermission {
    name: String,
    org_id: String,
    function: Vec<u8>,
}

impl SmartPermission {
    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn org_id(&self) -> &String {
        &self.org_id
    }

    pub fn function(&self) -> &[u8] {
        &self.function
    }

    pub fn into_builder(self) -> SmartPermissionBuilder {
        SmartPermissionBuilder::new()
            .with_name(self.name)
            .with_org_id(self.org_id)
            .with_function(self.function)
    }
}

impl FromProto<protos::smart_permission::SmartPermission> for SmartPermission {
    fn from_proto(
        proto: protos::smart_permission::SmartPermission,
    ) -> Result<Self, ProtoConversionError> {
        Ok(SmartPermission {
            name: proto.get_name().to_string(),
            org_id: proto.get_org_id().to_string(),
            function: proto.get_function().to_vec(),
        })
    }
}

impl FromNative<SmartPermission> for protos::smart_permission::SmartPermission {
    fn from_native(smart_permission: SmartPermission) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::smart_permission::SmartPermission::new();

        proto.set_name(smart_permission.name().to_string());
        proto.set_org_id(smart_permission.org_id().to_string());
        proto.set_function(smart_permission.function().to_vec());

        Ok(proto)
    }
}

impl FromBytes<SmartPermission> for SmartPermission {
    fn from_bytes(bytes: &[u8]) -> Result<SmartPermission, ProtoConversionError> {
        let proto: protos::smart_permission::SmartPermission = protobuf::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get SmartPermission from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for SmartPermission {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from SmartPermission".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::smart_permission::SmartPermission> for SmartPermission {}
impl IntoNative<SmartPermission> for protos::smart_permission::SmartPermission {}

#[derive(Debug)]
pub enum SmartPermissionBuildError {
    MissingField(String),
}

impl StdError for SmartPermissionBuildError {
    fn description(&self) -> &str {
        match *self {
            SmartPermissionBuildError::MissingField(ref msg) => msg,
        }
    }
}

impl std::fmt::Display for SmartPermissionBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            SmartPermissionBuildError::MissingField(ref s) => write!(f, "MissingField: {}", s),
        }
    }
}

/// Builder used to create a SmartPermission
#[derive(Default, Clone)]
pub struct SmartPermissionBuilder {
    name: Option<String>,
    org_id: Option<String>,
    function: Vec<u8>,
}

impl SmartPermissionBuilder {
    pub fn new() -> Self {
        SmartPermissionBuilder::default()
    }

    pub fn with_name(mut self, name: String) -> SmartPermissionBuilder {
        self.name = Some(name);
        self
    }

    pub fn with_org_id(mut self, org_id: String) -> SmartPermissionBuilder {
        self.org_id = Some(org_id);
        self
    }

    pub fn with_function(mut self, function: Vec<u8>) -> SmartPermissionBuilder {
        self.function = function;
        self
    }

    pub fn build(self) -> Result<SmartPermission, SmartPermissionBuildError> {
        let name = self.name.ok_or_else(|| {
            SmartPermissionBuildError::MissingField("'name' field is required".to_string())
        })?;

        let org_id = self.org_id.ok_or_else(|| {
            SmartPermissionBuildError::MissingField("'org_id' field is required".to_string())
        })?;

        let function = {
            if !self.function.is_empty() {
                self.function
            } else {
                return Err(SmartPermissionBuildError::MissingField(
                    "'function' field is required".to_string(),
                ));
            }
        };

        Ok(SmartPermission {
            name,
            org_id,
            function,
        })
    }
}

// Native implementation for SmartPermissionList
#[derive(Default, Debug, Clone, PartialEq)]
pub struct SmartPermissionList {
    smart_permissions: Vec<SmartPermission>,
}

impl SmartPermissionList {
    pub fn smart_permissions(&self) -> &[SmartPermission] {
        &self.smart_permissions
    }
}

impl FromProto<protos::smart_permission::SmartPermissionList> for SmartPermissionList {
    fn from_proto(
        proto: protos::smart_permission::SmartPermissionList,
    ) -> Result<Self, ProtoConversionError> {
        Ok(SmartPermissionList {
            smart_permissions: proto
                .get_smart_permissions()
                .to_vec()
                .into_iter()
                .map(SmartPermission::from_proto)
                .collect::<Result<Vec<SmartPermission>, ProtoConversionError>>()?,
        })
    }
}

impl FromNative<SmartPermissionList> for protos::smart_permission::SmartPermissionList {
    fn from_native(
        smart_permissions_list: SmartPermissionList,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::smart_permission::SmartPermissionList::new();
        proto.set_smart_permissions(RepeatedField::from_vec(
            smart_permissions_list
                .smart_permissions()
                .to_vec()
                .into_iter()
                .map(SmartPermission::into_proto)
                .collect::<Result<Vec<protos::smart_permission::SmartPermission>, ProtoConversionError>>()?,
        ));

        Ok(proto)
    }
}

impl FromBytes<SmartPermissionList> for SmartPermissionList {
    fn from_bytes(bytes: &[u8]) -> Result<SmartPermissionList, ProtoConversionError> {
        let proto: protos::smart_permission::SmartPermissionList =
            protobuf::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get SmartPermissionList from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for SmartPermissionList {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from SmartPermissionList".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::smart_permission::SmartPermissionList> for SmartPermissionList {}
impl IntoNative<SmartPermissionList> for protos::smart_permission::SmartPermissionList {}

#[derive(Debug)]
pub enum SmartPermissionListBuildError {
    MissingField(String),
}

impl StdError for SmartPermissionListBuildError {
    fn description(&self) -> &str {
        match *self {
            SmartPermissionListBuildError::MissingField(ref msg) => msg,
        }
    }
}

impl std::fmt::Display for SmartPermissionListBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            SmartPermissionListBuildError::MissingField(ref s) => write!(f, "MissingField: {}", s),
        }
    }
}

/// Builder used to create a SmartPermissionList
#[derive(Default, Clone)]
pub struct SmartPermissionListBuilder {
    smart_permissions: Vec<SmartPermission>,
}

impl SmartPermissionListBuilder {
    pub fn new() -> Self {
        SmartPermissionListBuilder::default()
    }

    pub fn with_smart_permissions(
        mut self,
        smart_permissions: Vec<SmartPermission>,
    ) -> SmartPermissionListBuilder {
        self.smart_permissions = smart_permissions;
        self
    }

    pub fn build(self) -> Result<SmartPermissionList, SmartPermissionListBuildError> {
        let smart_permissions = self.smart_permissions;

        Ok(SmartPermissionList { smart_permissions })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // check that a contract registry is built correctly
    fn check_contract_registry() {
        let builder = VersionBuilder::new();
        let version = builder
            .with_version("0.0.0".to_string())
            .with_contract_sha512("sha512".to_string())
            .with_creator("The Creator".to_string())
            .build()
            .unwrap();

        let builder = ContractRegistryBuilder::new();
        let contract_registry = builder
            .with_name("Tests".to_string())
            .with_versions(vec![version.clone()])
            .with_owners(vec!["owner".to_string()])
            .build()
            .unwrap();

        assert_eq!(contract_registry.name(), "Tests");
        assert_eq!(contract_registry.versions(), [version]);
        assert_eq!(contract_registry.owners(), ["owner"]);
    }

    #[test]
    // check that a contract registry can be converted to bytes and back
    fn check_contract_registry_bytes() {
        let builder = VersionBuilder::new();
        let version = builder
            .with_version("0.0.0".to_string())
            .with_contract_sha512("sha512".to_string())
            .with_creator("The Creator".to_string())
            .build()
            .unwrap();

        let builder = ContractRegistryBuilder::new();
        let original = builder
            .with_name("Tests".to_string())
            .with_versions(vec![version.clone()])
            .with_owners(vec!["owner".to_string()])
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let contract_registry = ContractRegistry::from_bytes(&bytes).unwrap();
        assert_eq!(contract_registry, original);
    }

    #[test]
    // check that a contract registry can be converted into builder
    fn check_contract_registry_into_builder() {
        let builder = VersionBuilder::new();
        let version = builder
            .with_version("0.0.0".to_string())
            .with_contract_sha512("sha512".to_string())
            .with_creator("The Creator".to_string())
            .build()
            .unwrap();

        let builder = ContractRegistryBuilder::new();
        let contract_registry = builder
            .with_name("Tests".to_string())
            .with_versions(vec![version.clone()])
            .with_owners(vec!["owner".to_string()])
            .build()
            .unwrap();

        let builder = contract_registry.into_builder();

        assert_eq!(builder.name, Some("Tests".to_string()));
        assert_eq!(builder.versions, [version]);
        assert_eq!(builder.owners, ["owner"]);
    }

    #[test]
    // check that a contract registry list is built correctly
    fn check_contract_registry_list() {
        let builder = VersionBuilder::new();
        let version = builder
            .with_version("0.0.0".to_string())
            .with_contract_sha512("sha512".to_string())
            .with_creator("The Creator".to_string())
            .build()
            .unwrap();

        let builder = ContractRegistryBuilder::new();
        let contract_registry = builder
            .with_name("Tests".to_string())
            .with_versions(vec![version.clone()])
            .with_owners(vec!["owner".to_string()])
            .build()
            .unwrap();

        let build = ContractRegistryListBuilder::new();
        let contract_registry_list = build
            .with_registries(vec![contract_registry.clone()])
            .build()
            .unwrap();

        assert_eq!(contract_registry_list.registries(), [contract_registry]);
    }

    #[test]
    // check that a contract registry list can be converted to bytes and back
    fn check_contract_registry_bytes_list() {
        let builder = VersionBuilder::new();
        let version = builder
            .with_version("0.0.0".to_string())
            .with_contract_sha512("sha512".to_string())
            .with_creator("The Creator".to_string())
            .build()
            .unwrap();

        let builder = ContractRegistryBuilder::new();
        let contract_registry = builder
            .with_name("Tests".to_string())
            .with_versions(vec![version.clone()])
            .with_owners(vec!["owner".to_string()])
            .build()
            .unwrap();

        let build = ContractRegistryListBuilder::new();
        let original = build
            .with_registries(vec![contract_registry.clone()])
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let contract_registry_list = ContractRegistryList::from_bytes(&bytes).unwrap();
        assert_eq!(contract_registry_list, original);
    }

    #[test]
    // check that a namespace registry is built correctly
    fn check_namespace_registry() {
        let builder = PermissionBuilder::new();
        let permission = builder
            .with_contract_name("Test".to_string())
            .with_read(true)
            .with_write(true)
            .build()
            .unwrap();

        let builder = NamespaceRegistryBuilder::new();
        let namespace_registry = builder
            .with_namespace("Tests".to_string())
            .with_owners(vec!["owner".to_string()])
            .with_permissions(vec![permission.clone()])
            .build()
            .unwrap();

        assert_eq!(namespace_registry.namespace(), "Tests");
        assert_eq!(namespace_registry.permissions(), [permission]);
        assert_eq!(namespace_registry.owners(), ["owner"]);
    }

    #[test]
    // check that a namespace registry can be converted to bytes and back
    fn check_namespace_registry_bytes() {
        let builder = PermissionBuilder::new();
        let permission = builder
            .with_contract_name("Test".to_string())
            .with_read(true)
            .with_write(true)
            .build()
            .unwrap();

        let builder = NamespaceRegistryBuilder::new();
        let original = builder
            .with_namespace("Tests".to_string())
            .with_owners(vec!["owner".to_string()])
            .with_permissions(vec![permission.clone()])
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let namespace_registry = NamespaceRegistry::from_bytes(&bytes).unwrap();
        assert_eq!(namespace_registry, original);
    }

    #[test]
    // check that a namespace registry can be conveted into a builder
    fn check_namespace_registry_into_build() {
        let builder = PermissionBuilder::new();
        let permission = builder
            .with_contract_name("Test".to_string())
            .with_read(true)
            .with_write(true)
            .build()
            .unwrap();

        let builder = NamespaceRegistryBuilder::new();
        let namespace_registry = builder
            .with_namespace("Tests".to_string())
            .with_owners(vec!["owner".to_string()])
            .with_permissions(vec![permission.clone()])
            .build()
            .unwrap();

        let builder = namespace_registry.into_builder();

        assert_eq!(builder.namespace, Some("Tests".to_string()));
        assert_eq!(builder.permissions, [permission]);
        assert_eq!(builder.owners, ["owner"]);
    }

    #[test]
    // check that a namespace registry list is built correctly
    fn check_namespace_registry_list() {
        let builder = PermissionBuilder::new();
        let permission = builder
            .with_contract_name("Test".to_string())
            .with_read(true)
            .with_write(true)
            .build()
            .unwrap();

        let builder = NamespaceRegistryBuilder::new();
        let namespace_registry = builder
            .with_namespace("Tests".to_string())
            .with_owners(vec!["owner".to_string()])
            .with_permissions(vec![permission.clone()])
            .build()
            .unwrap();

        let build = NamespaceRegistryListBuilder::new();
        let namespace_registry_list = build
            .with_registries(vec![namespace_registry.clone()])
            .build()
            .unwrap();

        assert_eq!(namespace_registry_list.registries(), [namespace_registry]);
    }

    #[test]
    // check that a namespace registry list can be converted to bytes and back
    fn check_namespace_registry_bytes_list() {
        let builder = PermissionBuilder::new();
        let permission = builder
            .with_contract_name("Test".to_string())
            .with_read(true)
            .with_write(true)
            .build()
            .unwrap();

        let builder = NamespaceRegistryBuilder::new();
        let namespace_registry = builder
            .with_namespace("Tests".to_string())
            .with_owners(vec!["owner".to_string()])
            .with_permissions(vec![permission.clone()])
            .build()
            .unwrap();

        let build = NamespaceRegistryListBuilder::new();
        let original = build
            .with_registries(vec![namespace_registry.clone()])
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let namespace_registry_list = NamespaceRegistryList::from_bytes(&bytes).unwrap();
        assert_eq!(namespace_registry_list, original);
    }

    #[test]
    // check that a contract is built correctly
    fn check_contract() {
        let builder = ContractBuilder::new();
        let contract = builder
            .with_name("Tests".to_string())
            .with_version("0.0.0".to_string())
            .with_inputs(vec!["input1".to_string(), "input2".to_string()])
            .with_outputs(vec!["output1".to_string(), "output2".to_string()])
            .with_creator("The Creator".to_string())
            .with_contract(b"test_contract".to_vec())
            .build()
            .unwrap();

        assert_eq!(contract.name(), "Tests");
        assert_eq!(contract.version(), "0.0.0");
        assert_eq!(
            contract.inputs(),
            ["input1".to_string(), "input2".to_string()]
        );
        assert_eq!(
            contract.outputs(),
            ["output1".to_string(), "output2".to_string()]
        );
        assert_eq!(contract.creator(), "The Creator");
        assert_eq!(contract.contract(), b"test_contract");
    }

    #[test]
    // check that a contract can be converted to bytes and back
    fn check_contract_bytes() {
        let builder = ContractBuilder::new();
        let original = builder
            .with_name("Tests".to_string())
            .with_version("0.0.0".to_string())
            .with_inputs(vec!["input1".to_string(), "input2".to_string()])
            .with_outputs(vec!["output1".to_string(), "output2".to_string()])
            .with_creator("The Creator".to_string())
            .with_contract(b"test_contract".to_vec())
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let contract = Contract::from_bytes(&bytes).unwrap();
        assert_eq!(contract, original);
    }

    #[test]
    // check that the contract can be converted into a builder
    fn check_contract_into_builder() {
        let builder = ContractBuilder::new();
        let contract = builder
            .with_name("Tests".to_string())
            .with_version("0.0.0".to_string())
            .with_inputs(vec!["input1".to_string(), "input2".to_string()])
            .with_outputs(vec!["output1".to_string(), "output2".to_string()])
            .with_creator("The Creator".to_string())
            .with_contract(b"test_contract".to_vec())
            .build()
            .unwrap();

        let builder = contract.into_builder();

        assert_eq!(builder.name, Some("Tests".to_string()));
        assert_eq!(builder.version, Some("0.0.0".to_string()));
        assert_eq!(builder.inputs, ["input1".to_string(), "input2".to_string()]);
        assert_eq!(
            builder.outputs,
            ["output1".to_string(), "output2".to_string()]
        );
        assert_eq!(builder.creator, Some("The Creator".to_string()));
        assert_eq!(builder.contract, b"test_contract".to_vec());
    }

    #[test]
    // check that a contract list is built correctly
    fn check_contract_list() {
        let builder = ContractBuilder::new();
        let contract = builder
            .with_name("Tests".to_string())
            .with_version("0.0.0".to_string())
            .with_inputs(vec!["input1".to_string(), "input2".to_string()])
            .with_outputs(vec!["output1".to_string(), "output2".to_string()])
            .with_creator("The Creator".to_string())
            .with_contract(b"test_contract".to_vec())
            .build()
            .unwrap();

        let builder = ContractListBuilder::new();
        let contract_list = builder
            .with_contracts(vec![contract.clone()])
            .build()
            .unwrap();

        assert_eq!(contract_list.contracts(), [contract]);
    }

    #[test]
    // check that a contract list can be converted to bytes and back
    fn check_contract_list_bytes() {
        let builder = ContractBuilder::new();
        let contract = builder
            .with_name("Tests".to_string())
            .with_version("0.0.0".to_string())
            .with_inputs(vec!["input1".to_string(), "input2".to_string()])
            .with_outputs(vec!["output1".to_string(), "output2".to_string()])
            .with_creator("The Creator".to_string())
            .with_contract(b"test_contract".to_vec())
            .build()
            .unwrap();

        let builder = ContractListBuilder::new();
        let original = builder
            .with_contracts(vec![contract.clone()])
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let contract_list = ContractList::from_bytes(&bytes).unwrap();
        assert_eq!(contract_list, original);
    }

    #[test]
    // check that a smart permission is built correctly
    fn check_smart_permission() {
        let builder = SmartPermissionBuilder::new();
        let smart_permission = builder
            .with_name("Tests".to_string())
            .with_org_id("org_id".to_string())
            .with_function(b"test_function".to_vec())
            .build()
            .unwrap();

        assert_eq!(smart_permission.name(), "Tests");
        assert_eq!(smart_permission.org_id(), "org_id");
        assert_eq!(smart_permission.function(), b"test_function");
    }

    #[test]
    // check that a smart permission can be converted to bytes and back
    fn check_smart_permission_bytes() {
        let builder = SmartPermissionBuilder::new();
        let original = builder
            .with_name("Tests".to_string())
            .with_org_id("org_id".to_string())
            .with_function(b"test_function".to_vec())
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let smart_permission = SmartPermission::from_bytes(&bytes).unwrap();
        assert_eq!(smart_permission, original);
    }

    #[test]
    // check that a smart permission can be converted to builder
    fn check_smart_permission_into_builder() {
        let builder = SmartPermissionBuilder::new();
        let smart_permission = builder
            .with_name("Tests".to_string())
            .with_org_id("org_id".to_string())
            .with_function(b"test_function".to_vec())
            .build()
            .unwrap();

        let builder = smart_permission.into_builder();

        assert_eq!(builder.name, Some("Tests".to_string()));
        assert_eq!(builder.org_id, Some("org_id".to_string()));
        assert_eq!(builder.function, b"test_function".to_vec());
    }

    #[test]
    // check that a smart permission list is built correctly
    fn check_smart_permission_list() {
        let builder = SmartPermissionBuilder::new();
        let smart_permission = builder
            .with_name("Tests".to_string())
            .with_org_id("org_id".to_string())
            .with_function(b"test_function".to_vec())
            .build()
            .unwrap();

        let builder = SmartPermissionListBuilder::new();
        let smart_permission_list = builder
            .with_smart_permissions(vec![smart_permission.clone()])
            .build()
            .unwrap();

        assert_eq!(
            smart_permission_list.smart_permissions(),
            [smart_permission]
        );
    }

    #[test]
    // check that a smart permission list can be converted to bytes and back
    fn check_smart_permission_list_bytes() {
        let builder = SmartPermissionBuilder::new();
        let smart_permission = builder
            .with_name("Tests".to_string())
            .with_org_id("org_id".to_string())
            .with_function(b"test_function".to_vec())
            .build()
            .unwrap();

        let builder = SmartPermissionListBuilder::new();
        let original = builder
            .with_smart_permissions(vec![smart_permission.clone()])
            .build()
            .unwrap();

        let bytes = original.clone().into_bytes().unwrap();

        let smart_permission_list = SmartPermissionList::from_bytes(&bytes).unwrap();
        assert_eq!(smart_permission_list, original);
    }
}

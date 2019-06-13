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

//! Provides a Sawtooth Transaction Handler for executing Sabre transactions.

use crypto::digest::Digest;
use crypto::sha2::Sha512;
use sabre_sdk::protocol::state::{
    ContractBuilder, ContractRegistry, ContractRegistryBuilder, NamespaceRegistry,
    NamespaceRegistryBuilder, PermissionBuilder, SmartPermissionBuilder, VersionBuilder,
};
use sawtooth_sdk::messages::processor::TpProcessRequest;
use sawtooth_sdk::processor::handler::ApplyError;
use sawtooth_sdk::processor::handler::TransactionContext;
use sawtooth_sdk::processor::handler::TransactionHandler;

use crate::payload::SabreRequestPayload;
use crate::state::SabreState;
use crate::wasm_executor::wasm_module::WasmModule;
use sabre_sdk::protocol::payload::{
    Action, CreateContractAction, CreateContractRegistryAction, CreateNamespaceRegistryAction,
    CreateNamespaceRegistryPermissionAction, CreateSmartPermissionAction, DeleteContractAction,
    DeleteContractRegistryAction, DeleteNamespaceRegistryAction,
    DeleteNamespaceRegistryPermissionAction, DeleteSmartPermissionAction, ExecuteContractAction,
    UpdateContractRegistryOwnersAction, UpdateNamespaceRegistryOwnersAction,
    UpdateSmartPermissionAction,
};

/// The namespace registry prefix for global state (00ec00)
const NAMESPACE_REGISTRY_PREFIX: &str = "00ec00";

/// The contract registry prefix for global state (00ec01)
const CONTRACT_REGISTRY_PREFIX: &str = "00ec01";

/// The contract prefix for global state (00ec02)
const CONTRACT_PREFIX: &str = "00ec02";

/// Handles Sabre Transactions
///
/// This handler implements the Sawtooth TransactionHandler trait, in order to execute Sabre
/// transaction payloads.  These payloads include on-chain smart contracts executed in a
/// WebAssembly virtual machine.
///
/// WebAssembly (Wasm) is a stack-based virtual machine newly implemented in major browsers. It is
/// well-suited for the purposes of smart contract execution due to its sandboxed design, growing
/// popularity, and tool support.
pub struct SabreTransactionHandler {
    family_name: String,
    family_versions: Vec<String>,
    namespaces: Vec<String>,
}

impl SabreTransactionHandler {
    /// Constructs a new SabreTransactionHandler
    pub fn new() -> SabreTransactionHandler {
        SabreTransactionHandler {
            family_name: "sabre".into(),
            family_versions: vec!["0.3".into()],
            namespaces: vec![
                NAMESPACE_REGISTRY_PREFIX.into(),
                CONTRACT_REGISTRY_PREFIX.into(),
                CONTRACT_PREFIX.into(),
            ],
        }
    }
}

impl TransactionHandler for SabreTransactionHandler {
    fn family_name(&self) -> String {
        self.family_name.clone()
    }

    fn family_versions(&self) -> Vec<String> {
        self.family_versions.clone()
    }

    fn namespaces(&self) -> Vec<String> {
        self.namespaces.clone()
    }

    fn apply(
        &self,
        request: &TpProcessRequest,
        context: &mut dyn TransactionContext,
    ) -> Result<(), ApplyError> {
        let payload = SabreRequestPayload::new(request.get_payload());

        let payload = match payload {
            Err(e) => return Err(e),
            Ok(payload) => payload,
        };
        let payload = match payload {
            Some(x) => x,
            None => {
                return Err(ApplyError::InvalidTransaction(String::from(
                    "Request must contain a payload",
                )));
            }
        };

        let signer = request.get_header().get_signer_public_key();
        let mut state = SabreState::new(context);

        info!(
            "{} {:?} {:?}",
            payload.get_action(),
            request.get_header().get_inputs(),
            request.get_header().get_outputs()
        );

        match payload.get_action() {
            Action::CreateContract(create_contract_payload) => {
                create_contract(create_contract_payload, signer, &mut state)
            }
            Action::DeleteContract(delete_contract_payload) => {
                delete_contract(delete_contract_payload, signer, &mut state)
            }
            Action::ExecuteContract(execute_contract_payload) => execute_contract(
                execute_contract_payload,
                signer,
                request.get_signature(),
                &mut state,
            ),
            Action::CreateContractRegistry(create_contract_registry_payload) => {
                create_contract_registry(create_contract_registry_payload, signer, &mut state)
            }
            Action::DeleteContractRegistry(delete_contract_registry_payload) => {
                delete_contract_registry(delete_contract_registry_payload, signer, &mut state)
            }
            Action::UpdateContractRegistryOwners(update_contract_registry_owners_payload) => {
                update_contract_registry_owners(
                    update_contract_registry_owners_payload,
                    signer,
                    &mut state,
                )
            }
            Action::CreateNamespaceRegistry(create_namespace_registry_payload) => {
                create_namespace_registry(create_namespace_registry_payload, signer, &mut state)
            }
            Action::DeleteNamespaceRegistry(delete_namespace_registry_payload) => {
                delete_namespace_registry(delete_namespace_registry_payload, signer, &mut state)
            }
            Action::UpdateNamespaceRegistryOwners(update_namespace_registry_owners_payload) => {
                update_namespace_registry_owners(
                    update_namespace_registry_owners_payload,
                    signer,
                    &mut state,
                )
            }
            Action::CreateNamespaceRegistryPermission(
                create_namespace_registry_permission_payload,
            ) => create_namespace_registry_permission(
                create_namespace_registry_permission_payload,
                signer,
                &mut state,
            ),
            Action::DeleteNamespaceRegistryPermission(
                delete_namespace_registry_permission_payload,
            ) => delete_namespace_registry_permission(
                delete_namespace_registry_permission_payload,
                signer,
                &mut state,
            ),
            Action::CreateSmartPermission(payload) => {
                create_smart_permission(payload, signer, &mut state)
            }
            Action::UpdateSmartPermission(payload) => {
                update_smart_permission(payload, signer, &mut state)
            }
            Action::DeleteSmartPermission(payload) => {
                delete_smart_permission(payload, signer, &mut state)
            }
        }
    }
}

fn create_contract(
    payload: CreateContractAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let name = payload.name();
    let version = payload.version();
    match state.get_contract(name, version) {
        Ok(None) => (),
        Ok(Some(_)) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Contract already exists: {}, {}",
                name, version,
            )));
        }
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )));
        }
    };

    // update or create the contract registry for the contract
    let contract_registry = match state.get_contract_registry(name) {
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "The Contract Registry does not exist: {}",
                name,
            )));
        }
        Ok(Some(contract_registry)) => contract_registry,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )));
        }
    };

    if !contract_registry.owners().contains(&signer.into()) {
        return Err(ApplyError::InvalidTransaction(format!(
            "Only owners can submit new versions of contracts: {}",
            signer,
        )));
    }

    let contract = ContractBuilder::new()
        .with_name(name.into())
        .with_version(version.into())
        .with_inputs(payload.inputs().to_vec())
        .with_outputs(payload.outputs().to_vec())
        .with_creator(signer.into())
        .with_contract(payload.contract().to_vec())
        .build()
        .map_err(|_| ApplyError::InvalidTransaction(String::from("Cannot build contract")))?;

    state.set_contract(name, version, contract)?;

    let mut sha = Sha512::new();
    sha.input(payload.contract());

    let contract_registry_version = VersionBuilder::new()
        .with_version(version.into())
        .with_contract_sha512(sha.result_str().into())
        .with_creator(signer.into())
        .build()
        .map_err(|_| {
            ApplyError::InvalidTransaction(String::from("Cannot build contract version"))
        })?;

    let mut versions = contract_registry.versions().to_vec();
    versions.push(contract_registry_version);

    let contract_registry = contract_registry
        .into_builder()
        .with_versions(versions)
        .build()
        .map_err(|_| {
            ApplyError::InvalidTransaction(String::from("Cannot build contract registry"))
        })?;

    state.set_contract_registry(name, contract_registry)
}

fn delete_contract(
    payload: DeleteContractAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let name = payload.name();
    let version = payload.version();

    match state.get_contract(name, version) {
        Ok(Some(_)) => (),
        Ok(_) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Contract does not exist: {}, {}",
                name, version,
            )));
        }
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )));
        }
    };

    // update the contract registry for the contract
    let contract_registry = match state.get_contract_registry(name) {
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Contract Registry does not exist {}",
                name,
            )));
        }
        Ok(Some(contract_registry)) => contract_registry,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )));
        }
    };

    if !(contract_registry.owners().contains(&signer.into())) {
        return Err(ApplyError::InvalidTransaction(format!(
            "Signer is not an owner of this contract: {}",
            signer,
        )));
    }
    let mut versions = contract_registry.versions().to_vec();
    for (index, contract_registry_version) in versions.iter().enumerate() {
        if contract_registry_version.version() == version {
            versions.remove(index);
            break;
        }
    }

    let contract_registry = contract_registry
        .into_builder()
        .with_versions(versions)
        .build()
        .map_err(|_| {
            ApplyError::InvalidTransaction(String::from("Cannot build contract registry"))
        })?;

    state.set_contract_registry(name, contract_registry)?;
    state.delete_contract(name, version)
}

fn execute_contract(
    payload: ExecuteContractAction,
    signer: &str,
    signature: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let name = payload.name();
    let version = payload.version();

    let contract = match state.get_contract(name, version) {
        Ok(Some(contract)) => contract,
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Contract does not exist: {}, {}",
                name, version,
            )));
        }
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )));
        }
    };

    for input in payload.inputs() {
        let namespace = match input.get(..6) {
            Some(namespace) => namespace,
            None => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Input must have at least 6 characters: {}",
                    input,
                )));
            }
        };
        let registries = match state.get_namespace_registries(namespace) {
            Ok(Some(registries)) => registries,
            Ok(None) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Namespace Registry does not exist: {}",
                    namespace,
                )));
            }
            Err(err) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Unable to check state: {}",
                    err,
                )));
            }
        };

        let mut namespace_registry = None;
        for registry in registries.registries() {
            if input.starts_with(registry.namespace()) {
                namespace_registry = Some(registry)
            }
        }

        let mut permissioned = false;
        match namespace_registry {
            Some(registry) => {
                for permission in registry.permissions() {
                    if name == permission.contract_name() && permission.read() {
                        permissioned = true;
                        break;
                    }
                }
                if !permissioned {
                    return Err(ApplyError::InvalidTransaction(format!(
                        "Contract does not have permission to read from state : {} {}",
                        name, input
                    )));
                }
            }
            None => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "No namespace registry exists for namespace: {} input: {}",
                    namespace, input
                )));
            }
        }
    }

    for output in payload.outputs() {
        let namespace = match output.get(..6) {
            Some(namespace) => namespace,
            None => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Output must have at least 6 characters: {}",
                    output,
                )));
            }
        };
        let registries = match state.get_namespace_registries(namespace) {
            Ok(Some(registries)) => registries,
            Ok(None) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Namespace Registry does not exist: {}",
                    namespace,
                )));
            }
            Err(err) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Unable to check state: {}",
                    err,
                )));
            }
        };

        let mut namespace_registry = None;
        for registry in registries.registries() {
            if output.starts_with(registry.namespace()) {
                namespace_registry = Some(registry)
            }
        }
        let mut permissioned = false;
        match namespace_registry {
            Some(registry) => {
                for permission in registry.permissions() {
                    if name == permission.contract_name() && permission.write() {
                        permissioned = true;
                        break;
                    }
                }
                if !permissioned {
                    return Err(ApplyError::InvalidTransaction(format!(
                        "Contract does not have permission to write to state: {}, {}",
                        name, output
                    )));
                }
            }
            None => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "No namespace registry exists for namespace: {} output: {}",
                    namespace, output
                )));
            }
        }
    }

    let mut module = WasmModule::new(contract.contract(), state.context())
        .expect("Failed to create can_add module");

    let result = module
        .entrypoint(payload.payload().to_vec(), signer.into(), signature.into())
        .map_err(|e| ApplyError::InvalidTransaction(format!("{:?}", e)))?;

    match result {
        None => Err(ApplyError::InvalidTransaction(format!(
            "Wasm contract did not return a result: {}, {}",
            name, version,
        ))),
        Some(1) => Ok(()),
        Some(-3) => Err(ApplyError::InvalidTransaction(format!(
            "Wasm contract returned invalid transaction: {}, {}",
            name, version,
        ))),
        Some(num) => Err(ApplyError::InternalError(format!(
            "Wasm contract returned internal error: {}",
            num
        ))),
    }
}

fn create_contract_registry(
    payload: CreateContractRegistryAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let name = payload.name();

    match state.get_contract_registry(name) {
        Ok(None) => (),
        Ok(Some(_)) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Contract Registry already exists: {}",
                name,
            )));
        }
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )));
        }
    };

    let setting = match state.get_admin_setting() {
        Ok(Some(setting)) => setting,
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Only admins can create a contract registry: {}",
                signer,
            )));
        }
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )));
        }
    };

    for entry in setting.get_entries() {
        if entry.key == "sawtooth.swa.administrators" {
            let values = entry.value.split(',');
            let value_vec: Vec<&str> = values.collect();
            if !value_vec.contains(&signer) {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Only admins can create a contract registry: {}",
                    signer,
                )));
            }
        }
    }

    let contract_registry = ContractRegistryBuilder::new()
        .with_name(name.into())
        .with_owners(payload.owners().to_vec())
        .build()
        .map_err(|_| {
            ApplyError::InvalidTransaction(String::from("Cannot build contract registry"))
        })?;

    state.set_contract_registry(name, contract_registry)
}

fn delete_contract_registry(
    payload: DeleteContractRegistryAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let name = payload.name();
    let contract_registry = match state.get_contract_registry(name) {
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Contract Registry does not exist: {}",
                name,
            )));
        }
        Ok(Some(contract_registry)) => contract_registry,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )));
        }
    };

    if !contract_registry.versions().is_empty() {
        return Err(ApplyError::InvalidTransaction(format!(
            "Contract Registry can only be deleted if there are no versions: {}",
            name,
        )));
    }

    // Check if signer is an owner or an admin
    can_update_contract_registry(contract_registry.clone(), signer, state)?;

    state.delete_contract_registry(name)
}

fn update_contract_registry_owners(
    payload: UpdateContractRegistryOwnersAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let name = payload.name();
    let contract_registry = match state.get_contract_registry(name) {
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Contract Registry does not exist: {}",
                name,
            )));
        }
        Ok(Some(contract_registry)) => contract_registry,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )));
        }
    };

    // Check if signer is an owner or an admin
    can_update_contract_registry(contract_registry.clone(), signer, state)?;

    let contract_registry = contract_registry
        .into_builder()
        .with_owners(payload.owners().to_vec())
        .build()
        .map_err(|_| {
            ApplyError::InvalidTransaction(String::from("Cannot build contract registry"))
        })?;

    state.set_contract_registry(name, contract_registry)
}

fn create_namespace_registry(
    payload: CreateNamespaceRegistryAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let namespace = payload.namespace();

    if namespace.len() < 6 {
        return Err(ApplyError::InvalidTransaction(format!(
            "Namespace must be at least 6 characters: {}",
            namespace,
        )));
    }

    match state.get_namespace_registry(namespace) {
        Ok(None) => (),
        Ok(Some(_)) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Namespace Registry already exists: {}",
                namespace,
            )));
        }
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )));
        }
    }

    let setting = match state.get_admin_setting() {
        Ok(Some(setting)) => setting,
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Only admins can create a namespace registry: {}",
                signer,
            )));
        }
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )));
        }
    };

    for entry in setting.get_entries() {
        if entry.key == "sawtooth.swa.administrators" {
            let values = entry.value.split(',');
            let value_vec: Vec<&str> = values.collect();
            if !value_vec.contains(&signer) {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Only admins can create a namespace registry: {}",
                    signer,
                )));
            }
        }
    }

    let namespace_registry = NamespaceRegistryBuilder::new()
        .with_namespace(namespace.into())
        .with_owners(payload.owners().to_vec())
        .build()
        .map_err(|_| {
            ApplyError::InvalidTransaction(String::from("Cannot build namespace registry"))
        })?;

    state.set_namespace_registry(namespace, namespace_registry)
}

fn delete_namespace_registry(
    payload: DeleteNamespaceRegistryAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let namespace = payload.namespace();

    let namespace_registry = match state.get_namespace_registry(namespace) {
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Namespace Registry does not exist: {}",
                namespace,
            )));
        }
        Ok(Some(namespace_registry)) => namespace_registry,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )));
        }
    };
    can_update_namespace_registry(namespace_registry.clone(), signer, state)?;

    if !namespace_registry.permissions().is_empty() {
        return Err(ApplyError::InvalidTransaction(format!(
            "Namespace Registry can only be deleted if there are no permissions: {}",
            namespace,
        )));
    }
    state.delete_namespace_registry(namespace)
}

fn update_namespace_registry_owners(
    payload: UpdateNamespaceRegistryOwnersAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let namespace = payload.namespace();

    let namespace_registry = match state.get_namespace_registry(namespace) {
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Namespace Registry does not exist: {}",
                namespace,
            )));
        }
        Ok(Some(namespace_registry)) => namespace_registry,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )));
        }
    };

    // Check if signer is an owner or an admin
    can_update_namespace_registry(namespace_registry.clone(), signer, state)?;
    let namespace_registry = namespace_registry
        .into_builder()
        .with_owners(payload.owners().to_vec())
        .build()
        .map_err(|_| {
            ApplyError::InvalidTransaction(String::from("Cannot build namespace registry"))
        })?;

    state.set_namespace_registry(namespace, namespace_registry)
}

fn create_namespace_registry_permission(
    payload: CreateNamespaceRegistryPermissionAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let namespace = payload.namespace();
    let contract_name = payload.contract_name();
    let namespace_registry = match state.get_namespace_registry(namespace) {
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Namespace Registry does not exist: {}",
                namespace,
            )));
        }
        Ok(Some(namespace_registry)) => namespace_registry,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )));
        }
    };
    // Check if signer is an owner or an admin
    can_update_namespace_registry(namespace_registry.clone(), signer, state)?;

    let new_permission = PermissionBuilder::new()
        .with_contract_name(contract_name.into())
        .with_read(payload.read())
        .with_write(payload.write())
        .build()
        .map_err(|_| {
            ApplyError::InvalidTransaction(String::from(
                "Cannot build namespace registry permission",
            ))
        })?;

    // remove old permission for contract if one exists and replace with the new permission
    let mut permissions = namespace_registry.permissions().to_vec();
    let mut index = None;
    for (count, permission) in permissions.iter().enumerate() {
        if permission.contract_name() == contract_name {
            index = Some(count);
            break;
        }
    }

    if let Some(x) = index {
        permissions.remove(x);
    }

    permissions.push(new_permission);

    let namespace_registry = namespace_registry
        .into_builder()
        .with_permissions(permissions)
        .build()
        .map_err(|_| {
            ApplyError::InvalidTransaction(String::from("Cannot build namespace registry"))
        })?;

    state.set_namespace_registry(namespace, namespace_registry)
}

fn delete_namespace_registry_permission(
    payload: DeleteNamespaceRegistryPermissionAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let namespace = payload.namespace();
    let contract_name = payload.contract_name();

    let namespace_registry = match state.get_namespace_registry(namespace) {
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Namespace Registry does not exist: {}",
                namespace,
            )));
        }
        Ok(Some(namespace_registry)) => namespace_registry,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )));
        }
    };
    // Check if signer is an owner or an admin
    can_update_namespace_registry(namespace_registry.clone(), signer, state)?;

    // remove old permission for contract
    let mut permissions = namespace_registry.permissions().to_vec();
    let mut index = None;
    for (count, permission) in permissions.iter().enumerate() {
        if permission.contract_name() == contract_name {
            index = Some(count);
            break;
        }
    }

    match index {
        Some(x) => {
            permissions.remove(x);
        }
        None => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Namespace Registry does not have a permission for : {}",
                contract_name,
            )));
        }
    };

    let namespace_registry = namespace_registry
        .into_builder()
        .with_permissions(permissions)
        .build()
        .map_err(|_| {
            ApplyError::InvalidTransaction(String::from("Cannot build namespace registry"))
        })?;
    state.set_namespace_registry(namespace, namespace_registry)
}

pub(crate) fn is_admin(
    signer: &str,
    org_id: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let admin = match state.get_agent(signer) {
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Signer is not an agent: {}",
                signer,
            )));
        }
        Ok(Some(admin)) => admin,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Failed to retrieve state: {}",
                err,
            )));
        }
    };

    if admin.org_id() != org_id {
        return Err(ApplyError::InvalidTransaction(format!(
            "Signer is not associated with the organization: {}",
            signer,
        )));
    }
    if !admin.roles().contains(&"admin".to_string()) {
        return Err(ApplyError::InvalidTransaction(format!(
            "Signer is not an admin: {}",
            signer,
        )));
    };

    if !admin.active() {
        return Err(ApplyError::InvalidTransaction(format!(
            "Admin is not currently an active agent: {}",
            signer,
        )));
    }
    Ok(())
}

fn create_smart_permission(
    payload: CreateSmartPermissionAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    // verify the signer of the transaction is authorized to create smart permissions
    is_admin(signer, payload.org_id(), state)?;

    // Check if the smart permissions already exists
    match state.get_smart_permission(payload.org_id(), payload.name()) {
        Ok(None) => (),
        Ok(Some(_)) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Smart Permission already exists: {} ",
                payload.name(),
            )));
        }
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Failed to retrieve state: {}",
                err,
            )));
        }
    };

    // Check that organizations exists
    match state.get_organization(payload.org_id()) {
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Organization does not exist exists: {}",
                payload.org_id(),
            )));
        }
        Ok(Some(_)) => (),
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Failed to retrieve state: {}",
                err,
            )));
        }
    };

    let smart_permission = SmartPermissionBuilder::new()
        .with_name(payload.name().to_string())
        .with_org_id(payload.org_id().to_string())
        .with_function(payload.function().to_vec())
        .build()
        .map_err(|_| {
            ApplyError::InvalidTransaction(String::from("Cannot build smart permission"))
        })?;

    state.set_smart_permission(payload.org_id(), payload.name(), smart_permission)
}

fn update_smart_permission(
    payload: UpdateSmartPermissionAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    // verify the signer of the transaction is authorized to update smart permissions
    is_admin(signer, payload.org_id(), state)?;

    // verify that the smart permission exists
    let smart_permission = match state.get_smart_permission(payload.org_id(), payload.name()) {
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Smart Permission does not exist: {} ",
                payload.name(),
            )));
        }
        Ok(Some(smart_permission)) => smart_permission,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Failed to retrieve state: {}",
                err,
            )));
        }
    };

    let smart_permission = smart_permission
        .into_builder()
        .with_function(payload.function().to_vec())
        .build()
        .map_err(|_| {
            ApplyError::InvalidTransaction(String::from("Cannot build smart permission"))
        })?;
    state.set_smart_permission(payload.org_id(), payload.name(), smart_permission)
}

fn delete_smart_permission(
    payload: DeleteSmartPermissionAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    // verify the signer of the transaction is authorized to delete smart permissions
    is_admin(signer, payload.org_id(), state)?;

    // verify that the smart permission exists
    match state.get_smart_permission(payload.org_id(), payload.name()) {
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Smart Permission does not exists: {} ",
                payload.name(),
            )));
        }
        Ok(Some(_)) => (),
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Failed to retrieve state: {}",
                err,
            )));
        }
    };

    state.delete_smart_permission(payload.org_id(), payload.name())
}

// helper function to check if the signer is allowed to update a namespace_registry
fn can_update_namespace_registry(
    namespace_registry: NamespaceRegistry,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    if !namespace_registry.owners().contains(&signer.into()) {
        let setting = match state.get_admin_setting() {
            Ok(Some(setting)) => setting,
            Ok(None) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Only owners or admins can update or delete a namespace registry: {}",
                    signer,
                )));
            }
            Err(err) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Unable to check state: {}",
                    err,
                )));
            }
        };

        for entry in setting.get_entries() {
            if entry.key == "sawtooth.swa.administrators" {
                let values = entry.value.split(',');
                let value_vec: Vec<&str> = values.collect();
                if !value_vec.contains(&signer) {
                    return Err(ApplyError::InvalidTransaction(format!(
                        "Only owners or admins can update or delete a namespace registry: {}",
                        signer,
                    )));
                }
            }
        }
    }
    Ok(())
}

// helper function to check if the signer is allowed to update a contract_registry
fn can_update_contract_registry(
    contract_registry: ContractRegistry,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    if !contract_registry.owners().contains(&signer.into()) {
        let setting = match state.get_admin_setting() {
            Ok(Some(setting)) => setting,
            Ok(None) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Only owners or admins can update or delete a contract registry: {}",
                    signer,
                )));
            }
            Err(err) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Unable to check state: {}",
                    err,
                )));
            }
        };

        for entry in setting.get_entries() {
            if entry.key == "sawtooth.swa.administrators" {
                let values = entry.value.split(',');
                let value_vec: Vec<&str> = values.collect();
                if !value_vec.contains(&signer) {
                    return Err(ApplyError::InvalidTransaction(format!(
                        "Only owners or admins can update or delete a contract registry: {}",
                        signer,
                    )));
                }
            }
        }
    }
    Ok(())
}

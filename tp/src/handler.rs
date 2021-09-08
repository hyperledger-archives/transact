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
    NamespaceRegistryBuilder, PermissionBuilder, VersionBuilder,
};
use sawtooth_sdk::messages::processor::TpProcessRequest;
use sawtooth_sdk::processor::handler::ApplyError;
use sawtooth_sdk::processor::handler::TransactionContext;
use sawtooth_sdk::processor::handler::TransactionHandler;

use crate::admin::AdminPermission;
use crate::payload::SabreRequestPayload;
use crate::state::SabreState;
use crate::wasm_executor::wasm_module::WasmModule;
use sabre_sdk::protocol::payload::{
    Action, CreateContractAction, CreateContractRegistryAction, CreateNamespaceRegistryAction,
    CreateNamespaceRegistryPermissionAction, DeleteContractAction, DeleteContractRegistryAction,
    DeleteNamespaceRegistryAction, DeleteNamespaceRegistryPermissionAction, ExecuteContractAction,
    UpdateContractRegistryOwnersAction, UpdateNamespaceRegistryOwnersAction,
};
use sabre_sdk::protocol::SABRE_PROTOCOL_VERSION;

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
    admin_permissions: Box<dyn AdminPermission>,
}

impl SabreTransactionHandler {
    /// Constructs a new SabreTransactionHandler
    pub fn new(admin_permissions: Box<dyn AdminPermission>) -> SabreTransactionHandler {
        SabreTransactionHandler {
            family_name: "sabre".into(),
            family_versions: vec!["0.5".into(), "0.6".into(), SABRE_PROTOCOL_VERSION.into()],
            namespaces: vec![
                NAMESPACE_REGISTRY_PREFIX.into(),
                CONTRACT_REGISTRY_PREFIX.into(),
                CONTRACT_PREFIX.into(),
            ],
            admin_permissions,
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
                create_contract_registry(
                    create_contract_registry_payload,
                    signer,
                    &mut state,
                    &*self.admin_permissions,
                )
            }
            Action::DeleteContractRegistry(delete_contract_registry_payload) => {
                delete_contract_registry(
                    delete_contract_registry_payload,
                    signer,
                    &mut state,
                    &*self.admin_permissions,
                )
            }
            Action::UpdateContractRegistryOwners(update_contract_registry_owners_payload) => {
                update_contract_registry_owners(
                    update_contract_registry_owners_payload,
                    signer,
                    &mut state,
                    &*self.admin_permissions,
                )
            }
            Action::CreateNamespaceRegistry(create_namespace_registry_payload) => {
                create_namespace_registry(
                    create_namespace_registry_payload,
                    signer,
                    &mut state,
                    &*self.admin_permissions,
                )
            }
            Action::DeleteNamespaceRegistry(delete_namespace_registry_payload) => {
                delete_namespace_registry(
                    delete_namespace_registry_payload,
                    signer,
                    &mut state,
                    &*self.admin_permissions,
                )
            }
            Action::UpdateNamespaceRegistryOwners(update_namespace_registry_owners_payload) => {
                update_namespace_registry_owners(
                    update_namespace_registry_owners_payload,
                    signer,
                    &mut state,
                    &*self.admin_permissions,
                )
            }
            Action::CreateNamespaceRegistryPermission(
                create_namespace_registry_permission_payload,
            ) => create_namespace_registry_permission(
                create_namespace_registry_permission_payload,
                signer,
                &mut state,
                &*self.admin_permissions,
            ),
            Action::DeleteNamespaceRegistryPermission(
                delete_namespace_registry_permission_payload,
            ) => delete_namespace_registry_permission(
                delete_namespace_registry_permission_payload,
                signer,
                &mut state,
                &*self.admin_permissions,
            ),
            Action::CreateSmartPermission(_)
            | Action::UpdateSmartPermission(_)
            | Action::DeleteSmartPermission(_) => {
                return Err(ApplyError::InvalidTransaction(
                    "Smart Permissions not supported in this version of Sabre".into(),
                ));
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
        .with_contract_sha512(sha.result_str())
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
    admin_permissions: &dyn AdminPermission,
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

    if !admin_permissions.is_admin(signer, state)? {
        return Err(ApplyError::InvalidTransaction(format!(
            "Only admins can create a contract registry: {}",
            signer,
        )));
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
    admin_permissions: &dyn AdminPermission,
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
    can_update_contract_registry(contract_registry, signer, state, admin_permissions)?;

    state.delete_contract_registry(name)
}

fn update_contract_registry_owners(
    payload: UpdateContractRegistryOwnersAction,
    signer: &str,
    state: &mut SabreState,
    admin_permissions: &dyn AdminPermission,
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
    can_update_contract_registry(contract_registry.clone(), signer, state, admin_permissions)?;

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
    admin_permissions: &dyn AdminPermission,
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

    if !admin_permissions.is_admin(signer, state)? {
        return Err(ApplyError::InvalidTransaction(format!(
            "Only admins can create a namespace registry: {}",
            signer,
        )));
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
    admin_permissions: &dyn AdminPermission,
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
    can_update_namespace_registry(namespace_registry.clone(), signer, state, admin_permissions)?;

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
    admin_permissions: &dyn AdminPermission,
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
    can_update_namespace_registry(namespace_registry.clone(), signer, state, admin_permissions)?;
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
    admin_permissions: &dyn AdminPermission,
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
    can_update_namespace_registry(namespace_registry.clone(), signer, state, admin_permissions)?;

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
    admin_permissions: &dyn AdminPermission,
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
    can_update_namespace_registry(namespace_registry.clone(), signer, state, admin_permissions)?;

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

// helper function to check if the signer is allowed to update a namespace_registry
fn can_update_namespace_registry(
    namespace_registry: NamespaceRegistry,
    signer: &str,
    state: &mut SabreState,
    admin_permissions: &dyn AdminPermission,
) -> Result<(), ApplyError> {
    if !namespace_registry.owners().contains(&signer.into()) {
        match admin_permissions.is_admin(signer, state) {
            Ok(true) => (),
            Ok(false) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Only owners or admins can update or delete a namespace registry: {}",
                    signer,
                )));
            }
            Err(err) => {
                return Err(err);
            }
        };
    }
    Ok(())
}

// helper function to check if the signer is allowed to update a contract_registry
fn can_update_contract_registry(
    contract_registry: ContractRegistry,
    signer: &str,
    state: &mut SabreState,
    admin_permissions: &dyn AdminPermission,
) -> Result<(), ApplyError> {
    if !contract_registry.owners().contains(&signer.into()) {
        match admin_permissions.is_admin(signer, state) {
            Ok(true) => (),
            Ok(false) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Only owners or admins can update or delete a contract registry: {}",
                    signer,
                )));
            }
            Err(err) => {
                return Err(err);
            }
        };
    }
    Ok(())
}

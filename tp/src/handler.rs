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
use protobuf::RepeatedField;
use sawtooth_sdk::messages::processor::TpProcessRequest;
use sawtooth_sdk::processor::handler::ApplyError;
use sawtooth_sdk::processor::handler::TransactionContext;
use sawtooth_sdk::processor::handler::TransactionHandler;

use crate::payload::{Action, SabreRequestPayload};
use crate::protos::contract::Contract;
use crate::protos::contract_registry::{ContractRegistry, ContractRegistry_Version};
use crate::protos::namespace_registry::{NamespaceRegistry, NamespaceRegistry_Permission};
use crate::protos::payload::{
    CreateContractAction, CreateContractRegistryAction, CreateNamespaceRegistryAction,
    CreateNamespaceRegistryPermissionAction, CreateSmartPermissionAction, DeleteContractAction,
    DeleteContractRegistryAction, DeleteNamespaceRegistryAction,
    DeleteNamespaceRegistryPermissionAction, DeleteSmartPermissionAction, ExecuteContractAction,
    UpdateContractRegistryOwnersAction, UpdateNamespaceRegistryOwnersAction,
    UpdateSmartPermissionAction,
};
use crate::protos::smart_permission::SmartPermission;
use crate::state::SabreState;
use crate::wasm_executor::wasm_module::WasmModule;

/// The namespace registry prefix for global state (00ec00)
const NAMESPACE_REGISTRY_PREFIX: &'static str = "00ec00";

/// The contract registry prefix for global state (00ec01)
const CONTRACT_REGISTRY_PREFIX: &'static str = "00ec01";

/// The contract prefix for global state (00ec02)
const CONTRACT_PREFIX: &'static str = "00ec02";

pub struct SabreTransactionHandler {
    family_name: String,
    family_versions: Vec<String>,
    namespaces: Vec<String>,
}

impl SabreTransactionHandler {
    pub fn new() -> SabreTransactionHandler {
        SabreTransactionHandler {
            family_name: "sabre".into(),
            family_versions: vec!["0.0".into()],
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
        return self.family_name.clone();
    }

    fn family_versions(&self) -> Vec<String> {
        return self.family_versions.clone();
    }

    fn namespaces(&self) -> Vec<String> {
        return self.namespaces.clone();
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
    let name = payload.get_name();
    let version = payload.get_version();
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
    let mut contract_registry = match state.get_contract_registry(name) {
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

    if !contract_registry.owners.contains(&signer.into()) {
        return Err(ApplyError::InvalidTransaction(format!(
            "Only owners can submit new versions of contracts: {}",
            signer,
        )));
    }

    let mut contract = Contract::new();
    contract.set_name(name.into());
    contract.set_version(version.into());
    contract.set_inputs(RepeatedField::from_vec(payload.get_inputs().to_vec()));
    contract.set_outputs(RepeatedField::from_vec(payload.get_outputs().to_vec()));
    contract.set_creator(signer.into());
    contract.set_contract(payload.get_contract().to_vec());

    state.set_contract(name, version, contract)?;

    let mut sha = Sha512::new();
    sha.input(payload.get_contract());

    let mut contract_registry_version = ContractRegistry_Version::new();
    contract_registry_version.set_version(version.into());
    contract_registry_version.set_contract_sha512(sha.result_str().into());
    contract_registry_version.set_creator(signer.into());
    contract_registry.versions.push(contract_registry_version);

    state.set_contract_registry(name, contract_registry)
}

fn delete_contract(
    payload: DeleteContractAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let name = payload.get_name();
    let version = payload.get_version();

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
    let mut contract_registry = match state.get_contract_registry(name) {
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

    if !(contract_registry.owners.contains(&signer.into())) {
        return Err(ApplyError::InvalidTransaction(format!(
            "Signer is not an owner of this contract: {}",
            signer,
        )));
    }
    let versions = contract_registry.versions.clone();
    for (index, contract_registry_version) in versions.iter().enumerate() {
        if contract_registry_version.version == version {
            contract_registry.versions.remove(index);
            break;
        }
    }
    state.set_contract_registry(name, contract_registry)?;
    state.delete_contract(name, version)
}

fn execute_contract<'a>(
    payload: ExecuteContractAction,
    signer: &str,
    signature: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let name = payload.get_name();
    let version = payload.get_version();

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

    for input in payload.get_inputs() {
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
        for registry in registries.get_registries() {
            if input.starts_with(&registry.namespace) {
                namespace_registry = Some(registry)
            }
        }

        let mut permissioned = false;
        match namespace_registry {
            Some(registry) => {
                for permission in registry.get_permissions() {
                    if name == permission.contract_name && permission.read {
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

    for output in payload.get_outputs() {
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
        for registry in registries.get_registries() {
            if output.starts_with(&registry.namespace) {
                namespace_registry = Some(registry)
            }
        }
        let mut permissioned = false;
        match namespace_registry {
            Some(registry) => {
                for permission in registry.get_permissions() {
                    if name == permission.contract_name && permission.write {
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

    let mut module = WasmModule::new(contract.get_contract(), state.context())
        .expect("Failed to create can_add module");

    let result = module
        .entrypoint(
            payload.get_payload().to_vec(),
            signer.into(),
            signature.into(),
        )
        .map_err(|e| ApplyError::InvalidTransaction(format!("{:?}", e)))?;

    match result {
        None => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Wasm contract did not return a result: {}, {}",
                name, version,
            )));
        }
        Some(1) => Ok(()),
        Some(-3) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Wasm contract returned invalid transaction: {}, {}",
                name, version,
            )));
        }
        Some(num) => {
            return Err(ApplyError::InternalError(format!(
                "Wasm contract returned internal error: {}",
                num
            )));
        }
    }
}

fn create_contract_registry(
    payload: CreateContractRegistryAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let name = payload.get_name();

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
            let values = entry.value.split(",");
            let value_vec: Vec<&str> = values.collect();
            if !value_vec.contains(&signer) {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Only admins can create a contract registry: {}",
                    signer,
                )));
            }
        }
    }

    let mut contract_registry = ContractRegistry::new();
    contract_registry.set_name(name.into());
    contract_registry.set_owners(RepeatedField::from_vec(payload.get_owners().to_vec()));

    state.set_contract_registry(name, contract_registry)
}

fn delete_contract_registry(
    payload: DeleteContractRegistryAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let name = payload.get_name();
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

    if contract_registry.versions.len() != 0 {
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
    let name = payload.get_name();
    let mut contract_registry = match state.get_contract_registry(name) {
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

    contract_registry.set_owners(RepeatedField::from_vec(payload.get_owners().to_vec()));
    state.set_contract_registry(name, contract_registry)
}

fn create_namespace_registry(
    payload: CreateNamespaceRegistryAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let namespace = payload.get_namespace();

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
            let values = entry.value.split(",");
            let value_vec: Vec<&str> = values.collect();
            if !value_vec.contains(&signer) {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Only admins can create a namespace registry: {}",
                    signer,
                )));
            }
        }
    }

    let mut namespace_registry = NamespaceRegistry::new();
    namespace_registry.set_namespace(namespace.into());
    namespace_registry.set_owners(RepeatedField::from_vec(payload.get_owners().to_vec()));

    state.set_namespace_registry(namespace, namespace_registry)
}

fn delete_namespace_registry(
    payload: DeleteNamespaceRegistryAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let namespace = payload.get_namespace();

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

    if namespace_registry.permissions.len() != 0 {
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
    let namespace = payload.get_namespace();

    let mut namespace_registry = match state.get_namespace_registry(namespace) {
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

    namespace_registry.set_owners(RepeatedField::from_vec(payload.get_owners().to_vec()));
    state.set_namespace_registry(namespace, namespace_registry)
}

fn create_namespace_registry_permission(
    payload: CreateNamespaceRegistryPermissionAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let namespace = payload.get_namespace();
    let contract_name = payload.get_contract_name();
    let mut namespace_registry = match state.get_namespace_registry(namespace) {
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

    let mut new_permission = NamespaceRegistry_Permission::new();
    new_permission.set_contract_name(contract_name.into());
    new_permission.set_read(payload.get_read());
    new_permission.set_write(payload.get_write());

    // remove old permission for contract if one exists and replace with the new permission
    let permissions = namespace_registry.get_permissions().to_vec();
    let mut index = None;
    let mut count = 0;
    for permission in permissions.clone() {
        if permission.contract_name == contract_name {
            index = Some(count);
            break;
        }
        count = count + 1;
    }

    match index {
        Some(x) => {
            namespace_registry.permissions.remove(x);
        }
        None => (),
    };
    namespace_registry.permissions.push(new_permission);
    state.set_namespace_registry(namespace, namespace_registry)
}

fn delete_namespace_registry_permission(
    payload: DeleteNamespaceRegistryPermissionAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    let namespace = payload.get_namespace();
    let contract_name = payload.get_contract_name();

    let mut namespace_registry = match state.get_namespace_registry(namespace) {
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
    let permissions = namespace_registry.get_permissions().to_vec();
    let mut index = None;
    let mut count = 0;
    for permission in permissions.clone() {
        if permission.contract_name == contract_name {
            index = Some(count);
            break;
        }
        count = count + 1;
    }

    match index {
        Some(x) => {
            namespace_registry.permissions.remove(x);
        }
        None => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Namespace Registry does not have a permission for : {}",
                contract_name,
            )));
        }
    };
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

    if admin.get_org_id() != org_id {
        return Err(ApplyError::InvalidTransaction(format!(
            "Signer is not associated with the organization: {}",
            signer,
        )));
    }
    if !admin.roles.contains(&"admin".to_string()) {
        return Err(ApplyError::InvalidTransaction(format!(
            "Signer is not an admin: {}",
            signer,
        )));
    };

    if !admin.active {
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
    is_admin(signer, payload.get_org_id(), state)?;

    // Check if the smart permissions already exists
    match state.get_smart_permission(payload.get_org_id(), payload.get_name()) {
        Ok(None) => (),
        Ok(Some(_)) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Smart Permission already exists: {} ",
                payload.get_name(),
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
    match state.get_organization(payload.get_org_id()) {
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Organization does not exist exists: {}",
                payload.get_org_id(),
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

    let mut smart_permission = SmartPermission::new();
    smart_permission.set_org_id(payload.get_org_id().to_string());
    smart_permission.set_name(payload.get_name().to_string());
    smart_permission.set_function(payload.get_function().to_vec());
    state.set_smart_permission(payload.get_org_id(), payload.get_name(), smart_permission)
}

fn update_smart_permission(
    payload: UpdateSmartPermissionAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    // verify the signer of the transaction is authorized to update smart permissions
    is_admin(signer, payload.get_org_id(), state)?;

    // verify that the smart permission exists
    let mut smart_permission =
        match state.get_smart_permission(payload.get_org_id(), payload.get_name()) {
            Ok(None) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Smart Permission does not exists: {} ",
                    payload.get_name(),
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

    smart_permission.set_function(payload.get_function().to_vec());
    state.set_smart_permission(payload.get_org_id(), payload.get_name(), smart_permission)
}

fn delete_smart_permission(
    payload: DeleteSmartPermissionAction,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    // verify the signer of the transaction is authorized to delete smart permissions
    is_admin(signer, payload.get_org_id(), state)?;

    // verify that the smart permission exists
    match state.get_smart_permission(payload.get_org_id(), payload.get_name()) {
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Smart Permission does not exists: {} ",
                payload.get_name(),
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

    state.delete_smart_permission(payload.get_org_id(), payload.get_name())
}

// helper function to check if the signer is allowed to update a namespace_registry
fn can_update_namespace_registry(
    namespace_registry: NamespaceRegistry,
    signer: &str,
    state: &mut SabreState,
) -> Result<(), ApplyError> {
    if !namespace_registry.owners.contains(&signer.into()) {
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
                let values = entry.value.split(",");
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
    if !contract_registry.owners.contains(&signer.into()) {
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
                let values = entry.value.split(",");
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

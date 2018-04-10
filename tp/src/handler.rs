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
use protobuf;
use protobuf::RepeatedField;

use crypto::digest::Digest;
use crypto::sha2::Sha512;

use wasm_executor::wasm_module::WasmModule;

use sawtooth_sdk::processor::handler::ApplyError;
use sawtooth_sdk::processor::handler::TransactionContext;
use sawtooth_sdk::processor::handler::TransactionHandler;
use sawtooth_sdk::messages::processor::TpProcessRequest;
use sawtooth_sdk::messages::setting::Setting;

use addressing::{get_sawtooth_admins_address, make_contract_address,
                 make_contract_registry_address, make_namespace_registry_address};

use protos::contract::{Contract, ContractList};
use protos::contract_registry::{ContractRegistry, ContractRegistryList, ContractRegistry_Version};
use protos::namespace_registry::{NamespaceRegistry, NamespaceRegistryList,
                                 NamespaceRegistry_Permission};
use protos::payload::{CreateContractAction, CreateContractRegistryAction,
                      CreateNamespaceRegistryAction, CreateNamespaceRegistryPermissionAction,
                      DeleteContractAction, DeleteContractRegistryAction,
                      DeleteNamespaceRegistryAction, DeleteNamespaceRegistryPermissionAction,
                      ExecuteContractAction, SabrePayload, SabrePayload_Action,
                      UpdateContractRegistryOwnersAction, UpdateNamespaceRegistryOwnersAction};

/// The namespace registry prefix for global state (00ec00)
const NAMESPACE_REGISTRY_PREFIX: &'static str = "00ec00";

/// The contract registry prefix for global state (00ec01)
const CONTRACT_REGISTRY_PREFIX: &'static str = "00ec01";

/// The contract prefix for global state (00ec02)
const CONTRACT_PREFIX: &'static str = "00ec02";

#[derive(Debug, Clone)]
enum Action {
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
}

struct SabreRequestPayload {
    action: Action,
}

impl SabreRequestPayload {
    pub fn new(payload: &[u8]) -> Result<Option<SabreRequestPayload>, ApplyError> {
        let payload: SabrePayload = match protobuf::parse_from_bytes(payload) {
            Ok(payload) => payload,
            Err(_) => {
                return Err(ApplyError::InvalidTransaction(String::from(
                    "Cannot deserialize payload",
                )))
            }
        };

        let sabre_action = payload.get_action();
        let action = match sabre_action {
            SabrePayload_Action::CREATE_CONTRACT => {
                let create_contract = payload.get_create_contract();
                if create_contract.get_name().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract name cannot be an empty string",
                    )));
                }
                if create_contract.get_version().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract version cannot be an empty string",
                    )));
                }
                if create_contract.get_inputs().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract inputs cannot be an empty",
                    )));
                }
                if create_contract.get_outputs().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract outputs cannot be an empty",
                    )));
                }
                if create_contract.get_contract().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract bytes cannot be an empty",
                    )));
                }
                Action::CreateContract(create_contract.clone())
            }
            SabrePayload_Action::DELETE_CONTRACT => {
                let delete_contract = payload.get_delete_contract();
                if delete_contract.get_name().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract name cannot be an empty string",
                    )));
                }
                if delete_contract.get_version().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract version cannot be an empty string",
                    )));
                }
                Action::DeleteContract(delete_contract.clone())
            }
            SabrePayload_Action::EXECUTE_CONTRACT => {
                let execute_contract = payload.get_execute_contract();
                if execute_contract.get_name().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract name cannot be an empty string",
                    )));
                }
                if execute_contract.get_version().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract version cannot be an empty string",
                    )));
                }
                if execute_contract.get_inputs().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract inputs cannot be an empty",
                    )));
                }
                if execute_contract.get_outputs().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract outputs cannot be an empty",
                    )));
                }
                if execute_contract.get_payload().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract payload cannot be an empty",
                    )));
                }
                Action::ExecuteContract(execute_contract.clone())
            }
            SabrePayload_Action::CREATE_CONTRACT_REGISTRY => {
                let create_contract_registry = payload.get_create_contract_registry();
                if create_contract_registry.get_name().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract Registry name cannot be an empty string",
                    )));
                }
                if create_contract_registry.get_owners().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract Registry owners cannot be an empty",
                    )));
                }
                Action::CreateContractRegistry(create_contract_registry.clone())
            }
            SabrePayload_Action::DELETE_CONTRACT_REGISTRY => {
                let delete_contract_registry = payload.get_delete_contract_registry();
                if delete_contract_registry.get_name().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract Registry name cannot be an empty string",
                    )));
                };
                Action::DeleteContractRegistry(delete_contract_registry.clone())
            }
            SabrePayload_Action::UPDATE_CONTRACT_REGISTRY_OWNERS => {
                let update_contract_registry_owners = payload.get_update_contract_registry_owners();
                if update_contract_registry_owners.get_name().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract Registry name cannot be an empty string",
                    )));
                }
                if update_contract_registry_owners.get_owners().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract Registry owners cannot be an empty",
                    )));
                }
                Action::UpdateContractRegistryOwners(update_contract_registry_owners.clone())
            }
            SabrePayload_Action::CREATE_NAMESPACE_REGISTRY => {
                let create_namespace_registry = payload.get_create_namespace_registry();
                if create_namespace_registry.get_namespace().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Namespace Registry namespace cannot be an empty string",
                    )));
                }
                if create_namespace_registry.get_owners().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Namespace owners cannot be an empty",
                    )));
                }
                Action::CreateNamespaceRegistry(create_namespace_registry.clone())
            }
            SabrePayload_Action::DELETE_NAMESPACE_REGISTRY => {
                let delete_namespace_registry = payload.get_delete_namespace_registry();
                if delete_namespace_registry.get_namespace().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Namespace Registry namespace cannot be an empty string",
                    )));
                }
                Action::DeleteNamespaceRegistry(delete_namespace_registry.clone())
            }
            SabrePayload_Action::UPDATE_NAMESPACE_REGISTRY_OWNERS => {
                let update_namespace_registry_owners =
                    payload.get_update_namespace_registry_owners();
                if update_namespace_registry_owners.get_namespace().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Namespace Registry namespace cannot be an empty string",
                    )));
                }
                if update_namespace_registry_owners.get_owners().is_empty() {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Namespace owners cannot be an empty",
                    )));
                }
                Action::UpdateNamespaceRegistryOwners(update_namespace_registry_owners.clone())
            }
            SabrePayload_Action::CREATE_NAMESPACE_REGISTRY_PERMISSION => {
                let create_namespace_registry_permission =
                    payload.get_create_namespace_registry_permission();
                if create_namespace_registry_permission
                    .get_namespace()
                    .is_empty()
                {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Namespace Registry namespace cannot be an empty string",
                    )));
                }
                if create_namespace_registry_permission
                    .get_contract_name()
                    .is_empty()
                {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract name cannot be an empty string",
                    )));
                }
                Action::CreateNamespaceRegistryPermission(
                    create_namespace_registry_permission.clone(),
                )
            }
            SabrePayload_Action::DELETE_NAMESPACE_REGISTRY_PERMISSION => {
                let delete_namespace_registry_permission =
                    payload.get_delete_namespace_registry_permission();
                if delete_namespace_registry_permission
                    .get_namespace()
                    .is_empty()
                {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Namespace Registry namespace cannot be an empty string",
                    )));
                }
                if delete_namespace_registry_permission
                    .get_contract_name()
                    .is_empty()
                {
                    return Err(ApplyError::InvalidTransaction(String::from(
                        "Contract name cannot be an empty string",
                    )));
                }
                Action::DeleteNamespaceRegistryPermission(
                    delete_namespace_registry_permission.clone(),
                )
            }
            SabrePayload_Action::ACTION_UNSET => {
                return Err(ApplyError::InvalidTransaction(String::from(
                    "Action is not set",
                )));
            }
        };
        Ok(Some(SabreRequestPayload { action: action }))
    }

    pub fn get_action(&self) -> Action {
        self.action.clone()
    }
}

pub struct SabreState<'a> {
    context: &'a mut TransactionContext,
}

impl<'a> SabreState<'a> {
    pub fn new(context: &'a mut TransactionContext) -> SabreState {
        SabreState { context: context }
    }

    pub fn get_admin_setting(&mut self) -> Result<Option<Setting>, ApplyError> {
        let address = get_sawtooth_admins_address()?;
        let d = self.context.get_state(&address)?;
        match d {
            Some(packed) => {
                let setting: Setting = match protobuf::parse_from_bytes(packed.as_slice()) {
                    Ok(setting) => setting,
                    Err(err) => {
                        return Err(ApplyError::InternalError(format!(
                            "Cannot deserialize setting: {:?}",
                            err,
                        )))
                    }
                };
                Ok(Some(setting))
            }
            None => Ok(None),
        }
    }

    pub fn get_contract(
        &mut self,
        name: &str,
        version: &str,
    ) -> Result<Option<Contract>, ApplyError> {
        let address = make_contract_address(name, version)?;
        let d = self.context.get_state(&address)?;
        match d {
            Some(packed) => {
                let contracts: ContractList = match protobuf::parse_from_bytes(packed.as_slice()) {
                    Ok(contracts) => contracts,
                    Err(err) => {
                        return Err(ApplyError::InternalError(format!(
                            "Cannot deserialize contract list: {:?}",
                            err,
                        )))
                    }
                };

                for contract in contracts.get_contracts() {
                    if contract.name == name {
                        return Ok(Some(contract.clone()));
                    }
                }
                Ok(None)
            }
            None => Ok(None),
        }
    }

    pub fn set_contract(
        &mut self,
        name: &str,
        version: &str,
        new_contract: Contract,
    ) -> Result<(), ApplyError> {
        let address = make_contract_address(name, version)?;
        let d = self.context.get_state(&address)?;
        let mut contract_list = match d {
            Some(packed) => match protobuf::parse_from_bytes(packed.as_slice()) {
                Ok(contracts) => contracts,
                Err(err) => {
                    return Err(ApplyError::InternalError(format!(
                        "Cannot deserialize contract list: {}",
                        err,
                    )))
                }
            },
            None => ContractList::new(),
        };
        // remove old contract if it exists and sort the contracts by name
        let contracts = contract_list.get_contracts().to_vec();
        let mut index = None;
        let mut count = 0;
        for contract in contracts.clone() {
            if contract.name == name {
                index = Some(count);
                break;
            }
            count = count + 1;
        }

        match index {
            Some(x) => {
                contract_list.contracts.remove(x);
            }
            None => (),
        };
        contract_list.contracts.push(new_contract);
        contract_list.contracts.sort_by_key(|c| c.clone().name);
        let serialized = match protobuf::Message::write_to_bytes(&contract_list) {
            Ok(serialized) => serialized,
            Err(_) => {
                return Err(ApplyError::InternalError(String::from(
                    "Cannot serialize contract list",
                )))
            }
        };
        self.context
            .set_state(&address, serialized.as_ref())
            .map_err(|err| ApplyError::InternalError(format!("{}", err)))?;
        Ok(())
    }

    pub fn delete_contract(&mut self, name: &str, version: &str) -> Result<(), ApplyError> {
        let address = make_contract_address(name, version)?;
        let d = self.context.delete_state(vec![address.clone()])?;
        let deleted: Vec<String> = match d {
            Some(deleted) => deleted.to_vec(),
            None => {
                return Err(ApplyError::InternalError(String::from(
                    "Cannot delete contract",
                )))
            }
        };
        if !deleted.contains(&address) {
            return Err(ApplyError::InternalError(String::from(
                "Cannot delete contract",
            )));
        };
        Ok(())
    }

    pub fn get_contract_registry(
        &mut self,
        name: &str,
    ) -> Result<Option<ContractRegistry>, ApplyError> {
        let address = make_contract_registry_address(name)?;
        let d = self.context.get_state(&address)?;
        match d {
            Some(packed) => {
                let contract_registries: ContractRegistryList =
                    match protobuf::parse_from_bytes(packed.as_slice()) {
                        Ok(contract_registries) => contract_registries,
                        Err(err) => {
                            return Err(ApplyError::InternalError(format!(
                                "Cannot deserialize contract registry list: {:?}",
                                err,
                            )))
                        }
                    };

                for contract_registry in contract_registries.get_registries() {
                    if contract_registry.name == name {
                        return Ok(Some(contract_registry.clone()));
                    }
                }
                Ok(None)
            }
            None => Ok(None),
        }
    }

    pub fn set_contract_registry(
        &mut self,
        name: &str,
        new_contract_registry: ContractRegistry,
    ) -> Result<(), ApplyError> {
        let address = make_contract_registry_address(name)?;
        let d = self.context.get_state(&address)?;
        let mut contract_registry_list = match d {
            Some(packed) => match protobuf::parse_from_bytes(packed.as_slice()) {
                Ok(contract_registries) => contract_registries,
                Err(err) => {
                    return Err(ApplyError::InternalError(format!(
                        "Cannot deserialize contract registry list: {}",
                        err,
                    )))
                }
            },
            None => ContractRegistryList::new(),
        };
        // remove old agent if it exists and sort the contract regisitries by name
        let contract_registries = contract_registry_list.get_registries().to_vec();
        let mut index = None;
        let mut count = 0;
        for contract_registry in contract_registries.clone() {
            if contract_registry.name == name {
                index = Some(count);
                break;
            }
            count = count + 1;
        }

        match index {
            Some(x) => {
                contract_registry_list.registries.remove(x);
            }
            None => (),
        };
        contract_registry_list
            .registries
            .push(new_contract_registry);
        contract_registry_list
            .registries
            .sort_by_key(|c| c.clone().name);
        let serialized = match protobuf::Message::write_to_bytes(&contract_registry_list) {
            Ok(serialized) => serialized,
            Err(_) => {
                return Err(ApplyError::InternalError(String::from(
                    "Cannot serialize contract registry list",
                )))
            }
        };
        self.context
            .set_state(&address, serialized.as_ref())
            .map_err(|err| ApplyError::InternalError(format!("{}", err)))?;
        Ok(())
    }

    pub fn delete_contract_registry(&mut self, name: &str) -> Result<(), ApplyError> {
        let address = make_contract_registry_address(name)?;
        let d = self.context.delete_state(vec![address.clone()])?;
        let deleted: Vec<String> = match d {
            Some(deleted) => deleted.to_vec(),
            None => {
                return Err(ApplyError::InternalError(String::from(
                    "Cannot delete contract registry",
                )))
            }
        };
        if !deleted.contains(&address) {
            return Err(ApplyError::InternalError(String::from(
                "Cannot delete contract registry",
            )));
        };
        Ok(())
    }

    pub fn get_namespace_registry(
        &mut self,
        namespace: &str,
    ) -> Result<Option<NamespaceRegistry>, ApplyError> {
        let address = make_namespace_registry_address(namespace)?;
        let d = self.context.get_state(&address)?;
        match d {
            Some(packed) => {
                let namespace_registries: NamespaceRegistryList =
                    match protobuf::parse_from_bytes(packed.as_slice()) {
                        Ok(namespace_registries) => namespace_registries,
                        Err(err) => {
                            return Err(ApplyError::InternalError(format!(
                                "Cannot deserialize namespace registry list: {:?}",
                                err,
                            )))
                        }
                    };

                for namespace_registry in namespace_registries.get_registries() {
                    if namespace_registry.namespace == namespace {
                        return Ok(Some(namespace_registry.clone()));
                    }
                }
                Ok(None)
            }
            None => Ok(None),
        }
    }

    pub fn get_namespace_registries(
        &mut self,
        namespace: &str,
    ) -> Result<Option<NamespaceRegistryList>, ApplyError> {
        let address = make_namespace_registry_address(namespace)?;
        let d = self.context.get_state(&address)?;
        match d {
            Some(packed) => {
                let namespace_registries: NamespaceRegistryList =
                    match protobuf::parse_from_bytes(packed.as_slice()) {
                        Ok(namespace_registries) => namespace_registries,
                        Err(err) => {
                            return Err(ApplyError::InternalError(format!(
                                "Cannot deserialize namespace registry list: {:?}",
                                err,
                            )))
                        }
                    };
                Ok(Some(namespace_registries))
            }
            None => Ok(None),
        }
    }

    pub fn set_namespace_registry(
        &mut self,
        namespace: &str,
        new_namespace_registry: NamespaceRegistry,
    ) -> Result<(), ApplyError> {
        let address = make_namespace_registry_address(namespace)?;
        let d = self.context.get_state(&address)?;
        let mut namespace_registry_list = match d {
            Some(packed) => match protobuf::parse_from_bytes(packed.as_slice()) {
                Ok(namespace_registries) => namespace_registries,
                Err(err) => {
                    return Err(ApplyError::InternalError(format!(
                        "Cannot deserialize namespace registry list: {}",
                        err,
                    )))
                }
            },
            None => NamespaceRegistryList::new(),
        };
        // remove old agent if it exists and sort the namespace regisitries by namespace
        let namespace_registries = namespace_registry_list.get_registries().to_vec();
        let mut index = None;
        let mut count = 0;
        for namespace_registry in namespace_registries.clone() {
            if namespace_registry.namespace == namespace {
                index = Some(count);
                break;
            }
            count = count + 1;
        }

        match index {
            Some(x) => {
                namespace_registry_list.registries.remove(x);
            }
            None => (),
        };
        namespace_registry_list
            .registries
            .push(new_namespace_registry);
        namespace_registry_list
            .registries
            .sort_by_key(|nr| nr.clone().namespace);
        let serialized = match protobuf::Message::write_to_bytes(&namespace_registry_list) {
            Ok(serialized) => serialized,
            Err(_) => {
                return Err(ApplyError::InternalError(String::from(
                    "Cannot serialize namespace registry list",
                )))
            }
        };
        self.context
            .set_state(&address, serialized.as_ref())
            .map_err(|err| ApplyError::InternalError(format!("{}", err)))?;
        Ok(())
    }

    pub fn delete_namespace_registry(&mut self, namespace: &str) -> Result<(), ApplyError> {
        let address = make_namespace_registry_address(namespace)?;
        let d = self.context.delete_state(vec![address.clone()])?;
        let deleted: Vec<String> = match d {
            Some(deleted) => deleted.to_vec(),
            None => {
                return Err(ApplyError::InternalError(String::from(
                    "Cannot delete namespace registry",
                )))
            }
        };
        if !deleted.contains(&address) {
            return Err(ApplyError::InternalError(String::from(
                "Cannot delete namespace registry",
            )));
        };
        Ok(())
    }
}

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
        context: &mut TransactionContext,
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
                )))
            }
        };

        let signer = request.get_header().get_signer_public_key();
        let context_clone = context.clone();
        let mut state = SabreState::new(context);

        info!(
            "{:?} {:?} {:?}",
            payload.get_action(),
            request.get_header().get_inputs(),
            request.get_header().get_outputs()
        );

        match payload.action {
            Action::CreateContract(create_contract_payload) => {
                create_contract(create_contract_payload, signer, &mut state)
            }
            Action::DeleteContract(delete_contract_payload) => {
                delete_contract(delete_contract_payload, signer, &mut state)
            }
            Action::ExecuteContract(execute_contract_payload) => {
                execute_contract(execute_contract_payload, signer, &mut state, context_clone)
            }
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
            )))
        }
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )))
        }
    };

    let mut contract = Contract::new();
    contract.set_name(name.into());
    contract.set_version(version.into());
    contract.set_inputs(RepeatedField::from_vec(payload.get_inputs().to_vec()));
    contract.set_outputs(RepeatedField::from_vec(payload.get_outputs().to_vec()));
    contract.set_creator(signer.into());
    contract.set_contract(payload.get_contract().to_vec());

    state.set_contract(name, version, contract)?;

    // update or create the contract registry for the contract
    let mut contract_registry = match state.get_contract_registry(name) {
        Ok(None) => {
            let mut contract_registry = ContractRegistry::new();
            contract_registry.set_name(name.into());
            contract_registry
        }
        Ok(Some(contract_registry)) => contract_registry,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )))
        }
    };

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
            )))
        }
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )))
        }
    };

    // update the contract registry for the contract
    let mut contract_registry = match state.get_contract_registry(name) {
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Contract Registry does not exist {}",
                name,
            )))
        }
        Ok(Some(contract_registry)) => contract_registry,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )))
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

fn execute_contract(
    payload: ExecuteContractAction,
    signer: &str,
    state: &mut SabreState,
    context: TransactionContext,
) -> Result<(), ApplyError> {
    let name = payload.get_name();
    let version = payload.get_version();

    let contract = match state.get_contract(name, version) {
        Ok(Some(contract)) => contract,
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Contract does not exist: {}, {}",
                name, version,
            )))
        }
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )))
        }
    };

    for input in payload.get_inputs() {
        let namespace = match input.get(..6) {
            Some(namespace) => namespace,
            None => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Input must have at least 6 characters: {}",
                    input,
                )))
            }
        };
        let registries = match state.get_namespace_registries(namespace) {
            Ok(Some(registries)) => registries,
            Ok(None) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Namespace Registry does not exist: {}",
                    namespace,
                )))
            }
            Err(err) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Unable to check state: {}",
                    err,
                )))
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
                )))
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
                )))
            }
        };
        let registries = match state.get_namespace_registries(namespace) {
            Ok(Some(registries)) => registries,
            Ok(None) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Namespace Registry does not exist: {}",
                    namespace,
                )))
            }
            Err(err) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Unable to check state: {}",
                    err,
                )))
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
                )))
            }
        }
    }

    let module =
        WasmModule::new(contract.get_contract(), context).expect("Failed to create can_add module");

    let result = module
        .entrypoint(payload.get_payload().to_vec(), signer.into())
        .map_err(|e| ApplyError::InvalidTransaction(format!("{:?}", e)))?;

    match result {
        None => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Wasm contract did not return a result: {}, {}",
                name, version,
            )))
        }
        Some(1) => Ok(()),
        Some(-3) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Wasm contract returned invalid transaction: {}, {}",
                name, version,
            )))
        }
        Some(num) => {
            return Err(ApplyError::InternalError(format!(
                "Wasm contract returned internal error: {}",
                num
            )))
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
            )))
        }
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )))
        }
    };

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
            )))
        }
        Ok(Some(contract_registry)) => contract_registry,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )))
        }
    };

    if !(contract_registry.owners.contains(&signer.into())) {
        return Err(ApplyError::InvalidTransaction(format!(
            "Signer must be an owner to delete a contract registry: {}",
            signer,
        )));
    }

    if contract_registry.versions.len() != 0 {
        return Err(ApplyError::InvalidTransaction(format!(
            "Contract Registry can only be deleted if there are no versions: {}",
            name,
        )));
    }

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
            )))
        }
        Ok(Some(contract_registry)) => contract_registry,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )))
        }
    };

    if !(contract_registry.owners.contains(&signer.into())) {
        return Err(ApplyError::InvalidTransaction(format!(
            "Signer must be an owner to update the owners of the contract: {}",
            signer,
        )));
    }

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
            )))
        }
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )))
        }
    }

    let setting = match state.get_admin_setting() {
        Ok(Some(setting)) => setting,
        Ok(None) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Only owners or admins can update or delete a namespace registry: {}",
                signer,
            )))
        }
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )))
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
            )))
        }
        Ok(Some(namespace_registry)) => namespace_registry,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )))
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
            )))
        }
        Ok(Some(namespace_registry)) => namespace_registry,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )))
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
            )))
        }
        Ok(Some(namespace_registry)) => namespace_registry,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )))
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
            )))
        }
        Ok(Some(namespace_registry)) => namespace_registry,
        Err(err) => {
            return Err(ApplyError::InvalidTransaction(format!(
                "Unable to check state: {}",
                err,
            )))
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
            )))
        }
    };
    state.set_namespace_registry(namespace, namespace_registry)
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
                )))
            }
            Err(err) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Unable to check state: {}",
                    err,
                )))
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

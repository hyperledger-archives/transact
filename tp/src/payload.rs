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
use sawtooth_sdk::processor::handler::ApplyError;

use crate::protos::payload::{
    CreateContractAction, CreateContractRegistryAction, CreateNamespaceRegistryAction,
    CreateNamespaceRegistryPermissionAction, CreateSmartPermissionAction, DeleteContractAction,
    DeleteContractRegistryAction, DeleteNamespaceRegistryAction,
    DeleteNamespaceRegistryPermissionAction, DeleteSmartPermissionAction, ExecuteContractAction,
    SabrePayload, SabrePayload_Action, UpdateContractRegistryOwnersAction,
    UpdateNamespaceRegistryOwnersAction, UpdateSmartPermissionAction,
};

#[derive(Debug, Clone)]
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

pub struct SabreRequestPayload {
    action: Action,
}

impl SabreRequestPayload {
    pub fn new(payload: &[u8]) -> Result<Option<SabreRequestPayload>, ApplyError> {
        let payload: SabrePayload = match protobuf::parse_from_bytes(payload) {
            Ok(payload) => payload,
            Err(_) => {
                return Err(ApplyError::InvalidTransaction(String::from(
                    "Cannot deserialize payload",
                )));
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
            SabrePayload_Action::CREATE_SMART_PERMISSION => {
                let action = payload.get_create_smart_permission();

                if action.get_org_id().is_empty() {
                    return Err(ApplyError::InvalidTransaction(
                        "Organization ID required".into(),
                    ));
                }

                if action.get_name().is_empty() {
                    return Err(ApplyError::InvalidTransaction(
                        "Smart permission name required".into(),
                    ));
                }

                if action.get_function().is_empty() {
                    return Err(ApplyError::InvalidTransaction(
                        "Function body required".into(),
                    ));
                }

                Action::CreateSmartPermission(action.clone())
            }
            SabrePayload_Action::UPDATE_SMART_PERMISSION => {
                let action = payload.get_update_smart_permission();

                if action.get_org_id().is_empty() {
                    return Err(ApplyError::InvalidTransaction(
                        "Organization ID required".into(),
                    ));
                }

                if action.get_name().is_empty() {
                    return Err(ApplyError::InvalidTransaction(
                        "Smart permission name required".into(),
                    ));
                }

                if action.get_function().is_empty() {
                    return Err(ApplyError::InvalidTransaction(
                        "Function body required".into(),
                    ));
                }

                Action::UpdateSmartPermission(action.clone())
            }
            SabrePayload_Action::DELETE_SMART_PERMISSION => {
                let action = payload.get_delete_smart_permission();

                if action.get_org_id().is_empty() {
                    return Err(ApplyError::InvalidTransaction(
                        "Organization ID required".into(),
                    ));
                }

                if action.get_name().is_empty() {
                    return Err(ApplyError::InvalidTransaction(
                        "Smart permission name required".into(),
                    ));
                }

                Action::DeleteSmartPermission(action.clone())
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

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
use sawtooth_sdk::messages::setting::Setting;
use sawtooth_sdk::processor::handler::ApplyError;
use sawtooth_sdk::processor::handler::TransactionContext;

use addressing::{
    compute_agent_address, compute_org_address, compute_smart_permission_address,
    get_sawtooth_admins_address, make_contract_address, make_contract_registry_address,
    make_namespace_registry_address,
};

use protos::contract::{Contract, ContractList};
use protos::contract_registry::{ContractRegistry, ContractRegistryList};
use protos::namespace_registry::{NamespaceRegistry, NamespaceRegistryList};
use protos::smart_permission::{
    Agent, AgentList, Organization, OrganizationList, SmartPermission, SmartPermissionList,
};

pub struct SabreState<'a> {
    context: &'a mut dyn TransactionContext,
}

impl<'a> SabreState<'a> {
    pub fn new(context: &'a mut dyn TransactionContext) -> SabreState {
        SabreState { context: context }
    }

    pub fn context(&mut self) -> &mut dyn TransactionContext {
        self.context
    }

    pub fn get_admin_setting(&mut self) -> Result<Option<Setting>, ApplyError> {
        let address = get_sawtooth_admins_address()?;
        let d = self.context.get_state_entry(&address)?;
        match d {
            Some(packed) => {
                let setting: Setting = match protobuf::parse_from_bytes(packed.as_slice()) {
                    Ok(setting) => setting,
                    Err(err) => {
                        return Err(ApplyError::InternalError(format!(
                            "Cannot deserialize setting: {:?}",
                            err,
                        )));
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
        let d = self.context.get_state_entry(&address)?;
        match d {
            Some(packed) => {
                let contracts: ContractList = match protobuf::parse_from_bytes(packed.as_slice()) {
                    Ok(contracts) => contracts,
                    Err(err) => {
                        return Err(ApplyError::InternalError(format!(
                            "Cannot deserialize contract list: {:?}",
                            err,
                        )));
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
        let d = self.context.get_state_entry(&address)?;
        let mut contract_list = match d {
            Some(packed) => match protobuf::parse_from_bytes(packed.as_slice()) {
                Ok(contracts) => contracts,
                Err(err) => {
                    return Err(ApplyError::InternalError(format!(
                        "Cannot deserialize contract list: {}",
                        err,
                    )));
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
                )));
            }
        };
        self.context
            .set_state_entry(address, serialized)
            .map_err(|err| ApplyError::InternalError(format!("{}", err)))?;
        Ok(())
    }

    pub fn delete_contract(&mut self, name: &str, version: &str) -> Result<(), ApplyError> {
        let address = make_contract_address(name, version)?;
        let d = self.context.delete_state_entry(&address)?;
        let deleted = match d {
            Some(deleted) => deleted,
            None => {
                return Err(ApplyError::InternalError(String::from(
                    "Cannot delete contract",
                )));
            }
        };
        if deleted != address {
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
        let d = self.context.get_state_entry(&address)?;
        match d {
            Some(packed) => {
                let contract_registries: ContractRegistryList =
                    match protobuf::parse_from_bytes(packed.as_slice()) {
                        Ok(contract_registries) => contract_registries,
                        Err(err) => {
                            return Err(ApplyError::InternalError(format!(
                                "Cannot deserialize contract registry list: {:?}",
                                err,
                            )));
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
        let d = self.context.get_state_entry(&address)?;
        let mut contract_registry_list = match d {
            Some(packed) => match protobuf::parse_from_bytes(packed.as_slice()) {
                Ok(contract_registries) => contract_registries,
                Err(err) => {
                    return Err(ApplyError::InternalError(format!(
                        "Cannot deserialize contract registry list: {}",
                        err,
                    )));
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
                )));
            }
        };
        self.context
            .set_state_entry(address, serialized)
            .map_err(|err| ApplyError::InternalError(format!("{}", err)))?;
        Ok(())
    }

    pub fn delete_contract_registry(&mut self, name: &str) -> Result<(), ApplyError> {
        let address = make_contract_registry_address(name)?;
        let d = self.context.delete_state_entry(&address)?;
        let deleted = match d {
            Some(deleted) => deleted,
            None => {
                return Err(ApplyError::InternalError(String::from(
                    "Cannot delete contract registry",
                )));
            }
        };
        if deleted != address {
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
        let d = self.context.get_state_entry(&address)?;
        match d {
            Some(packed) => {
                let namespace_registries: NamespaceRegistryList =
                    match protobuf::parse_from_bytes(packed.as_slice()) {
                        Ok(namespace_registries) => namespace_registries,
                        Err(err) => {
                            return Err(ApplyError::InternalError(format!(
                                "Cannot deserialize namespace registry list: {:?}",
                                err,
                            )));
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
        let d = self.context.get_state_entry(&address)?;
        match d {
            Some(packed) => {
                let namespace_registries: NamespaceRegistryList =
                    match protobuf::parse_from_bytes(packed.as_slice()) {
                        Ok(namespace_registries) => namespace_registries,
                        Err(err) => {
                            return Err(ApplyError::InternalError(format!(
                                "Cannot deserialize namespace registry list: {:?}",
                                err,
                            )));
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
        let d = self.context.get_state_entry(&address)?;
        let mut namespace_registry_list = match d {
            Some(packed) => match protobuf::parse_from_bytes(packed.as_slice()) {
                Ok(namespace_registries) => namespace_registries,
                Err(err) => {
                    return Err(ApplyError::InternalError(format!(
                        "Cannot deserialize namespace registry list: {}",
                        err,
                    )));
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
                )));
            }
        };
        self.context
            .set_state_entry(address, serialized)
            .map_err(|err| ApplyError::InternalError(format!("{}", err)))?;
        Ok(())
    }

    pub fn delete_namespace_registry(&mut self, namespace: &str) -> Result<(), ApplyError> {
        let address = make_namespace_registry_address(namespace)?;
        let d = self.context.delete_state_entry(&address)?;
        let deleted = match d {
            Some(deleted) => deleted,
            None => {
                return Err(ApplyError::InternalError(String::from(
                    "Cannot delete namespace registry",
                )));
            }
        };
        if deleted != address {
            return Err(ApplyError::InternalError(String::from(
                "Cannot delete namespace registry",
            )));
        };
        Ok(())
    }

    pub fn get_smart_permission(
        &mut self,
        org_id: &str,
        name: &str,
    ) -> Result<Option<SmartPermission>, ApplyError> {
        let address = compute_smart_permission_address(org_id, name);
        let d = self.context.get_state_entry(&address)?;
        match d {
            Some(packed) => {
                let smart_permissions: SmartPermissionList =
                    match protobuf::parse_from_bytes(packed.as_slice()) {
                        Ok(smart_permissions) => smart_permissions,
                        Err(err) => {
                            return Err(ApplyError::InternalError(format!(
                                "Cannot deserialize smart permission list: {:?}",
                                err,
                            )));
                        }
                    };

                for smart_permission in smart_permissions.get_smart_permissions() {
                    if smart_permission.name == name {
                        return Ok(Some(smart_permission.clone()));
                    }
                }
                Ok(None)
            }
            None => Ok(None),
        }
    }

    pub fn set_smart_permission(
        &mut self,
        org_id: &str,
        name: &str,
        new_smart_permission: SmartPermission,
    ) -> Result<(), ApplyError> {
        let address = compute_smart_permission_address(org_id, name);
        let d = self.context.get_state_entry(&address)?;
        let mut smart_permission_list = match d {
            Some(packed) => match protobuf::parse_from_bytes(packed.as_slice()) {
                Ok(smart_permissions) => smart_permissions,
                Err(err) => {
                    return Err(ApplyError::InternalError(format!(
                        "Cannot deserialize smart permission list: {}",
                        err,
                    )));
                }
            },
            None => SmartPermissionList::new(),
        };
        // remove old smart_permission if it exists and sort the smart_permission by name
        let smart_permissions = smart_permission_list.get_smart_permissions().to_vec();
        let mut index = None;
        let mut count = 0;
        for smart_permission in smart_permissions.clone() {
            if smart_permission.name == name {
                index = Some(count);
                break;
            }
            count = count + 1;
        }

        match index {
            Some(x) => {
                smart_permission_list.smart_permissions.remove(x);
            }
            None => (),
        };
        smart_permission_list
            .smart_permissions
            .push(new_smart_permission);
        smart_permission_list
            .smart_permissions
            .sort_by_key(|sp| sp.clone().name);
        let serialized = match protobuf::Message::write_to_bytes(&smart_permission_list) {
            Ok(serialized) => serialized,
            Err(_) => {
                return Err(ApplyError::InternalError(String::from(
                    "Cannot serialize smart permission list",
                )));
            }
        };
        self.context
            .set_state_entry(address, serialized)
            .map_err(|err| ApplyError::InternalError(format!("{}", err)))?;
        Ok(())
    }

    pub fn delete_smart_permission(&mut self, org_id: &str, name: &str) -> Result<(), ApplyError> {
        let address = compute_smart_permission_address(org_id, name);
        let d = self.context.delete_state_entry(&address.clone())?;
        let deleted = match d {
            Some(deleted) => deleted,
            None => {
                return Err(ApplyError::InternalError(String::from(
                    "Cannot delete smart_permission",
                )));
            }
        };
        if deleted != address {
            return Err(ApplyError::InternalError(String::from(
                "Cannot delete smart_permission",
            )));
        };
        Ok(())
    }

    pub fn get_organization(&mut self, id: &str) -> Result<Option<Organization>, ApplyError> {
        let address = compute_org_address(id);
        let d = self.context.get_state_entry(&address)?;
        match d {
            Some(packed) => {
                let orgs: OrganizationList = match protobuf::parse_from_bytes(packed.as_slice()) {
                    Ok(orgs) => orgs,
                    Err(err) => {
                        return Err(ApplyError::InternalError(format!(
                            "Cannot deserialize organization list: {:?}",
                            err,
                        )));
                    }
                };

                for org in orgs.get_organizations() {
                    if org.org_id == id {
                        return Ok(Some(org.clone()));
                    }
                }
                Ok(None)
            }
            None => Ok(None),
        }
    }

    pub fn get_agent(&mut self, public_key: &str) -> Result<Option<Agent>, ApplyError> {
        let address = compute_agent_address(public_key);
        let d = self.context.get_state_entry(&address)?;
        match d {
            Some(packed) => {
                let agents: AgentList = match protobuf::parse_from_bytes(packed.as_slice()) {
                    Ok(agents) => agents,
                    Err(err) => {
                        return Err(ApplyError::InternalError(format!(
                            "Cannot deserialize record container: {:?}",
                            err,
                        )));
                    }
                };

                for agent in agents.get_agents() {
                    if agent.public_key == public_key {
                        return Ok(Some(agent.clone()));
                    }
                }
                Ok(None)
            }
            None => Ok(None),
        }
    }
}

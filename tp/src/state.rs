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
use sabre_sdk::protocol::pike::state::{Agent, AgentList, Organization, OrganizationList};
use sabre_sdk::protocol::state::{
    Contract, ContractList, ContractListBuilder, ContractRegistry, ContractRegistryList,
    ContractRegistryListBuilder, NamespaceRegistry, NamespaceRegistryList,
    NamespaceRegistryListBuilder, SmartPermission, SmartPermissionList, SmartPermissionListBuilder,
};
use sabre_sdk::protocol::ADMINISTRATORS_SETTING_ADDRESS;
use sabre_sdk::protos::{FromBytes, IntoBytes};
use sawtooth_sdk::messages::setting::Setting;
use sawtooth_sdk::processor::handler::ApplyError;
use sawtooth_sdk::processor::handler::TransactionContext;

use crate::addressing::{
    compute_agent_address, compute_org_address, compute_smart_permission_address,
    make_contract_address, make_contract_registry_address, make_namespace_registry_address,
};

pub struct SabreState<'a> {
    context: &'a mut dyn TransactionContext,
}

impl<'a> SabreState<'a> {
    pub fn new(context: &'a mut dyn TransactionContext) -> SabreState {
        SabreState { context }
    }

    pub fn context(&mut self) -> &mut dyn TransactionContext {
        self.context
    }

    pub fn get_admin_setting(&mut self) -> Result<Option<Setting>, ApplyError> {
        let d = self
            .context
            .get_state_entry(ADMINISTRATORS_SETTING_ADDRESS)?;
        match d {
            Some(packed) => {
                let setting: Setting =
                    protobuf::parse_from_bytes(packed.as_slice()).map_err(|err| {
                        ApplyError::InvalidTransaction(format!(
                            "Cannot deserialize setting: {:?}",
                            err,
                        ))
                    })?;

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
                let contracts = ContractList::from_bytes(packed.as_slice()).map_err(|err| {
                    ApplyError::InvalidTransaction(format!(
                        "Cannot deserialize contract list: {:?}",
                        err,
                    ))
                })?;
                Ok(contracts
                    .contracts()
                    .iter()
                    .find(|c| c.name() == name)
                    .cloned())
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
        let mut contracts = match d {
            Some(packed) => match ContractList::from_bytes(packed.as_slice()) {
                Ok(contracts) => {
                    // remove old contract if it exists
                    contracts
                        .contracts()
                        .iter()
                        .filter(|c| c.name() != name)
                        .cloned()
                        .collect::<Vec<Contract>>()
                }
                Err(err) => {
                    return Err(ApplyError::InvalidTransaction(format!(
                        "Cannot deserialize contract list: {}",
                        err,
                    )));
                }
            },
            None => vec![],
        };
        contracts.push(new_contract);
        // sort the contracts by name
        contracts.sort_by_key(|c| c.name().to_string());

        // build new ContractList and set in state
        let contract_list = ContractListBuilder::new()
            .with_contracts(contracts)
            .build()
            .map_err(|_| {
                ApplyError::InvalidTransaction(String::from("Cannot build contract list"))
            })?;

        let serialized = contract_list.into_bytes().map_err(|err| {
            ApplyError::InvalidTransaction(format!("Cannot serialize contract list: {:?}", err,))
        })?;
        self.context
            .set_state_entry(address, serialized)
            .map_err(|err| ApplyError::InvalidTransaction(format!("{}", err)))?;
        Ok(())
    }

    pub fn delete_contract(&mut self, name: &str, version: &str) -> Result<(), ApplyError> {
        let address = make_contract_address(name, version)?;
        let d = self.context.delete_state_entry(&address)?;
        let deleted = match d {
            Some(deleted) => deleted,
            None => {
                return Err(ApplyError::InvalidTransaction(String::from(
                    "Cannot delete contract",
                )));
            }
        };
        if deleted != address {
            return Err(ApplyError::InvalidTransaction(String::from(
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
                let contract_registries = ContractRegistryList::from_bytes(packed.as_slice())
                    .map_err(|err| {
                        ApplyError::InvalidTransaction(format!(
                            "Cannot deserialize contract registry list: {:?}",
                            err,
                        ))
                    })?;

                Ok(contract_registries
                    .registries()
                    .iter()
                    .find(|reg| reg.name() == name)
                    .cloned())
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
        let mut contract_registries = match d {
            Some(packed) => match ContractRegistryList::from_bytes(packed.as_slice()) {
                Ok(contract_registries) => {
                    // remove old contract_registry if it exists
                    contract_registries
                        .registries()
                        .iter()
                        .filter(|c| c.name() != name)
                        .cloned()
                        .collect::<Vec<ContractRegistry>>()
                }
                Err(err) => {
                    return Err(ApplyError::InvalidTransaction(format!(
                        "Cannot deserialize contract registry list: {}",
                        err,
                    )));
                }
            },
            None => vec![],
        };

        contract_registries.push(new_contract_registry);
        // sort the contract regisitries by name
        contract_registries.sort_by_key(|c| c.name().to_string());
        let contract_registry_list = ContractRegistryListBuilder::new()
            .with_registries(contract_registries)
            .build()
            .map_err(|_| {
                ApplyError::InvalidTransaction(String::from("Cannot build contract registry list"))
            })?;

        let serialized = contract_registry_list.into_bytes().map_err(|err| {
            ApplyError::InvalidTransaction(format!(
                "Cannot serialize contract registry list: {:?}",
                err,
            ))
        })?;
        self.context
            .set_state_entry(address, serialized)
            .map_err(|err| ApplyError::InvalidTransaction(format!("{}", err)))?;
        Ok(())
    }

    pub fn delete_contract_registry(&mut self, name: &str) -> Result<(), ApplyError> {
        let address = make_contract_registry_address(name)?;
        let d = self.context.delete_state_entry(&address)?;
        let deleted = match d {
            Some(deleted) => deleted,
            None => {
                return Err(ApplyError::InvalidTransaction(String::from(
                    "Cannot delete contract registry",
                )));
            }
        };
        if deleted != address {
            return Err(ApplyError::InvalidTransaction(String::from(
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
                let namespace_registries = NamespaceRegistryList::from_bytes(packed.as_slice())
                    .map_err(|err| {
                        ApplyError::InvalidTransaction(format!(
                            "Cannot deserialize namespace registry list: {:?}",
                            err,
                        ))
                    })?;

                Ok(namespace_registries
                    .registries()
                    .iter()
                    .find(|reg| reg.namespace() == namespace)
                    .cloned())
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
                let namespace_registries = NamespaceRegistryList::from_bytes(packed.as_slice())
                    .map_err(|err| {
                        ApplyError::InvalidTransaction(format!(
                            "Cannot deserialize namespace registry list: {:?}",
                            err,
                        ))
                    })?;
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
        let mut namespace_registries = match d {
            Some(packed) => match NamespaceRegistryList::from_bytes(packed.as_slice()) {
                Ok(namespace_registries) => {
                    // remove old namespace rgistry if it exists
                    namespace_registries
                        .registries()
                        .iter()
                        .filter(|nr| nr.namespace() != namespace)
                        .cloned()
                        .collect::<Vec<NamespaceRegistry>>()
                }
                Err(err) => {
                    return Err(ApplyError::InvalidTransaction(format!(
                        "Cannot deserialize namespace registry list: {}",
                        err,
                    )));
                }
            },
            None => vec![],
        };
        namespace_registries.push(new_namespace_registry);
        // sort the namespace registries by namespace
        namespace_registries.sort_by_key(|nr| nr.namespace().to_string());
        let namespace_registry_list = NamespaceRegistryListBuilder::new()
            .with_registries(namespace_registries)
            .build()
            .map_err(|_| {
                ApplyError::InvalidTransaction(String::from("Cannot build namespace registry list"))
            })?;

        let serialized = namespace_registry_list.into_bytes().map_err(|err| {
            ApplyError::InvalidTransaction(format!(
                "Cannot serialize namespace registry list: {:?}",
                err,
            ))
        })?;
        self.context
            .set_state_entry(address, serialized)
            .map_err(|err| ApplyError::InvalidTransaction(format!("{}", err)))?;
        Ok(())
    }

    pub fn delete_namespace_registry(&mut self, namespace: &str) -> Result<(), ApplyError> {
        let address = make_namespace_registry_address(namespace)?;
        let d = self.context.delete_state_entry(&address)?;
        let deleted = match d {
            Some(deleted) => deleted,
            None => {
                return Err(ApplyError::InvalidTransaction(String::from(
                    "Cannot delete namespace registry",
                )));
            }
        };
        if deleted != address {
            return Err(ApplyError::InvalidTransaction(String::from(
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
                let smart_permissions = SmartPermissionList::from_bytes(packed.as_slice())
                    .map_err(|err| {
                        ApplyError::InvalidTransaction(format!(
                            "Cannot deserialize smart permissions list: {:?}",
                            err,
                        ))
                    })?;

                Ok(smart_permissions
                    .smart_permissions()
                    .iter()
                    .find(|sp| sp.name() == name)
                    .cloned())
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
        let mut smart_permissions = match d {
            Some(packed) => match SmartPermissionList::from_bytes(packed.as_slice()) {
                Ok(smart_permissions) => {
                    // remove old smart_permission if it exists
                    smart_permissions
                        .smart_permissions()
                        .iter()
                        .filter(|sp| sp.name() != name)
                        .cloned()
                        .collect::<Vec<SmartPermission>>()
                }
                Err(err) => {
                    return Err(ApplyError::InvalidTransaction(format!(
                        "Cannot deserialize smart permission list: {}",
                        err,
                    )));
                }
            },
            None => vec![],
        };

        smart_permissions.push(new_smart_permission);
        // sort the smart_permission by name
        smart_permissions.sort_by_key(|sp| sp.name().to_string());

        let smart_permission_list = SmartPermissionListBuilder::new()
            .with_smart_permissions(smart_permissions)
            .build()
            .map_err(|_| {
                ApplyError::InvalidTransaction(String::from("Cannot build smart permission list"))
            })?;

        let serialized = smart_permission_list.into_bytes().map_err(|err| {
            ApplyError::InvalidTransaction(format!(
                "Cannot serialize smart permission list: {:?}",
                err,
            ))
        })?;
        self.context
            .set_state_entry(address, serialized)
            .map_err(|err| ApplyError::InvalidTransaction(format!("{}", err)))?;
        Ok(())
    }

    pub fn delete_smart_permission(&mut self, org_id: &str, name: &str) -> Result<(), ApplyError> {
        let address = compute_smart_permission_address(org_id, name);
        let d = self.context.delete_state_entry(&address)?;
        let deleted = match d {
            Some(deleted) => deleted,
            None => {
                return Err(ApplyError::InvalidTransaction(String::from(
                    "Cannot delete smart_permission",
                )));
            }
        };
        if deleted != address {
            return Err(ApplyError::InvalidTransaction(String::from(
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
                let orgs = OrganizationList::from_bytes(packed.as_slice()).map_err(|err| {
                    ApplyError::InvalidTransaction(format!(
                        "Cannot deserialize organization list: {:?}",
                        err,
                    ))
                })?;

                Ok(orgs
                    .organizations()
                    .iter()
                    .find(|org| org.org_id() == id)
                    .cloned())
            }
            None => Ok(None),
        }
    }

    pub fn get_agent(&mut self, public_key: &str) -> Result<Option<Agent>, ApplyError> {
        let address = compute_agent_address(public_key);
        let d = self.context.get_state_entry(&address)?;
        match d {
            Some(packed) => {
                let agents = AgentList::from_bytes(packed.as_slice()).map_err(|err| {
                    ApplyError::InvalidTransaction(format!(
                        "Cannot deserialize agent list: {:?}",
                        err,
                    ))
                })?;

                Ok(agents
                    .agents()
                    .iter()
                    .find(|agent| agent.public_key() == public_key)
                    .cloned())
            }
            None => Ok(None),
        }
    }
}

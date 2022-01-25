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

use crate::handler::{ApplyError, TransactionContext};
use crate::protocol::sabre::state::{
    Contract, ContractList, ContractListBuilder, ContractRegistry, ContractRegistryList,
    ContractRegistryListBuilder, NamespaceRegistry, NamespaceRegistryList,
    NamespaceRegistryListBuilder,
};
use crate::protocol::sabre::ADMINISTRATORS_SETTING_ADDRESS;
use crate::protos::sabre_payload::Setting;
use crate::protos::{FromBytes, IntoBytes};

use super::addressing::{
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
                    Message::parse_from_bytes(packed.as_slice()).map_err(|err| {
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
}

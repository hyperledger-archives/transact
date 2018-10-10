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

use std::collections::HashMap;
use std::string::FromUtf8Error;
use std::error::Error as StdError;
use std::fmt;

use sawtooth_sdk::processor::handler::{ContextError, TransactionContext};
use wasm_executor::wasmi::{Error, Externals, FuncInstance, FuncRef, HostError, MemoryDescriptor,
                           MemoryInstance, MemoryRef, ModuleImportResolver, RuntimeArgs,
                           RuntimeValue, Signature, Trap, TrapKind, ValueType, Module,
                           ModuleInstance, ImportsBuilder};
use wasm_executor::wasmi::memory_units::Pages;

// External function indices

/// Args
///
/// 1) Pointer offset in memory for address string
/// 2) Length of address string
///
const GET_STATE_IDX: usize = 0;

/// Args
///
/// 1) Offset in memory for address string
/// 2) Length of address string
/// 3) Offset of byte data
/// 4) Length of byte data
///
const SET_STATE_IDX: usize = 1;

/// Args
///
/// 1) Pointer value
///
const GET_PTR_LEN_IDX: usize = 2;

/// Args
///
/// 1) Pointer value
///
const GET_PTR_CAP_IDX: usize = 3;

/// Args
///
/// 1) size of allocated region
///
/// Returns - raw pointer to allocated block
///
const ALLOC_IDX: usize = 4;

/// Args
///
/// 1) offset of byte in memory
///
/// Returns - byte value stored at offset
///
const READ_BYTE_IDX: usize = 5;

/// Args
///
/// 1) ptr to write to
///
/// 2) offset to realtive to ptr to write byte to
///
/// 3) byte to be written to offset
///
/// Returns - 1 if successful, or a negative value if failure
///
const WRITE_BYTE_IDX: usize = 6;

/// Args
///
/// 1) First pointer in pointer list
///
/// Returns - length of collection if collection exists, and -1 otherwise
///
const GET_COLLECTION_LEN_IDX: usize = 7;

/// Args
///
/// 1) First pointer in pointer collection
///
/// 2) index of pointer request
///
/// Returns - raw pointer at index if index and collection are
/// valid, and -1 otherwise
///
const GET_PTR_FROM_COLLECTION_IDX: usize = 8;

/// Args
///
/// 1) Pointer offset in memory for address string
/// 2) Length of address string
///
const DELETE_STATE_IDX: usize = 9;

/// Args
///
/// 1) First pointer in pointer collection
///
/// Returns - returns the raw head pointer if the collection is
/// valid, and -1 otherwise
///
const CREATE_COLLECTION: usize = 10;

/// Args
///
/// 1) First pointer in pointer collection
///
/// 2) New pointer that should be added to the collection
///
/// Returns - returns the raw head pointer if adding new pointer to the collection was
/// valid, and -1 otherwise
///
const ADD_TO_COLLECTION: usize = 11;

const SMART_PERMISSION: usize = 12;

pub struct WasmExternals {
    pub memory_ref: MemoryRef,
    context: TransactionContext,
    ptrs: HashMap<u32, Pointer>,
    ptr_collections: HashMap<u32, Vec<u32>>,
    memory_write_offset: u32,
}

impl WasmExternals {
    pub fn new(
        memory_ref: Option<MemoryRef>,
        context: TransactionContext,
    ) -> Result<WasmExternals, ExternalsError> {
        let m_ref = if let Some(m) = memory_ref {
            m
        } else {
            MemoryInstance::alloc(Pages(256), None)?
        };

        Ok(WasmExternals {
            memory_ref: m_ref,
            context,
            ptrs: HashMap::new(),
            ptr_collections: HashMap::new(),
            memory_write_offset: 0,
        })
    }

    fn ptr_to_string(&mut self, raw_ptr: u32) -> Result<String, ExternalsError> {
        if let Some(p) = self.ptrs.get(&raw_ptr) {
            let bytes = self.get_memory_ref().get(p.raw, p.length)?;

            String::from_utf8(bytes).map_err(ExternalsError::from)
        } else {
            Err(ExternalsError::from(format!(
                "ptr referencing {} not found",
                raw_ptr
            )))
        }
    }

    fn ptr_to_vec(&mut self, raw_ptr: u32) -> Result<Vec<u8>, ExternalsError> {
        if let Some(p) = self.ptrs.get(&raw_ptr) {
            self.get_memory_ref()
                .get(p.raw, p.length)
                .map_err(ExternalsError::from)
        } else {
            Err(ExternalsError::from(format!(
                "ptr referencing {} not found",
                raw_ptr
            )))
        }
    }

    fn get_memory_ref(&self) -> MemoryRef {
        self.memory_ref.clone()
    }

    pub fn write_data(&mut self, data: Vec<u8>) -> Result<u32, ExternalsError> {
        self.get_memory_ref().set(self.memory_write_offset, &data)?;

        let ptr = Pointer {
            raw: self.memory_write_offset,
            length: data.len(),
            capacity: data.capacity(),
        };

        let raw_ptr = ptr.raw;

        self.ptrs.insert(self.memory_write_offset, ptr);
        self.memory_write_offset += data.capacity() as u32;

        Ok(raw_ptr)
    }

    /// Takes a list of pointers and associates them,
    /// effectively creating a list
    ///
    /// Returns a result either containing the raw value
    /// of the first pointer in the list or an externals
    /// error
    pub fn collect_ptrs(&mut self, raw_ptrs: Vec<u32>) -> Result<u32, ExternalsError> {
        info!("associating pointers: {:?}", raw_ptrs);
        if raw_ptrs.iter().all(|x| self.ptrs.contains_key(&x)) {
            self.ptr_collections.insert(raw_ptrs[0], raw_ptrs.clone());
            Ok(raw_ptrs[0])
        } else {
            Err(ExternalsError::from("Attempting to create a ptr collection with nonexistant pointers"))
        }
    }

    pub fn add_to_collection(&mut self, head: u32, raw_ptr: u32) -> Result<u32, ExternalsError> {
        info!("adding to collection: {:?}", raw_ptr);
        if let Some(x) = self.ptr_collections.get_mut(&head) {
            x.push(raw_ptr);
            Ok(head)
        } else {
            Err(ExternalsError::from("Attempting to add a ptr to nonexistant collecttion"))
        }
    }

    pub fn create_collection(&mut self, head: u32) -> Result<u32, ExternalsError> {
        info!("create_collection: {:?}", head);
        self.ptr_collections.insert(head, vec![head]);
        Ok(head)
    }
}

impl Externals for WasmExternals {
    fn invoke_index(
        &mut self,
        index: usize,
        args: RuntimeArgs,
    ) -> Result<Option<RuntimeValue>, Trap> {
        match index {
            GET_STATE_IDX => {
                let head_ptr: u32 = args.nth(0);
                let addresses = match self.ptr_collections.get(&head_ptr) {
                    Some(addresses) => addresses.clone(),
                    None => return Ok(Some(RuntimeValue::I32(-1)))
                };
                let mut addr_vec = Vec::new();
                for addr in addresses {
                    let address = self.ptr_to_string(addr).map_err(ExternalsError::from)?;
                    addr_vec.push(address);
                }

                info!("Attempting to get state, addresses: {:?}", addr_vec);

                let state = self.context
                    .get_state(addr_vec)
                    .map_err(ExternalsError::from)?
                    .unwrap_or(Vec::new());

                let raw_ptr = self.write_data(state)?;

                Ok(Some(RuntimeValue::I32(raw_ptr as i32)))
            }
            SET_STATE_IDX => {
                let addr_ptr: i32 = args.nth(0);
                let state_ptr: i32 = args.nth(1);

                let addr = self.ptr_to_string(addr_ptr as u32)?;
                info!("Attempting to set state, address: {}", addr);

                let state = self.ptr_to_vec(state_ptr as u32)?;
                let mut sets = HashMap::new();
                sets.insert(addr, state);
                match self.context.set_state(sets){
                    Ok(()) => {
                        Ok(Some(RuntimeValue::I32(1)))
                    },
                    Err(err) => {
                        info!("Set Error: {}", err);
                        Ok(Some(RuntimeValue::I32(0)))
                    }

                }
            }
            DELETE_STATE_IDX => {
                let head_ptr: u32 = args.nth(0);

                let addresses = match self.ptr_collections.get(&head_ptr) {
                    Some(addresses) => addresses.clone(),
                    None => return Ok(Some(RuntimeValue::I32(-1)))
                };
                let mut addr_vec = Vec::new();
                for addr in addresses {
                    let address = self.ptr_to_string(addr).map_err(ExternalsError::from)?;
                    addr_vec.push(address);
                }
                info!("Attempting to delete state, addresses: {:?}", addr_vec);
                let result = self.context
                    .delete_state(addr_vec)
                    .map_err(ExternalsError::from)?
                    .unwrap_or(Vec::new());

                let mut ptr_vec = Vec::new();
                for addr in result{
                    let raw_ptr = self.write_data(addr.as_bytes().to_vec())?;
                    ptr_vec.push(raw_ptr);
                }

                let raw_ptr = self.collect_ptrs(ptr_vec)?;

                Ok(Some(RuntimeValue::I32(raw_ptr as i32)))

            }
            GET_PTR_LEN_IDX => {
                let addr = args.nth(0);
                info!("Getting pointer length\nraw {}", addr);

                if let Some(ptr) = self.ptrs.get(&addr) {
                    info!("ptr: {:?}", ptr);
                    Ok(Some(RuntimeValue::I32(ptr.length as i32)))
                } else {
                    Ok(Some(RuntimeValue::I32(-1)))
                }
            }
            GET_PTR_CAP_IDX => {
                let addr = args.nth(0);

                if let Some(ptr) = self.ptrs.get(&addr) {
                    Ok(Some(RuntimeValue::I32(ptr.capacity as i32)))
                } else {
                    Ok(Some(RuntimeValue::I32(-1)))
                }
            }
            ALLOC_IDX => {
                let len: i32 = args.nth(0);

                info!("Allocating memory block of length: {}", len);
                let raw_ptr = self.write_data(vec![0; len as usize])?;
                info!("Block successfully allocated ptr: {}", raw_ptr as i32);

                Ok(Some(RuntimeValue::I32(raw_ptr as i32)))
            }
            READ_BYTE_IDX => {
                let offset: i32 = args.nth(0);
                let byte = self.get_memory_ref()
                    .get(offset as u32, 1)
                    .map_err(ExternalsError::from)?[0];
                Ok(Some(RuntimeValue::I32(byte as i32)))
            }
            WRITE_BYTE_IDX => {
                let ptr: u32 = args.nth(0);
                let offset: u32 = args.nth(1);
                let data: i32 = args.nth(2);

                if let Some(p) = self.ptrs.get(&ptr) {
                    self.get_memory_ref()
                        .set(p.raw + offset, vec![data as u8].as_slice())
                        .map_err(ExternalsError::from)?;

                    Ok(Some(RuntimeValue::I32(1)))
                } else {
                    Ok(Some(RuntimeValue::I32(-1)))
                }
            }
            GET_COLLECTION_LEN_IDX => {
                let head_ptr: u32 = args.nth(0);

                info!("Retrieving collection length. Head pointer {}", head_ptr);

                if let Some(v) = self.ptr_collections.get(&head_ptr) {
                    info!("Collection found elements in collection: {}", v.len());
                    Ok(Some(RuntimeValue::I32(v.len() as i32)))
                } else {
                    Ok(Some(RuntimeValue::I32(-1)))
                }
            }
            GET_PTR_FROM_COLLECTION_IDX => {
                let head_ptr: u32 = args.nth(0);
                let index: u32 = args.nth(1);

                info!("Retrieving pointer head_ptr: {} index: {}", head_ptr, index);

                if let Some(v) = self.ptr_collections.get(&head_ptr) {
                    if index as usize >= v.len() {
                        info!("Invalid index");
                        Ok(Some(RuntimeValue::I32(-1)))
                    } else {
                        info!("Pointer retrieved: {}", v[index as usize]);
                        Ok(Some(RuntimeValue::I32(v[index as usize] as i32)))
                    }
                } else {
                    Ok(Some(RuntimeValue::I32(-1)))
                }
            }
            CREATE_COLLECTION => {
                let head_ptr: u32 = args.nth(0);
                self.create_collection(head_ptr)?;
                Ok(Some(RuntimeValue::I32(head_ptr as i32)))
            }
            ADD_TO_COLLECTION => {
                let head_ptr: u32 = args.nth(0);
                let raw_ptr: u32 = args.nth(1);

                self.add_to_collection(head_ptr, raw_ptr)?;
                Ok(Some(RuntimeValue::I32(head_ptr as i32)))
            }
            SMART_PERMISSION => {
                let contract_ptr: i32 = args.nth(0);
                let roles_head_ptr: u32 = args.nth(1);
                let org_id_ptr: i32 = args.nth(2);
                let public_key_ptr: i32 = args.nth(3);
                let payload_ptr: i32 = args.nth(4);

                let roles = match self.ptr_collections.get(&roles_head_ptr) {
                    Some(roles) => roles.clone(),
                    None => return Ok(Some(RuntimeValue::I32(-1)))
                };
                let mut role_vec = Vec::new();
                for role in roles {
                    let role_str = self.ptr_to_string(role).map_err(ExternalsError::from)?;
                    role_vec.push(role_str);
                }
                let org_id = self.ptr_to_string(org_id_ptr as u32)?;
                let public_key = self.ptr_to_string(public_key_ptr as u32)?;
                let payload = self.ptr_to_vec(payload_ptr as u32)?;
                let contract = self.ptr_to_vec(contract_ptr as u32)?;

                let cloned_context = self.context.clone();
                let module =
                    SmartPermissionModule::new(&contract, cloned_context)
                        .expect("Failed to create can_add module");
                let result = module
                    .entrypoint(role_vec, org_id, public_key, payload.to_vec())
                    .map_err(|e| ExternalsError::from(format!("{:?}", e)))?;

                match result {
                    Some(x) => {
                        Ok(Some(RuntimeValue::I32(x)))
                    }
                    None =>Err(ExternalsError::to_trap("No result returned".into()))
                }
            }
            _ => Err(ExternalsError::to_trap("Function does not exist".into())),
        }
    }
}

impl ModuleImportResolver for WasmExternals {
    fn resolve_func(&self, field_name: &str, _signature: &Signature) -> Result<FuncRef, Error> {
        match field_name {
            "get_state" => Ok(FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                GET_STATE_IDX,
            )),
            "set_state" => Ok(FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], Some(ValueType::I32)),
                SET_STATE_IDX,
            )),
            "delete_state" => Ok(FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                DELETE_STATE_IDX,
            )),
            "invoke_smart_permission" => Ok(FuncInstance::alloc_host(
                Signature::new(
                    &[ValueType::I32,
                    ValueType::I32,
                    ValueType::I32,
                    ValueType::I32,
                    ValueType::I32][..], Some(ValueType::I32)),
                SMART_PERMISSION,
            )),
            "get_ptr_len" => Ok(FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                GET_PTR_LEN_IDX,
            )),
            "get_ptr_capacity" => Ok(FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                GET_PTR_CAP_IDX,
            )),
            "alloc" => Ok(FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                ALLOC_IDX,
            )),
            "read_byte" => Ok(FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                READ_BYTE_IDX,
            )),
            "write_byte" => Ok(FuncInstance::alloc_host(
                Signature::new(
                    &[ValueType::I32, ValueType::I32, ValueType::I32][..],
                    Some(ValueType::I32),
                ),
                WRITE_BYTE_IDX,
            )),
            "get_ptr_collection_len" => Ok(FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                GET_COLLECTION_LEN_IDX,
            )),
            "get_ptr_from_collection" => Ok(FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], Some(ValueType::I32)),
                GET_PTR_FROM_COLLECTION_IDX,
            )),
            "create_collection" => Ok(FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                CREATE_COLLECTION,
            )),
            "add_to_collection" => Ok(FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], Some(ValueType::I32)),
                ADD_TO_COLLECTION,
            )),

            _ => Err(Error::Instantiation(format!(
                "Export {} not found",
                field_name
            ))),
        }
    }

    fn resolve_memory(
        &self,
        field_name: &str,
        _memory_type: &MemoryDescriptor,
    ) -> Result<MemoryRef, Error> {
        match field_name {
            "memory" => Ok(self.get_memory_ref()),
            _ => Err(Error::Instantiation(format!(
                "env module doesn't provide memory '{}'",
                field_name
            ))),
        }
    }
}

#[derive(Clone)]
struct Pointer {
    raw: u32,
    length: usize,
    capacity: usize,
}

impl fmt::Debug for Pointer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Pointer {{ raw: {}, length: {}, capacity {} }}",
            self.raw, self.length, self.capacity
        )
    }
}

#[derive(Debug)]
pub struct ExternalsError {
    message: String,
}

impl ExternalsError {
    fn to_trap(msg: String) -> Trap {
        Trap::from(TrapKind::Host(Box::new(ExternalsError::from(msg))))
    }
}

impl fmt::Display for ExternalsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error: {}", self.message)
    }
}

impl HostError for ExternalsError {}

impl<'a> From<&'a str> for ExternalsError {
    fn from(s: &'a str) -> Self {
        ExternalsError {
            message: String::from(s),
        }
    }
}

impl From<Error> for ExternalsError {
    fn from(e: Error) -> Self {
        ExternalsError {
            message: format!("{:?}", e),
        }
    }
}

impl From<String> for ExternalsError {
    fn from(s: String) -> Self {
        ExternalsError { message: s }
    }
}

impl From<FromUtf8Error> for ExternalsError {
    fn from(e: FromUtf8Error) -> Self {
        ExternalsError {
            message: e.description().to_string(),
        }
    }
}

impl From<ContextError> for ExternalsError {
    fn from(e: ContextError) -> Self {
        ExternalsError {
            message: format!("{:?}", e),
        }
    }
}

struct SmartPermissionModule {
    context: TransactionContext,
    module: Module
}

impl SmartPermissionModule {
    pub fn new(wasm: &[u8], context: TransactionContext) -> Result<SmartPermissionModule, ExternalsError> {
        let module = Module::from_buffer(wasm)?;
        Ok(SmartPermissionModule { context, module })
    }

    pub fn entrypoint(
        &self,
        roles: Vec<String>,
        org_id: String,
        public_key: String,
        payload: Vec<u8>
    ) -> Result<Option<i32>, ExternalsError> {
        let mut env =  WasmExternals::new(None, self.context.clone())?;

        let instance = ModuleInstance::new(&self.module, &ImportsBuilder::new().with_resolver("env", &env))?
            .assert_no_start();

        info!("Writing roles to memory");

        let roles_write_results: Vec<Result<u32, ExternalsError>> = roles
            .into_iter()
            .map(|i| env.write_data(i.into_bytes()))
            .collect();

        let mut role_ptrs = Vec::new();

        for i in roles_write_results {
            if i.is_err() {
                return Err(i.unwrap_err());
            }
            role_ptrs.push(i.unwrap());
        }

        let role_list_ptr = if role_ptrs.len() > 0 {
            env.collect_ptrs(role_ptrs)? as i32
        } else {
            -1
        };

        info!("Roles written to memory: {:?}", role_list_ptr);

        let org_id_ptr = env.write_data(org_id.into_bytes())? as i32;
        info!("Organization ID written to memory");

        let public_key_ptr = env.write_data(public_key.into_bytes())? as i32;
        info!("Public key written to memory");

        let payload_ptr = env.write_data(payload)? as i32;
        info!("Payload written to memory");

        let result = instance
            .invoke_export(
                "entrypoint",
                &vec![
                    RuntimeValue::I32(role_list_ptr),
                    RuntimeValue::I32(org_id_ptr),
                    RuntimeValue::I32(public_key_ptr),
                    RuntimeValue::I32(payload_ptr)
                ],
                &mut env
            )?;

        if let Some(RuntimeValue::I32(i)) = result {
            Ok(Some(i))
        } else {
            Ok(None)
        }
    }
}

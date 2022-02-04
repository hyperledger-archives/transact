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

use std::boxed::Box;
use std::collections::HashMap;
use std::fmt;
use std::string::FromUtf8Error;
use std::time::Instant;

use log::{max_level, LevelFilter};
use wasmi::memory_units::Pages;
use wasmi::{
    Error, Externals, FuncInstance, FuncRef, HostError, MemoryDescriptor, MemoryInstance,
    MemoryRef, ModuleImportResolver, RuntimeArgs, RuntimeValue, Signature, Trap, TrapKind,
    ValueType,
};

use crate::handler::{ContextError, TransactionContext};

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

/// Args
///
/// 1) log level
/// 2) the formated string to log
const LOG: usize = 13;

/// Returns the current logleel set on the transaction processor
const LOG_LEVEL: usize = 14;

/// Args
///
/// 1) Event type string, to identify the event
/// 2) Attributes list, optionally to filter on the received events
/// 2) Data, that is opaque to the validator when sending to the event listener
///
/// Returns - 0 if the add_event() is successful, -1 if failed to get
/// attributes list from the collection, 1 otherwise
///
const ADD_EVENT_IDX: usize = 15;

pub struct WasmExternals<'a> {
    pub memory_ref: MemoryRef,
    context: &'a mut dyn TransactionContext,
    ptrs: HashMap<u32, Pointer>,
    ptr_collections: HashMap<u32, Vec<u32>>,
    memory_write_offset: u32,
}

impl<'a> WasmExternals<'a> {
    pub fn new(
        memory_ref: Option<MemoryRef>,
        context: &'a mut dyn TransactionContext,
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

        // In case the data to be added is empty, keep the memory_write_offset
        // moving to the next location.
        let offset_to_add = if data.capacity() == 0 {
            1
        } else {
            data.capacity() as u32
        };

        self.ptrs.insert(self.memory_write_offset, ptr);
        self.memory_write_offset += offset_to_add;

        debug!("moved the pointer to {:?}", self.memory_write_offset);

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
        if raw_ptrs.iter().all(|x| self.ptrs.contains_key(x)) {
            self.ptr_collections.insert(raw_ptrs[0], raw_ptrs.clone());
            Ok(raw_ptrs[0])
        } else {
            Err(ExternalsError::from(
                "Attempting to create a ptr collection with nonexistant pointers",
            ))
        }
    }

    pub fn add_to_collection(&mut self, head: u32, raw_ptr: u32) -> Result<u32, ExternalsError> {
        info!("adding to collection: {:?}", raw_ptr);
        if let Some(x) = self.ptr_collections.get_mut(&head) {
            x.push(raw_ptr);
            Ok(head)
        } else {
            Err(ExternalsError::from(
                "Attempting to add a ptr to nonexistant collecttion",
            ))
        }
    }

    pub fn create_collection(&mut self, head: u32) -> Result<u32, ExternalsError> {
        info!("create_collection: {:?}", head);
        self.ptr_collections.insert(head, vec![head]);
        Ok(head)
    }

    fn get_state(
        &mut self,
        args: RuntimeArgs,
        timer: Instant,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let head_ptr: u32 = args.nth(0);
        let addresses = match self.ptr_collections.get(&head_ptr) {
            Some(addresses) => addresses.clone(),
            None => return Ok(Some(RuntimeValue::I32(-1))),
        };
        let mut addr_vec = Vec::new();
        for addr in addresses {
            let address = self.ptr_to_string(addr).map_err(ExternalsError::from)?;
            addr_vec.push(address);
        }

        info!("Attempting to get state, addresses: {:?}", addr_vec);

        let state = self
            .context
            .get_state_entries(&addr_vec)
            .map_err(ExternalsError::from)?;

        let mut ptr_vec = Vec::new();
        for (addr, data) in state {
            let addr_raw_ptr = self.write_data(addr.as_bytes().to_vec())?;
            ptr_vec.push(addr_raw_ptr);

            let data_raw_ptr = self.write_data(data)?;
            ptr_vec.push(data_raw_ptr);
        }

        // collect ptrs or return empty vec
        let raw_ptr = if ptr_vec.is_empty() {
            self.write_data(Vec::new())?
        } else {
            self.collect_ptrs(ptr_vec)?
        };

        info!(
            "GET_STATE Execution time: {} secs {} ms",
            timer.elapsed().as_secs(),
            timer.elapsed().subsec_millis()
        );

        Ok(Some(RuntimeValue::I32(raw_ptr as i32)))
    }

    fn set_state(
        &mut self,
        args: RuntimeArgs,
        timer: Instant,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let head_ptr: u32 = args.nth(0);
        let addr_state = match self.ptr_collections.get(&head_ptr) {
            Some(addresses) => addresses.clone(),
            None => return Ok(Some(RuntimeValue::I32(-1))),
        };

        // if the length is not even return deserialization error
        if (addr_state.len() % 2) != 0 {
            return Ok(Some(RuntimeValue::I32(-1)));
        }

        let mut entries = Vec::new();
        for entry in addr_state.chunks(2) {
            let address = self.ptr_to_string(entry[0]).map_err(ExternalsError::from)?;
            let data = self.ptr_to_vec(entry[1])?;
            entries.push((address, data));
        }

        info!("Attempting to set state, entries: {:?}", entries);

        match self.context.set_state_entries(entries) {
            Ok(()) => {
                info!(
                    "SET_STATE Execution time: {} secs {} ms",
                    timer.elapsed().as_secs(),
                    timer.elapsed().subsec_millis()
                );
                Ok(Some(RuntimeValue::I32(1)))
            }
            Err(err) => {
                error!("Set Error: {}", err);
                info!(
                    "SET_STATE Execution time: {} secs {} ms",
                    timer.elapsed().as_secs(),
                    timer.elapsed().subsec_millis()
                );
                Ok(Some(RuntimeValue::I32(0)))
            }
        }
    }

    fn delete_state(&mut self, args: RuntimeArgs) -> Result<Option<RuntimeValue>, Trap> {
        let head_ptr: u32 = args.nth(0);

        let addresses = match self.ptr_collections.get(&head_ptr) {
            Some(addresses) => addresses.clone(),
            None => return Ok(Some(RuntimeValue::I32(-1))),
        };
        let mut addr_vec = Vec::new();
        for addr in addresses {
            let address = self.ptr_to_string(addr).map_err(ExternalsError::from)?;
            addr_vec.push(address);
        }
        info!("Attempting to delete state, addresses: {:?}", addr_vec);
        let result = self
            .context
            .delete_state_entries(&addr_vec)
            .map_err(ExternalsError::from)?;

        let mut ptr_vec = Vec::new();
        for addr in result {
            let raw_ptr = self.write_data(addr.as_bytes().to_vec())?;
            ptr_vec.push(raw_ptr);
        }

        // collect ptrs or return empty vec
        let raw_ptr = if ptr_vec.is_empty() {
            self.write_data(Vec::new())?
        } else {
            self.collect_ptrs(ptr_vec)?
        };

        Ok(Some(RuntimeValue::I32(raw_ptr as i32)))
    }

    fn add_event(
        &mut self,
        event_type_ptr: u32,
        attribute_list_ptr: u32,
        data_ptr: u32,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let attribute_list = match self.ptr_collections.get(&attribute_list_ptr) {
            Some(attributes) => attributes.clone(),
            None => return Ok(Some(RuntimeValue::I32(-1))),
        };

        // if the length is even return deserialization error
        // the list should have a starting element followed by key, value pair
        if (attribute_list.len() % 2) == 0 {
            return Ok(Some(RuntimeValue::I32(-1)));
        }

        let mut attributes = Vec::new();
        if attribute_list.len() > 1 {
            for entry in attribute_list[1..].chunks(2) {
                let key = self.ptr_to_string(entry[0]).map_err(ExternalsError::from)?;
                let value = self.ptr_to_string(entry[1]).map_err(ExternalsError::from)?;
                attributes.push((key, value));
            }
        }

        let event_type = self.ptr_to_string(event_type_ptr)?;

        let data = self.ptr_to_vec(data_ptr)?;

        info!(
            "Attempting to add event, event_type: {:?}, attributes: {:?}, data: {:?}",
            event_type, attributes, data
        );

        match self.context.add_event(event_type, attributes, data) {
            Ok(()) => Ok(Some(RuntimeValue::I32(0))),
            Err(err) => {
                error!("Add event Error: {}", err);
                Ok(Some(RuntimeValue::I32(1)))
            }
        }
    }
}

impl<'a> Externals for WasmExternals<'a> {
    fn invoke_index(
        &mut self,
        index: usize,
        args: RuntimeArgs,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let timer = Instant::now();
        match index {
            GET_STATE_IDX => self.get_state(args, timer),
            SET_STATE_IDX => self.set_state(args, timer),
            DELETE_STATE_IDX => self.delete_state(args),
            ADD_EVENT_IDX => {
                let event_type = args.nth(0);
                let attributes = args.nth(1);
                let data = args.nth(2);
                self.add_event(event_type, attributes, data)
            }
            GET_PTR_LEN_IDX => {
                let addr = args.nth(0);

                if let Some(ptr) = self.ptrs.get(&addr) {
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

                let raw_ptr = self.write_data(vec![0; len as usize])?;
                info!(
                    "ALLOC Execution time: {} secs {} ms",
                    timer.elapsed().as_secs(),
                    timer.elapsed().subsec_millis()
                );

                Ok(Some(RuntimeValue::I32(raw_ptr as i32)))
            }
            READ_BYTE_IDX => {
                let offset: i32 = args.nth(0);
                let byte = self
                    .get_memory_ref()
                    .get(offset as u32, 1)
                    .map_err(ExternalsError::from)?[0];
                Ok(Some(RuntimeValue::I32(i32::from(byte))))
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
            LOG => {
                let log_level: u32 = args.nth(0);
                let log_ptr: u32 = args.nth(1);
                let log_string = self.ptr_to_string(log_ptr)?;
                match log_level {
                    0 => error!("{}", log_string),
                    1 => warn!("{}", log_string),
                    2 => info!("{}", log_string),
                    3 => debug!("{}", log_string),
                    4 => trace!("{}", log_string),
                    _ => warn!("Unknown log level requested: {}", log_level),
                }
                Ok(None)
            }
            LOG_LEVEL => match max_level() {
                LevelFilter::Trace => Ok(Some(RuntimeValue::I32(4))),
                LevelFilter::Debug => Ok(Some(RuntimeValue::I32(3))),
                LevelFilter::Info => Ok(Some(RuntimeValue::I32(2))),
                LevelFilter::Warn => Ok(Some(RuntimeValue::I32(1))),
                _ => Ok(Some(RuntimeValue::I32(0))),
            },
            _ => Err(ExternalsError::trap("Function does not exist".into())),
        }
    }
}

impl<'a> ModuleImportResolver for WasmExternals<'a> {
    fn resolve_func(&self, field_name: &str, _signature: &Signature) -> Result<FuncRef, Error> {
        match field_name {
            "get_state" => Ok(FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                GET_STATE_IDX,
            )),
            "set_state" => Ok(FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                SET_STATE_IDX,
            )),
            "delete_state" => Ok(FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                DELETE_STATE_IDX,
            )),
            "add_event" => Ok(FuncInstance::alloc_host(
                Signature::new(
                    &[ValueType::I32, ValueType::I32, ValueType::I32][..],
                    Some(ValueType::I32),
                ),
                ADD_EVENT_IDX,
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
            "log_buffer" => Ok(FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], None),
                LOG,
            )),
            "log_level" => Ok(FuncInstance::alloc_host(
                Signature::new(&[][..], Some(ValueType::I32)),
                LOG_LEVEL,
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
    fn trap(msg: String) -> Trap {
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
            message: e.to_string(),
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

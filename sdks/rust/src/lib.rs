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

#![allow(clippy::missing_safety_doc, renamed_and_removed_lints)]

mod externs;
pub mod log;
pub mod protocol;
pub mod protos;

use std::collections::HashMap;
use std::string::FromUtf8Error;

pub use crate::externs::{WasmPtr, WasmPtrList};

pub struct Header {
    signer: String,
}

impl Header {
    pub fn new(signer: String) -> Header {
        Header { signer }
    }

    pub fn get_signer_public_key(&self) -> &str {
        &self.signer
    }
}

pub struct TpProcessRequest<'a> {
    payload: Vec<u8>,
    header: &'a mut Header,
    signature: String,
}

impl<'a> TpProcessRequest<'a> {
    pub fn new(payload: Vec<u8>, header: &'a mut Header, signature: String) -> TpProcessRequest {
        TpProcessRequest {
            payload,
            header,
            signature,
        }
    }

    pub fn get_payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn get_header(&self) -> &Header {
        self.header
    }

    pub fn get_signature(&self) -> String {
        self.signature.to_string()
    }
}

pub trait TransactionContext {
    #[deprecated(
        since = "0.2.0",
        note = "please use `get_state_entry` or `get_state_entries` instead"
    )]
    /// get_state queries the validator state for data at each of the
    /// addresses in the given list. The addresses that have been set
    /// are returned. get_state is deprecated, please use get_state_entry or get_state_entries
    /// instead
    ///
    /// # Arguments
    ///
    /// * `addresses` - the addresses to fetch
    fn get_state(&self, addresses: &[String]) -> Result<Vec<(String, Vec<u8>)>, WasmSdkError> {
        self.get_state_entries(addresses)
    }
    /// get_state_entry queries the validator state for data at the
    /// address given. If the address is set, the data is returned.
    ///
    /// # Arguments
    ///
    /// * `address` - the address to fetch
    fn get_state_entry(&self, address: &str) -> Result<Option<Vec<u8>>, WasmSdkError> {
        Ok(self
            .get_state_entries(&[address.to_string()])?
            .into_iter()
            .map(|(_, val)| val)
            .next())
    }

    /// get_state_entries queries the validator state for data at each of the
    /// addresses in the given list. The addresses that have been set
    /// are returned.
    ///
    /// # Arguments
    ///
    /// * `addresses` - the addresses to fetch
    fn get_state_entries(
        &self,
        addresses: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, WasmSdkError>;

    #[deprecated(
        since = "0.2.0",
        note = "please use `set_state_entry` or `set_state_entries` instead"
    )]
    /// set_state requests that each address in the provided map be
    /// set in validator state to its corresponding value. set_state is deprecated, please use
    /// set_state_entry to set_state_entries instead
    ///
    /// # Arguments
    ///
    /// * `entries` - entries are a hashmap where the key is an address and value is the data
    fn set_state(&self, entries: HashMap<String, Vec<u8>>) -> Result<(), WasmSdkError> {
        let state_entries: Vec<(String, Vec<u8>)> = entries.into_iter().collect();
        self.set_state_entries(state_entries)
    }

    /// set_state_entry requests that the provided address is set in the validator state to its
    /// corresponding value.
    ///
    /// # Arguments
    ///
    /// * `address` - address of where to store the data
    /// * `data` - payload is the data to store at the address
    fn set_state_entry(&self, address: String, data: Vec<u8>) -> Result<(), WasmSdkError> {
        self.set_state_entries(vec![(address, data)])
    }

    /// set_state_entries requests that each address in the provided map be
    /// set in validator state to its corresponding value.
    ///
    /// # Arguments
    ///
    /// * `entries` - entries are a hashmap where the key is an address and value is the data
    fn set_state_entries(&self, entries: Vec<(String, Vec<u8>)>) -> Result<(), WasmSdkError>;

    /// delete_state requests that each of the provided addresses be unset
    /// in validator state. A list of successfully deleted addresses is returned.
    /// delete_state is deprecated, please use delete_state_entry to delete_state_entries instead
    ///
    /// # Arguments
    ///
    /// * `addresses` - the addresses to delete
    #[deprecated(
        since = "0.2.0",
        note = "please use `delete_state_entry` or `delete_state_entries` instead"
    )]
    fn delete_state(&self, addresses: &[String]) -> Result<Vec<String>, WasmSdkError> {
        self.delete_state_entries(addresses)
    }

    /// delete_state_entry requests that the provided address be unset
    /// in validator state. A list of successfully deleted addresses
    /// is returned.
    ///
    /// # Arguments
    ///
    /// * `address` - the address to delete
    fn delete_state_entry(&self, address: &str) -> Result<Option<String>, WasmSdkError> {
        Ok(self
            .delete_state_entries(&[address.to_string()])?
            .into_iter()
            .next())
    }

    /// delete_state_entries requests that each of the provided addresses be unset
    /// in validator state. A list of successfully deleted addresses
    /// is returned.
    ///
    /// # Arguments
    ///
    /// * `addresses` - the addresses to delete
    fn delete_state_entries(&self, addresses: &[String]) -> Result<Vec<String>, WasmSdkError>;

    /// add_event adds a new event to the execution result for this transaction.
    ///
    /// # Arguments
    ///
    /// * `event_type` -  This is used to subscribe to events. It should be globally unique and
    ///          describe what, in general, has occured.
    /// * `attributes` - Additional information about the event that is transparent to the
    ///          validator. Attributes can be used by subscribers to filter the type of events
    ///          they receive.
    /// * `data` - Additional information about the event that is opaque to the validator.
    fn add_event(
        &self,
        event_type: String,
        attributes: Vec<(String, String)>,
        data: &[u8],
    ) -> Result<(), WasmSdkError>;
}

#[derive(Default)]
pub struct SabreTransactionContext {}

impl SabreTransactionContext {
    pub fn new() -> SabreTransactionContext {
        SabreTransactionContext {}
    }
}

impl TransactionContext for SabreTransactionContext {
    fn get_state_entries(
        &self,
        addresses: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, WasmSdkError> {
        unsafe {
            if addresses.is_empty() {
                return Err(WasmSdkError::InvalidTransaction("No address to get".into()));
            }
            let head = &addresses[0];
            let header_address_buffer = WasmBuffer::new(head.as_bytes())?;
            externs::create_collection(header_address_buffer.to_raw());

            for addr in addresses[1..].iter() {
                let wasm_buffer = WasmBuffer::new(addr.as_bytes())?;
                externs::add_to_collection(header_address_buffer.to_raw(), wasm_buffer.to_raw());
            }

            let results =
                WasmBuffer::from_list(externs::get_state(header_address_buffer.to_raw()))?;
            let mut result_vec = Vec::new();

            if (result_vec.len() % 2) != 0 {
                return Err(WasmSdkError::InvalidTransaction(
                    "Get state returned incorrect data fmt".into(),
                ));
            }

            for result in results.chunks(2) {
                let addr = String::from_utf8(result[0].to_bytes())?;
                result_vec.push((addr, result[1].to_bytes()))
            }
            Ok(result_vec)
        }
    }

    fn set_state_entries(&self, entries: Vec<(String, Vec<u8>)>) -> Result<(), WasmSdkError> {
        unsafe {
            let mut entries_iter = entries.iter();
            let (head, head_data) = match entries_iter.next() {
                Some((addr, data)) => (addr, data),
                None => return Err(WasmSdkError::InvalidTransaction("No entries to set".into())),
            };

            let header_address_buffer = WasmBuffer::new(head.as_bytes())?;
            externs::create_collection(header_address_buffer.to_raw());

            let wasm_head_data_buffer = WasmBuffer::new(head_data)?;
            externs::add_to_collection(
                header_address_buffer.to_raw(),
                wasm_head_data_buffer.to_raw(),
            );

            for (address, data) in entries_iter {
                let wasm_addr_buffer = WasmBuffer::new(address.as_bytes())?;
                externs::add_to_collection(
                    header_address_buffer.to_raw(),
                    wasm_addr_buffer.to_raw(),
                );

                let wasm_data_buffer = WasmBuffer::new(data)?;
                externs::add_to_collection(
                    header_address_buffer.to_raw(),
                    wasm_data_buffer.to_raw(),
                );
            }

            let result = externs::set_state(header_address_buffer.to_raw());

            if result == 0 {
                return Err(WasmSdkError::InvalidTransaction(
                    "Unable to set state".into(),
                ));
            }
        }
        Ok(())
    }

    fn delete_state_entries(&self, addresses: &[String]) -> Result<Vec<String>, WasmSdkError> {
        unsafe {
            if addresses.is_empty() {
                return Err(WasmSdkError::InvalidTransaction(
                    "No address to delete".into(),
                ));
            }
            let head = &addresses[0];
            let header_address_buffer = WasmBuffer::new(head.as_bytes())?;
            externs::create_collection(header_address_buffer.to_raw());

            for addr in addresses[1..].iter() {
                let wasm_buffer = WasmBuffer::new(addr.as_bytes())?;
                externs::add_to_collection(header_address_buffer.to_raw(), wasm_buffer.to_raw());
            }
            let result =
                WasmBuffer::from_list(externs::delete_state(header_address_buffer.to_raw()))?;
            let mut result_vec = Vec::new();
            for i in result {
                let addr = String::from_utf8(i.data)?;
                result_vec.push(addr);
            }
            Ok(result_vec)
        }
    }

    fn add_event(
        &self,
        event_type: String,
        attributes: Vec<(String, String)>,
        data: &[u8],
    ) -> Result<(), WasmSdkError> {
        unsafe {
            // Get the WasmBuffer of event_type
            let event_type_buffer = WasmBuffer::new(&event_type.as_bytes())?;

            // Get the WasmBuffer of data
            let data_buffer = WasmBuffer::new(data)?;

            // Get attributes tuple stored in a collection
            // List starts with a dummy entry "attributes", this is to allow empty
            // attributes list from the SDK
            // Entry at odd index: Key
            // Entry at even index: Value
            let attributes_iter = attributes.iter();
            let attributes_buffer = WasmBuffer::new(b"attributes")?;
            externs::create_collection(attributes_buffer.to_raw());

            for (key, value) in attributes_iter {
                let key_buffer = WasmBuffer::new(key.as_bytes())?;
                externs::add_to_collection(attributes_buffer.to_raw(), key_buffer.to_raw());
                let value_buffer = WasmBuffer::new(value.as_bytes())?;
                externs::add_to_collection(attributes_buffer.to_raw(), value_buffer.to_raw());
            }

            let result = externs::add_event(
                event_type_buffer.to_raw(),
                attributes_buffer.to_raw(),
                data_buffer.to_raw(),
            );

            if result != 0 {
                return Err(WasmSdkError::InvalidTransaction(
                    "Unable to add event".into(),
                ));
            }

            Ok(())
        }
    }
}

// Mimics the sawtooth sdk TransactionHandler
pub trait TransactionHandler {
    fn family_name(&self) -> String;
    fn family_versions(&self) -> Vec<String>;
    fn namespaces(&self) -> Vec<String>;
    fn apply(
        &self,
        request: &TpProcessRequest,
        context: &mut dyn TransactionContext,
    ) -> Result<(), ApplyError>;
}

pub fn invoke_smart_permission(
    contract_addr: String,
    name: String,
    roles: Vec<String>,
    org_id: String,
    public_key: String,
    payload: &[u8],
) -> Result<i32, WasmSdkError> {
    unsafe {
        if roles.is_empty() {
            return Err(WasmSdkError::InvalidTransaction("No roles ".into()));
        }
        let head = &roles[0];
        let header_role_buffer = WasmBuffer::new(head.as_bytes())?;
        externs::create_collection(header_role_buffer.to_raw());

        for role in roles[1..].iter() {
            let wasm_buffer = WasmBuffer::new(role.as_bytes())?;
            externs::add_to_collection(header_role_buffer.to_raw(), wasm_buffer.to_raw());
        }
        let contract_addr_buffer = WasmBuffer::new(contract_addr.as_bytes())?;
        let name_buffer = WasmBuffer::new(name.as_bytes())?;
        let org_id_buffer = WasmBuffer::new(org_id.as_bytes())?;
        let public_key_buffer = WasmBuffer::new(public_key.as_bytes())?;
        let payload_buffer = WasmBuffer::new(payload)?;

        Ok(externs::invoke_smart_permission(
            contract_addr_buffer.to_raw(),
            name_buffer.to_raw(),
            header_role_buffer.to_raw(),
            org_id_buffer.to_raw(),
            public_key_buffer.to_raw(),
            payload_buffer.to_raw(),
        ))
    }
}

/// -1: Failed to deserialize payload
/// -2: Failed to deserialize signer
/// -3: apply returned InvalidTransaction
/// -4: apply returned InternalError
///
/// # Safety
///
/// This function is unsafe due to the call to WasmBuffer::from_raw which converts a WasmPtr
/// to a WasmBuffer to access location in executor memory
pub unsafe fn execute_entrypoint<F>(
    payload_ptr: WasmPtr,
    signer_ptr: WasmPtr,
    signature_ptr: WasmPtr,
    apply: F,
) -> i32
where
    F: Fn(&TpProcessRequest, &mut dyn TransactionContext) -> Result<bool, ApplyError>,
{
    let payload = if let Ok(i) = WasmBuffer::from_raw(payload_ptr) {
        i.to_bytes()
    } else {
        return -1;
    };

    let signature = if let Ok(i) = WasmBuffer::from_raw(signature_ptr) {
        match i.to_string() {
            Ok(s) => s,
            Err(_) => return -2,
        }
    } else {
        return -1;
    };

    let signer = if let Ok(i) = WasmBuffer::from_raw(signer_ptr) {
        match i.to_string() {
            Ok(s) => s,
            Err(_) => return -2,
        }
    } else {
        return -1;
    };

    let mut header = Header::new(signer);
    match apply(
        &TpProcessRequest::new(payload, &mut header, signature),
        &mut SabreTransactionContext::new(),
    ) {
        Ok(r) => {
            if r {
                1
            } else {
                0
            }
        }
        Err(ApplyError::InvalidTransaction(_)) => -3,
        Err(ApplyError::InternalError(_)) => -4,
    }
}

pub struct Request {
    roles: Vec<String>,
    org_id: String,
    public_key: String,
    payload: Vec<u8>,
}

impl Request {
    pub fn new(
        roles: Vec<String>,
        org_id: String,
        public_key: String,
        payload: Vec<u8>,
    ) -> Request {
        Request {
            roles,
            org_id,
            public_key,
            payload,
        }
    }

    pub fn get_roles(&self) -> Vec<String> {
        self.roles.clone()
    }

    pub fn get_org_id(&self) -> String {
        self.org_id.clone()
    }

    pub fn get_public_key(&self) -> String {
        self.public_key.clone()
    }

    pub fn get_state(&self, address: String) -> Result<Option<Vec<u8>>, WasmSdkError> {
        unsafe {
            let wasm_buffer = WasmBuffer::new(address.as_bytes())?;
            ptr_to_vec(externs::get_state(wasm_buffer.to_raw()))
        }
    }

    pub fn get_payload<T>(&self) -> Vec<u8> {
        self.payload.clone()
    }
}

/// Error Codes:
///
/// -1: Failed to deserialize roles
/// -2: Failed to deserialize org_id
/// -3: Failed to deserialize public_key
/// -4: Failed to deserialize payload
/// -5: Failed to execute smart permission
/// -6: StateSetError
/// -7: AllocError
/// -8: MemoryRetrievalError
/// -9: Utf8EncodeError
/// -10: ProtobufError
///
/// # Safety
///
/// This function is unsafe due to the call to WasmBuffer::from_raw which converts a WasmPtr
/// to a WasmBuffer to access a location in executor memory
pub unsafe fn execute_smart_permission_entrypoint<F>(
    roles_ptr: WasmPtrList,
    org_id_ptr: WasmPtr,
    public_key_ptr: WasmPtr,
    payload_ptr: WasmPtr,
    has_permission: F,
) -> i32
where
    F: Fn(Request) -> Result<bool, WasmSdkError>,
{
    let roles = if let Ok(i) = WasmBuffer::from_list(roles_ptr) {
        let results: Vec<Result<String, WasmSdkError>> = i.iter().map(|x| x.to_string()).collect();

        if results.iter().any(|x| x.is_err()) {
            return -1;
        } else {
            results.into_iter().map(|x| x.unwrap()).collect()
        }
    } else {
        return -1;
    };

    let org_id = if let Ok(i) = WasmBuffer::from_raw(org_id_ptr) {
        match i.to_string() {
            Ok(s) => s,
            Err(_) => {
                return -2;
            }
        }
    } else {
        return -2;
    };

    let public_key = if let Ok(i) = WasmBuffer::from_raw(public_key_ptr) {
        match i.to_string() {
            Ok(s) => s,
            Err(_) => {
                return -3;
            }
        }
    } else {
        return -3;
    };

    let payload = if let Ok(i) = WasmBuffer::from_raw(payload_ptr) {
        i.to_bytes()
    } else {
        return -4;
    };

    match has_permission(Request::new(roles, org_id, public_key, payload)) {
        Ok(r) => {
            if r {
                1
            } else {
                0
            }
        }
        Err(WasmSdkError::StateSetError(_)) => -5,
        Err(WasmSdkError::AllocError(_)) => -6,
        Err(WasmSdkError::MemoryWriteError(_)) => -7,
        Err(WasmSdkError::MemoryRetrievalError(_)) => -8,
        Err(WasmSdkError::Utf8EncodeError(_)) => -9,
        Err(WasmSdkError::ProtobufError(_)) => -10,
        Err(WasmSdkError::InvalidTransaction(_)) => -11,
        Err(WasmSdkError::InternalError(_)) => -12,
    }
}

/// A WasmBuffer is a wrapper around a wasm pointer.
///
/// It contains a raw wasm pointer to location in executor
/// memory and a bytes repesentation of it's contents.
///
/// It offers methods for accessing the data stored at the
/// location referenced by the raw pointer.
///
pub struct WasmBuffer {
    raw: WasmPtr,
    data: Vec<u8>,
}

impl WasmBuffer {
    pub unsafe fn new(buffer: &[u8]) -> Result<WasmBuffer, WasmSdkError> {
        let raw = externs::alloc(buffer.len());

        if raw < 0 {
            return Err(WasmSdkError::AllocError(
                "Failed to allocate host memory".into(),
            ));
        }

        for (i, byte) in buffer.iter().enumerate() {
            if externs::write_byte(raw, i as u32, *byte) < 0 {
                return Err(WasmSdkError::MemoryWriteError(
                    "Failed to write data to host memory".into(),
                ));
            }
        }

        Ok(WasmBuffer {
            raw,
            data: buffer.to_vec(),
        })
    }

    pub unsafe fn from_raw(raw: WasmPtr) -> Result<WasmBuffer, WasmSdkError> {
        let data = ptr_to_vec(raw)?.unwrap_or_default();
        Ok(WasmBuffer { raw, data })
    }

    pub unsafe fn from_list(ptr: WasmPtrList) -> Result<Vec<WasmBuffer>, WasmSdkError> {
        let mut wasm_buffers = Vec::new();

        if ptr >= 0 {
            for i in 0..externs::get_ptr_collection_len(ptr) {
                let ptr = externs::get_ptr_from_collection(ptr, i as u32);

                if ptr < 0 {
                    return Err(WasmSdkError::MemoryRetrievalError(
                        "pointer not found".into(),
                    ));
                }
                wasm_buffers.push(WasmBuffer::from_raw(ptr)?);
            }
        }

        Ok(wasm_buffers)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.data.clone()
    }

    pub fn to_raw(&self) -> WasmPtr {
        self.raw
    }

    pub fn to_string(&self) -> Result<String, WasmSdkError> {
        String::from_utf8(self.data.clone()).map_err(WasmSdkError::from)
    }
}

#[derive(Debug)]
pub enum WasmSdkError {
    InvalidTransaction(String),
    InternalError(String),
    StateSetError(String),
    AllocError(String),
    MemoryWriteError(String),
    MemoryRetrievalError(String),
    Utf8EncodeError(FromUtf8Error),
    ProtobufError(protobuf::ProtobufError),
}

impl std::fmt::Display for WasmSdkError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            WasmSdkError::InvalidTransaction(ref s) => write!(f, "InvalidTransactio: {}", s),
            WasmSdkError::InternalError(ref s) => write!(f, "InternalError: {}", s),
            WasmSdkError::StateSetError(ref s) => write!(f, "StateSetError: {}", s),
            WasmSdkError::AllocError(ref s) => write!(f, "AllocError: {}", s),
            WasmSdkError::MemoryWriteError(ref s) => write!(f, "MemoryWriteError: {}", s),
            WasmSdkError::MemoryRetrievalError(ref s) => write!(f, "MemoryRetrievalError: {}", s),
            WasmSdkError::Utf8EncodeError(ref err) => {
                write!(f, "Utf8EncodeError: {}", err)
            }
            WasmSdkError::ProtobufError(ref err) => {
                write!(f, "ProtobufError: {}", err)
            }
        }
    }
}

impl From<FromUtf8Error> for WasmSdkError {
    fn from(e: FromUtf8Error) -> Self {
        WasmSdkError::Utf8EncodeError(e)
    }
}

impl From<protobuf::ProtobufError> for WasmSdkError {
    fn from(e: protobuf::ProtobufError) -> Self {
        WasmSdkError::ProtobufError(e)
    }
}

#[derive(Debug)]
pub enum ApplyError {
    InvalidTransaction(String),
    InternalError(String),
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ApplyError::InvalidTransaction(ref s) => write!(f, "InvalidTransaction: {}", s),
            ApplyError::InternalError(ref s) => write!(f, "InternalError: {}", s),
        }
    }
}

impl From<WasmSdkError> for ApplyError {
    fn from(e: WasmSdkError) -> Self {
        match e {
            WasmSdkError::InternalError(..) => ApplyError::InternalError(format!("{}", e)),
            _ => ApplyError::InvalidTransaction(format!("{}", e)),
        }
    }
}

unsafe fn ptr_to_vec(ptr: WasmPtr) -> Result<Option<Vec<u8>>, WasmSdkError> {
    let mut vec = Vec::new();

    for i in 0..externs::get_ptr_len(ptr) {
        vec.push(externs::read_byte(ptr as isize + i));
    }

    if vec.is_empty() {
        return Ok(None);
    }
    Ok(Some(vec))
}

#[derive(PartialOrd, PartialEq, Copy, Clone)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

pub fn log_message(log_level: LogLevel, log_string: String) {
    unsafe {
        // WasmBuffer was created properly, log message otherwise ignore
        if let Ok(log_buffer) = WasmBuffer::new(log_string.as_bytes()) {
            match log_level {
                LogLevel::Trace => externs::log_buffer(4 as i32, log_buffer.to_raw()),
                LogLevel::Debug => externs::log_buffer(3 as i32, log_buffer.to_raw()),
                LogLevel::Info => externs::log_buffer(2 as i32, log_buffer.to_raw()),
                LogLevel::Warn => externs::log_buffer(1 as i32, log_buffer.to_raw()),
                LogLevel::Error => externs::log_buffer(0 as i32, log_buffer.to_raw()),
            };
        }
    }
}

pub fn log_level() -> LogLevel {
    unsafe {
        match externs::log_level() {
            4 => LogLevel::Trace,
            3 => LogLevel::Debug,
            2 => LogLevel::Info,
            1 => LogLevel::Warn,
            _ => LogLevel::Error,
        }
    }
}

pub fn log_enabled(lvl: LogLevel) -> bool {
    lvl >= log_level()
}

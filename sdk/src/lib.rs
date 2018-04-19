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

extern crate protobuf;
mod externs;

use std::error::Error;
use std::string::FromUtf8Error;
pub use externs::{WasmPtr, WasmPtrList};

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
}

impl<'a> TpProcessRequest<'a> {
    pub fn new(payload: Vec<u8>, header: &'a mut Header) -> TpProcessRequest {
        TpProcessRequest { payload, header }
    }

    pub fn get_payload(&self) -> &[u8] {
        return &self.payload;
    }

    pub fn get_header(&self) -> &Header {
        return self.header;
    }
}

pub struct TransactionContext {}

impl TransactionContext {
    pub fn new() -> TransactionContext {
        TransactionContext {}
    }
    pub fn get_state(&self, address: &str) -> Result<Option<Vec<u8>>, WasmSdkError> {
        unsafe {
            let wasm_buffer = WasmBuffer::new(address.to_string().as_bytes())?;
            ptr_to_vec(externs::get_state(wasm_buffer.into_raw()))
        }
    }

    pub fn set_state(&self, address: &str, state: &[u8]) -> Result<(), WasmSdkError> {
        unsafe {
            let wasm_address_buffer = WasmBuffer::new(address.to_string().as_bytes())?;
            let wasm_state_buffer = WasmBuffer::new(state)?;
            ptr_to_vec(externs::set_state(
                wasm_address_buffer.into_raw(),
                wasm_state_buffer.into_raw(),
            ))?;
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
        context: &mut TransactionContext,
    ) -> Result<(), ApplyError>;
}

/// -1: Failed to deserialize payload
/// -2: Failed to deserialize signer
/// -3: apply returned InvalidTransaction
/// -4: apply returned InternalError
pub unsafe fn execute_entrypoint<F>(payload_ptr: WasmPtr, signer_ptr: WasmPtr, apply: F) -> i32
where
    F: Fn(&TpProcessRequest, &mut TransactionContext) -> Result<bool, ApplyError>,
{
    let payload = if let Ok(i) = WasmBuffer::from_raw(payload_ptr) {
        i.into_bytes()
    } else {
        return -1;
    };

    let signer = if let Ok(i) = WasmBuffer::from_raw(signer_ptr) {
        match i.into_string() {
            Ok(s) => s,
            Err(_) => return -2,
        }
    } else {
        return -1;
    };

    let mut header = Header::new(signer);
    match apply(
        &TpProcessRequest::new(payload, &mut header),
        &mut TransactionContext::new(),
    ) {
        Ok(r) => if r {
            1
        } else {
            0
        },
        Err(ApplyError::InvalidTransaction(_)) => -3,
        Err(ApplyError::InternalError(_)) => -4,
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

        for i in 0..buffer.len() {
            if externs::write_byte(raw, i as u32, buffer[i]) < 0 {
                return Err(WasmSdkError::MemoryWriteError(
                    "Failed to write data to host memory".into(),
                ));
            }
        }

        Ok(WasmBuffer {
            raw,
            data: buffer.clone().to_vec(),
        })
    }

    pub unsafe fn from_raw(raw: WasmPtr) -> Result<WasmBuffer, WasmSdkError> {
        let data = ptr_to_vec(raw)?.unwrap_or(Vec::new());
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

    pub fn into_bytes(&self) -> Vec<u8> {
        self.data.clone()
    }

    pub fn into_raw(&self) -> WasmPtr {
        self.raw
    }

    pub fn into_string(&self) -> Result<String, WasmSdkError> {
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
                write!(f, "Utf8EncodeError: {}", err.description())
            }
            WasmSdkError::ProtobufError(ref err) => {
                write!(f, "ProtobufError: {}", err.description())
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
            ApplyError::InvalidTransaction(ref s) => write!(f, "InvalidTransactio: {}", s),
            ApplyError::InternalError(ref s) => write!(f, "InternalError: {}", s),
        }
    }
}

impl From<WasmSdkError> for ApplyError {
    fn from(e: WasmSdkError) -> Self {
        match e {
            WasmSdkError::InternalError(..) =>
                ApplyError::InternalError(format!("{}", e)),
            _ => ApplyError::InvalidTransaction(format!("{}", e)),
        }
    }
}

unsafe fn set_state(address: String, state: &[u8]) -> Result<(), WasmSdkError> {
    let addr_ptr = WasmBuffer::new(address.as_bytes())?.into_raw();
    let state_ptr = WasmBuffer::new(state)?.into_raw();

    let result = externs::set_state(addr_ptr, state_ptr);

    if result == 1 {
        Ok(())
    } else {
        Err(WasmSdkError::StateSetError("New state was not set".into()))
    }
}

unsafe fn ptr_to_vec(ptr: WasmPtr) -> Result<Option<Vec<u8>>, WasmSdkError> {
    let mut vec = Vec::new();

    for i in 0..externs::get_ptr_len(ptr) {
        vec.push(externs::read_byte(ptr as isize + i));
    }

    if vec.len() == 0 {
        return Ok(None);
    }
    Ok(Some(vec))
}

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

use std::error::Error as StdError;

use protobuf::Message;
use protobuf::RepeatedField;

use crate::protos::{
    self, FromBytes, FromNative, FromProto, IntoBytes, IntoNative, IntoProto, ProtoConversionError,
};

/// Native implementation for ValueType
#[derive(Debug, Clone, PartialEq)]
pub enum ValueType {
    Int64(i64),
    Int32(i32),
    UInt64(u64),
    UInt32(u32),
    String(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct StateEntryValue {
    key: String,
    value: ValueType,
}

impl StateEntryValue {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &ValueType {
        &self.value
    }
}

impl FromProto<protos::key_value_state::StateEntryValue> for StateEntryValue {
    fn from_proto(
        proto: protos::key_value_state::StateEntryValue,
    ) -> Result<Self, ProtoConversionError> {
        let value = match proto.get_value_type() {
            protos::key_value_state::StateEntryValue_ValueType::INT64 => {
                ValueType::Int64(proto.get_int64_value())
            }
            protos::key_value_state::StateEntryValue_ValueType::INT32 => {
                ValueType::Int32(proto.get_int32_value())
            }
            protos::key_value_state::StateEntryValue_ValueType::UINT64 => {
                ValueType::UInt64(proto.get_uint64_value())
            }
            protos::key_value_state::StateEntryValue_ValueType::UINT32 => {
                ValueType::UInt32(proto.get_uint32_value())
            }
            protos::key_value_state::StateEntryValue_ValueType::STRING => {
                ValueType::String(proto.get_string_value().to_string())
            }
            protos::key_value_state::StateEntryValue_ValueType::BYTES => {
                ValueType::Bytes(proto.get_bytes_value().to_vec())
            }
            protos::key_value_state::StateEntryValue_ValueType::TYPE_UNSET => {
                return Err(ProtoConversionError::InvalidTypeError(
                    "Cannot convert StateEntryValue_ValueType with type unset.".to_string(),
                ));
            }
        };

        Ok(StateEntryValue {
            key: proto.get_key().to_string(),
            value,
        })
    }
}

impl FromNative<StateEntryValue> for protos::key_value_state::StateEntryValue {
    fn from_native(native: StateEntryValue) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::key_value_state::StateEntryValue::new();
        proto.set_key(native.key);
        match native.value {
            ValueType::Int64(value) => {
                proto.set_value_type(protos::key_value_state::StateEntryValue_ValueType::INT64);
                proto.set_int64_value(value);
            }
            ValueType::Int32(value) => {
                proto.set_value_type(protos::key_value_state::StateEntryValue_ValueType::INT32);
                proto.set_int32_value(value);
            }
            ValueType::UInt64(value) => {
                proto.set_value_type(protos::key_value_state::StateEntryValue_ValueType::UINT64);
                proto.set_uint64_value(value);
            }
            ValueType::UInt32(value) => {
                proto.set_value_type(protos::key_value_state::StateEntryValue_ValueType::UINT32);
                proto.set_uint32_value(value);
            }
            ValueType::String(value) => {
                proto.set_value_type(protos::key_value_state::StateEntryValue_ValueType::STRING);
                proto.set_string_value(value);
            }
            ValueType::Bytes(value) => {
                proto.set_value_type(protos::key_value_state::StateEntryValue_ValueType::BYTES);
                proto.set_bytes_value(value);
            }
        }

        Ok(proto)
    }
}

impl FromBytes<StateEntryValue> for StateEntryValue {
    fn from_bytes(bytes: &[u8]) -> Result<StateEntryValue, ProtoConversionError> {
        let proto: protos::key_value_state::StateEntryValue = protobuf::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get StateEntryValue from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for StateEntryValue {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto: protos::key_value_state::StateEntryValue = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from StateEntryValue".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::key_value_state::StateEntryValue> for StateEntryValue {}
impl IntoNative<StateEntryValue> for protos::key_value_state::StateEntryValue {}

#[derive(Debug)]
pub enum StateEntryValueBuildError {
    MissingField(String),
}

impl StdError for StateEntryValueBuildError {
    fn cause(&self) -> Option<&dyn StdError> {
        match *self {
            StateEntryValueBuildError::MissingField(_) => None,
        }
    }
}

impl std::fmt::Display for StateEntryValueBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            StateEntryValueBuildError::MissingField(ref s) => {
                write!(f, "'{}' field is required", s)
            }
        }
    }
}

/// Builder used to create StateEntryValue
#[derive(Default, Clone, Debug)]
pub struct StateEntryValueBuilder {
    key: Option<String>,
    value: Option<ValueType>,
}

impl StateEntryValueBuilder {
    pub fn new() -> Self {
        StateEntryValueBuilder::default()
    }

    pub fn with_key(mut self, key: String) -> StateEntryValueBuilder {
        self.key = Some(key);
        self
    }

    pub fn with_value(mut self, value: ValueType) -> StateEntryValueBuilder {
        self.value = Some(value);
        self
    }

    pub fn build(self) -> Result<StateEntryValue, StateEntryValueBuildError> {
        let key = self
            .key
            .ok_or_else(|| StateEntryValueBuildError::MissingField("key".to_string()))?;

        let value = self
            .value
            .ok_or_else(|| StateEntryValueBuildError::MissingField("value".to_string()))?;

        Ok(StateEntryValue { key, value })
    }
}

/// Native implementation for StateEntry
#[derive(Default, Debug, Clone, PartialEq)]
pub struct StateEntry {
    normalized_key: String,
    state_entry_values: Vec<StateEntryValue>,
}

impl StateEntry {
    pub fn normalized_key(&self) -> &str {
        &self.normalized_key
    }

    pub fn state_entry_values(&self) -> &[StateEntryValue] {
        &self.state_entry_values
    }
}

impl FromProto<protos::key_value_state::StateEntry> for StateEntry {
    fn from_proto(
        proto: protos::key_value_state::StateEntry,
    ) -> Result<Self, ProtoConversionError> {
        Ok(StateEntry {
            normalized_key: proto.get_normalized_key().to_string(),
            state_entry_values: proto
                .get_state_entry_values()
                .to_vec()
                .into_iter()
                .map(StateEntryValue::from_proto)
                .collect::<Result<Vec<StateEntryValue>, ProtoConversionError>>()?,
        })
    }
}

impl FromNative<StateEntry> for protos::key_value_state::StateEntry {
    fn from_native(state_entry: StateEntry) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::key_value_state::StateEntry::new();
        proto.set_normalized_key(state_entry.normalized_key().to_string());
        proto.set_state_entry_values(RepeatedField::from_vec(
            state_entry
                .state_entry_values()
                .to_vec()
                .into_iter()
                .map(StateEntryValue::into_proto)
                .collect::<Result<
                    Vec<protos::key_value_state::StateEntryValue>,
                    ProtoConversionError,
                >>()?,
        ));

        Ok(proto)
    }
}

impl FromBytes<StateEntry> for StateEntry {
    fn from_bytes(bytes: &[u8]) -> Result<StateEntry, ProtoConversionError> {
        let proto: protos::key_value_state::StateEntry = protobuf::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get StateEntry from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for StateEntry {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from StateEntry".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::key_value_state::StateEntry> for StateEntry {}
impl IntoNative<StateEntry> for protos::key_value_state::StateEntry {}

#[derive(Debug)]
pub enum StateEntryBuildError {
    MissingField(String),
}

impl StdError for StateEntryBuildError {
    fn cause(&self) -> Option<&dyn StdError> {
        match *self {
            StateEntryBuildError::MissingField(_) => None,
        }
    }
}

impl std::fmt::Display for StateEntryBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            StateEntryBuildError::MissingField(ref s) => write!(f, "'{}' field is required", s),
        }
    }
}

/// Builder used to create StateEntry
#[derive(Default, Clone, Debug)]
pub struct StateEntryBuilder {
    normalized_key: Option<String>,
    state_entry_values: Option<Vec<StateEntryValue>>,
}

impl StateEntryBuilder {
    pub fn new() -> Self {
        StateEntryBuilder::default()
    }

    pub fn with_normalized_key(mut self, normalized_key: String) -> StateEntryBuilder {
        self.normalized_key = Some(normalized_key);
        self
    }

    pub fn with_state_entry_values(
        mut self,
        state_entry_values: Vec<StateEntryValue>,
    ) -> StateEntryBuilder {
        self.state_entry_values = Some(state_entry_values);
        self
    }

    pub fn build(self) -> Result<StateEntry, StateEntryBuildError> {
        let normalized_key = self
            .normalized_key
            .ok_or_else(|| StateEntryBuildError::MissingField("normalized_key".to_string()))?;
        let state_entry_values = self
            .state_entry_values
            .ok_or_else(|| StateEntryBuildError::MissingField("state_entry_values".to_string()))?;

        Ok(StateEntry {
            normalized_key,
            state_entry_values,
        })
    }
}

/// Native implementation for StateEntryList
#[derive(Default, Debug)]
pub struct StateEntryList {
    entries: Vec<StateEntry>,
}

impl StateEntryList {
    pub fn entries(&self) -> &[StateEntry] {
        &self.entries
    }

    pub fn contains(&self, normalized_key: String) -> bool {
        self.entries()
            .to_vec()
            .into_iter()
            .any(|e| e.normalized_key() == normalized_key)
    }
}

impl FromProto<protos::key_value_state::StateEntryList> for StateEntryList {
    fn from_proto(
        proto: protos::key_value_state::StateEntryList,
    ) -> Result<Self, ProtoConversionError> {
        Ok(StateEntryList {
            entries: proto
                .get_entries()
                .to_vec()
                .into_iter()
                .map(StateEntry::from_proto)
                .collect::<Result<Vec<StateEntry>, ProtoConversionError>>()?,
        })
    }
}

impl FromNative<StateEntryList> for protos::key_value_state::StateEntryList {
    fn from_native(state_entry_list: StateEntryList) -> Result<Self, ProtoConversionError> {
        let mut proto = protos::key_value_state::StateEntryList::new();
        proto.set_entries(RepeatedField::from_vec(
            state_entry_list
                .entries()
                .to_vec()
                .into_iter()
                .map(StateEntry::into_proto)
                .collect::<Result<Vec<protos::key_value_state::StateEntry>, ProtoConversionError>>(
                )?,
        ));

        Ok(proto)
    }
}

impl FromBytes<StateEntryList> for StateEntryList {
    fn from_bytes(bytes: &[u8]) -> Result<StateEntryList, ProtoConversionError> {
        let proto: protos::key_value_state::StateEntryList = protobuf::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get StateEntryList from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for StateEntryList {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from StateEntryList".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::key_value_state::StateEntryList> for StateEntryList {}
impl IntoNative<StateEntryList> for protos::key_value_state::StateEntryList {}

#[derive(Debug)]
pub enum StateEntryListBuildError {
    MissingField(String),
}

impl StdError for StateEntryListBuildError {
    fn cause(&self) -> Option<&dyn StdError> {
        match *self {
            StateEntryListBuildError::MissingField(_) => None,
        }
    }
}

impl std::fmt::Display for StateEntryListBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            StateEntryListBuildError::MissingField(ref s) => write!(f, "'{}' field is required", s),
        }
    }
}

/// Builder used to create StateEntryList
#[derive(Default, Clone)]
pub struct StateEntryListBuilder {
    entries: Option<Vec<StateEntry>>,
}

impl StateEntryListBuilder {
    pub fn new() -> Self {
        StateEntryListBuilder::default()
    }

    pub fn with_state_entries(mut self, entries: Vec<StateEntry>) -> StateEntryListBuilder {
        self.entries = Some(entries);
        self
    }

    pub fn build(self) -> Result<StateEntryList, StateEntryListBuildError> {
        let entries = self
            .entries
            .ok_or_else(|| StateEntryListBuildError::MissingField("entries".to_string()))?;

        Ok(StateEntryList { entries })
    }
}

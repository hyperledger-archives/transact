/*
 * Copyright 2019 Cargill Incorporated
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -----------------------------------------------------------------------------
 */

use protobuf::Message;
use protobuf::RepeatedField;

use crate::protos::{
    self, FromBytes, FromNative, FromProto, IntoBytes, IntoNative, IntoProto, ProtoConversionError,
};

#[derive(Debug)]
pub struct CommandPayload {
    commands: Vec<Command>,
}

impl CommandPayload {
    pub fn new(commands: Vec<Command>) -> Self {
        CommandPayload { commands }
    }

    pub fn commands(&self) -> &[Command] {
        &self.commands
    }
}

impl FromProto<protos::command::CommandPayload> for CommandPayload {
    fn from_proto(payload: protos::command::CommandPayload) -> Result<Self, ProtoConversionError> {
        Ok(CommandPayload {
            commands: payload
                .get_commands()
                .to_vec()
                .into_iter()
                .map(Command::from_proto)
                .collect::<Result<Vec<Command>, ProtoConversionError>>()?,
        })
    }
}

impl FromNative<CommandPayload> for protos::command::CommandPayload {
    fn from_native(payload: CommandPayload) -> Result<Self, ProtoConversionError> {
        let mut proto_payload = protos::command::CommandPayload::new();
        proto_payload.set_commands(RepeatedField::from_vec(
            payload
                .commands()
                .to_vec()
                .into_iter()
                .map(Command::into_proto)
                .collect::<Result<Vec<protos::command::Command>, ProtoConversionError>>()?,
        ));

        Ok(proto_payload)
    }
}

impl FromBytes<CommandPayload> for CommandPayload {
    fn from_bytes(bytes: &[u8]) -> Result<CommandPayload, ProtoConversionError> {
        let proto: protos::command::CommandPayload =
            protobuf::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get CommandPayload from byte".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for CommandPayload {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from CommandPayload".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::command::CommandPayload> for CommandPayload {}
impl IntoNative<CommandPayload> for protos::command::CommandPayload {}

/// A command to be executed
#[derive(Debug, Clone)]
pub enum Command {
    SetState(SetState),
    DeleteState(DeleteState),
    GetState(GetState),
    AddEvent(AddEvent),
    AddReceiptData(AddReceiptData),
    Sleep(Sleep),
    ReturnInvalid(ReturnInvalid),
    ReturnInternalError(ReturnInternalError),
}

impl FromProto<protos::command::Command> for Command {
    fn from_proto(command: protos::command::Command) -> Result<Self, ProtoConversionError> {
        match command.get_command_type() {
            protos::command::Command_CommandType::SET_STATE => Ok(Command::SetState(
                SetState::from_proto(command.get_set_state().clone())?,
            )),
            protos::command::Command_CommandType::GET_STATE => Ok(Command::GetState(
                GetState::from_proto(command.get_get_state().clone())?,
            )),
            protos::command::Command_CommandType::DELETE_STATE => Ok(Command::DeleteState(
                DeleteState::from_proto(command.get_delete_state().clone())?,
            )),
            protos::command::Command_CommandType::ADD_EVENT => Ok(Command::AddEvent(
                AddEvent::from_proto(command.get_add_event().clone())?,
            )),
            protos::command::Command_CommandType::ADD_RECEIPT_DATA => Ok(Command::AddReceiptData(
                AddReceiptData::from_proto(command.get_add_receipt_data().clone())?,
            )),
            protos::command::Command_CommandType::SLEEP => Ok(Command::Sleep(Sleep::from_proto(
                command.get_sleep().clone(),
            )?)),
            protos::command::Command_CommandType::RETURN_INVALID => Ok(Command::ReturnInvalid(
                ReturnInvalid::from_proto(command.get_return_invalid().clone())?,
            )),
            protos::command::Command_CommandType::RETURN_INTERNAL_ERROR => {
                Ok(Command::ReturnInternalError(
                    ReturnInternalError::from_proto(command.get_return_internal_error().clone())?,
                ))
            }
            _ => Err(ProtoConversionError::InvalidTypeError(
                "Cannot convert Command_CommandType with type unset.".to_string(),
            )),
        }
    }
}

impl FromNative<Command> for protos::command::Command {
    fn from_native(command: Command) -> Result<Self, ProtoConversionError> {
        let mut proto_command = protos::command::Command::new();

        match command {
            Command::SetState(payload) => {
                proto_command.set_command_type(protos::command::Command_CommandType::SET_STATE);
                proto_command.set_set_state(payload.into_proto()?);
                Ok(proto_command)
            }
            Command::GetState(payload) => {
                proto_command.set_command_type(protos::command::Command_CommandType::GET_STATE);
                proto_command.set_get_state(payload.into_proto()?);
                Ok(proto_command)
            }
            Command::DeleteState(payload) => {
                proto_command.set_command_type(protos::command::Command_CommandType::DELETE_STATE);
                proto_command.set_delete_state(payload.into_proto()?);
                Ok(proto_command)
            }
            Command::AddEvent(payload) => {
                proto_command.set_command_type(protos::command::Command_CommandType::ADD_EVENT);
                proto_command.set_add_event(payload.into_proto()?);
                Ok(proto_command)
            }
            Command::AddReceiptData(payload) => {
                proto_command
                    .set_command_type(protos::command::Command_CommandType::ADD_RECEIPT_DATA);
                proto_command.set_add_receipt_data(payload.into_proto()?);
                Ok(proto_command)
            }
            Command::Sleep(payload) => {
                proto_command.set_command_type(protos::command::Command_CommandType::SLEEP);
                proto_command.set_sleep(payload.into_proto()?);
                Ok(proto_command)
            }
            Command::ReturnInvalid(payload) => {
                proto_command
                    .set_command_type(protos::command::Command_CommandType::RETURN_INVALID);
                proto_command.set_return_invalid(payload.into_proto()?);
                Ok(proto_command)
            }
            Command::ReturnInternalError(payload) => {
                proto_command
                    .set_command_type(protos::command::Command_CommandType::RETURN_INTERNAL_ERROR);
                proto_command.set_return_internal_error(payload.into_proto()?);
                Ok(proto_command)
            }
        }
    }
}

impl FromBytes<Command> for Command {
    fn from_bytes(bytes: &[u8]) -> Result<Command, ProtoConversionError> {
        let proto: protos::command::Command = protobuf::parse_from_bytes(bytes).map_err(|_| {
            ProtoConversionError::SerializationError("Unable to get Command from bytes".to_string())
        })?;
        proto.into_native()
    }
}

impl IntoProto<protos::command::Command> for Command {}
impl IntoNative<Command> for protos::command::Command {}

/// Native implementation for BytesEntry
#[derive(Debug, Clone)]
pub struct BytesEntry {
    key: String,
    value: Vec<u8>,
}

impl BytesEntry {
    pub fn new(key: String, value: Vec<u8>) -> Self {
        BytesEntry { key, value }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

impl FromProto<protos::command::BytesEntry> for BytesEntry {
    fn from_proto(bytes_entry: protos::command::BytesEntry) -> Result<Self, ProtoConversionError> {
        Ok(BytesEntry {
            key: bytes_entry.get_key().to_string(),
            value: bytes_entry.get_value().to_vec(),
        })
    }
}

impl IntoBytes for Command {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError("Unable to get bytes from Command".to_string())
        })?;
        Ok(bytes)
    }
}

impl FromNative<BytesEntry> for protos::command::BytesEntry {
    fn from_native(bytes_entry: BytesEntry) -> Result<Self, ProtoConversionError> {
        let mut proto_bytes_entry = protos::command::BytesEntry::new();

        proto_bytes_entry.set_key(bytes_entry.key().to_string());
        proto_bytes_entry.set_value(bytes_entry.value().to_vec());

        Ok(proto_bytes_entry)
    }
}

impl FromBytes<BytesEntry> for BytesEntry {
    fn from_bytes(bytes: &[u8]) -> Result<BytesEntry, ProtoConversionError> {
        let proto: protos::command::BytesEntry =
            protobuf::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get BytesEntry from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for BytesEntry {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from BytesEntry".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::command::BytesEntry> for BytesEntry {}
impl IntoNative<BytesEntry> for protos::command::BytesEntry {}

/// Native implementation for SetState
#[derive(Debug, Clone)]
pub struct SetState {
    state_writes: Vec<BytesEntry>,
}

impl SetState {
    pub fn new(state_writes: Vec<BytesEntry>) -> Self {
        SetState { state_writes }
    }

    pub fn state_writes(&self) -> &[BytesEntry] {
        &self.state_writes
    }
}

impl FromProto<protos::command::SetState> for SetState {
    fn from_proto(set_state: protos::command::SetState) -> Result<Self, ProtoConversionError> {
        Ok(SetState {
            state_writes: set_state
                .get_state_writes()
                .to_vec()
                .into_iter()
                .map(BytesEntry::from_proto)
                .collect::<Result<Vec<BytesEntry>, ProtoConversionError>>()?,
        })
    }
}

impl FromNative<SetState> for protos::command::SetState {
    fn from_native(set_state: SetState) -> Result<Self, ProtoConversionError> {
        let mut proto_set_state = protos::command::SetState::new();

        proto_set_state.set_state_writes(RepeatedField::from_vec(
            set_state
                .state_writes()
                .to_vec()
                .into_iter()
                .map(BytesEntry::into_proto)
                .collect::<Result<Vec<protos::command::BytesEntry>, ProtoConversionError>>()?,
        ));

        Ok(proto_set_state)
    }
}

impl FromBytes<SetState> for SetState {
    fn from_bytes(bytes: &[u8]) -> Result<SetState, ProtoConversionError> {
        let proto: protos::command::SetState = protobuf::parse_from_bytes(bytes).map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get SetState from bytes".to_string(),
            )
        })?;
        proto.into_native()
    }
}

impl IntoBytes for SetState {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from SetState".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::command::SetState> for SetState {}
impl IntoNative<SetState> for protos::command::SetState {}

/// Native implementation for DeleteState
#[derive(Debug, Clone)]
pub struct DeleteState {
    state_keys: Vec<String>,
}

impl DeleteState {
    pub fn new(state_keys: Vec<String>) -> Self {
        DeleteState { state_keys }
    }

    pub fn state_keys(&self) -> &[String] {
        &self.state_keys
    }
}

impl FromProto<protos::command::DeleteState> for DeleteState {
    fn from_proto(
        delete_state: protos::command::DeleteState,
    ) -> Result<Self, ProtoConversionError> {
        Ok(DeleteState {
            state_keys: delete_state.get_state_keys().to_vec(),
        })
    }
}

impl FromNative<DeleteState> for protos::command::DeleteState {
    fn from_native(delete_state: DeleteState) -> Result<Self, ProtoConversionError> {
        let mut proto_delete_state = protos::command::DeleteState::new();
        proto_delete_state
            .set_state_keys(RepeatedField::from_vec(delete_state.state_keys().to_vec()));

        Ok(proto_delete_state)
    }
}

impl FromBytes<DeleteState> for DeleteState {
    fn from_bytes(bytes: &[u8]) -> Result<DeleteState, ProtoConversionError> {
        let proto: protos::command::DeleteState =
            protobuf::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get DeleteState from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for DeleteState {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from DeleteState".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::command::DeleteState> for DeleteState {}
impl IntoNative<DeleteState> for protos::command::DeleteState {}

/// Native implementation for GetState
#[derive(Debug, Clone)]
pub struct GetState {
    state_keys: Vec<String>,
}

impl GetState {
    pub fn new(state_keys: Vec<String>) -> Self {
        GetState { state_keys }
    }

    pub fn state_keys(&self) -> &[String] {
        &self.state_keys
    }
}

impl FromProto<protos::command::GetState> for GetState {
    fn from_proto(get_state: protos::command::GetState) -> Result<Self, ProtoConversionError> {
        Ok(GetState {
            state_keys: get_state.get_state_keys().to_vec(),
        })
    }
}

impl FromNative<GetState> for protos::command::GetState {
    fn from_native(get_state: GetState) -> Result<Self, ProtoConversionError> {
        let mut proto_get_state = protos::command::GetState::new();
        proto_get_state.set_state_keys(RepeatedField::from_vec(get_state.state_keys().to_vec()));

        Ok(proto_get_state)
    }
}

impl FromBytes<GetState> for GetState {
    fn from_bytes(bytes: &[u8]) -> Result<GetState, ProtoConversionError> {
        let proto: protos::command::GetState = protobuf::parse_from_bytes(bytes).map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get GetState from bytes".to_string(),
            )
        })?;
        proto.into_native()
    }
}

impl IntoBytes for GetState {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from GetState".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::command::GetState> for GetState {}
impl IntoNative<GetState> for protos::command::GetState {}

/// Native implementation for AddEvent
#[derive(Debug, Clone)]
pub struct AddEvent {
    event_type: String,
    attributes: Vec<BytesEntry>,
    data: Vec<u8>,
}

impl AddEvent {
    pub fn new(event_type: String, attributes: Vec<BytesEntry>, data: Vec<u8>) -> Self {
        AddEvent {
            event_type,
            attributes,
            data,
        }
    }

    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    pub fn attributes(&self) -> &[BytesEntry] {
        &self.attributes
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

impl FromProto<protos::command::AddEvent> for AddEvent {
    fn from_proto(add_event: protos::command::AddEvent) -> Result<Self, ProtoConversionError> {
        Ok(AddEvent {
            event_type: add_event.get_event_type().to_string(),
            attributes: add_event
                .get_attributes()
                .to_vec()
                .into_iter()
                .map(BytesEntry::from_proto)
                .collect::<Result<Vec<BytesEntry>, ProtoConversionError>>()?,
            data: add_event.get_data().to_vec(),
        })
    }
}

impl FromNative<AddEvent> for protos::command::AddEvent {
    fn from_native(add_event: AddEvent) -> Result<Self, ProtoConversionError> {
        let mut proto_add_event = protos::command::AddEvent::new();

        proto_add_event.set_event_type(add_event.event_type().to_string());
        proto_add_event.set_attributes(RepeatedField::from_vec(
            add_event
                .attributes()
                .to_vec()
                .into_iter()
                .map(BytesEntry::into_proto)
                .collect::<Result<Vec<protos::command::BytesEntry>, ProtoConversionError>>()?,
        ));
        proto_add_event.set_data(add_event.data().to_vec());

        Ok(proto_add_event)
    }
}

impl FromBytes<AddEvent> for AddEvent {
    fn from_bytes(bytes: &[u8]) -> Result<AddEvent, ProtoConversionError> {
        let proto: protos::command::AddEvent = protobuf::parse_from_bytes(bytes).map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get AddEvent from bytes".to_string(),
            )
        })?;
        proto.into_native()
    }
}

impl IntoBytes for AddEvent {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from AddEvent".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::command::AddEvent> for AddEvent {}
impl IntoNative<AddEvent> for protos::command::AddEvent {}

/// Native implementation for AddReceiptData
#[derive(Debug, Clone)]
pub struct AddReceiptData {
    receipt_data: Vec<u8>,
}

impl AddReceiptData {
    pub fn new(receipt_data: Vec<u8>) -> Self {
        AddReceiptData { receipt_data }
    }

    pub fn receipt_data(&self) -> &[u8] {
        &self.receipt_data
    }
}

impl FromProto<protos::command::AddReceiptData> for AddReceiptData {
    fn from_proto(
        add_receipt_data: protos::command::AddReceiptData,
    ) -> Result<Self, ProtoConversionError> {
        Ok(AddReceiptData {
            receipt_data: add_receipt_data.get_receipt_data().to_vec(),
        })
    }
}

impl FromNative<AddReceiptData> for protos::command::AddReceiptData {
    fn from_native(add_receipt_data: AddReceiptData) -> Result<Self, ProtoConversionError> {
        let mut proto_add_receipt_data = protos::command::AddReceiptData::new();
        proto_add_receipt_data.set_receipt_data(add_receipt_data.receipt_data().to_vec());
        Ok(proto_add_receipt_data)
    }
}

impl FromBytes<AddReceiptData> for AddReceiptData {
    fn from_bytes(bytes: &[u8]) -> Result<AddReceiptData, ProtoConversionError> {
        let proto: protos::command::AddReceiptData =
            protobuf::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get AddReceiptData from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for AddReceiptData {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from AddReceiptData".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::command::AddReceiptData> for AddReceiptData {}
impl IntoNative<AddReceiptData> for protos::command::AddReceiptData {}

#[derive(Debug, Clone)]
pub enum SleepType {
    Wait,
    BusyWait,
}

impl FromProto<protos::command::Sleep_SleepType> for SleepType {
    fn from_proto(
        sleep_type: protos::command::Sleep_SleepType,
    ) -> Result<Self, ProtoConversionError> {
        match sleep_type {
            protos::command::Sleep_SleepType::WAIT => Ok(SleepType::Wait),
            protos::command::Sleep_SleepType::BUSY_WAIT => Ok(SleepType::BusyWait),
        }
    }
}

impl FromNative<SleepType> for protos::command::Sleep_SleepType {
    fn from_native(sleep_type: SleepType) -> Result<Self, ProtoConversionError> {
        match sleep_type {
            SleepType::Wait => Ok(protos::command::Sleep_SleepType::WAIT),
            SleepType::BusyWait => Ok(protos::command::Sleep_SleepType::BUSY_WAIT),
        }
    }
}

impl IntoProto<protos::command::Sleep_SleepType> for SleepType {}
impl IntoNative<SleepType> for protos::command::Sleep_SleepType {}

/// Native implementation for Sleep
#[derive(Debug, Clone)]
pub struct Sleep {
    duration_millis: u32,
    sleep_type: SleepType,
}

impl Sleep {
    pub fn new(duration_millis: u32, sleep_type: SleepType) -> Self {
        Sleep {
            duration_millis,
            sleep_type,
        }
    }

    pub fn duration_millis(&self) -> &u32 {
        &self.duration_millis
    }

    pub fn sleep_type(&self) -> &SleepType {
        &self.sleep_type
    }
}

impl FromProto<protos::command::Sleep> for Sleep {
    fn from_proto(sleep: protos::command::Sleep) -> Result<Self, ProtoConversionError> {
        Ok(Sleep {
            duration_millis: sleep.get_duration_millis(),
            sleep_type: SleepType::from_proto(sleep.get_sleep_type())?,
        })
    }
}

impl FromNative<Sleep> for protos::command::Sleep {
    fn from_native(sleep: Sleep) -> Result<Self, ProtoConversionError> {
        let mut proto_sleep = protos::command::Sleep::new();
        proto_sleep.set_duration_millis(*sleep.duration_millis());
        proto_sleep.set_sleep_type(sleep.sleep_type().clone().into_proto()?);
        Ok(proto_sleep)
    }
}

impl FromBytes<Sleep> for Sleep {
    fn from_bytes(bytes: &[u8]) -> Result<Sleep, ProtoConversionError> {
        let proto: protos::command::Sleep = protobuf::parse_from_bytes(bytes).map_err(|_| {
            ProtoConversionError::SerializationError("Unable to get bytes from Sleep".to_string())
        })?;
        proto.into_native()
    }
}

impl IntoBytes for Sleep {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError("Unable to get bytes from Sleep".to_string())
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::command::Sleep> for Sleep {}
impl IntoNative<Sleep> for protos::command::Sleep {}

/// Native implementation for ReturnInvalid
#[derive(Debug, Clone)]
pub struct ReturnInvalid {
    error_message: String,
}

impl ReturnInvalid {
    pub fn new(error_message: String) -> Self {
        ReturnInvalid { error_message }
    }

    pub fn error_message(&self) -> &str {
        &self.error_message
    }
}

impl FromProto<protos::command::ReturnInvalid> for ReturnInvalid {
    fn from_proto(
        return_invalid: protos::command::ReturnInvalid,
    ) -> Result<Self, ProtoConversionError> {
        Ok(ReturnInvalid {
            error_message: return_invalid.get_error_message().to_string(),
        })
    }
}

impl FromNative<ReturnInvalid> for protos::command::ReturnInvalid {
    fn from_native(return_invalid: ReturnInvalid) -> Result<Self, ProtoConversionError> {
        let mut proto_return_invalid = protos::command::ReturnInvalid::new();
        proto_return_invalid.set_error_message(return_invalid.error_message().to_string());
        Ok(proto_return_invalid)
    }
}

impl FromBytes<ReturnInvalid> for ReturnInvalid {
    fn from_bytes(bytes: &[u8]) -> Result<ReturnInvalid, ProtoConversionError> {
        let proto: protos::command::ReturnInvalid =
            protobuf::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get ReturnInvalid from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for ReturnInvalid {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from ReturnInvalid".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::command::ReturnInvalid> for ReturnInvalid {}
impl IntoNative<ReturnInvalid> for protos::command::ReturnInvalid {}

/// Native implementation for ReturnInternalError
#[derive(Debug, Clone)]
pub struct ReturnInternalError {
    error_message: String,
}

impl ReturnInternalError {
    pub fn new(error_message: String) -> Self {
        ReturnInternalError { error_message }
    }

    pub fn error_message(&self) -> &str {
        &self.error_message
    }
}

impl FromProto<protos::command::ReturnInternalError> for ReturnInternalError {
    fn from_proto(
        return_internal_error: protos::command::ReturnInternalError,
    ) -> Result<Self, ProtoConversionError> {
        Ok(ReturnInternalError {
            error_message: return_internal_error.get_error_message().to_string(),
        })
    }
}

impl FromNative<ReturnInternalError> for protos::command::ReturnInternalError {
    fn from_native(
        return_internal_error: ReturnInternalError,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto_return_internal_error = protos::command::ReturnInternalError::new();
        proto_return_internal_error
            .set_error_message(return_internal_error.error_message().to_string());
        Ok(proto_return_internal_error)
    }
}

impl FromBytes<ReturnInternalError> for ReturnInternalError {
    fn from_bytes(bytes: &[u8]) -> Result<ReturnInternalError, ProtoConversionError> {
        let proto: protos::command::ReturnInternalError = protobuf::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get ReturnInvalid from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoBytes for ReturnInternalError {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from ReturnInternalError".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::command::ReturnInternalError> for ReturnInternalError {}
impl IntoNative<ReturnInternalError> for protos::command::ReturnInternalError {}

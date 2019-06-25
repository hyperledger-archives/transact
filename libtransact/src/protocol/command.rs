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

#[derive(Debug, Clone)]
pub enum SleepType {
    Wait,
    BusyWait,
}

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

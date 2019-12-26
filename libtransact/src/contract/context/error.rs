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

use crate::contract::address::AddresserError;
use crate::handler::ContextError;
use crate::protos::ProtoConversionError;

#[derive(Debug)]
pub enum ContractContextError {
    AddresserError(AddresserError),
    ProtoConversionError(ProtoConversionError),
    ProtocolBuildError(Box<dyn StdError>),
    TransactionContextError(ContextError),
}

impl StdError for ContractContextError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match *self {
            ContractContextError::AddresserError(_) => None,
            ContractContextError::ProtoConversionError(ref err) => Some(err),
            ContractContextError::ProtocolBuildError(ref err) => Some(&**err),
            ContractContextError::TransactionContextError(ref err) => Some(err),
        }
    }
}

impl std::fmt::Display for ContractContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ContractContextError::AddresserError(ref err) => {
                write!(f, "Error occurred while computing an address: {}", err)
            }
            ContractContextError::ProtoConversionError(ref err) => {
                write!(f, "Error occurred during protobuf conversion: {}", err)
            }
            ContractContextError::ProtocolBuildError(ref err) => write!(
                f,
                "Error occurred while building native protocol type: {}",
                err
            ),
            ContractContextError::TransactionContextError(ref err) => {
                write!(f, "Error occurred in TransactionContext method: {}", err)
            }
        }
    }
}

impl From<ProtoConversionError> for ContractContextError {
    fn from(e: ProtoConversionError) -> Self {
        ContractContextError::ProtoConversionError(e)
    }
}

impl From<AddresserError> for ContractContextError {
    fn from(e: AddresserError) -> Self {
        ContractContextError::AddresserError(e)
    }
}

impl From<ContextError> for ContractContextError {
    fn from(e: ContextError) -> Self {
        ContractContextError::TransactionContextError(e)
    }
}

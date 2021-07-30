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

use std::error::Error as StdError;

use sawtooth_sdk::signing;

#[derive(Debug)]
pub enum CliError {
    /// The user has provided invalid inputs; error string
    /// is appropriate for display to the user without additional context
    User(String),
    Io(std::io::Error),
    Signing(signing::Error),
    Protobuf(protobuf::ProtobufError),
    Hyper(hyper::Error),
}

impl StdError for CliError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            CliError::User(_) => None,
            CliError::Io(err) => Some(err),
            CliError::Signing(err) => Some(err),
            CliError::Protobuf(err) => Some(err),
            CliError::Hyper(err) => Some(err),
        }
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            CliError::User(ref s) => write!(f, "Error: {}", s),
            CliError::Io(ref err) => write!(f, "IoError: {}", err),
            CliError::Signing(ref err) => write!(f, "SigningError: {}", err),
            CliError::Protobuf(ref err) => write!(f, "ProtobufError: {}", err),
            CliError::Hyper(ref err) => write!(f, "HyperError: {}", err),
        }
    }
}

impl From<std::io::Error> for CliError {
    fn from(e: std::io::Error) -> Self {
        CliError::Io(e)
    }
}

impl From<protobuf::ProtobufError> for CliError {
    fn from(e: protobuf::ProtobufError) -> Self {
        CliError::Protobuf(e)
    }
}

impl From<signing::Error> for CliError {
    fn from(e: signing::Error) -> Self {
        CliError::Signing(e)
    }
}

impl From<hyper::Error> for CliError {
    fn from(e: hyper::Error) -> Self {
        CliError::Hyper(e)
    }
}

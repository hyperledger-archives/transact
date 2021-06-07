// Copyright 2021 Cargill Incorporated
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
// limitations under the License

use std::error::Error;
use std::fmt;

use clap::Error as ClapError;

#[derive(Debug)]
pub enum SabreCommandExecutorError {
    /// An error was detected by `clap`.
    ClapError(ClapError),
    /// An operation could not be completed because the state of the underlying struct is inconsistent.
    InvalidState(String),
    /// An argument passed to a function did not conform to the expected format.
    InvalidArgument(String),
    /// A failure occurred within the function due to an internal implementation detail of the function.
    Internal(String),
}

impl Error for SabreCommandExecutorError {}

impl fmt::Display for SabreCommandExecutorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SabreCommandExecutorError::ClapError(err) => f.write_str(&err.message),
            SabreCommandExecutorError::InvalidState(msg) => write!(f, "Invalid state: {}", msg),
            SabreCommandExecutorError::InvalidArgument(msg) => {
                write!(f, "Invalid argument: {}", msg)
            }
            SabreCommandExecutorError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl From<ClapError> for SabreCommandExecutorError {
    fn from(err: ClapError) -> Self {
        Self::ClapError(err)
    }
}

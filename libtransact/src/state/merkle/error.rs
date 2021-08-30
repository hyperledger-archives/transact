/*
 * Copyright 2021 Cargill Incorporated
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

use std::error::Error;
use std::fmt;

use crate::error::{InternalError, InvalidStateError};

/// Error variants that may occur while reading Merkle Radix tree leaves.
#[derive(Debug)]
pub enum MerkleRadixLeafReadError {
    /// An internal error occurred.
    ///
    /// This error may be caused by an issue outside of the control of the application, such as
    /// invalid storage.
    InternalError(InternalError),
    /// An invalid state error occurred.
    ///
    /// This error may occur in cases where the reader has been provided invalid state, such as a
    /// non-existent state root hash.
    InvalidStateError(InvalidStateError),
}

impl fmt::Display for MerkleRadixLeafReadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MerkleRadixLeafReadError::InternalError(err) => f.write_str(&err.to_string()),
            MerkleRadixLeafReadError::InvalidStateError(err) => f.write_str(&err.to_string()),
        }
    }
}

impl Error for MerkleRadixLeafReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            MerkleRadixLeafReadError::InternalError(ref err) => Some(err),
            MerkleRadixLeafReadError::InvalidStateError(ref err) => Some(err),
        }
    }
}

impl From<InternalError> for MerkleRadixLeafReadError {
    fn from(err: InternalError) -> Self {
        MerkleRadixLeafReadError::InternalError(err)
    }
}

impl From<InvalidStateError> for MerkleRadixLeafReadError {
    fn from(err: InvalidStateError) -> Self {
        MerkleRadixLeafReadError::InvalidStateError(err)
    }
}

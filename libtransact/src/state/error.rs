/*
 * Copyright 2018 Bitwise IO, Inc.
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

/// An error that may occur on state writes.
#[derive(Debug)]
pub enum StateWriteError {
    /// Attempted to write to state from a given state id, and that state id
    /// does not exist
    InvalidStateId(String),
    /// An error occurred with the underlying storage mechanism
    StorageError(Box<dyn Error>),
}

impl fmt::Display for StateWriteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StateWriteError::InvalidStateId(msg) => write!(f, "Invalid State Id: {}", msg),
            StateWriteError::StorageError(err) => write!(f, "Storage Error: {}", err),
        }
    }
}

impl Error for StateWriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            StateWriteError::InvalidStateId(_) => None,
            StateWriteError::StorageError(err) => Some(err.as_ref()),
        }
    }
}

/// An error that may occur on state reads.
#[derive(Debug)]
pub enum StateReadError {
    /// Attempted to read to state from a given state id, and that state id
    /// does not exist.
    InvalidStateId(String),
    /// An poorly formed or invalid key was provided.
    InvalidKey(String),
    /// An error occurred with the underlying storage mechanism
    StorageError(Box<dyn Error>),
}

impl fmt::Display for StateReadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StateReadError::InvalidStateId(msg) => write!(f, "Invalid State Id: {}", msg),
            StateReadError::InvalidKey(key) => write!(f, "Invalid Key: {}", key),
            StateReadError::StorageError(err) => write!(f, "Storage Error: {}", err),
        }
    }
}

impl Error for StateReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            StateReadError::InvalidStateId(_) | StateReadError::InvalidKey(_) => None,
            StateReadError::StorageError(err) => Some(err.as_ref()),
        }
    }
}

/// An error that may occur on a state prune.
#[derive(Debug)]
pub enum StatePruneError {
    /// Attempted to prune a state id that does not exist.
    InvalidStateId(String),
    /// An error occurred with the underlying storage mechanism.
    StorageError(Box<dyn Error>),
}

impl fmt::Display for StatePruneError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StatePruneError::InvalidStateId(msg) => write!(f, "Invalid State Id: {}", msg),
            StatePruneError::StorageError(err) => write!(f, "Storage Error: {}", err),
        }
    }
}

impl Error for StatePruneError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            StatePruneError::InvalidStateId(_) => None,
            StatePruneError::StorageError(err) => Some(err.as_ref()),
        }
    }
}

#[derive(Debug)]
pub enum StateError {
    Internal(InternalError),
    InvalidState(InvalidStateError),
}

impl fmt::Display for StateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Internal(err) => f.write_str(&err.to_string()),
            Self::InvalidState(err) => f.write_str(&err.to_string()),
        }
    }
}

impl Error for StateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Internal(err) => Some(err),
            Self::InvalidState(err) => Some(err),
        }
    }
}

impl From<InternalError> for StateError {
    fn from(err: InternalError) -> Self {
        Self::Internal(err)
    }
}

impl From<InvalidStateError> for StateError {
    fn from(err: InvalidStateError) -> Self {
        Self::InvalidState(err)
    }
}

impl From<StateError> for StateWriteError {
    fn from(err: StateError) -> Self {
        match err {
            StateError::Internal(err) => Self::StorageError(Box::new(err)),
            StateError::InvalidState(err) => Self::InvalidStateId(err.to_string()),
        }
    }
}

impl From<StateError> for StateReadError {
    fn from(err: StateError) -> Self {
        match err {
            StateError::Internal(err) => Self::StorageError(Box::new(err)),
            StateError::InvalidState(err) => Self::InvalidStateId(err.to_string()),
        }
    }
}

impl From<StateError> for StatePruneError {
    fn from(err: StateError) -> Self {
        match err {
            StateError::Internal(err) => Self::StorageError(Box::new(err)),
            StateError::InvalidState(err) => Self::InvalidStateId(err.to_string()),
        }
    }
}

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

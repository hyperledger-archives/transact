/*
 * Copyright 2018 Bitwise IO, Inc.
 * Copyright 2019-2021 Cargill Incorporated
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

#[cfg(feature = "workload-batch-gen")]
use crate::error::{InternalError, InvalidStateError};

#[cfg(feature = "workload-runner")]
#[derive(Debug, PartialEq)]
pub enum WorkloadRunnerError {
    /// Error raised when failing to submit the batch
    SubmitError(String),
    TooManyRequests,
    /// Error raised when adding workload to the runner
    WorkloadAddError(String),
    /// Error raised when removing workload from the runner
    WorkloadRemoveError(String),
    /// Error raised when retrieving a batch status
    BatchStatusError(String),
}

#[cfg(feature = "workload-runner")]
impl std::error::Error for WorkloadRunnerError {}

#[cfg(feature = "workload-runner")]
impl std::fmt::Display for WorkloadRunnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            WorkloadRunnerError::SubmitError(ref err) => {
                write!(f, "Unable to submit batch: {}", err)
            }
            WorkloadRunnerError::TooManyRequests => {
                write!(f, "Unable to submit batch because of TooManyRequests")
            }
            WorkloadRunnerError::WorkloadAddError(ref err) => {
                write!(f, "Unable to add workload: {}", err)
            }
            WorkloadRunnerError::WorkloadRemoveError(ref err) => {
                write!(f, "Unable to remove workload: {}", err)
            }
            WorkloadRunnerError::BatchStatusError(ref err) => {
                write!(f, "Error occurred while retrieving batch status: {}", err)
            }
        }
    }
}

// Errors that may occur during the generation of batches from a source.
#[cfg(feature = "workload-batch-gen")]
#[derive(Debug)]
pub enum BatchingError {
    InternalError(InternalError),
    InvalidStateError(InvalidStateError),
}

#[cfg(feature = "workload-batch-gen")]
impl std::fmt::Display for BatchingError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            BatchingError::InternalError(err) => f.write_str(&err.to_string()),
            BatchingError::InvalidStateError(err) => f.write_str(&err.to_string()),
        }
    }
}

#[cfg(feature = "workload-batch-gen")]
impl std::error::Error for BatchingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BatchingError::InternalError(ref err) => Some(err),
            BatchingError::InvalidStateError(ref err) => Some(err),
        }
    }
}

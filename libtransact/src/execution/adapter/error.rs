/*
 * Copyright 2018 Bitwise IO, Inc.
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

use crate::protocol::transaction::TransactionPair;
use std::{error::Error, fmt};

/// During processing of the Transaction, something unexpected happened.
/// The `Executor` immediately retries the `TransactionPair` for all of these
/// errors.
#[derive(Debug)]
pub enum ExecutionAdapterError {
    /// Executing the transaction took too much time and so abort
    TimeoutError(Box<TransactionPair>),
    /// This ExecutionAdaptor does not have the capability to process the `TransactionPair`
    /// given to it. This can happen due to a timing error in routing the `TransactionPair`
    /// to the `ExecutionAdapter`.
    RoutingError(Box<TransactionPair>),

    GeneralExecutionError(Box<dyn Error + Send>),
}

impl Error for ExecutionAdapterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            ExecutionAdapterError::GeneralExecutionError(ref err) => Some(&**err),
            _ => None,
        }
    }
}

impl fmt::Display for ExecutionAdapterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExecutionAdapterError::TimeoutError(ref pair) => write!(
                f,
                "Timeout while processing {}/{}: {}",
                pair.header().family_name(),
                pair.header().family_version(),
                pair.transaction().header_signature()
            ),
            ExecutionAdapterError::RoutingError(pair) => write!(
                f,
                "Unable to route {}/{}",
                pair.header().family_name(),
                pair.header().family_version()
            ),
            ExecutionAdapterError::GeneralExecutionError(err) => {
                write!(f, "General Execution Error: {}", &err)
            }
        }
    }
}

#[derive(Debug)]
pub enum ExecutionOperationError {
    /// An error occurred on `ExecutionAdaptor.start`
    StartError(String),

    /// An error occurred on `ExecutionAdaptor.execute`
    ExecuteError(String),

    /// An error occurred on `ExecutionAdaptor.stop`
    StopError(String),
}

impl Error for ExecutionOperationError {}

impl fmt::Display for ExecutionOperationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExecutionOperationError::StartError(s) => write!(f, "Start Error: {}", s),
            ExecutionOperationError::ExecuteError(s) => write!(f, "Execute Error: {}", s),
            ExecutionOperationError::StopError(s) => write!(f, "Execute Error: {}", s),
        }
    }
}

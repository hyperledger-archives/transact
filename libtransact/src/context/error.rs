/*
 * Copyright 2019 Bitwise IO, Inc.
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

use crate::protocol::receipt::TransactionReceiptBuilderError;
use crate::state::error::StateReadError;

#[derive(Debug)]
pub enum ContextManagerError {
    MissingContextError(String),
    TransactionReceiptBuilderError(TransactionReceiptBuilderError),
    StateReadError(StateReadError),
}

impl Error for ContextManagerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            ContextManagerError::MissingContextError(_) => Some(self),
            ContextManagerError::TransactionReceiptBuilderError(ref err) => Some(err),
            ContextManagerError::StateReadError(ref err) => Some(err),
        }
    }
}

impl std::fmt::Display for ContextManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ContextManagerError::MissingContextError(ref s) => {
                write!(f, "Unable to find specified Context: {:?}", s)
            }
            ContextManagerError::TransactionReceiptBuilderError(ref err) => {
                write!(f, "A TransactionReceiptBuilder error occured: {}", err)
            }
            ContextManagerError::StateReadError(ref err) => {
                write!(f, "A State Read error occured: {}", err)
            }
        }
    }
}

impl From<TransactionReceiptBuilderError> for ContextManagerError {
    fn from(err: TransactionReceiptBuilderError) -> Self {
        ContextManagerError::TransactionReceiptBuilderError(err)
    }
}

impl From<StateReadError> for ContextManagerError {
    fn from(err: StateReadError) -> Self {
        ContextManagerError::StateReadError(err)
    }
}

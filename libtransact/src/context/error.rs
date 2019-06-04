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
use std::sync::mpsc::{RecvError, SendError};

use crate::context::manager::thread::{
    ContextManagerCoreError, ContextOperationMessage, ContextOperationResponse,
};
use crate::protocol::receipt::TransactionReceiptBuilderError;
use crate::state::error::StateReadError;

#[derive(Debug)]
pub enum ContextManagerError {
    MissingContextError(String),
    TransactionReceiptBuilderError(TransactionReceiptBuilderError),
    StateReadError(StateReadError),
    InternalError(Box<dyn Error>),
}

impl Error for ContextManagerError {
    fn description(&self) -> &str {
        match *self {
            ContextManagerError::MissingContextError(ref msg) => msg,
            ContextManagerError::TransactionReceiptBuilderError(ref err) => err.description(),
            ContextManagerError::StateReadError(ref err) => err.description(),
            ContextManagerError::InternalError(ref err) => err.description(),
        }
    }

    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            ContextManagerError::MissingContextError(_) => Some(self),
            ContextManagerError::TransactionReceiptBuilderError(ref err) => Some(err),
            ContextManagerError::StateReadError(ref err) => Some(err),
            ContextManagerError::InternalError(ref err) => Some(&**err),
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
            ContextManagerError::InternalError(ref err) => {
                write!(f, "An internal error occured: {}", err)
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

impl From<ContextManagerCoreError> for ContextManagerError {
    fn from(err: ContextManagerCoreError) -> Self {
        ContextManagerError::InternalError(Box::new(err))
    }
}

impl From<RecvError> for ContextManagerError {
    fn from(err: RecvError) -> Self {
        ContextManagerError::InternalError(Box::new(err))
    }
}

impl From<SendError<ContextOperationMessage>> for ContextManagerError {
    fn from(err: SendError<ContextOperationMessage>) -> Self {
        ContextManagerError::InternalError(Box::new(err))
    }
}

impl From<SendError<ContextOperationResponse>> for ContextManagerError {
    fn from(err: SendError<ContextOperationResponse>) -> Self {
        ContextManagerError::InternalError(Box::new(err))
    }
}

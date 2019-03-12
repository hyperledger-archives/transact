/*
 * Copyright 2019 Bitwise IO, Inc.
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
use std::error::Error;
use std::sync::mpsc::{RecvError, SendError};

use crate::context::manager::thread::{ContextOperationMessage, ContextOperationResponse};

#[derive(Debug)]
pub enum ContextManagerCoreError {
    HandlerSendError(SendError<ContextOperationMessage>),
    CoreSendError(SendError<ContextOperationResponse>),
    CoreReceiveError(RecvError),
    HandlerError(String),
}

impl Error for ContextManagerCoreError {
    fn description(&self) -> &str {
        match *self {
            ContextManagerCoreError::HandlerSendError(ref err) => err.description(),
            ContextManagerCoreError::CoreSendError(ref err) => err.description(),
            ContextManagerCoreError::CoreReceiveError(ref err) => err.description(),
            ContextManagerCoreError::HandlerError(ref msg) => msg,
        }
    }
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            ContextManagerCoreError::HandlerSendError(ref err) => Some(err),
            ContextManagerCoreError::CoreSendError(ref err) => Some(err),
            ContextManagerCoreError::CoreReceiveError(ref err) => Some(err),
            ContextManagerCoreError::HandlerError(_) => Some(self),
        }
    }
}

impl std::fmt::Display for ContextManagerCoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ContextManagerCoreError::HandlerSendError(ref err) => write!(
                f,
                "A Send Error occurred from the Context Manager handler: {}",
                err
            ),
            ContextManagerCoreError::CoreSendError(ref err) => write!(
                f,
                "A Send Error occurred from the Context Manager thread: {}",
                err
            ),
            ContextManagerCoreError::CoreReceiveError(ref err) => {
                write!(f, "A RecvError occurred: {}", err)
            }
            ContextManagerCoreError::HandlerError(ref s) => {
                write!(f, "Error occurred in the Context Manager handler: {}", s)
            }
        }
    }
}

impl From<SendError<ContextOperationMessage>> for ContextManagerCoreError {
    fn from(err: SendError<ContextOperationMessage>) -> Self {
        ContextManagerCoreError::HandlerSendError(err)
    }
}

impl From<SendError<ContextOperationResponse>> for ContextManagerCoreError {
    fn from(err: SendError<ContextOperationResponse>) -> Self {
        ContextManagerCoreError::CoreSendError(err)
    }
}

impl From<RecvError> for ContextManagerCoreError {
    fn from(err: RecvError) -> Self {
        ContextManagerCoreError::CoreReceiveError(err)
    }
}

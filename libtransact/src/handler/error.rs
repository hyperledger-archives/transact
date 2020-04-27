/*
 * Copyright 2017 Bitwise IO, Inc.
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

#[derive(Debug)]
pub enum ApplyError {
    /// Returned for an Invalid Transaction.
    InvalidTransaction(String),
    /// Returned when an internal error occurs during transaction processing.
    InternalError(String),
}

impl Error for ApplyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ApplyError::InvalidTransaction(ref s) => write!(f, "InvalidTransaction: {}", s),
            ApplyError::InternalError(ref s) => write!(f, "InternalError: {}", s),
        }
    }
}

#[derive(Debug)]
pub enum ContextError {
    /// Returned for an authorization error
    AuthorizationError(String),
    /// Returned when a error occurs due to missing info in a response
    ResponseAttributeError(String),
    /// Returned when there is an issues setting receipt data or events.
    TransactionReceiptError(String),
    /// Returned when a ProtobufError is returned during serializing
    SerializationError(Box<dyn Error>),
    /// Returned when an error is returned when sending a message
    SendError(Box<dyn Error>),
    /// Returned when an error is returned when sending a message
    ReceiveError(Box<dyn Error>),
}

impl Error for ContextError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ContextError::AuthorizationError(_) => None,
            ContextError::ResponseAttributeError(_) => None,
            ContextError::TransactionReceiptError(_) => None,
            ContextError::SerializationError(err) => Some(&**err),
            ContextError::SendError(err) => Some(&**err),
            ContextError::ReceiveError(err) => Some(&**err),
        }
    }
}

impl std::fmt::Display for ContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ContextError::AuthorizationError(ref s) => write!(f, "AuthorizationError: {}", s),
            ContextError::ResponseAttributeError(ref s) => {
                write!(f, "ResponseAttributeError: {}", s)
            }
            ContextError::TransactionReceiptError(ref s) => {
                write!(f, "TransactionReceiptError: {}", s)
            }
            ContextError::SerializationError(ref err) => write!(f, "SerializationError: {}", err),
            ContextError::SendError(ref err) => write!(f, "SendError: {}", err),
            ContextError::ReceiveError(ref err) => write!(f, "ReceiveError: {}", err),
        }
    }
}

impl From<ContextError> for ApplyError {
    fn from(context_error: ContextError) -> Self {
        match context_error {
            ContextError::TransactionReceiptError(..) => {
                ApplyError::InternalError(format!("{}", context_error))
            }
            _ => ApplyError::InvalidTransaction(format!("{}", context_error)),
        }
    }
}

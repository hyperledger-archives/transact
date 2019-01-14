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

//! Contains execution adapter components and interfaces that proxy the `Transaction`
//! and its associated state.

pub mod error;

pub use crate::execution::adapter::error::ExecutionAdapterError;

use crate::execution::ContextId;
use crate::receipts::TransactionProcessingResult;
use crate::transaction::TransactionPair;

pub type OnDoneCallback<K, V> =
    FnMut(Result<TransactionProcessingResult<K, V>, ExecutionAdapterError>);
pub type OnRegisterCallback = FnMut(TransactionFamily);
pub type OnUnregisterCallback = FnMut(TransactionFamily);

/// Implementers of this trait proxy the transaction to the correct component to execute
/// the transaction.
pub trait ExecutionAdapter<K, V> {
    /// Register a callback to be fired when the execution adapter registers a new
    /// capability.
    fn on_register(&self, callback: Box<OnRegisterCallback>);

    /// Register a callback to be fired when the execution adapter unregisters a
    /// new capability.
    fn on_unregister(&self, callback: Box<OnUnregisterCallback>);

    /// The on_done callback fires whenever a `TransactionPair` has been executed.
    fn on_done(&self, callback: Box<OnDoneCallback<K, V>>);

    /// Execute the transaction and provide an callback that handles the result.
    ///
    ///
    /// The `on_done` callback is fired when the transaction returns from processing or there
    /// is an error.
    fn execute(&self, transaction_pair: TransactionPair, context_id: ContextId);
}

#[derive(Eq, PartialEq, Debug)]
pub struct TransactionFamily {
    family_name: String,
    family_version: String,
}

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

mod error;
pub mod static_adapter;
#[cfg(test)]
pub mod test_adapter;

pub use crate::execution::adapter::error::{ExecutionAdapterError, ExecutionOperationError};

use crate::context::ContextId;
use crate::execution::ExecutionRegistry;
use crate::protocol::transaction::TransactionPair;
use crate::scheduler::ExecutionTaskCompletionNotification;

/// Implementers of this trait proxy the transaction to the correct component to execute
/// the transaction.
pub trait ExecutionAdapter: Send {
    fn start(
        &mut self,
        execution_registry: Box<dyn ExecutionRegistry>,
    ) -> Result<(), ExecutionOperationError>;

    /// Execute the transaction and provide an callback that handles the result.
    ///
    ///
    /// The `on_done` callback is fired when the transaction returns from processing or there
    /// is an error.
    fn execute(
        &self,
        transaction_pair: TransactionPair,
        context_id: ContextId,
        on_done: Box<
            dyn Fn(Result<ExecutionTaskCompletionNotification, ExecutionAdapterError>) + Send,
        >,
    ) -> Result<(), ExecutionOperationError>;

    /// Stop the internal threads and the Executor will no longer call execute.
    fn stop(self: Box<Self>) -> Result<(), ExecutionOperationError>;
}

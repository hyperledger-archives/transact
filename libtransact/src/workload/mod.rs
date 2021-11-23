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

//! Traits for generating transactions and batches.

#[cfg(feature = "workload-batch-gen")]
pub mod batch_gen;
#[cfg(feature = "workload-batch-gen")]
mod batch_reader;
mod error;
#[cfg(feature = "workload-runner")]
mod runner;
#[cfg(feature = "workload-batch-gen")]
mod transaction_reader;

use crate::error::InvalidStateError;
use crate::protocol::batch::BatchPair;
use crate::protocol::transaction::TransactionPair;
#[cfg(feature = "workload-runner")]
pub use crate::workload::runner::{HttpRequestCounter, WorkloadRunner};

/// `TransactionWorkload` provides an API for generating transactions
pub trait TransactionWorkload: Send {
    /// Get a `TransactionPair` and the result that is expected when that transaction is executed
    fn next_transaction(
        &mut self,
    ) -> Result<(TransactionPair, Option<ExpectedBatchResult>), InvalidStateError>;
}

/// `BatchWorkload` provides an API for generating batches
pub trait BatchWorkload: Send {
    /// Get a `BatchPair` and the result that is expected when that batch is processed and its
    /// transactions are executed
    fn next_batch(&mut self)
        -> Result<(BatchPair, Option<ExpectedBatchResult>), InvalidStateError>;
}

#[derive(Clone)]
pub enum ExpectedBatchResult {
    Invalid,
    Valid,
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn test_transaction_workload(workload: &mut dyn TransactionWorkload) {
        workload.next_transaction().unwrap();
        workload.next_transaction().unwrap();
    }

    pub fn test_batch_workload(workload: &mut dyn BatchWorkload) {
        workload.next_batch().unwrap();
        workload.next_batch().unwrap();
    }
}

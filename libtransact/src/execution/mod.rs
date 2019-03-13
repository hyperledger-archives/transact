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

//! Contains components that are used to directly execute a `Transaction`
//! and return a `execution::adapter::ExecutionResult`.

pub mod adapter;
pub mod executor;

use crate::protocol::transaction::TransactionPair;

/// A Transaction Family Descriptor
#[derive(Eq, PartialEq, Debug, Hash, Clone)]
pub struct TransactionFamily {
    family_name: String,
    family_version: String,
}

impl TransactionFamily {
    /// Constructs a new Transaction Family Descriptor.
    pub fn new(family_name: String, family_version: String) -> Self {
        TransactionFamily {
            family_name,
            family_version,
        }
    }

    /// Creates a Transaction Family Descriptor using the information in a TransactionPair.
    pub fn from_pair(transaction_pair: &TransactionPair) -> Self {
        Self::new(
            transaction_pair.header().family_name().to_string(),
            transaction_pair.header().family_version().to_string(),
        )
    }

    pub fn family_name(&self) -> &str {
        &self.family_name
    }

    pub fn family_version(&self) -> &str {
        &self.family_version
    }
}

/// The registry of transaction families
pub trait ExecutionRegistry: Send {
    /// Register the given transaction family.
    ///
    /// Adding a family to the registry indicates that the family can be processed by an
    /// ExecutionAdapter.
    fn register_transaction_family(&mut self, family: TransactionFamily);

    /// Unregister the given transaction family.
    ///
    /// Signals that a transaction family can no longer be processed by an ExecutionAdapter.
    fn unregister_transaction_family(&mut self, family: &TransactionFamily);
}

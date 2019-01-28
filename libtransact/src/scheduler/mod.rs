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
pub mod tree;

use crate::context::ContextId;
use crate::execution::adapter::ExecutionResult;
use crate::transaction::TransactionPair;

/// The `TransactionPair` along with the information needed to execute that
/// `Transaction`
pub struct ExecutionTask {
    pair: TransactionPair,
    context_id: ContextId,
}

impl ExecutionTask {
    pub fn pair(&self) -> &TransactionPair {
        &self.pair
    }

    pub fn context_id(&self) -> &ContextId {
        &self.context_id
    }

    pub fn take(self) -> (TransactionPair, ContextId) {
        (self.pair, self.context_id)
    }
}

/// Scheduler functionality used by the Executor.
pub trait SchedulePair: Send {
    fn add_execution_result(&self, execution_result: ExecutionResult);

    fn get_schedule_iterator(&self) -> Box<Iterator<Item = ExecutionTask> + Send>;
}

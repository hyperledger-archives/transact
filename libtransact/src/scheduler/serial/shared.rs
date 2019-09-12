/*
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

//! Internal serial scheduler state shared across threads.

use crate::protocol::batch::BatchPair;
use crate::scheduler::BatchExecutionResult;
use crate::scheduler::SchedulerError;
use crate::scheduler::{default_error_callback, default_result_callback};

use std::collections::VecDeque;

/// Stores all serial scheduler data which is shared between threads.
pub struct Shared {
    finalized: bool,
    result_callback: Box<dyn Fn(Option<BatchExecutionResult>) + Send>,
    error_callback: Box<dyn Fn(SchedulerError) + Send>,
    unscheduled_batches: VecDeque<BatchPair>,
}

impl Default for Shared {
    fn default() -> Self {
        Self::new()
    }
}

impl Shared {
    pub fn new() -> Self {
        Shared {
            finalized: false,
            result_callback: Box::new(default_result_callback),
            error_callback: Box::new(default_error_callback),
            unscheduled_batches: VecDeque::new(),
        }
    }

    pub fn finalized(&self) -> bool {
        self.finalized
    }

    pub fn result_callback(&self) -> &(dyn Fn(Option<BatchExecutionResult>) + Send) {
        &*self.result_callback
    }

    pub fn error_callback(&self) -> &(dyn Fn(SchedulerError) + Send) {
        &*self.error_callback
    }

    pub fn set_finalized(&mut self, finalized: bool) {
        self.finalized = finalized;
    }

    pub fn set_result_callback(
        &mut self,
        callback: Box<dyn Fn(Option<BatchExecutionResult>) + Send>,
    ) {
        self.result_callback = callback;
    }

    pub fn set_error_callback(&mut self, callback: Box<dyn Fn(SchedulerError) + Send>) {
        self.error_callback = callback;
    }

    pub fn batch_already_queued(&self, batch: &BatchPair) -> bool {
        self.unscheduled_batches.contains(batch)
    }

    pub fn unscheduled_batches_is_empty(&self) -> bool {
        self.unscheduled_batches.is_empty()
    }

    pub fn add_unscheduled_batch(&mut self, batch: BatchPair) {
        self.unscheduled_batches.push_back(batch);
    }

    pub fn drain_unscheduled_batches(&mut self) -> Vec<BatchPair> {
        self.unscheduled_batches.drain(0..).collect()
    }

    pub fn pop_unscheduled_batch(&mut self) -> Option<BatchPair> {
        self.unscheduled_batches.pop_front()
    }
}

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

//! Internal MultiScheduler state shared across threads.

use crate::protocol::batch::BatchPair;
use crate::scheduler::{
    default_error_callback, default_result_callback, BatchExecutionResult, Scheduler,
    SchedulerError,
};

use std::collections::HashMap;

/// Stores all MultiScheduler data which is shared between threads.
pub struct MultiSchedulerShared {
    finalized: bool,
    /// The MultiScheduler's result callback; this is called when all sub-schedulers have returned
    /// the same result for a batch.
    result_callback: Box<dyn Fn(Option<BatchExecutionResult>) + Send>,
    error_callback: Box<dyn Fn(SchedulerError) + Send>,
    /// Tracks which sub-schedulers have returned results for the given batch pair.
    pending_results: HashMap<BatchPair, HashMap<usize, BatchExecutionResult>>,
    /// The sub-schedulers of this MultiScheduler.
    schedulers: Vec<Box<dyn Scheduler>>,
}

impl MultiSchedulerShared {
    pub fn new(schedulers: Vec<Box<dyn Scheduler>>) -> Self {
        MultiSchedulerShared {
            finalized: false,
            result_callback: Box::new(default_result_callback),
            error_callback: Box::new(default_error_callback),
            pending_results: HashMap::new(),
            schedulers,
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

    pub fn set_result_callback(
        &mut self,
        callback: Box<dyn Fn(Option<BatchExecutionResult>) + Send>,
    ) {
        self.result_callback = callback;
    }

    pub fn set_error_callback(&mut self, callback: Box<dyn Fn(SchedulerError) + Send>) {
        self.error_callback = callback;
    }

    pub fn batch_already_pending(&self, batch: &BatchPair) -> bool {
        self.pending_results.contains_key(batch)
    }

    pub fn add_batch(&mut self, batch: BatchPair) -> Result<(), SchedulerError> {
        for (i, scheduler) in self.schedulers.iter_mut().enumerate() {
            scheduler.add_batch(batch.clone()).map_err(|err| {
                SchedulerError::Internal(format!(
                    "failed to add batch to sub-scheduler {}: {}",
                    i, err
                ))
            })?;
        }
        self.pending_results.insert(batch, HashMap::new());
        Ok(())
    }

    pub fn cancel(&mut self) -> Result<Vec<BatchPair>, SchedulerError> {
        for (i, scheduler) in self.schedulers.iter_mut().enumerate() {
            scheduler.cancel().map_err(|err| {
                SchedulerError::Internal(format!("failed to cancel sub-scheduler {}: {}", i, err))
            })?;
        }
        Ok(self
            .pending_results
            .drain()
            .map(|(batch, _)| batch)
            .collect())
    }

    pub fn finalize(&mut self) -> Result<(), SchedulerError> {
        for (i, scheduler) in self.schedulers.iter_mut().enumerate() {
            scheduler.finalize().map_err(|err| {
                SchedulerError::Internal(format!("failed to finalize sub-scheduler {}: {}", i, err))
            })?;
        }
        self.finalized = true;
        Ok(())
    }

    pub fn pending_results(&self) -> &HashMap<BatchPair, HashMap<usize, BatchExecutionResult>> {
        &self.pending_results
    }

    pub fn pending_results_mut(
        &mut self,
    ) -> &mut HashMap<BatchPair, HashMap<usize, BatchExecutionResult>> {
        &mut self.pending_results
    }

    pub fn schedulers(&self) -> &Vec<Box<dyn Scheduler>> {
        &self.schedulers
    }

    pub fn schedulers_mut(&mut self) -> &mut Vec<Box<dyn Scheduler>> {
        &mut self.schedulers
    }
}

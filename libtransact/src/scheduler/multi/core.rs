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

//! Implementation of core MultiScheduler thread.

use crate::scheduler::{BatchExecutionResult, SchedulerError};

use std::collections::{HashMap, HashSet};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};

use super::shared::MultiSchedulerShared;

/// An enum of messages which can be sent to the MultiSchedulerCore via a
/// `Sender<MultiSchedulerCoreMessage>`
pub enum MultiSchedulerCoreMessage {
    /// The sub-scheduler at the given index in the sub-schedulers list has gotten the given batch
    /// result from the executor
    BatchResult(usize, Option<BatchExecutionResult>),
    /// The sub-scheduler at the given index in the sub-schedulers list encountered an error
    SubSchedulerError(usize, SchedulerError),
}

pub struct MultiSchedulerCore {
    shared_lock: Arc<Mutex<MultiSchedulerShared>>,
    rx: Receiver<MultiSchedulerCoreMessage>,
    /// Tracks which sub-schedulers have returned a None result, indicating they have executed all
    /// of their batches.
    done_schedulers: HashSet<usize>,
}

impl MultiSchedulerCore {
    pub fn new(
        shared_lock: Arc<Mutex<MultiSchedulerShared>>,
        rx: Receiver<MultiSchedulerCoreMessage>,
    ) -> Self {
        MultiSchedulerCore {
            shared_lock,
            rx,
            done_schedulers: HashSet::new(),
        }
    }

    fn run(&mut self) -> Result<(), SchedulerError> {
        loop {
            // Receive batch result callbacks from the sub-schedulers
            match self.rx.recv() {
                Ok(MultiSchedulerCoreMessage::BatchResult(scheduler_index, batch_result)) => {
                    let mut shared = self.shared_lock.lock()?;
                    let num_schedulers = shared.schedulers().len();

                    if self.done_schedulers.contains(&scheduler_index) {
                        shared.error_callback()(SchedulerError::Internal(format!(
                            "got callback from sub-scheduler {}, which is already done",
                            scheduler_index,
                        )));
                        continue;
                    }

                    let batch_result = match batch_result {
                        Some(res) => res,
                        None => {
                            // If a sub-scheduler returns a None result, it has completed all of
                            // its batches
                            self.done_schedulers.insert(scheduler_index);
                            // If all sub-schedulers are done, return `None` result to indicate that
                            // the MultiScheduler is also done.
                            if self.done_schedulers.len() == num_schedulers {
                                // All pending results should have been returned if they executed
                                // or drained if the scheduler was cancelled; if not, a subscheduler
                                // must have failed to return an execution result.
                                if !shared.pending_results().is_empty() {
                                    shared.error_callback()(SchedulerError::Internal(format!(
                                        "all sub-schedulers are done, but some results not \
                                         returned: {:?}",
                                        shared.pending_results(),
                                    )));
                                }
                                shared.result_callback()(None);
                                break;
                            }
                            continue;
                        }
                    };

                    let pending_results = shared.pending_results_mut();

                    // Add this scheduler's result to the pending list and check if all schedulers
                    // have reported a result for this batch
                    let batch_done = match pending_results.get_mut(&batch_result.batch) {
                        Some(result_list) => {
                            result_list.insert(scheduler_index, batch_result.clone());
                            result_list.len() == num_schedulers
                        }
                        None => {
                            shared.error_callback()(SchedulerError::Internal(format!(
                                "got callback from sub-scheduler {} for batch ({}) that's not \
                                 pending",
                                scheduler_index,
                                batch_result.batch.batch().header_signature(),
                            )));
                            continue;
                        }
                    };

                    // If all schedulers have now reported a result for the batch, remove the
                    // pending result and call the appropriate callback (result callback if all
                    // results match, error callback if there's a mismatch)
                    if batch_done {
                        let mut results = pending_results
                            .remove(&batch_result.batch)
                            // This unwrap can't fail; if the pending result doesn't exist, the
                            // code above will continue to the next iteration of the loop
                            .unwrap()
                            .drain()
                            // Turn the scheduler -> result map into a result -> schedulers map
                            .fold(
                                HashMap::new(),
                                |mut acc: HashMap<BatchExecutionResult, HashSet<usize>>,
                                 (scheduler, result)| {
                                    match acc.get_mut(&result) {
                                        Some(schedulers) => {
                                            schedulers.insert(scheduler);
                                        }
                                        None => {
                                            let mut schedulers = HashSet::new();
                                            schedulers.insert(scheduler);
                                            acc.insert(result, schedulers);
                                        }
                                    }
                                    acc
                                },
                            );

                        if results.len() == 1 {
                            // Only one result, which means they all match. This unwrap can't fail
                            // because the length of results was already checked.
                            let (result, _) = results.drain().next().unwrap();
                            shared.result_callback()(Some(result));
                        } else {
                            shared.error_callback()(SchedulerError::Internal(format!(
                                "mismatched results for batch {}: {:?}",
                                batch_result.batch.batch().header_signature(),
                                results,
                            )));
                        }
                    }
                }
                Ok(MultiSchedulerCoreMessage::SubSchedulerError(scheduler_index, err)) => {
                    self.shared_lock.lock()?.error_callback()(SchedulerError::Internal(format!(
                        "scheduler {} encountered error: {}",
                        scheduler_index, err,
                    )));
                }
                Err(err) => {
                    // This is expected if the other side shuts down
                    // before this end. However, it would be more
                    // elegant to gracefully handle it by sending a
                    // close message across.
                    warn!("Thread-MultiScheduler recv error: {}", err);
                    break;
                }
            }
        }

        Ok(())
    }

    fn send_scheduler_error(&self, error: SchedulerError) -> Result<(), SchedulerError> {
        self.shared_lock.lock()?.error_callback()(error);
        Ok(())
    }

    pub fn start(mut self) -> Result<std::thread::JoinHandle<()>, SchedulerError> {
        std::thread::Builder::new()
            .name(String::from("Thread-MultiScheduler"))
            .spawn(move || {
                if let Err(err) = self.run() {
                    // Attempt to send notification using the error callback; if that fails, just
                    // log it.
                    let error = SchedulerError::Internal(format!(
                        "serial scheduler's internal thread ended due to error: {}",
                        err
                    ));
                    self.send_scheduler_error(error.clone())
                        .unwrap_or_else(|_| error!("{}", error));
                }
            })
            .map_err(|err| {
                SchedulerError::Internal(format!(
                    "could not build a thread for the scheduler: {}",
                    err
                ))
            })
    }
}

/*
 * Copyright 2021 Cargill Incorporated
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

//! Provides a runner to submit `BatchWorkload`s

use std::collections::HashMap;
use std::fmt;
use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Sender, TryRecvError};
use std::{thread, time};

use reqwest::{blocking::Client, header, StatusCode};

use crate::protos::IntoBytes;

use super::batch_gen::BatchListFeeder;
use super::error::WorkloadRunnerError;
use super::BatchWorkload;

pub const DEFAULT_LOG_TIME_SECS: u32 = 30; // time in seconds

/// Keeps track of the currenlty running workloads.
///
/// The `WorkloadRunner` enables running different workloads, against different targets at
/// different rates without needing to set up multiple runners.
#[derive(Default)]
pub struct WorkloadRunner {
    workloads: HashMap<String, Worker>,
}

impl WorkloadRunner {
    /// Starts running a new workload
    ///
    /// # Arguments
    ///
    /// * `id` - A unique ID for the workload
    /// * `workload` - The `BatchWorkload` used to generate the batches that will be submitted
    /// * `targets` - A list of URL for submitting the batches. The URL provided must be the full
    ///              URL before adding `/batches` for submission
    /// * `rate`- How many tranactions per second to submit
    /// * `auth` - The string to be set in the Authorization header for the request
    /// * `update_time` - The time between updates on the workload
    ///
    /// Returns an error if a workload with that ID is already running or if the workload thread
    /// could not be started
    pub fn add_workload(
        &mut self,
        id: String,
        workload: Box<dyn BatchWorkload>,
        targets: Vec<String>,
        rate: u32,
        auth: String,
        update_time: u32,
    ) -> Result<(), WorkloadRunnerError> {
        if self.workloads.contains_key(&id) {
            return Err(WorkloadRunnerError::WorkloadAddError(format!(
                "Workload already running with ID: {}",
                id,
            )));
        }

        let worker = WorkerBuilder::default()
            .with_id(id.to_string())
            .with_workload(workload)
            .with_targets(targets)
            .with_rate(rate)
            .with_auth(auth)
            .with_update_time(update_time)
            .build()?;

        self.workloads.insert(id, worker);

        Ok(())
    }

    /// Stops running a workload
    ///
    /// # Arguments
    ///
    /// * `id` - A unique ID for the workload that should be stopped
    ///
    /// Returns an error if a workload with that ID does not exist or if the workload cannot be
    /// cleanly shutdown
    pub fn remove_workload(&mut self, id: &str) -> Result<(), WorkloadRunnerError> {
        if let Some(mut worker) = self.workloads.remove(id) {
            debug!("Shutting down worker {}", worker.id);
            if worker.sender.send(ShutdownMessage).is_err() {
                return Err(WorkloadRunnerError::WorkloadRemoveError(format!(
                    "Failed to send shutdown messages to {}",
                    id,
                )));
            }

            if let Some(thread) = worker.thread.take() {
                if let Err(err) = thread.join() {
                    return Err(WorkloadRunnerError::WorkloadRemoveError(format!(
                        "Failed to cleanly join worker thread {}: {:?}",
                        id, err,
                    )));
                }
            }
        } else {
            return Err(WorkloadRunnerError::WorkloadRemoveError(format!(
                "Workload with ID {} does not exist",
                id,
            )));
        }

        Ok(())
    }

    /// Shutsdown all running workloads
    pub fn shutdown(self) {
        for (_, worker) in self.workloads.iter() {
            if worker.sender.send(ShutdownMessage).is_err() {
                warn!("Failed to send shutdown messages to {}", worker.id);
            }
        }

        for (_, mut worker) in &mut self.workloads.into_iter() {
            debug!("Shutting down worker {}", worker.id);
            if let Some(thread) = worker.thread.take() {
                if let Err(_err) = thread.join() {
                    warn!("Failed to cleanly join worker thread {}", worker.id);
                }
            }
        }
    }
}

/// Sent to a workload to signal it should stop
struct ShutdownMessage;

/// Represents a running workload
struct Worker {
    id: String,
    thread: Option<thread::JoinHandle<()>>,
    sender: Sender<ShutdownMessage>,
}

#[derive(Default)]
struct WorkerBuilder {
    id: Option<String>,
    workload: Option<Box<dyn BatchWorkload>>,
    targets: Option<Vec<String>>,
    rate: Option<u32>,
    auth: Option<String>,
    update_time: Option<u32>,
}

impl WorkerBuilder {
    /// Sets the ID of the worker
    ///
    /// # Arguments
    ///
    ///  * `id` - The unique ID of the worker
    pub fn with_id(mut self, id: String) -> WorkerBuilder {
        self.id = Some(id);
        self
    }

    /// Sets the workload that will be run against the targets
    ///
    /// # Arguments
    ///
    ///  * `workload` - The workload that will return batches to submit
    pub fn with_workload(mut self, workload: Box<dyn BatchWorkload>) -> WorkerBuilder {
        self.workload = Some(workload);
        self
    }

    /// Sets the targets for the worker
    ///
    /// # Arguments
    ///
    ///  * `targets` - A list of URL for submitting the batches. The URL provided must be the full
    ///               URL before adding `/batches` for submission
    pub fn with_targets(mut self, targets: Vec<String>) -> WorkerBuilder {
        self.targets = Some(targets);
        self
    }

    /// Sets the rate for the worker
    ///
    /// # Arguments
    ///
    ///  * `rate` - How many batches to submit per second
    pub fn with_rate(mut self, rate: u32) -> WorkerBuilder {
        self.rate = Some(rate);
        self
    }

    /// Sets the auth for the worker
    ///
    /// # Arguments
    ///
    ///  * `auth` - The auth string to set against the Authorization header for the http request
    pub fn with_auth(mut self, auth: String) -> WorkerBuilder {
        self.auth = Some(auth);
        self
    }

    /// Sets the update time of the worker
    ///
    /// # Arguments
    ///
    ///  * `update_time` - How often to provide an update about the workload
    pub fn with_update_time(mut self, update_time: u32) -> WorkerBuilder {
        self.update_time = Some(update_time);
        self
    }

    pub fn build(self) -> Result<Worker, WorkloadRunnerError> {
        let id = self.id.ok_or_else(|| {
            WorkloadRunnerError::WorkloadAddError(
                "unable to build, missing field: `id`".to_string(),
            )
        })?;

        let rate = self.rate.ok_or_else(|| {
            WorkloadRunnerError::WorkloadAddError(
                "unable to build, missing field: `rate`".to_string(),
            )
        })?;

        let workload = self.workload.ok_or_else(|| {
            WorkloadRunnerError::WorkloadAddError(
                "unable to build, missing field: `workload`".to_string(),
            )
        })?;

        let targets = self.targets.ok_or_else(|| {
            WorkloadRunnerError::WorkloadAddError(
                "unable to build, missing field: `target`".to_string(),
            )
        })?;

        let auth = self.auth.ok_or_else(|| {
            WorkloadRunnerError::WorkloadAddError(
                "unable to build, missing field: `auth`".to_string(),
            )
        })?;

        let update_time = self.update_time.unwrap_or(DEFAULT_LOG_TIME_SECS);

        let (sender, receiver) = channel();

        let time_to_wait = time::Duration::from_secs(1) / rate;
        let thread_id = id.to_string();
        let thread = Some(
            thread::Builder::new()
                .name(id.to_string())
                .spawn(move || {
                    // set first target
                    let mut next_target = 0;
                    let mut workload = workload;
                    // keep track of status of http requests for logging
                    let http_counter = HTTPRequestCounter::new(thread_id.to_string());
                    // the last time http request information was logged
                    let mut last_log_time = time::Instant::now();
                    loop {
                        match receiver.try_recv() {
                            // recieved shutdown
                            Ok(_) => {
                                info!("Worker received shutdown");
                                break;
                            }
                            Err(TryRecvError::Empty) => {
                                // get target to submit batch to
                                let target = match targets.get(next_target) {
                                    Some(target) => target,
                                    None => {
                                        error!("No targets provided");
                                        break;
                                    }
                                };

                                // get next batch
                                let batch = workload.next_batch().expect("Unable to get batch");
                                let batch_bytes = match vec![batch.batch().clone()].into_bytes() {
                                    Ok(bytes) => bytes,
                                    Err(err) => {
                                        error!("Unable to get batch bytes {}", err);
                                        break;
                                    }
                                };

                                // submit batch to the target
                                match submit_batch(target, &auth, batch_bytes) {
                                    Ok(()) => http_counter.increment_sent(),
                                    Err(err) => {
                                        if err == WorkloadRunnerError::TooManyRequests {
                                            http_counter.increment_queue_full()
                                        } else {
                                            error!("{}:{}", thread_id, err);
                                        }
                                    }
                                }

                                // log http submission stats if its been longer then update time
                                log(&http_counter, &mut last_log_time, update_time);

                                // get next target, round robin
                                next_target = (next_target + 1) % targets.len();
                                thread::sleep(time_to_wait);
                            }

                            Err(TryRecvError::Disconnected) => {
                                error!("Channel has disconnected");
                                break;
                            }
                        }
                    }
                })
                .map_err(|err| {
                    WorkloadRunnerError::WorkloadAddError(format!(
                        "Unable to spawn worker thread: {}",
                        err
                    ))
                })?,
        );

        Ok(Worker { id, thread, sender })
    }
}

#[derive(Deserialize)]
pub struct ServerError {
    pub message: String,
}

fn submit_batch(target: &str, auth: &str, batch_bytes: Vec<u8>) -> Result<(), WorkloadRunnerError> {
    Client::new()
        .post(&format!("{}/batches", target))
        .header(header::CONTENT_TYPE, "octet-stream")
        .header("Authorization", auth)
        .body(batch_bytes)
        .send()
        .map_err(|err| WorkloadRunnerError::SubmitError(format!("Failed to submit batch: {}", err)))
        .and_then(|res| {
            let status = res.status();
            if status.is_success() {
                Ok(())
            } else {
                if status == StatusCode::TOO_MANY_REQUESTS {
                    return Err(WorkloadRunnerError::TooManyRequests);
                };

                let message = res
                    .json::<ServerError>()
                    .map_err(|_| {
                        WorkloadRunnerError::SubmitError(format!(
                            "Batch submit request failed with status code '{}', but \
                                 error response was not valid",
                            status
                        ))
                    })?
                    .message;

                Err(WorkloadRunnerError::SubmitError(format!(
                    "Failed to submit batch: {}",
                    message
                )))
            }
        })
}

/// Counts sent and queue full for Batches submmissions from the target REST Api.
pub struct HTTPRequestCounter {
    id: String,
    sent_count: AtomicUsize,
    queue_full_count: AtomicUsize,
}

impl HTTPRequestCounter {
    pub fn new(id: String) -> Self {
        HTTPRequestCounter {
            id,
            sent_count: AtomicUsize::new(0),
            queue_full_count: AtomicUsize::new(0),
        }
    }

    pub fn increment_sent(&self) {
        self.sent_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_queue_full(&self) {
        self.queue_full_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn log(&self, seconds: u64, nanoseconds: u32) {
        let update = seconds as f64 + f64::from(nanoseconds) * 1e-9;
        println!(
            "{}, Batches/s {:.3}",
            self,
            self.sent_count.load(Ordering::Relaxed) as f64 / update
        );

        self.sent_count.store(0, Ordering::Relaxed);
        self.queue_full_count.store(0, Ordering::Relaxed);
    }
}

impl fmt::Display for HTTPRequestCounter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let time = chrono::Utc::now();
        write!(
            f,
            "{0}: {1}, Sent: {2}, Queue Full {3}",
            self.id,
            time.format("%h-%d-%Y %H:%M:%S%.3f").to_string(),
            self.sent_count.load(Ordering::Relaxed),
            self.queue_full_count.load(Ordering::Relaxed)
        )
    }
}

/// Log if time since last log is greater than update time.
pub fn log(counter: &HTTPRequestCounter, last_log_time: &mut time::Instant, update_time: u32) {
    let log_time = time::Instant::now() - *last_log_time;
    if log_time.as_secs() as u32 >= update_time {
        counter.log(log_time.as_secs(), log_time.subsec_nanos());
        *last_log_time = time::Instant::now();
    }
}

/// Helper function to submit a list of batches from a source
pub fn submit_batches_from_source(
    source: &mut dyn Read,
    input_file: String,
    targets: Vec<String>,
    rate: u32,
    auth: String,
    update: u32,
) {
    let mut workload = BatchListFeeder::new(source);
    let time_to_wait = time::Duration::from_secs(1) / rate;
    // set first target
    let mut next_target = 0;
    // keep track of status of http requests for logging
    let http_counter = HTTPRequestCounter::new(format!("File: {}", input_file));
    // the last time http request information was logged
    let mut last_log_time = time::Instant::now();
    loop {
        let target = match targets.get(next_target) {
            Some(target) => target,
            None => {
                error!("No targets provided");
                break;
            }
        };

        // get next batch
        let batch = match workload.next() {
            Some(Ok(batch)) => batch,
            Some(Err(err)) => {
                error!("Unable to get batch: {}", err);
                break;
            }
            None => {
                info!("All batches submitted");
                break;
            }
        };

        let batch_bytes = match vec![batch.batch().clone()].into_bytes() {
            Ok(bytes) => bytes,
            Err(err) => {
                error!("Unable to get batch bytes {}", err);
                break;
            }
        };

        // submit batch to the target
        match submit_batch(target, &auth, batch_bytes) {
            Ok(()) => http_counter.increment_sent(),
            Err(err) => {
                if err == WorkloadRunnerError::TooManyRequests {
                    http_counter.increment_queue_full()
                } else {
                    error!("{}", err);
                }
            }
        }

        // log http submission stats if its been longer then update time
        log(&http_counter, &mut last_log_time, update);

        // get next target, round robin
        next_target = (next_target + 1) % targets.len();
        thread::sleep(time_to_wait);
    }

    // log http submission stats for remaning workload
    log(&http_counter, &mut last_log_time, 0);
}

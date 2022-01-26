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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{thread, time};

use reqwest::{blocking::Client, header, StatusCode};

use crate::protos::IntoBytes;

use super::error::WorkloadRunnerError;
use super::BatchWorkload;
use super::ExpectedBatchResult;

/// This type maps the status link used to check the result of the batch after it has been submitted
/// to the target URL that a batch was submitted to and the result that was expected of the batch
type ExpectedBatchResults = Arc<Mutex<HashMap<String, (String, Option<ExpectedBatchResult>)>>>;

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
    /// * `time_to_wait`- The amount of time to wait between batch submissions
    /// * `auth` - The string to be set in the Authorization header for the request
    /// * `get_batch_status` - Determines if the workload should compare the result of a batch after
    ///                      it is submitted to the expected result
    /// * `duration` - The amount of time the workload should run for
    /// * `request_counter` - Tracks the submitted requests for logging
    ///
    /// Returns an error if a workload with that ID is already running or if the workload thread
    /// could not be started
    #[allow(clippy::too_many_arguments)]
    pub fn add_workload(
        &mut self,
        id: String,
        workload: Box<dyn BatchWorkload>,
        targets: Vec<String>,
        time_to_wait: Duration,
        auth: String,
        get_batch_status: bool,
        duration: Option<Duration>,
        request_counter: Arc<HttpRequestCounter>,
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
            .with_time_to_wait(time_to_wait)
            .with_auth(auth)
            .get_batch_status(get_batch_status)
            .with_duration(duration)
            .with_request_counter(request_counter)
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

            if let Some(mut batch_status_checker) = worker.batch_status_checker {
                batch_status_checker.remove_batch_status_checker()?;
            }
        } else {
            return Err(WorkloadRunnerError::WorkloadRemoveError(format!(
                "Workload with ID {} does not exist",
                id,
            )));
        }

        Ok(())
    }

    /// Return a WorkerShutdownSignaler, used to send a shutdown signal to the `Worker` threads.
    pub fn shutdown_signaler(&self) -> WorkerShutdownSignaler {
        WorkerShutdownSignaler {
            senders: self
                .workloads
                .iter()
                .map(|(_, worker)| (worker.id.clone(), worker.sender.clone()))
                .collect(),
        }
    }

    /// Block until the threads have shutdown.
    pub fn wait_for_shutdown(self) -> Result<(), WorkloadRunnerError> {
        for (_, mut worker) in &mut self.workloads.into_iter() {
            if let Some(mut batch_status_checker) = worker.batch_status_checker {
                if let Some(thread) = batch_status_checker.thread.take() {
                    thread.join().map_err(|_| {
                        WorkloadRunnerError::WorkloadAddError(
                            "Failed to join batch status checker thread".to_string(),
                        )
                    })?
                }
            }
            if let Some(thread) = worker.thread.take() {
                thread.join().map_err(|_| {
                    WorkloadRunnerError::WorkloadAddError(
                        "Failed to join worker thread".to_string(),
                    )
                })?
            }
        }
        Ok(())
    }
}

/// The senders for all running `Worker`s in a `WorkloadRunner`.
pub struct WorkerShutdownSignaler {
    senders: Vec<(String, Sender<ShutdownMessage>)>,
}

impl WorkerShutdownSignaler {
    /// Send a shutdown message to each `Worker` to signal it should stop
    pub fn signal_shutdown(&self) -> Result<(), WorkloadRunnerError> {
        for (id, sender) in &self.senders {
            debug!("Shutting down worker {}", id);
            sender.send(ShutdownMessage).map_err(|_| {
                WorkloadRunnerError::WorkloadRemoveError(
                    "Failed to send shutdown message".to_string(),
                )
            })?
        }
        Ok(())
    }
}

/// Sent to a workload to signal it should stop
struct ShutdownMessage;

/// Represents a running workload
struct Worker {
    id: String,
    thread: Option<thread::JoinHandle<()>>,
    sender: Sender<ShutdownMessage>,
    batch_status_checker: Option<BatchStatusChecker>,
}

#[derive(Default)]
struct WorkerBuilder {
    id: Option<String>,
    workload: Option<Box<dyn BatchWorkload>>,
    targets: Option<Vec<String>>,
    time_to_wait: Option<Duration>,
    auth: Option<String>,
    get_batch_status: Option<bool>,
    duration: Option<Duration>,
    request_counter: Option<Arc<HttpRequestCounter>>,
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

    /// Sets the wait time between batches for the worker
    ///
    /// # Arguments
    ///
    ///  * `time_to_wait` - The amount of time to wait between batch submissions
    pub fn with_time_to_wait(mut self, time_to_wait: Duration) -> WorkerBuilder {
        self.time_to_wait = Some(time_to_wait);
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

    /// Sets the total duration that the worker will run for
    ///
    /// # Arguments
    ///
    ///  * `duration` - How long the worker should run for
    pub fn with_duration(mut self, duration: Option<Duration>) -> WorkerBuilder {
        self.duration = duration;
        self
    }

    /// Sets a boolean value indicating if the status of submitted batches should be checked
    ///
    /// # Arguments
    ///
    ///  * `get_batch_status` - Whether or not the status of submitted batches should be checked
    pub fn get_batch_status(mut self, get_batch_status: bool) -> WorkerBuilder {
        self.get_batch_status = Some(get_batch_status);
        self
    }

    /// Sets the [`HttpRequestCounter`] that will track the submitted requests
    ///
    /// # Arguments
    ///
    ///  * `request_counter` - The [`HttpRequestCounter`] for the worker
    pub fn with_request_counter(
        mut self,
        request_counter: Arc<HttpRequestCounter>,
    ) -> WorkerBuilder {
        self.request_counter = Some(request_counter);
        self
    }

    /// Starts a thread that generates batches and submits them to the set targets, returns a
    /// `Worker` containing the thread, an ID and a sender.
    pub fn build(self) -> Result<Worker, WorkloadRunnerError> {
        let id = self.id.ok_or_else(|| {
            WorkloadRunnerError::WorkloadAddError(
                "unable to build, missing field: `id`".to_string(),
            )
        })?;

        let time_to_wait = self.time_to_wait.ok_or_else(|| {
            WorkloadRunnerError::WorkloadAddError(
                "unable to build, missing field: `time_to_wait`".to_string(),
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

        let http_counter = self.request_counter.ok_or_else(|| {
            WorkloadRunnerError::WorkloadAddError(
                "unable to build, missing field: `request_counter`".to_string(),
            )
        })?;

        let get_batch_status = self.get_batch_status.unwrap_or(false);

        // calculate the end time based on the duration given
        let end_time = self.duration.map(|d| time::Instant::now() + d);

        let (sender, receiver) = channel();

        // create a channel for the batch status checker so that the sender can be used by the
        // worker thread to send a shutdown message to the batch status checker when the worker
        // shuts down
        let (batch_status_checker_sender, batch_status_checker_receiver) = channel();
        let batch_status_checker_shutdown = batch_status_checker_sender.clone();

        let batch_status_links: ExpectedBatchResults = Arc::new(Mutex::new(HashMap::new()));
        // start the batch status checker, this is a separate thread that checks the status links
        // of submitted batches and compares the returned result to the expected result
        let batch_status_checker = if get_batch_status {
            Some(BatchStatusChecker::new(
                batch_status_links.clone(),
                auth.clone(),
                id.clone(),
                sender.clone(),
                batch_status_checker_sender,
                batch_status_checker_receiver,
            )?)
        } else {
            None
        };

        let thread_id = id.to_string();
        let thread = Some(
            thread::Builder::new()
                .name(id.to_string())
                .spawn(move || {
                    // set first target
                    let mut next_target = 0;
                    let mut workload = workload;
                    let mut start_time = time::Instant::now();
                    // total number of batches that have been submitted
                    let mut submitted_batches = 0;
                    let mut submission_start = time::Instant::now();
                    let mut submission_avg: Option<time::Duration> = None;
                    loop {
                        if let Some(end_time) = end_time {
                            if time::Instant::now() > end_time {
                                signal_batch_status_checker_shutdown(
                                    batch_status_checker_shutdown,
                                    thread_id,
                                    get_batch_status,
                                );
                                break;
                            }
                        }
                        match receiver.try_recv() {
                            // recieved shutdown
                            Ok(_) => {
                                info!("Worker received shutdown");
                                signal_batch_status_checker_shutdown(
                                    batch_status_checker_shutdown,
                                    thread_id,
                                    get_batch_status,
                                );
                                break;
                            }
                            Err(TryRecvError::Empty) => {
                                // get target to submit batch to
                                let target = match targets.get(next_target) {
                                    Some(target) => target,
                                    None => {
                                        error!("No targets provided");
                                        signal_batch_status_checker_shutdown(
                                            batch_status_checker_shutdown,
                                            thread_id,
                                            get_batch_status,
                                        );
                                        break;
                                    }
                                };

                                // get next batch
                                let (batch, expected_result) = match workload.next_batch() {
                                    Ok((batch, expected_result)) => (batch, expected_result),
                                    Err(_) => {
                                        error!("Failed to get next batch");
                                        signal_batch_status_checker_shutdown(
                                            batch_status_checker_shutdown,
                                            thread_id,
                                            get_batch_status,
                                        );
                                        break;
                                    }
                                };
                                let batch_bytes = match vec![batch.batch().clone()].into_bytes() {
                                    Ok(bytes) => bytes,
                                    Err(err) => {
                                        error!("Unable to get batch bytes {}", err);
                                        signal_batch_status_checker_shutdown(
                                            batch_status_checker_shutdown,
                                            thread_id,
                                            get_batch_status,
                                        );
                                        break;
                                    }
                                };

                                // submit batch to the target
                                match submit_batch(target, &auth, batch_bytes.clone()) {
                                    Ok(link) => {
                                        if get_batch_status {
                                            match batch_status_links.lock() {
                                                Ok(mut l) => l.insert(
                                                    link,
                                                    (target.to_string(), expected_result),
                                                ),
                                                Err(_) => {
                                                    error!("ExpectedBatchResults lock poisoned");
                                                    signal_batch_status_checker_shutdown(
                                                        batch_status_checker_shutdown,
                                                        thread_id,
                                                        get_batch_status,
                                                    );
                                                    break;
                                                }
                                            };
                                        }
                                        submitted_batches += 1;
                                        http_counter.increment_sent()
                                    }
                                    Err(err) => {
                                        if err == WorkloadRunnerError::TooManyRequests {
                                            http_counter.increment_queue_full();

                                            // attempt to resubmit batch
                                            match slow_rate(
                                                target,
                                                &auth,
                                                batch_bytes.clone(),
                                                start_time,
                                                submitted_batches,
                                                &receiver,
                                                end_time,
                                            ) {
                                                Ok((true, _)) => {
                                                    signal_batch_status_checker_shutdown(
                                                        batch_status_checker_shutdown,
                                                        thread_id,
                                                        get_batch_status,
                                                    );
                                                    break;
                                                }
                                                Ok((false, Some(link))) => {
                                                    if get_batch_status {
                                                        match batch_status_links.lock() {
                                                            Ok(mut l) => l.insert(
                                                                link,
                                                                (target.to_string(), expected_result),
                                                            ),
                                                            Err(_) => {
                                                                error!("ExpectedBatchResults lock poisoned");
                                                                signal_batch_status_checker_shutdown(
                                                                    batch_status_checker_shutdown,
                                                                    thread_id,
                                                                    get_batch_status,
                                                                );
                                                                break;
                                                            }
                                                        };
                                                    }
                                                    submitted_batches = 1;
                                                    start_time = time::Instant::now();
                                                    http_counter.increment_sent()
                                                }
                                                Ok((false, None)) => {
                                                    if get_batch_status {
                                                        error!("Failed to get batch status link");
                                                    }
                                                    submitted_batches = 1;
                                                    start_time = time::Instant::now();
                                                    http_counter.increment_sent()
                                                }
                                                Err(err) => error!("{}:{}", thread_id, err),
                                            }
                                        } else {
                                            error!("{}:{}", thread_id, err);
                                        }
                                    }
                                }

                                // get next target, round robin
                                next_target = (next_target + 1) % targets.len();
                                let diff = time::Instant::now() - submission_start;
                                let submission_time = match submission_avg {
                                    Some(val) => (diff + val) / 2,
                                    None => diff,
                                };
                                submission_avg = Some(submission_time);

                                let wait_time = time_to_wait.saturating_sub(submission_time);

                                thread::sleep(wait_time);
                                submission_start = time::Instant::now();
                            }

                            Err(TryRecvError::Disconnected) => {
                                error!("Channel has disconnected");
                                signal_batch_status_checker_shutdown(
                                    batch_status_checker_shutdown,
                                    thread_id,
                                    get_batch_status,
                                );
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
        Ok(Worker {
            id,
            thread,
            sender,
            batch_status_checker,
        })
    }
}

#[derive(Deserialize)]
pub struct ServerError {
    pub message: String,
}

fn slow_rate(
    target: &str,
    auth: &str,
    batch_bytes: Vec<u8>,
    start_time: time::Instant,
    submitted_batches: u32,
    receiver: &Receiver<ShutdownMessage>,
    end_time: Option<time::Instant>,
) -> Result<(bool, Option<String>), WorkloadRunnerError> {
    debug!("Received TooManyRequests message from target, attempting to resubmit batch");
    let mut shutdown = false;
    let mut link = None;

    let time = (time::Instant::now() - start_time).as_secs() as u32;

    // calculate the sleep time using the effective rate of submission
    // default to one second if the rate is calculated to be 0
    let wait = match time {
        0 => time::Duration::from_secs(1),
        sec => match submitted_batches / sec {
            0 => time::Duration::from_secs(1),
            rate => time::Duration::from_secs(1) / rate,
        },
    };
    // sleep
    thread::sleep(wait);
    loop {
        if let Some(end_time) = end_time {
            if time::Instant::now() > end_time {
                shutdown = true;
                break;
            }
        }
        match receiver.try_recv() {
            // recieved shutdown
            Ok(_) => {
                info!("Worker received shutdown");
                shutdown = true;
                break;
            }
            Err(TryRecvError::Empty) => {
                // attempt to submit batch again
                match submit_batch(target, auth, batch_bytes.clone()) {
                    Ok(l) => {
                        link = Some(l);
                        break;
                    }
                    Err(WorkloadRunnerError::TooManyRequests) => thread::sleep(wait),
                    Err(err) => {
                        return Err(WorkloadRunnerError::SubmitError(format!(
                            "Failed to submit batch: {}",
                            err
                        )))
                    }
                }
            }
            Err(TryRecvError::Disconnected) => {
                error!("Channel has disconnected");
                break;
            }
        }
    }
    Ok((shutdown, link))
}

fn submit_batch(
    target: &str,
    auth: &str,
    batch_bytes: Vec<u8>,
) -> Result<String, WorkloadRunnerError> {
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
                let status_link: Link = res.json().map_err(|_| {
                    WorkloadRunnerError::SubmitError(
                        "Failed to deserialize response body".to_string(),
                    )
                })?;
                Ok(status_link.link)
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

/// Helper function that will send a shutdown message to the batch status checker thread if a batch
/// status checker is running
fn signal_batch_status_checker_shutdown(
    batch_status_checker_sender: Sender<ShutdownMessage>,
    id: String,
    check_batch_status: bool,
) {
    if check_batch_status {
        debug!(
            "Shutting down batch status checker BatchStatusChecker-{}",
            id
        );
        if batch_status_checker_sender.send(ShutdownMessage).is_err() {
            error!(
                "Failed to send shutdown message to BatchStatusChecker-{}",
                id,
            );
        }
    }
}

struct BatchStatusChecker {
    id: String,
    sender: Sender<ShutdownMessage>,
    thread: Option<thread::JoinHandle<()>>,
}

impl BatchStatusChecker {
    /// Takes a mutable hashmap of batch status links and starts a thread that continuously loops
    /// through the links, checking each link to get the status of the associated batch. If the
    /// returned status of the batch matches the result that was expected for that batch, the
    /// status link is removed from the hashmap. If the returned status is "pending", the link is
    /// left in the map to be checked again.
    ///
    /// # Arguments
    ///
    /// `status_links` - A hashmap of batch status links mapped to the target URL that the
    ///  associated  batc was originally submitted to and the expected result of the
    ///  batch
    /// `auth` - The string used in the authorization header of the request
    /// `id` - The id of the `Worker` this `BatchStatusChecker` is associated with
    /// `worker_sender` - The sender for with the `Worker` this `BatchStatusChecker` is associated
    ///  with. It is used to send a shutdown signal to the worker thread if a batch result does not
    ///  match the expected result
    /// `sender` - The sender that will be used to send a shutdown message when the
    ///  `BatchStatusChecker` needs to be shutdown
    /// `reciever` - The reciever used to check if the `BatchStatusChecker` has recieved a shutdown
    ///  message
    ///
    /// # Errors
    ///
    /// * Returns a `WorkloadRunnerError` if the thread cannot be created
    /// * Exits with 1 if the status of a batch does not match the expected result
    fn new(
        status_links: ExpectedBatchResults,
        auth: String,
        id: String,
        worker_sender: Sender<ShutdownMessage>,
        sender: Sender<ShutdownMessage>,
        reciever: Receiver<ShutdownMessage>,
    ) -> Result<Self, WorkloadRunnerError> {
        let id = format!("BatchStatusChecker-{}", id);
        let thread = Some(
            thread::Builder::new()
                .name(id.clone())
                .spawn(move || {
                    'outer: loop {
                        match reciever.try_recv() {
                            // Recieved shutdown.
                            Ok(_) => {
                                info!("Batch status checker received shutdown");
                                break;
                            }
                            Err(TryRecvError::Empty) => {
                                // Check if the list of batch status links is empty
                                let is_empty = match status_links.lock() {
                                    Ok(l) => l.is_empty(),
                                    Err(_) => {
                                        error!("ExpectedBatchResults lock poisoned");
                                        break;
                                    }
                                };
                                // Sleep for two seconds if the list of batch status links is
                                // empty
                                if is_empty {
                                    thread::sleep(Duration::new(2, 0))
                                } else {
                                    let (status_link, (target, expected_result)) =
                                        match status_links.lock() {
                                            Ok(l) => match l.iter().next() {
                                                Some((s, (t, e))) => {
                                                    (s.clone(), (t.clone(), e.clone()))
                                                }
                                                None => {
                                                    error!("Status links empty");
                                                    break;
                                                }
                                            },
                                            Err(_) => {
                                                error!("ExpectedBatchResults lock poisoned");
                                                break;
                                            }
                                        };

                                    // Get the full batch status URL
                                    let url =
                                        get_batch_status_url(target.to_string(), &status_link);
                                    match Client::new()
                                        .get(url)
                                        .header("Authorization", &auth)
                                        .send()
                                    {
                                        Ok(res) => {
                                            let status = res.status();
                                            if status.is_success() {
                                                let batch_info: Vec<BatchInfo> = match res.json() {
                                                    Ok(b) => b,
                                                    Err(_) => {
                                                        error!(
                                                            "Failed to deserialize response body"
                                                        );
                                                        break;
                                                    }
                                                };
                                                // Compare the returned batch result to the expected
                                                // result, if they do not match send a shutdown
                                                // message to the `Worker` and break the loop to end
                                                // the batch status checker thread.
                                                for info in batch_info {
                                                    match compare_batch_results(
                                                        status_links.clone(),
                                                        status_link.clone(),
                                                        expected_result.clone(),
                                                        info,
                                                    ) {
                                                        Ok(()) => (),
                                                        Err(e) => {
                                                            error!("{}", e);
                                                            match worker_sender
                                                                .send(ShutdownMessage)
                                                            {
                                                                Ok(_) => break 'outer,
                                                                Err(_) => {
                                                                    error!(
                                                                        "Failed to send shutdown \
                                                                        message to worker thread"
                                                                    );
                                                                    break 'outer;
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            } else {
                                                let message = match res.json::<ServerError>() {
                                                    Ok(r) => r.message,
                                                    Err(_) => {
                                                        error!(
                                                            "Batch status request failed with \
                                                            status code '{}', but error response \
                                                            was not valid",
                                                            status
                                                        );
                                                        break;
                                                    }
                                                };
                                                error!(
                                                    "Failed to get submitted batch status: {}",
                                                    message
                                                );
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed send batch status request: {}", e);
                                            break;
                                        }
                                    }
                                }
                                thread::sleep(Duration::from_millis(250));
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
                        "Unable to spawn batch status checker thread: {}",
                        err
                    ))
                })?,
        );
        Ok(BatchStatusChecker { id, sender, thread })
    }

    fn remove_batch_status_checker(&mut self) -> Result<(), WorkloadRunnerError> {
        debug!("Shutting down batch status checker {}", self.id);
        if self.sender.send(ShutdownMessage).is_err() {
            return Err(WorkloadRunnerError::WorkloadRemoveError(format!(
                "Failed to send shutdown messages to {}",
                &self.id,
            )));
        }
        if let Some(thread) = self.thread.take() {
            if let Err(err) = thread.join() {
                return Err(WorkloadRunnerError::WorkloadRemoveError(format!(
                    "Failed to cleanly join batch status checker thread {}: {:?}",
                    &self.id, err,
                )));
            }
        }
        Ok(())
    }
}

/// Takes the original target URL and a batch status link and removes the overlapping portion of the
/// target URL that is also given in the status link so that the two can be combined to create the
/// full URL that can be used to check the status of the associated batch
///
/// For example, if the target is 'http://127.0.0.1:8080/service/12345-ABCDE/a000' and the
/// status_link is '/service/12345-ABCDE/a000/batch_statuses?ids=6ff35474a572087e08fd6a54d56' the
/// then the duplicate '/service/12345-ABCDE/a000' will be removed from the target making it
/// 'http://127.0.0.1:8080' so that it can be combined with the status link to create the full URL
/// that can be queried to get the batch status,
/// 'http://127.0.0.1:8080/service/12345-ABCDE/a000/batch_statuses?ids=6ff35474a572087e08fd6a54d56'
fn get_batch_status_url(mut target: String, status_link: &str) -> String {
    let status_link_parts = status_link.splitn(5, '/');
    for p in status_link_parts {
        if !p.is_empty() && target.contains(format!("/{}", p).as_str()) {
            target = target.replacen(format!("/{}", p).as_str(), "", 1);
        }
    }
    format!("{}{}", target, status_link)
}

/// Helper function that takes the batch info returned after querrying the status link, and compares
/// it to the intended result for the given batch. If the returned batch results matches the
/// expected result, the entry is removed from the list of status links
///
/// # Errors
///
/// * Returns a `WorkloadRunnerError` if the batch result does not math the expected result
/// * Returns a `WorkloadRunnerError` if the `ExpectedBatchResults` lock is poisoned
fn compare_batch_results(
    status_links: ExpectedBatchResults,
    current_status_link: String,
    expected_result: Option<ExpectedBatchResult>,
    returned_batch_info: BatchInfo,
) -> Result<(), WorkloadRunnerError> {
    match returned_batch_info.status {
        BatchStatus::Invalid(invalid_txns) => match expected_result {
            Some(ExpectedBatchResult::Valid) => {
                return Err(WorkloadRunnerError::BatchStatusError(format!(
                    "Expected valid result, received invalid {:?}",
                    invalid_txns
                )))
            }
            Some(ExpectedBatchResult::Invalid) => {
                status_links
                    .lock()
                    .map_err(|_| {
                        WorkloadRunnerError::BatchStatusError(
                            "ExpectedBatchResults lock poisoned".into(),
                        )
                    })?
                    .remove(&current_status_link);
            }
            // If the expected result is not set there is nothing to be done
            // for the batch
            None => (),
        },
        // If the transactions have been successfully applied and returned
        // a `BatchStatus::Valid` or `BatchStatus::Committed` response, remove
        // the status link from the list
        BatchStatus::Valid(valid_txns) | BatchStatus::Committed(valid_txns) => {
            match expected_result {
                Some(ExpectedBatchResult::Valid) => {
                    status_links
                        .lock()
                        .map_err(|_| {
                            WorkloadRunnerError::BatchStatusError(
                                "ExpectedBatchResults lock poisoned".into(),
                            )
                        })?
                        .remove(&current_status_link);
                }
                Some(ExpectedBatchResult::Invalid) => {
                    return Err(WorkloadRunnerError::BatchStatusError(format!(
                        "Expected valid result, received valid {:?}",
                        valid_txns
                    )))
                }
                // If the expected result is not set there is nothing to be done
                // for the batch
                None => (),
            }
        }
        // If the status is pending or unknown leave it in the list to be
        // checked again later and skip to the next batch
        BatchStatus::Pending | BatchStatus::Unknown => (),
    }
    Ok(())
}

/// Used for deserializing `POST /batches` responses.
#[derive(Debug, Deserialize)]
pub struct Link {
    link: String,
}

/// This struct is a subset of the `GET /batch_status` response.  The complete response looks like
/// [BatchInfo {
///     id: "<id string>",
///     status: BatchStatus,
///     timestamp:
///         SystemTime {
///             tv_sec: <tv_sec>,
///             tv_nsec: <tv_nsec>
///         }
/// }]
#[derive(Debug, Deserialize)]
struct BatchInfo {
    pub status: BatchStatus,
}

/// Used by `BatchInfo` for deserializing `GET /batch_status` responses.
#[derive(Debug, Deserialize)]
#[serde(tag = "statusType", content = "message")]
enum BatchStatus {
    Unknown,
    Pending,
    Invalid(Vec<InvalidTransaction>),
    Valid(Vec<ValidTransaction>),
    Committed(Vec<ValidTransaction>),
}

/// Allow dead code because this struct is deserialized as part of the batch status response and
/// available for debugging.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ValidTransaction {
    transaction_id: String,
}

/// Allow dead code because this struct is deserialized as part of the batch status response and
/// available for debugging.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct InvalidTransaction {
    transaction_id: String,
    error_message: String,
    error_data: Vec<u8>,
}

/// Counts sent and queue full for Batches submmissions from the target REST Api.
pub struct HttpRequestCounter {
    id: String,
    sent_count: AtomicUsize,
    queue_full_count: AtomicUsize,
}

impl HttpRequestCounter {
    pub fn new(id: String) -> Self {
        HttpRequestCounter {
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

    pub fn reset_sent_count(&self) {
        self.sent_count.store(0, Ordering::Relaxed);
    }

    pub fn reset_queue_full_count(&self) {
        self.queue_full_count.store(0, Ordering::Relaxed);
    }

    pub fn get_batches_per_second(&self, update: f64) -> f64 {
        self.sent_count.load(Ordering::Relaxed) as f64 / update
    }
}

impl fmt::Display for HttpRequestCounter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let time = chrono::Utc::now();
        write!(
            f,
            "{0}: {1}, Sent: {2}, Queue Full {3}",
            self.id,
            time.format("%h-%d-%Y %H:%M:%S%.3f"),
            self.sent_count.load(Ordering::Relaxed),
            self.queue_full_count.load(Ordering::Relaxed)
        )
    }
}

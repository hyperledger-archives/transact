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
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::{thread, time};

use reqwest::{blocking::Client, header, StatusCode};

use crate::protos::IntoBytes;

use super::batch_gen::BatchListFeeder;
use super::error::WorkloadRunnerError;
use super::BatchWorkload;
use super::ExpectedBatchResult;

const DEFAULT_LOG_TIME_SECS: u32 = 30; // time in seconds

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
    /// * `update_time` - The time between updates on the workload
    /// * `get_batch_status` - Determines if the workload should compare the result of a batch after
    ///                      it is submitted to the expected result
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
        update_time: u32,
        get_batch_status: bool,
        duration: Option<Duration>,
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
            .with_update_time(update_time)
            .get_batch_status(get_batch_status)
            .with_duration(duration)
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

    /// Block until for the thread has shutdown.
    pub fn wait_for_shutdown(self) -> Result<(), WorkloadRunnerError> {
        for (_, mut worker) in &mut self.workloads.into_iter() {
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
}

#[derive(Default)]
struct WorkerBuilder {
    id: Option<String>,
    workload: Option<Box<dyn BatchWorkload>>,
    targets: Option<Vec<String>>,
    time_to_wait: Option<Duration>,
    auth: Option<String>,
    update_time: Option<u32>,
    get_batch_status: Option<bool>,
    duration: Option<Duration>,
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

    /// Sets the update time of the worker
    ///
    /// # Arguments
    ///
    ///  * `update_time` - How often to provide an update about the workload
    pub fn with_update_time(mut self, update_time: u32) -> WorkerBuilder {
        self.update_time = Some(update_time);
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

        let get_batch_status = self.get_batch_status.unwrap_or(false);

        let update_time = self.update_time.unwrap_or(DEFAULT_LOG_TIME_SECS);

        // calculate the end time based on the duration given
        let end_time = self.duration.map(|d| time::Instant::now() + d);

        let (sender, receiver) = channel();

        let batch_status_links: ExpectedBatchResults = Arc::new(Mutex::new(HashMap::new()));

        let thread_id = id.to_string();
        let thread = Some(
            thread::Builder::new()
                .name(id.to_string())
                .spawn(move || {
                    // set first target
                    let mut next_target = 0;
                    let mut workload = workload;
                    // keep track of status of http requests for logging
                    let http_counter = HttpRequestCounter::new(thread_id.to_string());
                    // the last time http request information was logged
                    let mut last_log_time = time::Instant::now();
                    let mut start_time = time::Instant::now();
                    // total number of batches that have been submitted
                    let mut submitted_batches = 0;
                    let mut submission_start = time::Instant::now();
                    let mut submission_avg: Option<time::Duration> = None;
                    loop {
                        if let Some(end_time) = end_time {
                            if time::Instant::now() > end_time {
                                break;
                            }
                        }
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
                                let (batch, expected_result) = match workload.next_batch() {
                                    Ok((batch, expected_result)) => (batch, expected_result),
                                    Err(_) => {
                                        error!("Failed to get next batch");
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
                                                Ok((true, _)) => break,
                                                Ok((false, Some(link))) => {
                                                    if get_batch_status {
                                                        match batch_status_links.lock() {
                                                            Ok(mut l) => l.insert(
                                                                link,
                                                                (target.to_string(), expected_result),
                                                            ),
                                                            Err(_) => {
                                                                error!("ExpectedBatchResults lock poisoned");
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

                                // log http submission stats if its been longer then update time
                                log(&http_counter, &mut last_log_time, update_time);

                                if get_batch_status {
                                    match check_batch_status(batch_status_links, &auth) {
                                        // set `batch_status_links` to be the updated list of links
                                        // returned by `check_batch_status`
                                        Ok(status_links) => batch_status_links = status_links,
                                        Err(err) => {
                                            error!("{}", err);
                                            break;
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

/// Takes a mutable hashmap of batch status links and checks each link once to get the status of
/// the associated batch. If the returned status of the batch matches the result that was
/// expected for that batch, the status link is removed from the hashmap. If the returned status is
/// "pending", the link is left in the map to be checked again in a future call. The updated hash
/// map of status links is returned after all links have been checked.
///
/// # Arguments
///
/// `status_links` - A hashmap of batch status links mapped to the target URL that the associated
///  batch was originally submitted to and the expected result of the batch
/// `auth` - The string used in the authorization header of the request
///
/// # Errors
///
/// Returns a `WorkloadRunnerError` if the status of a batch does not match the expected result
fn check_batch_status(
    mut status_links: ExpectedBatchResults,
    auth: &str,
) -> Result<ExpectedBatchResults, WorkloadRunnerError> {
    for (status_link, (target, expected_result)) in status_links.clone() {
        let url = get_batch_status_url(target, &status_link);
        Client::new()
            .get(url)
            .header("Authorization", auth)
            .send()
            .map_err(|err| {
                WorkloadRunnerError::SubmitError(format!(
                    "Failed to get status of submitted batch: {}",
                    err
                ))
            })
            .and_then(|res| {
                let status = res.status();
                if status.is_success() {
                    let batch_info: Vec<BatchInfo> = res.json().map_err(|_| {
                        WorkloadRunnerError::SubmitError(
                            "Failed to deserialize response body".to_string(),
                        )
                    })?;
                    for info in batch_info {
                        match info.status {
                            BatchStatus::Invalid(invalid_txns) => match expected_result {
                                Some(ExpectedBatchResult::Valid) => {
                                    return Err(WorkloadRunnerError::SubmitError(format!(
                                        "Expected valid result, received invalid {:?}",
                                        invalid_txns
                                    )));
                                }
                                Some(ExpectedBatchResult::Invalid) => {
                                    status_links.remove(&status_link);
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
                                        status_links.remove(&status_link);
                                    }
                                    Some(ExpectedBatchResult::Invalid) => {
                                        return Err(WorkloadRunnerError::SubmitError(format!(
                                            "expected invalid result, received valid {:?}",
                                            valid_txns
                                        )));
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
                    }
                    Ok(())
                } else {
                    let message = res
                        .json::<ServerError>()
                        .map_err(|_| {
                            WorkloadRunnerError::SubmitError(format!(
                                "Batch status request failed with status code '{}', but \
                                    error response was not valid",
                                status
                            ))
                        })?
                        .message;

                    Err(WorkloadRunnerError::SubmitError(format!(
                        "Failed to get submitted batch status: {}",
                        message
                    )))
                }
            })?;
    }
    Ok(status_links)
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

/// Used for deserializing `POST /batches` responses.
#[derive(Debug, Deserialize)]
pub struct Link {
    link: String,
}

/// Used for deserializing `GET /batch_status` responses.
#[derive(Debug, Deserialize)]
struct BatchInfo {
    pub id: String,
    pub status: BatchStatus,
    pub timestamp: SystemTime,
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

/// Used by `BatchStatus` for deserializing `GET /batch_status` responses.
#[derive(Debug, Deserialize)]
struct ValidTransaction {
    pub transaction_id: String,
}

/// Used by `BatchStatus` for deserializing `GET /batch_status` responses.
#[derive(Debug, Deserialize)]
struct InvalidTransaction {
    pub transaction_id: String,
    pub error_message: String,
    pub error_data: Vec<u8>,
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

impl fmt::Display for HttpRequestCounter {
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
pub fn log(counter: &HttpRequestCounter, last_log_time: &mut time::Instant, update_time: u32) {
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
    time_to_wait: Duration,
    auth: String,
    update: u32,
) {
    let mut workload = BatchListFeeder::new(source);
    // set first target
    let mut next_target = 0;
    // keep track of status of http requests for logging
    let http_counter = HttpRequestCounter::new(format!("File: {}", input_file));
    // the last time http request information was logged
    let mut last_log_time = time::Instant::now();
    let mut submission_start = time::Instant::now();
    let mut submission_avg: Option<time::Duration> = None;
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
            Ok(_) => http_counter.increment_sent(),
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

    // log http submission stats for remaning workload
    log(&http_counter, &mut last_log_time, 0);
}

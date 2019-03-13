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

use super::internal::{RegistrationExecutionEvent, RegistrationExecutionEventSender};

use crate::scheduler::ExecutionTask;
use crate::scheduler::ExecutionTaskCompletionNotifier;
use log::warn;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::channel,
    Arc,
};
use std::thread::{self, JoinHandle};

/// The `ExecutionTaskReader` sends all of the `Item`s from an `Iterator` along a single channel.
///
/// In the normal course of an executor there will be many `ExecutionTaskReader`s, one for each `Scheduler`.
pub struct ExecutionTaskReader {
    id: usize,
    threads: Option<(JoinHandle<()>, JoinHandle<()>)>,
    stop: Arc<AtomicBool>,
}

impl ExecutionTaskReader {
    pub fn new(id: usize) -> Self {
        ExecutionTaskReader {
            id,
            threads: None,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn start(
        &mut self,
        task_iterator: Box<Iterator<Item = ExecutionTask> + Send>,
        notifier: Box<ExecutionTaskCompletionNotifier>,
        internal: RegistrationExecutionEventSender,
        done_callback: Box<FnMut(usize) + Send>,
    ) -> Result<(), std::io::Error> {
        let stop = Arc::clone(&self.stop);

        let mut done_callback = done_callback;

        if self.threads.is_none() {
            let (sender, receiver) = channel();

            let join_handle = thread::Builder::new()
                .name(format!("ExecutionTaskReader-{}", self.id))
                .spawn(move || {
                    for execution_task in task_iterator {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }

                        let execution_event = (sender.clone(), execution_task);
                        let event =
                            RegistrationExecutionEvent::Execution(Box::new(execution_event));

                        if let Err(err) = internal.send(event) {
                            warn!("During sending on the internal executor channel: {}", err)
                        }
                    }
                })?;

            let stop = Arc::clone(&self.stop);
            let id = self.id;

            let join_handle_receive = thread::Builder::new()
                .name(format!("ExecutionTaskReader-receive_thread-{}", self.id))
                .spawn(move || loop {
                    while let Ok(notification) = receiver.recv() {
                        notifier.notify(notification);

                        if stop.load(Ordering::Relaxed) {
                            done_callback(id);
                            break;
                        }
                    }
                })?;

            self.threads = Some((join_handle, join_handle_receive));
        }
        Ok(())
    }

    pub fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some((send, receive)) = self.threads {
            Self::shutdown(send);
            Self::shutdown(receive);
        }
    }

    fn shutdown(join_handle: JoinHandle<()>) {
        if let Err(err) = join_handle.join() {
            warn!("Error joining with ExecutionTaskReader thread: {:?}", err);
        }
    }
}

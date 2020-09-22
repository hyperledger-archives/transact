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

use super::internal::{ExecutorCommand, ExecutorCommandSender};

use crate::scheduler::ExecutionTask;
use crate::scheduler::ExecutionTaskCompletionNotifier;
use log::warn;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};

/// The `ExecutionTaskReader` sends all of the `Item`s from an `Iterator` along a single channel.
///
/// In the normal course of an executor there will be many `ExecutionTaskReader`s, one for each `Scheduler`.
pub struct ExecutionTaskReader {
    id: usize,
    threads: Option<JoinHandle<()>>,
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
        task_iterator: Box<dyn Iterator<Item = ExecutionTask> + Send>,
        notifier: Box<dyn ExecutionTaskCompletionNotifier>,
        internal: ExecutorCommandSender,
    ) -> Result<(), std::io::Error> {
        let stop = Arc::clone(&self.stop);

        if self.threads.is_none() {
            let reader_id = self.id;
            let join_handle = thread::Builder::new()
                .name(format!("ExecutionTaskReader-{}", self.id))
                .spawn(move || {
                    for execution_task in task_iterator {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }

                        let execution_event = (notifier.clone(), execution_task);
                        let event = ExecutorCommand::Execution(Box::new(execution_event));

                        if let Err(err) = internal.send(event) {
                            warn!("During sending on the internal executor channel: {}", err)
                        }
                    }
                    debug!("Completed task iterator!");
                    if let Err(err) = internal.send(ExecutorCommand::ReaderDone(reader_id)) {
                        warn!("Unable to send done signal: {}", err)
                    }
                })?;

            self.threads = Some(join_handle);
        }
        Ok(())
    }

    pub fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join_handle) = self.threads {
            if let Err(err) = join_handle.join() {
                warn!("Error joining with ExecutionTaskReader thread: {:?}", err);
            }
        }
    }
}

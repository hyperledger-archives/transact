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

//! Implementation of the components used for interfacing with the component
//! reponsible for the execution of transactions (usually the Executor).

use crate::scheduler::ExecutionTask;
use crate::scheduler::ExecutionTaskCompletionNotification;
use crate::scheduler::ExecutionTaskCompletionNotifier;

use std::sync::mpsc::{Receiver, Sender};

use super::core::CoreMessage;

pub struct SerialExecutionTaskIterator {
    tx: Sender<CoreMessage>,
    rx: Receiver<ExecutionTask>,
}

impl SerialExecutionTaskIterator {
    pub fn new(tx: Sender<CoreMessage>, rx: Receiver<ExecutionTask>) -> Self {
        SerialExecutionTaskIterator { tx, rx }
    }
}

impl Iterator for SerialExecutionTaskIterator {
    type Item = ExecutionTask;

    /// Return the next execution task which is available to be executed.
    fn next(&mut self) -> Option<ExecutionTask> {
        // Send a message to the scheduler requesting the next task be sent.
        match self.tx.send(CoreMessage::Next) {
            Ok(_) => {
                match self.rx.recv() {
                    Ok(task) => Some(task),
                    Err(_) => {
                        // This is expected if the other side shuts down before this
                        // end.
                        None
                    }
                }
            }
            Err(err) => {
                error!(
                    "failed to send request for next in execution task iterator: {}",
                    err
                );
                None
            }
        }
    }
}

pub struct SerialExecutionTaskCompletionNotifier {
    tx: Sender<CoreMessage>,
}

impl SerialExecutionTaskCompletionNotifier {
    pub fn new(tx: Sender<CoreMessage>) -> Self {
        SerialExecutionTaskCompletionNotifier { tx }
    }
}

impl ExecutionTaskCompletionNotifier for SerialExecutionTaskCompletionNotifier {
    fn notify(&self, notification: ExecutionTaskCompletionNotification) {
        self.tx
            .send(CoreMessage::ExecutionResult(notification))
            .unwrap();
    }
}

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

#[derive(Debug)]
pub enum ExecutorError {
    // The Executor has not been started, and so calling `execute` will return an error.
    NotStarted,
    // The Executor has had start called more than once.
    AlreadyStarted(String),

    ResourcesUnavailable(String),
}

impl std::error::Error for ExecutorError {}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ExecutorError::NotStarted => f.write_str("Executor not started"),
            ExecutorError::AlreadyStarted(ref msg) => {
                write!(f, "Executor already started: {}", msg)
            }
            ExecutorError::ResourcesUnavailable(ref msg) => {
                write!(f, "Resource Unavailable: {}", msg)
            }
        }
    }
}

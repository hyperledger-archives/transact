/*
 * Copyright 2018 Bitwise IO, Inc.
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
#![cfg_attr(feature = "nightly", feature(test))]

pub mod batch;
pub mod context;
pub mod database;
pub mod execution;
pub mod handler;
#[allow(renamed_and_removed_lints)]
pub mod protos;
pub mod receipts;
pub mod scheduler;
pub mod signing;
pub mod state;
pub mod transaction;
#[cfg(test)]
pub mod workload;

#[macro_use]
extern crate log;

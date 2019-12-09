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

//! Transact structs for batches, transactions and receipts.
//!
//! These structs cover the core protocols of the Transact system.  Batches of transactions are
//! scheduled and executed.  The resuls of execution are stored in transaction receipts.

pub mod batch;
pub mod command;
#[cfg(feature = "key-value-state")]
pub mod key_value_state;
pub mod receipt;
pub mod transaction;

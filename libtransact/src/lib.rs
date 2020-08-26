/*
 * Copyright 2018 Bitwise IO, Inc.
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

//! # Hyperledger Transact
//!
//! Hyperledger Transact makes writing distributed ledger software easier by providing a shared
//! software library that handles the execution of smart contracts, including all aspects of
//! scheduling, transaction dispatch, and state management.
//!
//! Hyperledger framework-level projects and custom distributed ledgers can use Transact's
//! advanced transaction execution and state management to simplify the transaction execution code
//! in their projects and to take advantage of Transact’s additional features.
//!
//! More specifically, Transact provides an extensible approach to implementing new smart contract
//! languages called “smart contract engines.” Each smart contract engine implements a virtual
//! machine or interpreter that processes smart contracts. Examples include
//! [Seth](https://sawtooth.hyperledger.org/docs/seth/nightly/master/introduction.html), which
//! processes Ethereum Virtual Machine (EVM) smart contracts, and
//! [Sabre](https://sawtooth.hyperledger.org/docs/sabre/nightly/master/sabre_transaction_family.html),
//! which handles WebAssembly smart contracts. Transact also provides SDKs for implementing smart
//! contracts and smart contract engines, which makes it easy to write smart contract business
//! logic in a variety of programming languages.
//!
//! Hyperledger Transact is inspired by Hyperledger Sawtooth and uses architectural elements from
//! Sawtooth's current transaction execution platform, including the approach to scheduling,
//! transaction isolation, and state management. Transact is further informed by requirements from
//! Hyperledger Fabric to support different database backends and joint experiences between
//! Sawtooth and Fabric for flexible models for execution adapters (e.g., lessons from integrating
//! Hyperledger Burrow).
//!
//! ## Diving Deeper
//!
//! Transact is fundamentally a transaction processing system for state transitions. State data is
//! generally stored in a [Merkle-Radix
//! tree](https://sawtooth.hyperledger.org/docs/core/nightly/master/glossary.html#term-merkle-radix-tree),
//! a key-value database, or an SQL database. Given an initial state and a transaction, Transact
//! executes the transaction to produce a new state.  These state transitions are considered “pure”
//! because only the initial state and the transaction are used as input. (In contrast, other
//! systems such as Ethereum combine state and block information to produce the new state.) As a
//! result, Transact is agnostic about framework features other than transaction execution and
//! state. Awesome, right?
//!
//! Transact deliberately omits other features such as consensus, blocks, chaining, and peering.
//! These features are the responsibility of the Hyperledger frameworks (such as Sawtooth and
//! Fabric) and other distributed ledger implementations. The focus on smart contract execution
//! means that Transact can be used for smart contract execution without conflicting with other
//! platform-level architectural design elements.
//!
//! Transact includes the following components:
//!
//! * State. The Transact state implementation provides get, set, and delete operations against a
//!   database. For the Merkle-Radix tree state implementation, the tree structure is implemented on
//!   top of LMDB or an in-memory database.
//! * Context manager. In Transact, state reads and writes are scoped (sandboxed) to a specific
//!   "context" that contains a reference to a state ID (such as a Merkle-Radix state root hash) and
//!   one or more previous contexts. The context manager implements the context lifecycle and
//!   services the calls that read, write, and delete data from state.
//! * Scheduler. This component controls the order of transactions to be executed. Concrete
//!   implementations include a serial scheduler and a parallel scheduler. Parallel transaction
//!   execution is an important innovation for increasing network throughput.
//! * Executor. The Transact executor obtains transactions from the scheduler and executes them
//!   against a specific context. Execution is handled by sending the transaction to specific
//!   execution adapters (such as ZMQ or a static in-process adapter) which, in turn, send the
//!   transaction to a specific smart contract.
//! * Smart Contract Engines. These components provide the virtual machine implementations and
//!   interpreters that run the smart contracts. Examples of engines include WebAssembly, Ethereum
//!   Virtual Machine, Sawtooth Transactions Processors, and Fabric Chain Code.
//!
//! ## Transact Features
//!
//! Hyperledger Transact includes the following current and upcoming features (not a complete
//! list):
//!
//! * Transaction execution adapters that allow different mechanisms of execution. For example,
//!   Transact initially provides adapters that support both in-process and external transaction
//!   execution. The in-process adapter allows creation of a single (custom) process that can execute
//!   specific types of transactions. The external adapter allows execution to occur in a separate
//!   process.
//! * Serial and parallel transaction scheduling that provides options for flexibility and
//!   performance. Serial scheduling executes one transaction at a time, in order. Parallel
//!   scheduling executes multiple transactions at a time, possibly out of order, with specific
//!   constraints to guarantee a resulting state that matches the in-order execution that would occur
//!   with serial scheduling. Parallel scheduling provides a substantial performance benefit.
//! * Pluggable state backends with initial support for a LMDB-backed Merkle-Radix tree
//!   implementation and an in-memory Merkle-Radix tree (primarily useful for testing). Key-value
//!   databases and SQL databases will also be supported in the future.
//! * Transaction receipts, which contain the resulting state changes and other information from
//!   transaction execution.
//! * Events that can be generated by smart contracts. These events are captured and stored in the
//!   transaction receipt.
//! * SDKs for the following languages: Rust, Python, Javascript, Go, Java (including Android),
//!   Swift (iOS), C++, and .NET.
//! * Support for multiple styles of smart contracts including Sabre (WebAssembly smart contracts)
//!   and Seth (EVM smart contracts).
//!
//! ## Sawtooth Compatibility Layer
//!
//! Hyperledger Transact provides optional support for smart contract engines implemented for
//! Hyperledger Sawtooth through the `sawtooth-compat` feature.

#![cfg_attr(feature = "nightly", feature(test))]

pub mod context;
#[cfg(feature = "contract")]
pub mod contract;
pub mod database;
pub mod execution;
pub mod families;
pub mod handler;
pub mod protocol;
#[allow(renamed_and_removed_lints)]
pub mod protos;
#[cfg(feature = "sawtooth-compat")]
pub mod sawtooth;
pub mod scheduler;
pub mod state;
#[cfg(feature = "workload")]
pub mod workload;

#[macro_use]
extern crate log;
#[cfg(feature = "contract-archive")]
#[macro_use]
extern crate serde_derive;

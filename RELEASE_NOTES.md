# Release Notes

## Changes in Transact 0.3.6

* Update the `cylinder` dependency of libtransact and the `simple_xo` example to
  version `0.2`

## Changes in Transact 0.3.5

### libtransact Updates

* Add a 32bit-usize-friendly lmdb DEFAULT_SIZE value

## Changes in Transact 0.3.4

### libtransact Updates

* Fix a serial scheduler execution task iterator test that was failing due to
  timing issues.

## Changes in Transact 0.3.3

### libtransact Updates

* Update the executor's internal thread to shutdown the fan-out threads after
  its main loop has terminated to ensure they get shut down properly every time.
* Fix a bug where the task readers in the executor were always being given an
  index of 0.
* Add better management of task readers to the executor to enable shutting down
  the task readers when they have completed. Previously, these task readers were
  not being cleaned up, which leaked memory.
* Fix a bug in the serial scheduler's shutdown process where the scheduler's
  core thread did not always send a message to terminate the execution task
  iterator.
* Update the serial scheduler's execution task iterator to log an error whenever
  the scheduler's core thread shuts down without notifying the iterator.

## Changes in Transact 0.3.2

### libtransact Updates

* Fix an issue in `state::merkle::MerkleRadixTree` where an empty set of state
  changes would produce an empty state id. This has been updated to return the
  current state root hash.
* Fix two issues in the `SerialScheduler` with call ordering of `cancel` and
  `finalize`.  These methods can be called in either order.

## Changes in Transact 0.3.1

### Highlights

* Update the `sawtooth-sdk` version to 0.5. The sawtooth-sdk is used for the
  `sawtooth-compat` feature.

## Changes in Transact 0.3.0

### Highlights

* The `contract-archive` feature has been stabilized. This feature guards the
  smart contract archive (scar) functionality.
* The `transact::signing` module has been replaced by the `cylinder` library
  crate.

### libtransact Updates

* Update the `sawtooth-sdk` and `sawtooth-xo` dependencies to `0.4`.
* Add a `clone_box` method to `ContextLifecycle` that enables the trait to be
  clone-able.
* Add the `ExecutionTaskSubmitter` for submitting tasks to the executor. This
  struct is thread-safe and allows a single executor to be used across threads.
* Update the `families::command::make_command_transaction` function to take a
  signer for signing the transaction.
* Refactor the `XoTransactionWorkload` and `XoBatchWorkload` to take optional
  workload seeds and signers.

#### Scheduler

* Add the `SchedulerFactory` trait for creating generic schedulers, along with
  implementations for `MultiScheduler` and `SerialScheduler`.
* Require implementations of the `Scheduler` trait to have the `Send` marker
  trait.
* Change the `Scheduler::cancel` method to abort and return any in-progress
  batches.
* Change the `Scheduler::cancel` method to send a `None` execution result if the
  scheduler is finished.
* Update all schedulers to shut themselves down when they are finished and have
  no more batches to execute.
* Fix a bug in the serial scheduler where a `None` execution result was not sent
  when the scheduler was finalized with a non-empty batch queue.

### CI Changes

* Enable nightly builds to catch any breaking changes in dependencies or
  upstream tools.

## Changes in Transact 0.2.4

### Experimental Changes

* Add a field to the `SqliteDatabaseBuilder` to modify the "synchronous" setting
  via PRAGMA statement. This setting provides different guarantees of data
  safety for situations such as OS crash or power loss.
* Make the `transact::workload` module publicly available with the experimental
  feature "workload".
* Make the xo workload tools publicly available with the
  `transact::families::xo` module and the experimental feature "family-xo".
* Make the `CommandTransactionHandler` and associated methods for the command
  transaction family publicly available with the `transact::families::command`
  module and the experimental feature "family-command".

### Other Changes

* Add a justfile to enable running `just build`, `just lint`, and `just test`

## Changes in Transact 0.2.3

### Experimental Changes

* Allow configuring "Write-Ahead Logging" (WAL) journal mode for SQLite
  databases. This mode offers significant performance improvements over the
  standard atomic commit and rollback mode.

### Other Changes

* Add `TryFrom` implementations to convert `TransactionResult` enums and
  `TransactionReceipt` structs into `StateChange` structs.

## Changes in Transact 0.2.2

### Experimental Changes

* Update the `SmartContractArchive::from_scar_file` constructor to take multiple
  search paths for .scar files.
* Add a configurable prefix for table names in SQLite databases.
* Enable reading state via the cursor when using the SQLite database writer.

## Changes in Transact 0.2.1:

### Highlights:

* Enable loading a .scar file into a native `SmartContractArchive` with the
  experimental feature "contract-archive"

## Changes in Transact 0.2.0:

### Highlights:

* Add experimental SQLite support for state storage
* Add an executable example using the Addresser trait implementations to
  generate radix addresses for up to three natural keys
* Add support for storing invalid transaction results as transaction receipts

### Breaking Changes:

* Update DatabaseReader trait such that its "get" method returns a result
* Remove change_log from the Merkle state implementation's public API
* Update transaction receipt builder to allow building transaction receipts for
  invalid transactions.
* Capture invalid transaction results as transaction receipts

### Experimental Changes:

* Add all experimental features to the Rust docs
* Add a SQLite-backed implementation of the Database trait behind the
  experimental feature "sqlite-db"
* Add KeyValueTransactionContext implementation behind the experimental
  "contract-context-key-value" feature, which uses a key-value state
  representation
* Update Addresser error messages to be more accurate
* Add more robust validation and error handling within the Addresser trait
  implementations
* Add experimental "key-value-state" feature

### Other Changes:

* Add conversion from `TransactionBuilder` into `BatchBuilder` for easier batch
  building
* Support converting a `BatchList` to and from a native `Vec<Batch>`
* Add "state-merkle" feature for conditional compilation of the Merkle state
  implementation
* Fix bug that converted transaction receipt header signatures to hex twice

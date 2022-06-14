# Release Notes

## Changes in Transact 0.4.5

### Highlights

* Use of `SqlMerkleState` in an existing transaction is now stable.
* State pruning has now be fixed for `SqlMerkleState`.

### libtransact

* Stabilize "version 2" state traits under the feature `"state-in-transaction"`.
* Stabilize `InTransaction-` variants of the Postgres and SQLite `Backend`
  implementations behind the feature `"state-merkle-sql-in-transaction"`.
* Remove the use of `immediate_transaction` in SQLite merkle state operations.
* Fix state pruning in `SqlMerkleState` by implemented reference counting of the
  `merkle_radix_tree_node` entries.  As it is easy to produce duplicate nodes,
  the reference tracks the number of duplicate entries.  As state is pruned, the
  reference count is reduced. At `0`, the record is considered complete pruned.
  The change log tables are also marked as pruned, via a timestamp.
* Add `remove_pruned_entries` to `SqlMerkleState` to clean up all records that
  have been marked as pruned.

## Changes in Transact 0.4.4

### Highlights

* When using `SqlMerkleState`, large state values are cached. This cache
  mitigates issues when used with large state values and Postgres, where the
  frequent return of large data (for example, when smart contracts are in play)
  causes a cloud instance to throttle bandwidth.

### libtransact

* Add caching to `SqlMerkleState` for large state values.  This cache is
  configurable via the `SqlMerkleStateBuilder`.
* Add experimental "version 2" State traits.  These traits provide a more
  flexible division of State operations and include stronger type guarantees
  with certain parameters.
* Add experimental `InTransaction-` variants of the SQLite and Postgres
  `Backend` implementations.  These back-ends allow for commits to state to
  occur within a broader transaction such that many tables, including the merkle
  state tables, can be updated atomically.

## CLI

* Add a 0.25 second sleep to the end of the loop in the `BatchStatusChecker`
  thread to lower command workload CPU usage.

## Changes in Transact 0.4.3

* Alter column type for `merkle_radix_leaf.address` in SQLite. This fixes a bug
  where address that were valid numeric values would have their leading 0s
  removed (as expressed in the hex string of the address).
* Move `CommandTransactionBuilder` from `transact::families::command::workload`
  to `transact::families::command`. The original version remains and is now
  deprecated.
* Update rustdoc for transact::state::merkle module with more detail
* Update rustdoc for transact::state::Prune trait for clarity

### libtransact

## Changes in Transact 0.4.2

### libtransact

* Update `SqliteMerkleRadixStore` with exclusive write.  This updates
  `SqliteMerkleRadixStore` such that that all writes are exclusive, but multiple
  readers will be possible. This mitigates bugs that may occur when concurrent
  writes are allowed.
* Add `From<Arc<RwLock<Pool...>>>` implementation for `SqliteBackend`.

## Changes in Transact 0.4.1

### Highlights

* Add workload CLI tool.  This CLI tool currently supports submitting `command`
  and `smallbank` (experimentally) transactions at configurable rates and
  durations.
* Add finer-grained features to allow for more flexible library usage.  With
  these new features, library consumers don't necessarily need to pull in all
  the modules when using Transact in their applications.
* Add smallbank smart contract example.  This contract has been ported from the
  sawtooth-rust-sdk.
* Add command smart contract.

### libtransact

* Implement `ContextManager::drop_context`, where contexts are reference
  counted.
* Update serial scheduler to drop contexts where appropriate.
* Change `BatchHeader::transaction_ids` from `Vec<Vec<u8>>` to `Vec<String>` to
  make all header signature fields consistently typed.
* Change `TransactionHeader::dependencies` from `Vec<Vec<u8>>` to `Vec<String>`
  to make all header signature fields consistently typed.
* Change `HashMethod:SHA512` to HashMethod::Sha512, following the recommended
  naming conventions.
* Drop experimental feature `"sqlite-db"`, in favor of `"state-merkle-sql"` and
  `"sqlite"`.
* Drop experimental feature `"redis-db"`, as Redis does not meet the
  transactional requirements of the Merkle State.
* Add `transact::state::merkle::kv::StateDatabaseError::InternalError` variant,
  wrapping `transact::error::InternalError`.
* Add `"sabre-compat"` feature for writing wasm-compatible smart contracts.
* Add the smallbank transaction handler.
* Add `xo`, `smallbank` and `command` implementations of the
  `TransactionWorkload` and `BatchWorkload` traits.
* Remove dependency on openssl.

### CLI

* Add a CLI command named `command` that has three subcommands `set-state`,
  `get-state`, and `show-state` which can be used to interact with the command
  smart contract.
* Add a `playlist` CLI command that has four subcommands `create`, `process`,
  `batch`, and `submit` which can be used to generate files of pregenerated
  payloads, transactions, and batches. The file containing the batches can then
  be submitted against a distributed ledger.
* Add a `workload` CLI command which can be used to submit a workload against a
  distributed ledger.

### Examples

* Add a sabre-compatible smallbank smart contract.
* Add a sabre-compatible command smart contract.
* Add an example application that submits command transactions directly to
  sabre.

# Changes in Transact 0.3.14

### libtransact updates

* Make `MerkleRadixStore` and the SQL implementation public.  This allows
  library consumers access to the lower-level APIs for interacting with the
  merkle-radix tree storage layer.

* Add `list_trees` to `MerkleRadixStore`.  This allows the user to list the
  available trees stored in the underlying store.

## Changes in Transact 0.3.13

### libtransact updates

* Add `delete_tree` to `SqlMerkleState`. This allows the entire tree to be
  deleted, include all state root hashes and leaf data associated with that
  tree.

## Changes in Transact 0.3.12

### libtransact updates

* Limit query recursion to single tree. This fixes an issue where queries in
  a database with trees with identical structure would cause an infinite loop
  in SQLite.

* Use SQLite immediate transactions. This improves multi-threaded support.

* Update SQLite PRAGMA for the WAL journal mode. This improves multi-threaded
  support.

* Remove manual ID sequences for Postgres. This improves multi-thread support.

* Update defaults for SQLite "synchronous" PRAGMA. This changes the default
  "synchronous" PRAGMA setting to explicitly be "Normal" or "Full", if the WAL
  journal mode is enabled.

## Changes in Transact 0.3.11

### Highlights

* The `"state-merkle-sql"` feature has been stabilized, along with the
  `"postgres"` and `"sqlite"` features.

### libtransact updates

* Stabilize `"state-merkle-sql"` by moving it to the `"stable"` feature group.

* Stabilize `"postgres"` by moving it to the `"stable"` feature group.

* Stabilize `"sqlite"` by moving it to the `"stable"` feature group.

* Replace `OverlayReader` and `-Writer` with `MerkleRadixStore`.

* Move operations, schema, and models to `store` module.

* Merge `insert_node` and `update_change_log` operations into single
  `write_changes` operation.

* Separate all Postgres- and SQLite-specific code into respective `postgres` and
  `sqlite` submodules, relative to the parent module.

* Allow migrations to be run against a single connection.  This allows
  migrations to be run in an instance where the caller does not have access to a
  connection pool or the connection string and, therefore, cannot use a
  `Backend` instance via the `MigrationManager` trait.

## Changes in Transact 0.3.10

### Highlights

* The `"state-merkle-leaf-reader"` feature has been stabilized by being removed.
  This makes the `MerkleLeafReader` trait part of the standard, stable API.

### libtransact updates

* Generalize the feature guard over `serde_derive`.  This change removes the
  possibility of the macros being unavailable if a specific libtransact feature
  is not enabled.  Now any feature that depends on `serde_derive` will have the
  macros available.

* Re-export `transact::state::merkle::kv::MerkelLeafIterator` in the parent
  module for backwards-compatibility.  This type was previously part of the
  public API.

* Soft-deprecate the type `transact::state::merkle::kv::MerkelLeafIterator`, as
  this type should not be part of the public API, and may be removed in a future
  release.

* Return `InvalidStateError` when a `StateDatabaseError::NotFound` variant is
  encountered during leaf iteration when implemented on `kv::MerkleState`.

* Stabilize `"state-merkle-leaf-reader"` feature by removing it. This makes the
  `MerkleLeafReader` trait part of the standard, stable API.

## Changes in Transact 0.3.9

### libtransact updates

* Remove `StateDatabaseError::InternalError` variant.  This change was not
  backwards compatible.

## Changes in Transact 0.3.8 (yanked)

### Highlights

* Experimental support for merkle state stored in Postgres and SQLite databases.
  This is available via new struct `SqlMerkleState` and activated by the
  features `"state-merkle-sql"` with `"postgres"` and/or `"sqlite"` enabled for
  either database.

### libtransact updates

* Move existing key-value `MerkleState` implementation to
  `transact::state::merkle::kv`.  This implementation is backed by the key-value
  `Database` abstraction.  It is re-exported in the `transact::state::merkle`
  module for backwards compatibility.

* Add experimental `SqlMerkelState` available in the module
  `transact::state::merkle::sql`. This is activated by the features
  `"state-merkle-sql"` with `"postgres"` and/or `"sqlite"` enabled for either
  database.

* Add `transact::error::InternalError`, copied from the splinter library.

* Add `transact::error::InvalidStateError`, copied from the splinter library.

* Expand SQLite journal configuration in experimental `SqliteDatabase`

## Changes in Transact 0.3.7

* Update `semver` dependency from `0.9` to `1.0`.
* Fix various clippy errors that were introduced with the release of Rust 1.50.
* Update `protobuf` dependency from `2.0` to `2.19`.

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

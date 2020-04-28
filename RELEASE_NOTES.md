# Release Notes

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

# Release Notes

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

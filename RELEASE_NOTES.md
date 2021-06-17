# Release Notes

## Changes in Sawtooth Sabre 0.8.0

* Unreleased version

## Changes in Sawtooth Sabre 0.7.1

* Update the allowed family version to include 0.5, 0.6 and update the current
  version to 1. This will allow the Sabre transaction processor to accept
  transactions from 0.5, 0.6 and 1 versions of Sabre.

## Changes in Sawtooth Sabre 0.7.0

* Unreleased version

## Changes in Sawtooth Sabre 0.6.1

* Update the `cylinder` dependency of the Rust SDK and the CLI to version `0.2`

## Changes in Sawtooth Sabre 0.6.0

* Add `add_events` to Sabre TransactionContext which adds events to  the
  execution result for a transaction. This brings the Sabre API closer to the
  Sawtooth TransactionContext API.
* Improve error messages when there is an InvalidTransaction caused by a
  transaction not being submitted by an authorized admin.
* Update sawtooth-sdk dependency version to 0.5.
* Update transact dependency version to 0.3.

## Changes in Sawtooth Sabre 0.5.2

* Add `Result` class to the AssemblyScript SDK for basic error handling
* Update inc-dec example AssemblyScript smart contract to use `Result` for
  errors
* Fix bug where the Rust SDK was computing addresses with incorrect lengths
* Add address prefixes and the administrators setting address as bytes constants
  to the Rust SDK
* Fix a bug where the administrators setting address was being converted to
  bytes incorrectly
* Add the Sabre protocol version to the Rust SDK as a constant to ensure the SDK
  builds transactions for the equivalent version of the Sabre transaction
  processor
* Fix bug where the agent address was calculated incorrectly by the
  `SabrePayloadBuilder::into_transaction_builder` method

## Changes in Sawtooth Sabre 0.5.1

* Update the family version for Sabre transactions in the Sabre transaction
  processor and Sabre CLI
* Fix a broken path in the Dockerfile for publishing the Sabre SDK to crates.io

## Changes in Sawtooth Sabre 0.5

### Highlights

* Add AssemblyScript SDK for smart contracts with an example "incdec" smart
  contract
* Add important address prefixes and address computation functions to the Rust
  SDK
* Add `into_payload_builder` methods to each `*ActionBuilder` in the Rust SDK
* Add `into_transaction_builder` method to `SabrePayloadBuilder` in the Rust SDK

### Breaking Changes

* Update all `*ActionBuilder` structs to use a single, common `ActionBuildError`

### Other Changes
* Package intkey_multiply example smart contract as .scar file
* Add an optional argument to the `sabre upload` CLI command for manually
  specifying the path of a .wasm contract
* Remove an unnecessary unwrap in the transaction processor library
* Use stable Rust instead of nightly for the sabre integration dockerfile
* Fix typos in the documentation
* Add safety warnings for unsafe functions
* Return a response when waiting for batch in CLI
* Update all Docker Compose files to pull latest Docker images for Sawtooth
* Implement `From<*Action>` traits for all `*Action` structs on the `Action`
  struct in the Rust SDK

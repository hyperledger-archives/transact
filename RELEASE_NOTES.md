# Release Notes

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

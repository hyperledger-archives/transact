# Release Notes

## Changes in Transact 0.1.7:

### Highlights:

* Add experimental Redis support for state storage
* Move the Sawtooth XO test to an executable example
* Add a contributing guide and security policy to the repository

### Details:

* Add multiple implementations of the Addresser trait, with tests, to support
  natural keys that are represented by up to three strings
* Add the Addresser trait to the address module
* Add initial contract and address modules to hold work behind the experimental
  “contract” and “contract-address” features, respectively
* Add a list of Transact features to the documentation, which is published to
  https://docs.rs/transact/
* Update the CI docker compose file to run tests with “redis-db” feature enabled
* Change unwrap() to expect() in tests to improve output for test failures
* Create database-agnostic integration tests
* Move Merkle state tests to the integration path, which is based on the Cargo
  default path
* Rename the DatabaseReaderCursor first and last methods to avoid conflicts with
  Iterator methods with the same name 
* Add a Redis-backed implementation of the Database trait behind the
  experimental feature “redis-db”
* Add secp256k1 signing from Hyperledger Ursa 
* Add a whitelist check to the Jenkinsfile
* Add a method to convert between the state module’s StateChange and the
  transact receipt module’s StateChange
* Remove unnecessary unwraps, which caused clippy errors
* Fix lintPR from Rust format updates and Clippy lint updates
* Add dyn keyword where dynamic traits are used
* Add a contribution guide in CONTRIBUTING.md
* Add a default security policy in SECURITY.md

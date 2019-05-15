****************************************
Sabre and Sawtooth Version Compatibility
****************************************

The following table shows the compatible versions of the Sabre transaction
processor, Sabre SDK, and Sawtooth Rust SDK. It also shows the Docker tag for
the Sabre transaction processor image.

 - The Sabre transaction processor versions are
   the versions used as part of the transaction processor’s registration.

 - The Sabre SDK versions are the Crate versions of the Rust library that should
   be set in the Cargo.toml file.

 - The Docker tag is the tag that should be used for the
   hyperledger/sawtooth-sabre-tp image if including in a docker-compose yaml
   file.

 - The Sawtooth Rust SDK versions are the Crate versions of the Rust library
   that should be set in the Cargo.toml file.

+------------+----------+-----------+---------+--------------------------------+
| Sabre      | Sabre SDK| Docker Tag| Sawtooth| Changes                        |
| Transaction|          |           | Rust SDK|                                |
| Processor  |          |           |         |                                |
+============+==========+===========+=========+================================+
| 0.0        | 0.1      | 0.1       |  0.2    |                                |
|            |          |           |         |                                |
|            |          |           |         |                                |
+------------+----------+-----------+---------+--------------------------------+
| 0.2        | 0.2      | 0.2       |  0.3    | - Transaction context is a     |
|            |          |           |         |   trait                        |
|            |          |           |         | - API has new get_state_entry  |
|            |          |           |         |   to get one entry and         |
|            |          |           |         |   get_state_entries to get     |
|            |          |           |         |   multiple entries (plus       |
|            |          |           |         |   corresponding functions for  |
|            |          |           |         |   set and delete)              |
+------------+----------+-----------+---------+--------------------------------+

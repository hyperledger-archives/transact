***********************************
Sabre Application Developer's Guide
***********************************
The guide covers development of Sabre smart contracts which can be stored on
chain and executed using the Sabre Transaction Processor. Topics covered
include how to write a smart contract, how to convert an already existing
transaction processor to a smart contract, and how to upload and execute the
contracts.

It would be a good idea to read through the Sabre Transaction Family
Specification before reading through this guide.

Currently, Sabre smart contracts can only be written in Rust. The following
guide will use an example smart contract called intkey-multiply. This smart
contract will take intkey values, multiply them, and store the new intkey key
value pair in the intkey state.

.. note: This guide assumes familiarity with Sawtooth Transaction Processors
  and that cargo/rust is already installed.

Writing A Sabre Smart Contract
==============================
The Saber Smart Contracts use a similar API to the Sawtooth transaction
processor API. If you are unfamiliar, please take a look at
https://sawtooth.hyperledger.org/docs/core/nightly/master/sdks.html.

Include the Sabre SDK in the dependencies list of the Cargo.toml file.

.. code-block:: none

  [dependencies]
  sabre-sdk = {path = "../../../sdk"}

The Sabre SDK the provides the following required structs needed to write
a smart contract.

.. code-block:: rust

  use sabre_sdk::ApplyError;
  use sabre_sdk::TransactionContext;
  use sabre_sdk::TransactionHandler;
  use sabre_sdk::TpProcessRequest;
  use sabre_sdk::{WasmPtr, execute_entrypoint};

- ApplyError: Used to raise an InvalidTransaction or an InternalError
- TransactionHandler: Mimics the Sawtooth SDK TransactionHandler and should be
    used when writting a smart contract that can also function as a Transaction
    Processor.
- TpProcessRequest: Mimics the TpProcessRequest used by the Transaction
    Processor API. The payload, header and signer can be retrieved from this
    struct.
- WasmPtr: Pointer to location in executor memory.
- execute_entrypoint: Required by the entrypoint function used by the wasm
    interpreter to execute the contract.

Every smart contract must contain an entrypoint function and an apply function
that can be passed into execute_entrypoint.

.. code-block:: rust

  #[no_mangle]
  pub unsafe fn entrypoint(payload: WasmPtr, signer: WasmPtr: signature: WasmPtr) -> i32 {
      execute_entrypoint(payload, signer, signature, apply)
  }

The apply method should have the following signature.

.. code-block:: rust

  fn apply(
      request: &TpProcessRequest,
      context: &mut TransactionContext,
  ) -> Result<bool, ApplyError>

The main function can be empty if you are only writing the smart contract to
be deployed, not started up as a transaction processor in a different process.

.. code-block:: rust

  fn main() {
  }

Converting a Transaction Processor to a Smart Contract
------------------------------------------------------
Due to the similar API used for the Sabre SDK and the Sawtooth Transaction
Processor SDK it is very simple to convert an existing transaction processor to
a smart contract. In fact the exact same code can be run in either format.

The Cargo.toml file should be updated to use Rust's built in cfg attribute.
Split the dependencies into what is required for normal transaction processors,
dependencies for the smart contract, and dependencies used by both.
Dependencies used by both should remain under ``[dependencies]``, while smart
contract dependencies should go under the
``[target.'cfg(target_arch = "wasm32")'.dependencies]`` and transaction processors
dependencies should go under ``[target.'cfg(unix)'.dependencies]``.

The following is an example for intkey-multiply

.. code-block:: none

  [package]
  name = "intkey-multiply"
  version = "0.1.0"
  authors = ["Cargill Incorporated"]

  [dependencies]
  clap = "2"
  protobuf = "2"
  cfg-if = "0.1"
  hex = "0.3.1"

  [target.'cfg(target_arch = "wasm32")'.dependencies]
  rust_crypto = {git = "https://github.com/agunde406/rust-crypto", branch="wasm_sha2"}
  sabre-sdk = {path = "../../../sdk"}

  [target.'cfg(unix)'.dependencies]
  rust-crypto = "0.2.36"
  sawtooth_sdk = { git = "https://github.com/hyperledger/sawtooth-core.git" }
  rustc-serialize = "0.3.22"
  log = "0.3.0"
  log4rs = "0.7.0"
  zmq = { git = "https://github.com/erickt/rust-zmq", branch = "release/v0.8" }


The main.rs file for the transaction processor should separate out the
different extern crate and use statements. This can be done using cfg_if. Make
the handler a public module and add an empty main function.

.. code-block:: rust

  extern crate cfg_if;

  cfg_if! {
      if #[cfg(target_arch = "wasm32")] {
          extern crate sabre_sdk;
      } else {
          #[macro_use]
          extern crate clap;
          extern crate log4rs;
          #[macro_use]
          extern crate log;
          extern crate rustc_serialize;
          extern crate sawtooth_sdk;
          use std::process;
          use log::LogLevelFilter;
          use log4rs::append::console::ConsoleAppender;
          use log4rs::config::{Appender, Config, Root};
          use log4rs::encode::pattern::PatternEncoder;
          use sawtooth_sdk::processor::TransactionProcessor;
          use handler::IntkeyMultiplyTransactionHandler;
      }
  }

  pub mod handler;

  #[cfg(target_arch = "wasm32")]
  fn main() {}

The handler.rs file should also separate out the use statements using cfg_if.
The smart contract apply function should wrap the existing TransactionHandler
and pass the TpProcessRequest to the handler's apply method. The smart contract
apply function should return Ok(true) if the transaction was valid, otherwise
return the returned ApplyError. Finally, the entrypoint function needs to be
added to the file. Note that the smart contract apply function and the
entrypoint function should include the ``#[cfg(target_arch = "wasm32")]``
decorator, so they will only be compiled when compiling into Wasm.

.. code-block:: rust

  cfg_if! {
      if #[cfg(target_arch = "wasm32")] {
          use sabre_sdk::ApplyError;
          use sabre_sdk::TransactionContext;
          use sabre_sdk::TransactionHandler;
          use sabre_sdk::TpProcessRequest;
          use sabre_sdk::{WasmPtr, execute_entrypoint};
      } else {
          use sawtooth_sdk::processor::handler::ApplyError;
          use sawtooth_sdk::processor::handler::TransactionContext;
          use sawtooth_sdk::processor::handler::TransactionHandler;
          use sawtooth_sdk::messages::processor::TpProcessRequest;
      }
  }

  #[cfg(target_arch = "wasm32")]
  // Sabre apply must return a bool
  fn apply(
      request: &TpProcessRequest,
      context: &mut TransactionContext,
  ) -> Result<bool, ApplyError> {

      let handler = IntkeyMultiplyTransactionHandler::new();
      match handler.apply(request, context) {
          Ok(_) => Ok(true),
          Err(err) => Err(err)
      }

  }

  #[cfg(target_arch = "wasm32")]
  #[no_mangle]
  pub unsafe fn entrypoint(payload: WasmPtr, signer: WasmPtr, signature: WasmPtr) -> i32 {
      execute_entrypoint(payload, signer, signature, apply)
  }

.. note:: Though the goal is compatibility with the transaction processor API,
  it is not always trivial to compile commonly used Rust dependencies into Wasm.
  This may improve over time as Wasm popularity grows, or it may persist into
  the future.

  For example, cbor-codec, cbor crate used in the intkey transaction processor,
  does not compile into wasm and serde_cbor is missing libm dependencies at
  runtime. To bypass this, custom intkey cbor encode and decode functions had
  to be written for intkey multiply.

For the full intkey-multiply example look at
sawtooth-sabre/example/intkey_multiply/processor

Compiling the Contract
======================
To compile your smart contract into wasm you need to use Rust's nightly tool
chain and need to add target wasm32-unknown-unknown.

.. code-block:: console

  $ rustup update
  $ rustup default nightly
  $ rustup target add wasm32-unknown-unknown --toolchain nightly

To compile the smart contract run the following command in
sawtooth-sabre/example/intkey_multiply/processor:

.. code-block:: console

  $ cargo build --target wasm32-unknown-unknown --release

.. note:: The compiled Wasm file is going to be quite large
  due to the fact that Rust does not have a proper linker yet. Here are a few
  simple things you can do to help reduce the size.

  - Compile in --release mode
  - Remove any "{:?}" from any format strings, as this pulls in a bunch of stuff
  - Use this script to reduce the size https://www.hellorust.com/news/native-wasm-target.html

Running Sabre
=============
This section will talk about how you can run a Sabre network and test your
smart contract.

A docker-compose file has been provided for starting up the network, plus an
example docker-compose file that can be linked to pull in intkey-multiply
specific containers. There is also a Sabre CLI that can be used to submit
Sabre Transactions.

Starting the network
--------------------
Only an administrator, whose public key is stored in
``sawtooth.swa.administrators`` are allowed to add namespace registries to the
Sabre network. As such this key, is shared between the Sawtooth Validator
container and the Sabre Cli container.

To start up the network run the following command from the top level
sawtooth-sabre directory:

.. code-block:: console

  $ docker-compose -f docker-compose.yaml -f example/intkey_multiply/docker-compose.yaml up

This will start up both the default Saber docker-compose file while also
linking a intkey-mulitply docker compose file that includes an intkey
transaction processor and the intkey-multiply cli.

Startup will take a while as it has to run cargo build on the rust components.

Intkey State
------------
Intkey-multiply multiplies already set intkey values and stores them in a new,
unset key. As such, some intkey state needs to be set before we can successfully
execute the new contract.

Enter the sabre-shell docker container from a different terminal window:

.. code-block:: console

  $ docker exec -it sabre-shell bash

Run the following commands to submit intkey transaction to set B to 10 and C to
5.

.. code-block:: console

  $ intkey set B 10 --url http://rest-api:9708
  $ intkey set C 5 --url http://rest-api:9708
  $ intkey list --url http://rest-api:9708

  B: 10
  C: 5

Logout of the container.

Generate Payload
----------------
The intkey-multiply example includes a CLI that can generate the required
payloads needed to execute the contract using the Sabre CLI.

Enter the intkey-multiply-cli docker container:

.. code-block:: console

  $ docker exec -it intkey-multiply-cli bash

.. code-block:: console

  $ intkey-multiply multiply -h

  intkey-multiply-multiply
  multiply two intkey values together

  USAGE:
      intkey-multiply multiply [OPTIONS] <name_a> <name_b> <name_c>

  FLAGS:
      -h, --help       Prints help information
      -V, --version    Prints version information

  OPTIONS:
      -k, --key <key>          Signing key name
      -o, --output <output>    File name to write payload to.
          --url <url>          URL to the Sawtooth REST API

  ARGS:
      <name_a>    Intkey key to store multiplied value
      <name_b>    Intkey key for the first value to multiply
      <name_c>    Intkey key for the second value to multiply

The following command will create a payload to multiply B and C and store it in
A. By adding the --output option, the payload will be written out into a file
instead of sending the transaction to the REST-API.

.. code-block:: console

  $ intkey-multiply multiply A B C --output payload

Logout out of the container.

Uploading Contract
------------------
Enter the sabre-cli docker container:

.. code-block:: console

  $ docker exec -it sabre-cli bash

To upload a Sabre contract, you can use the following command

.. code-block:: console

  $ sabre upload -h

  sabre-upload
  upload a Sabre contract

  USAGE:
    sabre upload [OPTIONS] --filename <filename>

  FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

  OPTIONS:
    -f, --filename <filename>    Path to Sabre contract definition (*.yaml)
    -k, --key <key>              Signing key name
        --url <url>              URL to the Sawtooth REST API

The filename is the path to a Sabre contract definition. A contract definition
needs to include the contract name, version, path to the compiled wasm file,
and the contract's inputs and outputs. The definition must also be in yaml
format.

The intkey-multiply contract definition looks like the following:

.. code-block:: yaml

  name: intkey_multiply
  version: '1.0'
  wasm: processor/target/wasm32-unknown-unknown/release/intkey_multiply.wasm
  inputs:
    - '1cf126'
  outputs:
    - '1cf126'

Run the following to upload the intkey-multiply contract.

.. code-block:: console

  $ sabre upload --filename ../example/intkey_multiply/intkey_multiply.yaml --url http://rest-api:9708

Set up Namespace and Permissions
--------------------------------
If you were to try and execute the intkey-multiply contract right now, it will
fail. This is because the contract has not been granted read and/or write
permission for the intkey namespace.

Namespace can only be created by a sawtooth administrator, as mentioned above.
Once the namespace is created, any owner is allowed to update and delete
permission, as well as delete the namespace if it has no permissions.

.. code-block:: console

  $ sabre ns -h

  sabre-ns
  create, update, or delete a Sabre namespace

  USAGE:
      sabre ns [FLAGS] [OPTIONS] <namespace>

  FLAGS:
      -c, --create     Create the namespace
      -d, --delete     Delete the namespace
      -h, --help       Prints help information
      -u, --update     Update the namespace
      -V, --version    Prints version information

  OPTIONS:
      -k, --key <key>           Signing key name
      -O, --owner <owner>...    Owner of this namespace
      -U, --url <url>           URL to the Sawtooth REST API

  ARGS:
      <namespace>    A global state address prefix (namespace)

The following command creates the intkey namespace:

.. code-block:: console

  $ sabre ns --create 1cf126 --owner <owner-key> --url http://rest-api:9708

.. note:: The <owner-key> should be set to your public key.

Once the namespace is created, intkey_multiply needs to be given read and write
permissions for the intkey namespace. Only owners or administrators can update
the namespaces permissions. This can be done using the Sabre CLI.

.. code-block:: console

  $ sabre perm -h

  sabre-perm
  set or delete a Sabre namespace permission

  USAGE:
      sabre perm [FLAGS] [OPTIONS] <namespace> <contract>

  FLAGS:
      -d, --delete     Remove all permissions
      -h, --help       Prints help information
      -r, --read       Set read permission
      -V, --version    Prints version information
      -w, --write      Set write permission

  OPTIONS:
      -k, --key <key>    Signing key name
      -U, --url <url>    URL to the Sawtooth REST API

  ARGS:
      <namespace>    A global state address prefix (namespace)
      <contract>     Name of the contract

The following gives intkey_multiply read and write permissions to the intkey
namespace:

.. code-block:: console

  $ sabre perm  1cf126 intkey_multiply --read --write --url http://rest-api:9708

Execute Contract
----------------
We can finally execute our Sabre contract. To execute the contract, the contract
name and version, a payload file and any inputs or outputs need to be provided.
The inputs and outputs must be at least 6 characters long.

.. code-block:: console

  $ sabre exec -h

  sabre-exec
  execute a Sabre contract

  USAGE:
      sabre exec [OPTIONS] --contract <contract> --payload <payload>

  FLAGS:
      -h, --help       Prints help information
      -V, --version    Prints version information

  OPTIONS:
      -C, --contract <contract>    Name:Version of a Sabre contract
          --inputs <inputs>        Input addresses used by the contract
      -k, --key <key>              Signing key name
          --outputs <outputs>      Output addresses used by the contract
      -p, --payload <payload>      Path to Sabre contract payload
          --url <url>              URL to the Sawtooth REST API

The following command submits a transaction to execute the intkey_multiply,
version 1.0, smart contract with the payload that was created earlier. The
contract requires that the intkey namespace is in the inputs and outputs.

.. code-block:: console

  $ sabre exec --contract intkey_multiply:1.0 --payload /project/example/intkey_multiply/cli/payload  --inputs  1cf126 --outputs  1cf126 --url http://rest-api:9708

Logout out of the container.

Check State
-----------
To verify that the A was set to the multiple of B and C,
enter the sabre-shell docker container:

.. code-block:: console

  $ docker exec -it sabre-shell bash

Run the following command to list intkey state

.. code-block:: console

  $ intkey list --url http://rest-api:9708

  A 50
  B 10
  C 5

You should see A set to 50.

.. Licensed under Creative Commons Attribution 4.0 International License
.. https://creativecommons.org/licenses/by/4.0/

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

.. note:: This guide assumes familiarity with Sawtooth Transaction Processors
  and that cargo/rust is already installed.

.. _writing-sabre-sm-label:

Writing A Sabre Smart Contract
==============================
The Sabre Smart Contracts use a similar API to the Sawtooth transaction
processor API. If you are unfamiliar, please take a look at
https://sawtooth.hyperledger.org/docs/core/nightly/master/sdks.html.

Include the Sabre SDK in the dependencies list of the Cargo.toml file.

.. code-block:: none

  [dependencies]
  sabre-sdk = {git = "https://github.com/hyperledger/sawtooth-sabre"}

The Sabre SDK provides the following required structs needed to write
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
  pub unsafe fn entrypoint(payload: WasmPtr, signer: WasmPtr, signature: WasmPtr) -> i32 {
      execute_entrypoint(payload, signer, signature, apply)
  }

The apply method should have the following signature.

.. code-block:: rust

  fn apply(
      request: &TpProcessRequest,
      context: &mut dyn TransactionContext,
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
dependencies should go under ``[target.'cfg(not(target_arch = "wasm32"))'.dependencies]``.

The following is an example for intkey-multiply

.. code-block:: none

  [package]
  name = "intkey-multiply"
  version = "0.1.0"
  authors = ["Cargill Incorporated"]
  edition = "2018"

  [dependencies]
  clap = "2"
  protobuf = "2"
  cfg-if = "0.1"
  hex = "0.3.1"

  [target.'cfg(target_arch = "wasm32")'.dependencies]
  rust-crypto-wasm = "0.3"
  sabre-sdk = {path = "../../../sdks/rust"}

  [target.'cfg(not(target_arch = "wasm32"))'.dependencies]
  rust-crypto = "0.2.36"
  sawtooth-sdk = {git = "https://github.com/hyperledger/sawtooth-sdk-rust"}
  rustc-serialize = "0.3.22"
  log = "0.3.0"
  log4rs = "0.7.0"


The main.rs file for the transaction processor should separate out the
different extern crate and use statements. This can be done using cfg_if. Make
the handler a public module and add an empty main function.

.. code-block:: rust

  #[macro_use]
  extern crate cfg_if;

  cfg_if! {
      if #[cfg(target_arch = "wasm32")] {
          #[macro_use]
          extern crate sabre_sdk;
      } else {
          #[macro_use]
          extern crate clap;
          #[macro_use]
          extern crate log;
          use std::process;
          use log::LogLevelFilter;
          use log4rs::append::console::ConsoleAppender;
          use log4rs::config::{Appender, Config, Root};
          use log4rs::encode::pattern::PatternEncoder;
          use sawtooth_sdk::processor::TransactionProcessor;
          use handler::IntkeyMultiplyTransactionHandler;
      }
  }

  // Make sure this is here, otherwise the entrypoint is not reachable
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
      context: &mut dyn TransactionContext,
  ) -> Result<bool, ApplyError> {

      let handler = IntkeyMultiplyTransactionHandler::new();
      match handler.apply(request, context) {
          Ok(_) => Ok(true),
          Err(err) => {
              info!("{}", err);
              Err(err)
          }
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


.. _logging-in-smart-contracts:

Logging in a Sabre Smart Contract
=================================
The Sabre SDK provides log macros that match the provided log macros from the
`log crate <https://doc.rust-lang.org/1.1.0/log/macro.info!.html>`_. To use add
``#[macro_use]`` above ``extern crate sabre_sdk`` and use the macros as normal.

.. code-block:: rust

  info!(
      "payload: {} {} {}",
      payload.get_name_a(),
      payload.get_name_b(),
      payload.get_name_c()
  );

For debugging purposes, add a log statement to the final match statement in
the apply method:

.. code-block::

  #[cfg(target_arch = "wasm32")]
  // Sabre apply must return a bool
  fn apply(
      request: &TpProcessRequest,
      context: &mut dyn TransactionContext,
  ) -> Result<bool, ApplyError> {

      let handler = IntkeyMultiplyTransactionHandler::new();
      match handler.apply(request, context) {
          Ok(_) => Ok(true),
          Err(err) => {
              info!("{}", err);
              Err(err)
          }
      }

  }

The log level of the Sabre transaction processor is enforced for all smart
contracts. For example, if the Sabre transaction processor has a log level of
``info`` a ``debug`` statement in a smart contract will not be logged.

.. _compiling-smart-contract-label:

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


Running a Sabre Smart Contract
==============================

The previous section described an example smart contract, ``intkey-multiply``,
and explained how to compile it into WebAssembly (Wasm).

This procedure describes how to run this smart contract on Sawtooth Sabre using
Docker. You will start a Sawtooth node, create the required values and payload
file for the contract, set up the required items, then execute the smart
contract and check the results.

The ``sawtooth-sabre`` repository includes a Sawtooth Docker Compose file that
starts Sawtooth Sabre in Docker containers, plus another Compose file that adds
the required containers for the example ``intkey-multiply`` environment.


Prerequisites
-------------

This procedure requires a compiled ``intkey-multiply.wasm`` smart contract,
as described in :ref:`writing-sabre-sm-label`. The Docker Compose file in this
procedure sets up shared volumes so that the ``.wasm`` file can be shared
between your host system and the appropriate Docker containers.

.. Tip::

   If you do not already have a compiled ``intkey-multiply.wasm`` file,
   do the following steps before starting this procedure:

   * Clone the sawtooth-sabre repository.

   * Compile the example smart contract, ``intkey-multiply``, as described in
     :ref:`compiling-smart-contract-label`.

This procedure also requires the namespace prefixes for your contract's inputs
and outputs (areas of state that the smart contract will read from and write
to). For the ``intkey-multiply`` example, the required namespace prefixes are
included in the example contract definition file.


Step 1: Start Sawtooth Sabre with Docker
----------------------------------------

In this step, you will start a Sawtooth node that is running a validator, REST
API, and three transaction processors: Settings, IntegerKey (intkey), and Sabre.

1. Open a terminal window on your system.

#. Go to the top-level ``sawtooth-sabre`` directory and run the following
   command:

   .. code-block:: console

      $ docker-compose -f docker-compose.yaml -f example/intkey_multiply/docker-compose.yaml up

   .. note::

      Startup takes a long time, because the Compose file runs ``cargo build``
      on the Rust components.

   The first ``docker-compose.yaml`` file sets up the Sawtooth environment:

   * Starts a container for each Sawtooth component (validator, REST API, and
     the Settings and Sabre transaction processors), plus a sabre-shell and
     sabre-cli container
   * Generates keys for the validator and root user
   * Configures root as a Sawtooth administrator (with the
     ``sawtooth.swa.administrators`` setting)
   * Shares the administrator keys between the validator and sabre-cli
     containers

   The second file, ``example/intkey_multiply/docker-compose.yaml``, starts the
   intkey transaction processor and an ``intkey-multiply-cli`` container.

#. Wait until the terminal output stops before continuing to the next step.

   The rest of this procedure will use other terminal windows. This terminal
   window will continue to display Sawtooth log messages.


Step 2: Create Initial Values for the Smart Contract
----------------------------------------------------

In this step, you will use the ``sabre-shell`` container to set initial
values in state for your contract.

The ``intkey-multiply`` smart contract executes the simple function
`A=B*C`. This contract requires existing `B` and `C` values from state,
then stores the result `A` in state. This example also requires a payload
file that identifies these values by key name.

In this step, you will use the ``intkey set`` command to submit transactions
that store the initial values in "intkey state" (the namespace used by the
IntegerKey transaction family).

1. Open a new terminal window and connect to the ``sabre-shell`` Docker
   container.

   .. code-block:: console

     $ docker exec -it sabre-shell bash

#. Submit an intkey transaction to set B to 10.

   .. code-block:: console

      # intkey set B 10 --url http://rest-api:9708

#. Submit a second intkey transaction to set C to 5.

   .. code-block:: console

      # intkey set C 5 --url http://rest-api:9708

#. Check the results.

   .. code-block:: console

      $ intkey list --url http://rest-api:9708
      B: 10
      C: 5

#. Log out of the ``sabre-shell`` container.


Step 3: Generate the Payload File
---------------------------------

In this step, you will use the ``intkey-multiply`` command to generate a
payload file for your smart contract.  This payload file is required when
executing the ``intkey-multiply`` smart contract.

1. Connect to the ``intkey-multiply-cli`` container.

   .. code-block:: console

      $ docker exec -it intkey-multiply-cli bash

#. Change to the example's ``intkey_multiply/cli`` directory.

   .. code-block:: console

      # cd /project/example/intkey_multiply/cli

#. Run the following command to create the payload file for ``intkey-multiply``.

   .. code-block:: console

      # intkey-multiply multiply A B C --output payload

   This command creates a payload file that tells the smart contract to
   multiply B and C, then store the result in A. The ``--output`` option writes
   the payload to a file instead of sending the transaction directly to the REST
   API. For more information on command options, run
   ``intkey-multiply multiply --help``.

#. Log out of the ``intkey-multiply-cli`` container.


Step 4: Create a Contract Registry
----------------------------------

In this step, you will use the ``sabre-cli`` container to create a contract
registry for the ``intkey-multiply`` smart contract.

Each smart contract requires a contract registry so that Sabre can keep track of
the contract's versions and owners. A contract registry has the same name as its
contract and has one or more owners. For more information, see
:ref:`TPdoc-ContractRegistry-label`.

.. note::

   Only a Sawtooth administrator (defined in the ``sawtooth.swa.administrators``
   setting) can create a contract registry and set the initial owner or owners.
   The example Docker Compose file sets up root as a Sawtooth administrator and
   shares the root keys between the validator container and the ``sabre-cli``
   container.

1. Connect to the ``sabre-cli`` container.

   .. code-block:: console

      $ docker exec -it sabre-cli bash

#. Copy your public key.

   .. code-block:: console

      # cat /root/.sawtooth/keys/root.pub

   This example uses root as the contract registry owner. In a production
   environment, the owner would be a regular user, not root.

#. Use the ``sabre cr`` command to create a contract registry for the
   ``intkey_multiply`` smart contract. Replace ``{owner-public-key}`` with your
   public key.

   .. code-block:: console

      # sabre cr --create intkey_multiply --owner {owner-public-key} --url http://rest-api:9708

  This command creates a contract registry named ``intkey_multiply`` (the
  same name as the smart contract) with one owner.  To specify multiple owners,
  repeat the ``--owner`` option. For more information on command options, run
  ``sabre cr --help``.

Once the contract registry is created, any contract registry owner can add
and delete versions of the contract. An owner can also delete an empty
contract registry.


Step 5. Upload the Contract Definition File
-------------------------------------------

In this step, you will continue to use the ``sabre-cli`` container to upload
a contract definition file for the ``intkey-multiply`` smart contract.

Each Sabre smart contract requires a contract definition file in YAML format.
This file specifies the contract name, version, path to the compiled contract
(Wasm file), and the contract's inputs and outputs.
The ``sawtooth-sabre`` repository includes an example contract definition file,
``intkey_multiply.yaml``.

1. Display the example contract definition file.

   .. code-block:: console

      $ cat /project/example/intkey_multiply/intkey_multiply.yaml

2. Ensure that this file has the following contents:

   .. code-block:: none

      name: intkey_multiply
      version: '1.0'
      wasm: processor/target/wasm32-unknown-unknown/release/intkey-multiply.wasm
      inputs:
        - 'cad11d'
        - '1cf126'
        - '00ec03'
      outputs:
        - '1cf126'
        - 'cad11d'
        - '00ec03'

   The inputs and outputs specify the namespaces that the contract can read from
   and write to. This example uses the following namespace prefixes:

   * ``1cf126``: intkey namespace
   * ``00ec03``: Sabre smart permission namespace
   * ``cad11d``: Pike (identity management) namespace

3. Run the following command to upload this contract definition file to Sabre.

   .. code-block:: console

      # sabre upload --filename ../example/intkey_multiply/intkey_multiply.yaml --url http://rest-api:9708

   .. note::

      Only a Sawtooth administrator or contract registry owner can upload
      a new or updated smart contract.

   By default, the signing key name is set to your public key (root in this
   example). Use the ``--key`` option to specify a different signing key name.
   For more information on command options, run ``sabre upload --help``.


Step 6. Create a Namespace Registry and Set Contract Permissions
----------------------------------------------------------------

In this step, you will continue to use the ``sabre-cli`` container to create a
namespace registry, then set the namespace read and write permissions for your
contract.

Each smart contract requires a namespace registry that specifies the area in
state that the contract will read from and write to. You must also grant
explicit namespace read and write permissions to the contract.

.. note::

   Only a Sawtooth administrator (defined in the ``sawtooth.swa.administrators``
   setting) can create a namespace registry and set the initial owner or owners.
   Once the namespace registry is created, any namespace registry owner can
   change and delete contract permissions. An owner can also delete an empty
   namespace registry (one with no contract permissions).

1. Copy your public key.

   .. code-block:: console

      # cat /root/.sawtooth/keys/root.pub

   This example uses root as the namespace registry owner. In a production
   environment, the owner would be a regular user, not root.

#. Use ``sabre ns`` to create the namespace registry. Replace ``{owner-key}``
   with your public key.

   .. code-block:: console

      # sabre ns --create 1cf126 --owner {owner-key} --url http://rest-api:9708

   This command specifies the intkey namespace prefix (``1cf126``) and defines
   root as the namespace registry owner.
   For more information on command options, run ``sabre ns --help``.

#. Use ``sabre perm`` to grant the appropriate namespace permissions for your
   smart contract.

   .. code-block:: console

      # sabre perm  1cf126 intkey_multiply --read --write --url http://rest-api:9708

   This command gives ``intkey-multiply`` both read and write permissions for
   the intkey namespace (``1cf126``). For more information on command options,
   run ``sabre perm --help``.

#. Use ``sabre ns`` to create the namespace registry for pike. Replace
   ``{owner-key}`` with your public key.

   .. code-block:: console

      # sabre ns --create cad11d --owner {owner-key} --url http://rest-api:9708

   This command specifies the pike namespace prefix (``cad11d``) and defines
   root as the namespace registry owner.
   For more information on command options, run ``sabre ns --help``.

#. Use ``sabre perm`` to grant the appropriate namespace permissions for your
   smart contract.

   .. code-block:: console

     # sabre perm cad11d intkey_multiply --read --url http://rest-api:9708

   This command gives ``intkey-multiply`` read permissions for the pike
   namespace (``cad11d``). For more information on command options,
   run ``sabre perm --help``.

Step 7. Execute the Smart Contract
----------------------------------

At this point, all required items are in place for the ``intkey-multiply``
smart contract:

* Initial values for the contract are set in intkey state.

* The payload file exists at ``/project/example/intkey_multiply/cli/payload``.

* The contract registry identifies the contract name and owner.

* The contract definition file specifies the contract name, version, and path
  to the compiled Wasm smart contract.

* The namespace registry declares that the contract will use the intkey
  namespace and sets the owner.

* The ``sabre perm`` command has granted the necessary namespace
  permissions (both read and write) to the ``intkey_multiply`` contract.

In this step, you will continue to use the ``sabre-cli`` container to execute
the ``intkey-multiply`` smart contract, then use the ``sabre-shell`` container
to check the results.

1. Run the following command to execute the ``intkey-multiply`` smart contract.

   .. code-block:: console

      # sabre exec --contract intkey_multiply:1.0 \
        --payload /project/example/intkey_multiply/cli/payload  \
        --inputs  1cf126 --inputs cad11d --outputs  1cf126 \
        --url http://rest-api:9708

   This command submits a transaction to execute the ``intkey_multiply`` smart
   contract, version 1.0, with the specified payload file. The contract's inputs
   and outputs are set to the intkey namespace (``1cf126``).

   .. note::

      The ``sabre exec`` command requires namespace prefixes or addresses that
      are at least 6 characters long.  For more information on command options,
      run ``sabre exec --help``.

#. To check the results, connect to the ``sabre-shell`` container in a separate
   terminal window.

   .. code-block:: console

      $ docker exec -it sabre-shell bash

#. Run the following command to display intkey state values.  You should see
   that A is set to 50.

   .. code-block:: console

      # intkey list --url http://rest-api:9708
      A 50
      B 10
      C 5

#. Log out of the ``sabre-shell`` docker container.


Step 8: Stop the Sawtooth Environment
-------------------------------------

When you are done using this Sawtooth Sabre environment, use this procedure to
stop and reset the environment.

.. important::

  Any work done in this environment will be lost once the container exits,
  unless it is stored under the ``/project`` directory. To keep your work in
  other areas, you would need to take additional steps, such as mounting a host
  directory into the container. See the `Docker
  documentation <https://docs.docker.com/>`_ for more information.

1. Log out of the ``sabre-cli`` container and any other open containers.

#. Enter CTRL-c from the window where you originally ran ``docker-compose up``.

#. After all containers have shut down, run this ``docker-compose`` command:

   .. note::

      This command deletes all values in state.

   .. code-block:: console

      $ docker-compose -f docker-compose.yaml -f example/intkey_multiply/docker-compose.yaml down


.. Licensed under Creative Commons Attribution 4.0 International License
.. https://creativecommons.org/licenses/by/4.0/

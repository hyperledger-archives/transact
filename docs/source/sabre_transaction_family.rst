************************
Sabre Transaction Family
************************

Overview
=========
Sawtooth Sabre is a transaction family which implements on-chain smart
contracts executed in a WebAssembly virtual machine.

WebAssembly (Wasm) is a stack-based virtual machine newly implemented in major
browsers. It is well-suited for the purposes of smart contract execution due to
its sandboxed design, growing popularity, and tool support.

State
=====

All Sabre objects are serialized using Protocol Buffers before being stored in
state. Theses objects include namespace registries, contract registries, and
contracts. All objects are stored in a list to handle hash collisions.

NamespaceRegistry
-----------------

A namespace is a state address prefix used to identify a portion of state.
The NamespaceRegistry stores the namespace, the owners of the namespace, and the
permissions given to that namespace. NamespaceRegistry is uniquely identified
by its namespace.

Permissions are used to control read and/or write access to the namespace. It
includes a contract name that correlates to the name of a Sabre contract and
whether that contract is allowed to read and/or write to that namespace. The
permission is uniquely identified by the contract_name. If the contract is
executed but does not have the needed permission to read or write to state,
the transaction is considered invalid.

.. code-block:: protobuf

  message NamespaceRegistry {
    message Permission {
      string contract_name = 1;
      bool read = 2;
      bool write = 3;
    }

    string namespace = 1;
    repeated string owners = 2;

    repeated Permission permissions = 3;
  }

When the same address is computed for different namespace registries, a
collision occurs; all colliding namespace registries are stored in at the
address in a NamespaceRegistryList

.. code-block:: protobuf

  message NamespaceRegistryList {
    repeated NamespaceRegistry registries = 1;
  }

ContractRegistry
----------------
ContractRegistry keeps track of versions of the Sabre contract and the list of
owners. A ContractRegistry is uniquely identified by the name of its contract.

Versions represent the contract version and include the sha512 hash of the
contract and the public key of the creator. The hash can be used by a client to
verify this is the correct version of the contract that should be executed.

.. code-block:: protobuf

  message ContractRegistry {
    message Version {
      string version = 1;

      // used to verify if a contract is the same as the one the client intended
      // to invoke
      string contract_sha512 = 2;

      // for client information purposes only - the key that created this
      // contract on the chain
      string creator = 3;
    }

    string name = 1;
    repeated Version versions = 2;
    repeated string owners = 3;
  }

ContractRegistry entries whose addresses collide are stored in a
ContractRegsitryList.

.. code-block:: protobuf

  message ContractRegistryList {
    repeated ContractRegistry registries = 1;
  }

Contract
--------

A Contract represents the Sabre smart contract. It is uniquely
identified by its name and version number. The contract also contains the
expected inputs and outputs used when executing the contract, the public
key of the creator, and the compiled wasm code of the contract.

.. code-block:: protobuf

    message Contract {
      string name = 1;
      string version = 2;
      repeated string inputs = 3;
      repeated string outputs = 4;
      string creator = 5;
      bytes contract = 6;
    }

Contracts whose addresses collide are stored in a ContractList.

.. code-block:: protobuf

    message ContractList {
      repeated Contract contracts = 1;
    }

Addressing
----------

Sabre objects are stored under 3 namespaces:

  - ``00ec00``: Namespace for NamespaceRegistry
  - ``00ec01``: Namespace for ContractRegistry
  - ``00ec02``: Namespace for Contracts

The remaining 64 characters of the object's address is the following:
  - NamespaceRegistry: the first 64 characters of the hash of the first 6
    characters of the namespaces.
  - ContractRegistry: the first 64 characters of the hash of the name.
  - Contract: the first 64 characters of the hash of "name,version"

For example, the address for a contract with name "example" and version "1.0"
address would be:

.. code-block:: pycon

  >>> '00ec02' + get_hash("example,1.0")
  '00ec0248a8e00e3fbca83815668ec5eee730023e6eb61b03b54e8cae1729bf5a0bec64'


Transaction Payload and Execution
=================================

Below, the different payload actions are defined along with the inputs and
outputs that are required in the transaction header.

SabrePayload
------------

A SabrePayload contains an action enum and the associated action payload. This
allows for the action payload to be dispatched to the appropriate logic.

Only the defined actions are available and only one action payload should be
defined in the SabrePayload.

.. code-block:: protobuf

  message SabrePayload {
    enum Action {
      ACTION_UNSET = 0;
      CREATE_CONTRACT = 1;
      DELETE_CONTRACT = 2;
      EXECUTE_CONTRACT = 3;
      CREATE_CONTRACT_REGISTRY = 4;
      DELETE_CONTRACT_REGISTRY = 5;
      UPDATE_CONTRACT_REGISTRY_OWNERS = 6;
      CREATE_NAMESPACE_REGISTRY = 7;
      DELETE_NAMESPACE_REGISTRY = 8;
      UPDATE_NAMESPACE_REGISTRY_OWNERS = 9;
      CREATE_NAMESPACE_REGISTRY_PERMISSION = 10;
      DELETE_NAMESPACE_REGISTRY_PERMISSION = 11;
    }

    Action action = 1;

    CreateContractAction create_contract = 2;
    DeleteContractAction delete_contract = 3;
    ExecuteContractAction execute_contract = 4;

    CreateContractRegistryAction create_contract_registry = 5;
    DeleteContractRegistryAction delete_contract_registry = 6;
    UpdateContractRegistryOwnersAction update_contract_registry_owners = 7;

    CreateNamespaceRegistryAction create_namespace_registry = 8;
    DeleteNamespaceRegistryAction delete_namespace_registry = 9;
    UpdateNamespaceRegistryOwnersAction update_namespace_registry_owners = 10;
    CreateNamespaceRegistryPermissionAction create_namespace_registry_permission = 11;
    DeleteNamespaceRegistryPermissionAction delete_namespace_registry_permission = 12;
  }

CreateContractAction
--------------------

Creates a contract and updates the associated contract registry.

.. code-block:: protobuf

  message CreateContractAction {
    string name = 1;
    string version = 2;
    repeated string inputs = 3;
    repeated string outputs = 4;
    bytes contract = 5;
  }

If a contract with the name and version already exists the transaction is
considered invalid.

The contract registry is fetched from state and the transaction signer is
checked against the owners. If the signer is not an owner, the transaction is
considered invalid.

If the contract registry for the contract name does not exist, the transaction
is invalid.

Both the new contract and the updated contract registry are set in state.

The inputs for CreateContractAction must include:

* the address for the new contract
* the address for the contract registry

The outputs for CreateContractAction must include:

* the address for the new contract
* the address for the contract registry


DeleteContractAction
--------------------

Delete a contract and remove its entry from the associated contract registry.

.. code-block:: protobuf

  message DeleteContractAction {
    string name = 1;
    string version = 2;
  }

If the contract does not already exist or does not have an entry in the contract
registry, the transactions is invalid.

If the transaction signer is not an owner, they cannot delete the contract and
the transaction is invalid.

The contract is deleted and the version entry is removed from the
contract entry.

The inputs for DeleteContractAction must include:

* the address for the contract
* the address for the contract registry

The outputs for DeleteContractAction must include:

* the address for the contract
* the address for the contract registry

ExecuteContractAction
---------------------

Execute the contract.

.. code-block:: protobuf

  message ExecuteContractAction {
    string name = 1;
    string version = 2;
    repeated string inputs = 3;
    repeated string outputs = 4;
    bytes payload = 5;
  }

The contract is fetched from state. If the contract does not exist, the
transaction is invalid.

The inputs and outputs are then checked against the namespace registry
associated with the first 6 characters of each input or output. If the input
or output is less than 6 characters the transaction is invalid. For every
input, the namespace registry must have a read permission for the contract and
for every output the namespace registry must have a write permission for the
contract. If either are missing or the namespace registry does not exist,
the transaction is invalid.

The contract is then loaded into the wasm interpreter and run against the
provided payload. A result is returned. If the result is 1 the transaction
is okay and the contract data is stored in state. If the result is -3 the
transaction is invalid. If any other number result is returned there was an
internal error.

The inputs for ExecuteContractAction must include:

* the address for the contract
* the address for the contract registry
* any inputs that are required for executing the contract
* the addresses for every namespace registry required to check the provided
  contract inputs

The outputs for ExecuteContractAction must include:

* the address for the contract
* the address for the contract registry
* any outputs that are required for executing the contract
* the addresses for every namespace registry required to check the provided
  contract outputs

CreateContractRegistryAction
----------------------------

Create a contract registry with no version.

.. code-block:: protobuf

  message CreateContractRegistryAction {
    string name = 1;
    repeated string owners = 2;
  }

If the contract registry for the provided contract name already exists, the
the transaction is invalid.

Only those whose public keys are stored in ``sawtooth.swa.administrators`` are
allowed to create new contract registries. If the transaction signer is an
administrator, the new contract registry is set in state. Otherwise, the
transaction is invalid.

The new contract registry is created for the name and provided owners. The
owners should be a list of public keys of users that are allowed to add new
contract versions, delete old versions, and delete the registry.

The new contract registry is set in state.

The inputs for CreateContractRegistryAction must include:

* the address for the contract registry
* the settings address for ``sawtooth.swa.administrators``

The outputs for CreateContractRegistryAction must include:

* the address for the contract registry

DeleteContractRegistryAction
----------------------------

Deletes a contract registry if there are no versions.

.. code-block:: protobuf

  message DeleteContractRegistryAction {
    string name = 1;
  }

If the contract registry does not exist, the transaction is invalid. If the
transaction signer is not an owner or does not have their public key in
``sawtooth.swa.administrators`` or the contract registry has any number of
versions, the transaction is invalid.

The contract registry is deleted.

The inputs for DeleteContractRegistryAction must include:

* the address for the contract registry

The outputs for DeleteContractRegistryAction must include:

* the address for the contract registry
* the settings address for ``sawtooth.swa.administrators``

UpdateContractRegistryOwnersAction
----------------------------------

Update the contract registry's owners list.

.. code-block:: protobuf

  message UpdateContractRegistryOwnersAction {
    string name = 1;
    repeated string owners = 2;
  }

If the contract registry does not exist or the transaction signer is not an
owner or does not have their public key in ``sawtooth.swa.administrators``, the
transaction is invalid.

The new owner list will replace the current owner list and the updated contract
registry is set in state.

The inputs for UpdateContractRegistryOwnersAction must include:

* the address for the contract registry
* the settings address for ``sawtooth.swa.administrators``

The outputs for UpdateContractRegistryOwnersAction must include:

* the address for the contract registry

CreateNamespaceRegistryAction
-----------------------------

Creates a namespace registry with no permissions.

.. code-block:: protobuf

  message CreateNamespaceRegistryAction {
    string namespace = 1;
    repeated string owners = 2;
  }

The namespace must be at least 6 characters long. If the namespace registry
already exists, the transaction is invalid.

Only those whose public keys are stored in ``sawtooth.swa.administrators`` are
allowed to create new namespace registries. If the transaction signer is an
administrator, the new namespace registry is set in state. Otherwise, the
transaction is invalid.

The inputs for CreateNamespaceRegistryAction must include:

* the address for the namespace registry
* the settings address for ``sawtooth.swa.administrators``

The outputs for CreateNamespaceRegistryAction must include:

* the address for the namespace registry
* the settings address for ``sawtooth.swa.administrators``

DeleteNamespaceRegistryAction
-----------------------------

Deletes a namespace registry if it does not contains any permissions.

.. code-block:: protobuf

  message DeleteNamespaceRegistryAction {
    string namespace = 1;
  }

If the namespace registry does not exist or contain permissions, the
transaction is invalid.

If the transaction signer is either an owner in the namespace registry or has
their public key in ``sawtooth.swa.administrators``, the namespace registry
is deleted. Otherwise, the transaction is invalid.

The inputs for DeleteNamespaceRegistryAction must include:

* the address for the namespace registry
* the settings address for ``sawtooth.swa.administrators``

The outputs for DeleteNamespaceRegistryAction must include:

* the address for the namespace registry
* the settings address for ``sawtooth.swa.administrators``

UpdateNamespaceRegistryOwnersAction
-----------------------------------

Update the namespace registry's owners list.

.. code-block:: protobuf

  message UpdateNamespaceRegistryOwnersAction {
    string namespace = 1;
    repeated string owners = 2;
  }

If the namespace registry does not exist, the transaction is invalid.

If the transaction signer is either an owner in the namespace registry or has
their public key in ``sawtooth.swa.administrators``, the namespace registry's
owners are updated. Otherwise, the transaction is invalid.

The updated namespace registry is set in state.

The inputs for UpdateNamespaceRegistryOwnersAction must include:

* the address for the namespace registry
* the settings address for ``sawtooth.swa.administrators``

The outputs for UpdateNamespaceRegistryOwnersAction must include:

* the address for the namespace registry
* the settings address for ``sawtooth.swa.administrators``

CreateNamespaceRegistryPermissionAction
---------------------------------------

Adds a permission entry into a namespace registry for the associated namespace.

.. code-block:: protobuf

  message CreateNamespaceRegistryPermissionAction {
    string namespace = 1;
    string contract_name = 2;
    bool read = 3;
    bool write = 4;
  }

If the namespace registry does not exist, the transaction is invalid.

If the transaction signer is either an owner in the namespace registry or has
their public key in ``sawtooth.swa.administrators``, a new permission is
added for the provided contract_name. Otherwise, the transaction is invalid.

If there is already a permission for the contract_name in the namespace
registry, the old permission is removed and replaced with the new
permission.

The updated namespace registry is set in state.

The inputs for CreateNamespaceRegistryPermissionAction must include:

* the address for the namespace registry
* the settings address for ``sawtooth.swa.administrators``

The outputs for CreateNamespaceRegistryPermissionAction must include:

* the address for the namespace registry
* the settings address for ``sawtooth.swa.administrators``

DeleteNamespaceRegistryPermissionAction
---------------------------------------

Delete a permission entry in a namespace registry for the associated
namespace.

.. code-block:: protobuf

  message DeleteNamespaceRegistryPermissionAction {
    string namespace = 1;
    string contract_name = 2;
  }

If the namespace registry does not exist, the transaction is invalid. If the
transaction signer is either an owner in the namespace registry or has their
public key in ``sawtooth.swa.administrators``, the permission for the provided
contract name is removed. Otherwise, the transaction is invalid.

The inputs for DeleteNamespaceRegistryPermissionAction must include:

* the address for the namespace registry
* the settings address for ``sawtooth.swa.administrators``

The outputs for DeleteNamespaceRegistryPermissionAction must include:

* the address for the namespace registry
* the settings address for ``sawtooth.swa.administrators``

Transaction Header
==================

Inputs and Outputs
------------------

The required inputs and outputs are defined for each action payload above.

Dependencies
------------

No dependencies.

Family
------
- family_name: "sabre"
- family_version: "0.1"

.. Licensed under Creative Commons Attribution 4.0 International License
.. https://creativecommons.org/licenses/by/4.0/

*******************
Sabre CLI Reference
*******************

The Sabre CLI provides a way to upload and execute Sabre smart contracts from
the command line. It also provides the ability to manage namespace registries
and their permissions.

sabre
=====
``sabre`` is the top level command for Sabre and it contains 4 subcommands.
Theses commands are ``upload``, ``ns``, ``perm``, and ``exec``. The subcommands
have options and arguments that control their behavior. All subcommands include
``-key``, the name of the signing key, and ``--url``, the url to the Sawtooth
REST API.

.. literalinclude:: cli/output/sabre_usage.out
   :language: console

sabre upload
============

The ``sabre upload`` subcommand submits a Sabre transaction that adds a new
contract.

.. literalinclude:: cli/output/sabre_upload_usage.out
  :language: console

The command requires that a path to a contract definition is provided to
``--filename``. The contract definition should be a yaml file with the
following information:

.. code-block:: yaml

  name: <contract name>
  version: <contract version>
  wasm: <path to compiled wasm file>
  inputs:
    - <input addresses>
  outputs:
    - <output addresses>

sabre ns
========
The ``sabre ns`` subcommand submits a Sabre transaction that can create, update
or delete a namespace registry.

.. literalinclude:: cli/output/sabre_ns_usage.out
  :language: console

A namespace registry can only be created by an administrator. An administrator
has their public key stored in the setting ``sawtooth.swa.administrators``. At
least one ``--owner`` is required. An owner is the public key of those whose
who are allowed to update and delete namespaces.

Only an owner or an administrator is allowed to update owners of a namespace
registry or delete a namespace registry.

A namespace must be at least 6 characters long.

sabre perm
==========
The ``sabre perm`` subcommand submits a Sabre transaction that can create or
delete a namespace registry permissions.

.. literalinclude:: cli/output/sabre_perm_usage.out
  :language: console

A namespace registry permissions can only be created by an administrator or an
owner of the namespace registry. Include ``--read`` if the contract is
allowed to read from the namespace and ``--write`` if the contract is
allowed to write to the namespace.

Using ``--delete`` will remove all permissions for the provided contract name.
Again a permission can only be deleted by an owner or an administrator.

sabre exec
==========

The ``sabre exec`` subcommand submits a Sabre transaction that execute the
provided payload against an uploaded contract.

.. literalinclude:: cli/output/sabre_exec_usage.out
  :language: console

The ``--contract`` should be <contract_name:version_number>. The ``--inputs``
and ``--outputs`` should include any namespaces or addresses that the contract
needs to have access to. Finally the ``--payload`` should be a path to
the file that contains the Sabre contract bytes.

.. Licensed under Creative Commons Attribution 4.0 International License
.. https://creativecommons.org/licenses/by/4.0/

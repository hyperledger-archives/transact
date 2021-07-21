/*
 * Copyright 2021 Cargill Incorporated
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -----------------------------------------------------------------------------
 */

//! SQL-backed merkle-radix state implementation.
//!
//! A merkle-radix tree can be applied to a SQL database using the [`SqlMerkleState`] struct.  This
//! struct uses several specialized tables to represent the tree, with optimizations made depending
//! on the specific SQL database platform.
//!
//! A instance can be constructed using a [`Backend`] instance and a tree name.  The tree name
//! allows for multiple trees to be stored under the same set of tables.
//!
//! For example, an instance using SQLite:
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # use transact::state::merkle::sql::SqlMerkleStateBuilder;
//! # use transact::state::merkle::sql::migration::MigrationManager;
//! # use transact::state::merkle::sql::backend::{Backend, SqliteBackendBuilder};
//! let backend = SqliteBackendBuilder::new().with_memory_database().build()?;
//! # backend.run_migrations()?;
//!
//! let merkle_state = SqlMerkleStateBuilder::new()
//!     .with_backend(backend)
//!     .with_tree("example")
//!     .create_tree_if_necessary()
//!     .build()?;
//! # Ok(())
//! # }
//! ```
//!
//! The resulting `merkle_state` can then be used via the [`Read`], [`Write`] and
//! [`MerkleRadixLeafReader`] traits.

pub mod backend;
mod error;
pub mod migration;
mod models;
mod operations;
mod schema;

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use crate::error::{InternalError, InvalidStateError};
use crate::state::error::{StateReadError, StateWriteError};
#[cfg(feature = "state-merkle-leaf-reader")]
use crate::state::merkle::{MerkleRadixLeafReadError, MerkleRadixLeafReader};
use crate::state::{Read, StateChange, Write};

use super::node::Node;

use backend::{Backend, Connection};
pub use error::SqlMerkleStateBuildError;
use operations::get_leaves::MerkleRadixGetLeavesOperation as _;
use operations::get_or_create_tree::MerkleRadixGetOrCreateTreeOperation as _;
use operations::get_path::MerkleRadixGetPathOperation as _;
use operations::get_tree_by_name::MerkleRadixGetTreeByNameOperation as _;
use operations::has_root::MerkleRadixHasRootOperation as _;
use operations::insert_nodes::MerkleRadixInsertNodesOperation as _;
use operations::list_leaves::MerkleRadixListLeavesOperation as _;
use operations::update_index::MerkleRadixUpdateIndexOperation as _;
use operations::MerkleRadixOperations;

const TOKEN_SIZE: usize = 2;

/// Builds a new SqlMerkleState with a specific backend.
#[derive(Default)]
pub struct SqlMerkleStateBuilder<B: Backend + Clone> {
    backend: Option<B>,
    tree_name: Option<String>,
    create_tree: bool,
}

impl<B: Backend + Clone> SqlMerkleStateBuilder<B> {
    /// Constructs a new builder
    pub fn new() -> Self {
        Self {
            backend: None,
            tree_name: None,
            create_tree: false,
        }
    }

    /// Sets the backend instance to use with the resulting instance
    pub fn with_backend(mut self, backend: B) -> Self {
        self.backend = Some(backend);
        self
    }

    /// Sets the tree name that the resulting instance will operate on
    pub fn with_tree<S: Into<String>>(mut self, tree_name: S) -> Self {
        self.tree_name = Some(tree_name.into());
        self
    }

    /// Create the specified tree if it does not exist
    pub fn create_tree_if_necessary(mut self) -> Self {
        self.create_tree = true;
        self
    }
}

#[cfg(feature = "sqlite")]
impl SqlMerkleStateBuilder<backend::SqliteBackend> {
    /// Construct the final SqlMerkleState instance
    ///
    /// # Errors
    ///
    /// An error may be returned under the following circumstances:
    /// * If a Backend has not been provided
    /// * If a tree name has not been provided
    /// * If an internal error occurs while trying to create or lookup the tree
    pub fn build(self) -> Result<SqlMerkleState<backend::SqliteBackend>, SqlMerkleStateBuildError> {
        let backend = self
            .backend
            .ok_or_else(|| InvalidStateError::with_message("must provide a backend".into()))?;

        let tree_name = self
            .tree_name
            .ok_or_else(|| InvalidStateError::with_message("must provide a tree name".into()))?;

        let conn = backend.connection()?;
        let operations = MerkleRadixOperations::new(conn.as_inner());

        let tree_id: i64 = if self.create_tree {
            operations.get_or_create_tree(&tree_name)?
        } else {
            operations.get_tree_id_by_name(&tree_name)?.ok_or_else(|| {
                InvalidStateError::with_message("must provide the name of an existing tree".into())
            })?
        };

        Ok(SqlMerkleState { backend, tree_id })
    }
}

#[cfg(feature = "postgres")]
impl SqlMerkleStateBuilder<backend::PostgresBackend> {
    /// Construct the final SqlMerkleState instance
    ///
    /// # Errors
    ///
    /// An error may be returned under the following circumstances:
    /// * If a Backend has not been provided
    /// * If a tree name has not been provided
    /// * If an internal error occurs while trying to create or lookup the tree
    pub fn build(
        self,
    ) -> Result<SqlMerkleState<backend::PostgresBackend>, SqlMerkleStateBuildError> {
        let backend = self
            .backend
            .ok_or_else(|| InvalidStateError::with_message("must provide a backend".into()))?;

        let tree_name = self
            .tree_name
            .ok_or_else(|| InvalidStateError::with_message("must provide a tree name".into()))?;

        let conn = backend.connection()?;
        let operations = MerkleRadixOperations::new(conn.as_inner());

        let tree_id: i64 = if self.create_tree {
            operations.get_or_create_tree(&tree_name)?
        } else {
            operations.get_tree_id_by_name(&tree_name)?.ok_or_else(|| {
                InvalidStateError::with_message("must provide the name of an existing tree".into())
            })?
        };

        Ok(SqlMerkleState { backend, tree_id })
    }
}

/// SqlMerkleState provides a merkle-radix implementation over a SQL database.
///
/// Databases are implemented using a Backend to provide connections. Note that the database must
/// have the correct set of tables applied via migrations before this struct may be usable.
#[derive(Clone)]
pub struct SqlMerkleState<B: Backend + Clone> {
    backend: B,
    tree_id: i64,
}

impl<B: Backend + Clone> SqlMerkleState<B> {
    /// Returns the initial state root.
    ///
    /// This value is the state root that applies to a empty merkle radix tree.
    pub fn initial_state_root_hash(&self) -> Result<String, InternalError> {
        let (hash, _) = encode_and_hash(Node::default())?;

        Ok(hex::encode(&hash))
    }
}

#[cfg(feature = "sqlite")]
impl Write for SqlMerkleState<backend::SqliteBackend> {
    type StateId = String;
    type Key = String;
    type Value = Vec<u8>;

    fn commit(
        &self,
        state_id: &Self::StateId,
        state_changes: &[StateChange],
    ) -> Result<Self::StateId, StateWriteError> {
        let overlay =
            MerkleRadixOverlay::new(self.tree_id, &*state_id, SqlOverlay::new(&self.backend));

        let (next_state_id, changes) = overlay
            .generate_updates(state_changes)
            .map_err(|e| StateWriteError::StorageError(Box::new(e)))?;

        let deleted_addresses = state_changes
            .iter()
            .filter(|change| matches!(change, StateChange::Delete { .. }))
            .map(|change| change.key())
            .collect::<Vec<_>>();

        overlay
            .write_updates(&next_state_id, changes, &deleted_addresses)
            .map_err(|e| StateWriteError::StorageError(Box::new(e)))?;

        Ok(next_state_id)
    }

    fn compute_state_id(
        &self,
        state_id: &Self::StateId,
        state_changes: &[StateChange],
    ) -> Result<Self::StateId, StateWriteError> {
        let overlay =
            MerkleRadixOverlay::new(self.tree_id, &*state_id, SqlOverlay::new(&self.backend));

        let (next_state_id, _) = overlay
            .generate_updates(state_changes)
            .map_err(|e| StateWriteError::StorageError(Box::new(e)))?;

        Ok(next_state_id)
    }
}

#[cfg(feature = "sqlite")]
impl Read for SqlMerkleState<backend::SqliteBackend> {
    type StateId = String;
    type Key = String;
    type Value = Vec<u8>;

    fn get(
        &self,
        state_id: &Self::StateId,
        keys: &[Self::Key],
    ) -> Result<HashMap<Self::Key, Self::Value>, StateReadError> {
        let overlay =
            MerkleRadixOverlay::new(self.tree_id, &*state_id, SqlOverlay::new(&self.backend));

        if !overlay
            .has_root()
            .map_err(|e| StateReadError::StorageError(Box::new(e)))?
        {
            return Err(StateReadError::InvalidStateId(state_id.into()));
        }

        overlay
            .get_entries(keys)
            .map_err(|e| StateReadError::StorageError(Box::new(e)))
    }

    fn clone_box(
        &self,
    ) -> Box<dyn Read<StateId = Self::StateId, Key = Self::Key, Value = Self::Value>> {
        Box::new(self.clone())
    }
}

#[cfg(feature = "postgres")]
impl Write for SqlMerkleState<backend::PostgresBackend> {
    type StateId = String;
    type Key = String;
    type Value = Vec<u8>;

    fn commit(
        &self,
        state_id: &Self::StateId,
        state_changes: &[StateChange],
    ) -> Result<Self::StateId, StateWriteError> {
        let overlay =
            MerkleRadixOverlay::new(self.tree_id, &*state_id, SqlOverlay::new(&self.backend));

        let (next_state_id, changes) = overlay
            .generate_updates(state_changes)
            .map_err(|e| StateWriteError::StorageError(Box::new(e)))?;

        let deleted_addresses = state_changes
            .iter()
            .filter(|change| matches!(change, StateChange::Delete { .. }))
            .map(|change| change.key())
            .collect::<Vec<_>>();

        overlay
            .write_updates(&next_state_id, changes, &deleted_addresses)
            .map_err(|e| StateWriteError::StorageError(Box::new(e)))?;

        Ok(next_state_id)
    }

    fn compute_state_id(
        &self,
        state_id: &Self::StateId,
        state_changes: &[StateChange],
    ) -> Result<Self::StateId, StateWriteError> {
        let overlay =
            MerkleRadixOverlay::new(self.tree_id, &*state_id, SqlOverlay::new(&self.backend));

        let (next_state_id, _) = overlay
            .generate_updates(state_changes)
            .map_err(|e| StateWriteError::StorageError(Box::new(e)))?;

        Ok(next_state_id)
    }
}

#[cfg(feature = "postgres")]
impl Read for SqlMerkleState<backend::PostgresBackend> {
    type StateId = String;
    type Key = String;
    type Value = Vec<u8>;

    fn get(
        &self,
        state_id: &Self::StateId,
        keys: &[Self::Key],
    ) -> Result<HashMap<Self::Key, Self::Value>, StateReadError> {
        let overlay =
            MerkleRadixOverlay::new(self.tree_id, &*state_id, SqlOverlay::new(&self.backend));

        if !overlay
            .has_root()
            .map_err(|e| StateReadError::StorageError(Box::new(e)))?
        {
            return Err(StateReadError::InvalidStateId(state_id.into()));
        }

        overlay
            .get_entries(keys)
            .map_err(|e| StateReadError::StorageError(Box::new(e)))
    }

    fn clone_box(
        &self,
    ) -> Box<dyn Read<StateId = Self::StateId, Key = Self::Key, Value = Self::Value>> {
        Box::new(self.clone())
    }
}

#[cfg(feature = "state-merkle-leaf-reader")]
type IterResult<T> = Result<T, MerkleRadixLeafReadError>;
#[cfg(feature = "state-merkle-leaf-reader")]
type LeafIter<T> = Box<dyn Iterator<Item = IterResult<T>>>;

#[cfg(all(feature = "state-merkle-leaf-reader", feature = "sqlite"))]
impl MerkleRadixLeafReader for SqlMerkleState<backend::SqliteBackend> {
    /// Returns an iterator over the leaves of a merkle radix tree.
    /// By providing an optional address prefix, the caller can limit the iteration
    /// over the leaves in a specific subtree.
    fn leaves(
        &self,
        state_id: &Self::StateId,
        subtree: Option<&str>,
    ) -> IterResult<LeafIter<(Self::Key, Self::Value)>> {
        let conn = self.backend.connection()?;

        if &self.initial_state_root_hash()? == state_id {
            return Ok(Box::new(std::iter::empty()));
        }

        let leaves = MerkleRadixOperations::new(conn.as_inner()).list_leaves(
            self.tree_id,
            state_id,
            subtree,
        )?;

        Ok(Box::new(leaves.into_iter().map(Ok)))
    }
}

#[cfg(all(feature = "state-merkle-leaf-reader", feature = "postgres"))]
impl MerkleRadixLeafReader for SqlMerkleState<backend::PostgresBackend> {
    /// Returns an iterator over the leaves of a merkle radix tree.
    /// By providing an optional address prefix, the caller can limit the iteration
    /// over the leaves in a specific subtree.
    fn leaves(
        &self,
        state_id: &Self::StateId,
        subtree: Option<&str>,
    ) -> IterResult<LeafIter<(Self::Key, Self::Value)>> {
        let conn = self.backend.connection()?;

        if &self.initial_state_root_hash()? == state_id {
            return Ok(Box::new(std::iter::empty()));
        }

        let leaves = MerkleRadixOperations::new(conn.as_inner()).list_leaves(
            self.tree_id,
            state_id,
            subtree,
        )?;

        Ok(Box::new(leaves.into_iter().map(Ok)))
    }
}

struct MerkleRadixOverlay<'s, O> {
    tree_id: i64,
    state_root_hash: &'s str,
    inner: O,
}

// (Hash, packed bytes, path address)
type NodeChanges = Vec<(String, Node, String)>;

impl<'s, O> MerkleRadixOverlay<'s, O>
where
    O: OverlayReader + OverlayWriter,
{
    fn new(tree_id: i64, state_root_hash: &'s str, inner: O) -> Self {
        Self {
            tree_id,
            state_root_hash,
            inner,
        }
    }

    fn write_updates(
        &self,
        new_state_root: &str,
        node_changes: NodeChanges,
        deleted_addresses: &[&str],
    ) -> Result<(), InternalError> {
        self.inner.write_changes(
            self.tree_id,
            new_state_root,
            self.state_root_hash,
            node_changes,
            deleted_addresses,
        )?;

        Ok(())
    }

    fn has_root(&self) -> Result<bool, InternalError> {
        self.inner.has_root(self.tree_id, self.state_root_hash)
    }

    fn get_entries(&self, keys: &[String]) -> Result<HashMap<String, Vec<u8>>, InternalError> {
        let keys = keys.iter().map(|k| &**k).collect::<Vec<_>>();
        self.inner
            .get_entries(self.tree_id, self.state_root_hash, keys)
            .map(|result| result.into_iter().collect::<HashMap<_, _>>())
    }

    fn generate_updates(
        &self,
        state_changes: &[StateChange],
    ) -> Result<(String, NodeChanges), StateWriteError> {
        if state_changes.is_empty() {
            return Ok((self.state_root_hash.to_string(), vec![]));
        }

        let mut path_map = HashMap::new();

        let mut deletions = HashSet::new();
        let mut additions = HashSet::new();

        let mut delete_items = vec![];
        for state_change in state_changes {
            match state_change {
                StateChange::Set { key, value } => {
                    let mut set_path_map = self
                        .get_path(&key)
                        .map_err(|e| StateWriteError::StorageError(Box::new(e)))?;
                    {
                        let node = set_path_map
                            .get_mut(key)
                            .expect("Path map not correctly generated");
                        node.value = Some(value.to_vec());
                    }
                    for pkey in set_path_map.keys() {
                        additions.insert(pkey.clone());
                    }
                    path_map.extend(set_path_map);
                }
                StateChange::Delete { key } => {
                    let del_path_map = self
                        .get_path(&key)
                        .map_err(|e| StateWriteError::StorageError(Box::new(e)))?;
                    path_map.extend(del_path_map);
                    delete_items.push(key);
                }
            }
        }

        for del_address in delete_items.iter() {
            path_map.remove(*del_address);
            let (mut parent_address, mut path_branch) = parent_and_branch(del_address);
            while !parent_address.is_empty() {
                let remove_parent = {
                    let parent_node = path_map
                        .get_mut(parent_address)
                        .expect("Path map not correctly generated or entry is deleted");

                    if let Some(old_hash_key) = parent_node.children.remove(path_branch) {
                        deletions.insert(old_hash_key);
                    }

                    parent_node.children.is_empty()
                };

                // There's no children to the parent node already in the tree, remove it from
                // path_map if a new node doesn't have this as its parent
                if remove_parent && !additions.contains(parent_address) {
                    // Empty node delete it.
                    path_map.remove(parent_address);
                } else {
                    // Found a node that is not empty no need to continue
                    break;
                }

                let (next_parent, next_branch) = parent_and_branch(parent_address);
                parent_address = next_parent;
                path_branch = next_branch;

                if parent_address.is_empty() {
                    let parent_node = path_map
                        .get_mut(parent_address)
                        .expect("Path map not correctly generated");

                    if let Some(old_hash_key) = parent_node.children.remove(path_branch) {
                        deletions.insert(old_hash_key);
                    }
                }
            }
        }

        let mut sorted_paths: Vec<_> = path_map.keys().cloned().collect();
        // Sort by longest to shortest
        sorted_paths.sort_by_key(|a| Reverse(a.len()));

        // Initializing this to empty, to make the compiler happy
        let mut key_hash_hex = String::new();
        let mut batch = Vec::with_capacity(sorted_paths.len());
        for path in sorted_paths {
            let node = path_map
                .remove(&path)
                .expect("Path map keys are out of sink");
            let (hash_key, _) = encode_and_hash(node.clone())
                .map_err(|e| StateWriteError::StorageError(Box::new(e)))?;
            key_hash_hex = hex::encode(&hash_key);

            if !path.is_empty() {
                let (parent_address, path_branch) = parent_and_branch(&path);
                let parent = path_map
                    .get_mut(parent_address)
                    .expect("Path map not correctly generated");
                if let Some(old_hash_key) = parent
                    .children
                    .insert(path_branch.to_string(), key_hash_hex.clone())
                {
                    deletions.insert(old_hash_key);
                }
            }

            batch.push((key_hash_hex.clone(), node, path));
        }

        Ok((key_hash_hex, batch))
    }

    fn get_path(&self, address: &str) -> Result<HashMap<String, Node>, InternalError> {
        // Build up the address along the path, starting with the empty address for the root, and
        // finishing with the complete address.
        let addresses_along_path: Vec<String> = (0..address.len())
            .step_by(2)
            .map(|i| address[0..i].to_string())
            .chain(std::iter::once(address.to_string()))
            .collect();

        let node_path_iter = self
            .inner
            .get_path(self.tree_id, &self.state_root_hash, address)?
            .into_iter()
            .map(|(_, node)| node)
            // include empty nodes after the queried path, to cover cases where the branch doesn't
            // exist.
            .chain(std::iter::repeat(Node::default()));

        Ok(addresses_along_path
            .into_iter()
            .zip(node_path_iter)
            .collect::<HashMap<_, _>>())
    }
}

trait OverlayReader {
    fn has_root(&self, tree_id: i64, state_root_hash: &str) -> Result<bool, InternalError>;

    fn get_path(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        address: &str,
    ) -> Result<Vec<(String, Node)>, InternalError>;

    fn get_entries(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        keys: Vec<&str>,
    ) -> Result<Vec<(String, Vec<u8>)>, InternalError>;
}

trait OverlayWriter {
    fn write_changes(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        parent_state_root_hash: &str,
        changes: Vec<(String, Node, String)>,
        deleted_addresses: &[&str],
    ) -> Result<(), InternalError>;
}

struct SqlOverlay<'b, B: Backend> {
    backend: &'b B,
}

impl<'b, B: Backend> SqlOverlay<'b, B> {
    fn new(backend: &'b B) -> Self {
        Self { backend }
    }
}

#[cfg(feature = "sqlite")]
impl<'b> OverlayReader for SqlOverlay<'b, backend::SqliteBackend> {
    fn has_root(&self, tree_id: i64, state_root_hash: &str) -> Result<bool, InternalError> {
        let conn = self.backend.connection()?;

        let operations = MerkleRadixOperations::new(conn.as_inner());
        operations.has_root(tree_id, state_root_hash)
    }

    fn get_path(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        address: &str,
    ) -> Result<Vec<(String, Node)>, InternalError> {
        let conn = self.backend.connection()?;

        let operations = MerkleRadixOperations::new(conn.as_inner());
        operations.get_path(tree_id, state_root_hash, address)
    }

    fn get_entries(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        keys: Vec<&str>,
    ) -> Result<Vec<(String, Vec<u8>)>, InternalError> {
        let conn = self.backend.connection()?;

        let operations = MerkleRadixOperations::new(conn.as_inner());
        operations.get_leaves(tree_id, state_root_hash, keys)
    }
}

#[cfg(feature = "sqlite")]
impl<'b> OverlayWriter for SqlOverlay<'b, backend::SqliteBackend> {
    fn write_changes(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        parent_state_root_hash: &str,
        changes: Vec<(String, Node, String)>,
        deleted_addresses: &[&str],
    ) -> Result<(), InternalError> {
        let conn = self.backend.connection()?;
        let operations = MerkleRadixOperations::new(conn.as_inner());

        let insertable_changes = changes
            .into_iter()
            .map(
                |(hash, node, address)| operations::insert_nodes::InsertableNode {
                    hash,
                    node,
                    address,
                },
            )
            .collect::<Vec<_>>();

        let indexable_info = operations.insert_nodes(tree_id, &insertable_changes)?;

        let changes = indexable_info
            .iter()
            .map(
                |leaf_info| operations::update_index::ChangedLeaf::AddedOrUpdated {
                    address: &leaf_info.address,
                    leaf_id: leaf_info.leaf_id,
                },
            )
            .chain(
                deleted_addresses
                    .iter()
                    .map(|address| operations::update_index::ChangedLeaf::Deleted(address)),
            )
            .collect();

        operations.update_index(tree_id, state_root_hash, parent_state_root_hash, changes)?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
impl<'b> OverlayReader for SqlOverlay<'b, backend::PostgresBackend> {
    fn has_root(&self, tree_id: i64, state_root_hash: &str) -> Result<bool, InternalError> {
        let conn = self.backend.connection()?;

        let operations = MerkleRadixOperations::new(conn.as_inner());
        operations.has_root(tree_id, state_root_hash)
    }

    fn get_path(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        address: &str,
    ) -> Result<Vec<(String, Node)>, InternalError> {
        let conn = self.backend.connection()?;

        let operations = MerkleRadixOperations::new(conn.as_inner());
        operations.get_path(tree_id, state_root_hash, address)
    }

    fn get_entries(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        keys: Vec<&str>,
    ) -> Result<Vec<(String, Vec<u8>)>, InternalError> {
        let conn = self.backend.connection()?;

        let operations = MerkleRadixOperations::new(conn.as_inner());
        operations.get_leaves(tree_id, state_root_hash, keys)
    }
}

#[cfg(feature = "postgres")]
impl<'b> OverlayWriter for SqlOverlay<'b, backend::PostgresBackend> {
    fn write_changes(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        parent_state_root_hash: &str,
        changes: Vec<(String, Node, String)>,
        deleted_addresses: &[&str],
    ) -> Result<(), InternalError> {
        let conn = self.backend.connection()?;
        let operations = MerkleRadixOperations::new(conn.as_inner());

        let insertable_changes = changes
            .into_iter()
            .map(
                |(hash, node, address)| operations::insert_nodes::InsertableNode {
                    hash,
                    node,
                    address,
                },
            )
            .collect::<Vec<_>>();

        let indexable_info = operations.insert_nodes(tree_id, &insertable_changes)?;

        let changes = indexable_info
            .iter()
            .map(
                |leaf_info| operations::update_index::ChangedLeaf::AddedOrUpdated {
                    address: &leaf_info.address,
                    leaf_id: leaf_info.leaf_id,
                },
            )
            .chain(
                deleted_addresses
                    .iter()
                    .map(|address| operations::update_index::ChangedLeaf::Deleted(address)),
            )
            .collect();

        operations.update_index(tree_id, state_root_hash, parent_state_root_hash, changes)?;

        Ok(())
    }
}

/// Given a path, split it into its parent's path and the specific branch for
/// this path, such that the following assertion is true:
fn parent_and_branch(path: &str) -> (&str, &str) {
    let parent_address = if !path.is_empty() {
        &path[..path.len() - TOKEN_SIZE]
    } else {
        ""
    };

    let path_branch = if !path.is_empty() {
        &path[(path.len() - TOKEN_SIZE)..]
    } else {
        ""
    };

    (parent_address, path_branch)
}
/// Encodes the given node, and returns the hash of the bytes.
fn encode_and_hash(node: Node) -> Result<(Vec<u8>, Vec<u8>), InternalError> {
    let packed = node.into_bytes()?;
    let hash = hash(&packed);
    Ok((hash, packed))
}

/// Creates a hash of the given bytes
fn hash(input: &[u8]) -> Vec<u8> {
    let mut bytes: Vec<u8> = Vec::new();
    bytes.extend(openssl::sha::sha512(input).iter());
    let (hash, _rest) = bytes.split_at(bytes.len() / 2);
    hash.to_vec()
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::state::merkle::sql::backend::SqliteBackendBuilder;
    use crate::state::merkle::sql::migration::MigrationManager;

    /// This test creates multiple trees in the same backend/db instance and verifies that values
    /// added to one are not added to the other.
    #[test]
    fn test_multiple_trees() -> Result<(), Box<dyn std::error::Error>> {
        let backend = SqliteBackendBuilder::new().with_memory_database().build()?;

        backend.run_migrations()?;

        let tree_1 = SqlMerkleStateBuilder::new()
            .with_backend(backend.clone())
            .with_tree("test-1")
            .create_tree_if_necessary()
            .build()?;

        let initial_state_root_hash = tree_1.initial_state_root_hash()?;

        let state_change_set = StateChange::Set {
            key: "1234".to_string(),
            value: "state_value".as_bytes().to_vec(),
        };

        let new_root = tree_1
            .commit(&initial_state_root_hash, &[state_change_set])
            .unwrap();
        assert_read_value_at_address(&tree_1, &new_root, "1234", Some("state_value"));

        let tree_2 = SqlMerkleStateBuilder::new()
            .with_backend(backend)
            .with_tree("test-2")
            .create_tree_if_necessary()
            .build()?;

        assert!(tree_2.get(&new_root, &["1234".to_string()]).is_err());

        Ok(())
    }

    #[test]
    fn test_build_fails_without_explicit_create() -> Result<(), Box<dyn std::error::Error>> {
        let backend = SqliteBackendBuilder::new().with_memory_database().build()?;

        backend.run_migrations()?;

        assert!(SqlMerkleStateBuilder::new()
            .with_backend(backend.clone())
            .with_tree("test-1")
            .build()
            .is_err());

        Ok(())
    }

    fn assert_read_value_at_address<R>(
        merkle_read: &R,
        root_hash: &str,
        address: &str,
        expected_value: Option<&str>,
    ) where
        R: Read<StateId = String, Key = String, Value = Vec<u8>>,
    {
        let value = merkle_read
            .get(&root_hash.to_string(), &[address.to_string()])
            .and_then(|mut values| {
                Ok(values.remove(address).map(|value| {
                    String::from_utf8(value).expect("could not convert bytes to string")
                }))
            });

        match value {
            Ok(value) => assert_eq!(expected_value, value.as_deref()),
            Err(err) => panic!("value at address {} produced an error: {}", address, err),
        }
    }
}

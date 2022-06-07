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

//! Provides a trait for the low-level store operations required to read and write changes to the
//! merkle radix tree.

mod models;
mod operations;
#[cfg(feature = "postgres")]
mod postgres;
mod schema;
#[cfg(feature = "sqlite")]
mod sqlite;

use std::collections::HashSet;

use crate::error::InternalError;
use crate::state::merkle::sql::cache::DataCache;
use crate::state::merkle::{
    node::Node,
    sql::backend::{Backend, Connection},
};

// (Hash, packed bytes, path address)
type NodeChanges = Vec<(String, Node, String)>;

/// A set of updates to apply to the tree.
///
/// This updates include all of the new nodes and a list of all node hashes that have been not been
/// included in the resulting state root hash.
#[derive(Default)]
pub struct TreeUpdate {
    /// The new nodes to be applied to the tree.
    ///
    /// These are a tuple of (hash, packed bytes, path address).
    pub node_changes: NodeChanges,

    /// The hashes that will not be carried over to the new state root.
    pub deletions: HashSet<String>,
}

/// Low-level store operations for interacting with Merkle-Radix tree storage.
pub trait MerkleRadixStore {
    /// Returns a merkle tree ID, if it exists, or initialize a tree if not.
    ///
    /// # Parameters
    ///
    /// * `tree_name`: the name of the tree to either look up or create if not found
    /// * `initial_state_root_hash`: the initial state root hash to initialize the tree with, if it
    ///   requires creation
    ///
    /// # Returns
    ///
    /// The ID of the tree, either the existing tree or the newly created tree.
    ///
    /// # Errors
    ///
    /// Returns an [`InternalError`] if there is an issue with the underlying storage.
    fn get_or_create_tree(
        &self,
        tree_name: &str,
        initial_state_root_hash: &str,
    ) -> Result<i64, InternalError>;

    /// Returns a merkle tree ID by name.
    ///
    /// # Parameters
    ///
    /// * `tree_name`: the name of the tree to look up
    ///
    /// # Returns
    ///
    /// The ID of the tree, if it exists.
    ///
    /// # Errors
    ///
    /// Returns an [`InternalError`] if there is an issue with the underlying storage.
    fn get_tree_id_by_name(&self, tree_name: &str) -> Result<Option<i64>, InternalError>;

    /// Delete a specific tree, by ID.
    ///
    /// This should delete all underlying data associated with the given tree ID.
    ///
    /// The implementation should be idempotent.
    ///
    /// # Parameters
    ///
    /// * `tree_id`: the ID of the tree to be deleted
    ///
    /// # Errors
    ///
    /// Returns an [`InternalError`] if there is an issue with the underlying storage.
    fn delete_tree(&self, tree_id: i64) -> Result<(), InternalError>;

    /// Lists the names of all available trees.
    ///
    /// # Returns
    ///
    /// An fallable iterator over the names of the trees.
    ///
    /// # Errors
    ///
    /// Returns an [`InternalError`] if there is an issue with the underlying storage.
    fn list_trees(
        &self,
    ) -> Result<Box<dyn ExactSizeIterator<Item = Result<String, InternalError>>>, InternalError>;

    /// Query whether or not a given state root hash exists in a particular tree.
    ///
    /// # Parameters
    ///
    /// * `tree_id`: the ID of the tree to be queried
    /// * `state_root_hash`: the state root hash to validate its existence
    ///
    /// # Errors
    ///
    /// Returns an [`InternalError`] if there is an issue with the underlying storage.
    fn has_root(&self, tree_id: i64, state_root_hash: &str) -> Result<bool, InternalError>;

    /// Query whether or not a given state root hash exists in a particular tree.
    ///
    /// # Parameters
    ///
    /// * `tree_id`: the ID of the tree to be queried
    /// * `state_root_hash`: the state root hash to validate its existence
    ///
    /// # Returns
    ///
    /// The ID of the tree, if it exists.
    ///
    /// # Errors
    ///
    /// Returns an [`InternalError`] if there is an issue with the underlying storage.
    fn get_path(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        address: &str,
    ) -> Result<Vec<(String, Node)>, InternalError>;

    /// Return entries given set of addresses on a given state root.
    ///
    /// # Parameters
    ///
    /// * `tree_id`: the ID of the tree to be queried
    /// * `state_root_hash`: the state root hash under which the entries should be queried
    /// * `keys`: the list of addresses to be read
    ///
    /// # Returns
    ///
    /// The list of (address, byte) pairs found.  This may be a subset of the addresses requested.
    ///
    /// # Errors
    ///
    /// Returns an [`InternalError`] if there is an issue with the underlying storage.
    fn get_entries(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        keys: Vec<&str>,
    ) -> Result<Vec<(String, Vec<u8>)>, InternalError>;

    /// Return entries given under a given state root, with an optional address prefix
    ///
    /// This returns all the entries under a given state root.  Optionally, this can be provided an
    /// address prefix to only list the entries under a specific subtree.
    ///
    /// # Parameters
    ///
    /// * `tree_id`: the ID of the tree to be queried
    /// * `state_root_hash`: the state root hash under which the entries should be queried
    /// * `prefix`: an optional address prefix
    ///
    /// # Returns
    ///
    /// The list of (address, byte) pairs found.
    ///
    /// # Errors
    ///
    /// Returns an [`InternalError`] if there is an issue with the underlying storage.
    fn list_entries(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<(String, Vec<u8>)>, InternalError>;

    /// Write a set of tree updates.
    ///
    /// This is a rather low-level operation that writes the updates to a given tree.
    ///
    /// # Parameters
    ///
    /// * `tree_id`: the ID of the tree to be updated
    /// * `state_root_hash`: the state root hash to be applied
    /// * `parent_state_root_hash`: the previous state root hash being built upon
    /// * `tree_update`: the update to the tree.
    ///
    /// # Errors
    ///
    /// Returns an [`InternalError`] if there is an issue with the underlying storage.
    fn write_changes(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        parent_state_root_hash: &str,
        tree_update: TreeUpdate,
    ) -> Result<(), InternalError>;

    /// Prunes a state root hash from a given tree.
    ///
    /// # Parameters
    ///
    /// * `tree_id`: the ID of the tree to be pruned
    /// * `state_root_hash`: the state root hash to pruned
    ///
    /// # Errors
    ///
    /// Returns an [`InternalError`] if there is an issue with the underlying storage.
    fn prune(&self, tree_id: i64, state_root: &str) -> Result<Vec<String>, InternalError>;

    /// Removes pruned entries in a given tree.
    ///
    /// The SQL-backed merkle tree implementation prunes entries by marking them as pruned, either
    /// through a `pruned_at` column or a reference count of `0`.  This function deletes the
    /// records that match those criteria.
    ///
    /// # Parameters
    ///
    /// * `tree_id`: the ID of the tree from which entries are removed
    ///
    /// # Errors
    ///
    /// Returns an [`InternalError`] if there is an issue with the underlying storage.
    fn remove_pruned_entries(&self, tree_id: i64) -> Result<u64, InternalError>;
}

/// A MerkleRadixStore backed by a SQL back-end.
pub struct SqlMerkleRadixStore<'b, B: Backend, C> {
    pub backend: &'b B,
    _conn: std::marker::PhantomData<C>,
    pub cache: Option<&'b DataCache>,
}

impl<'b, B: Backend, C> SqlMerkleRadixStore<'b, B, C>
where
    B: Backend,
    <B as Backend>::Connection: Connection<ConnectionType = C>,
{
    /// Constructs a new store for a given back-end.
    pub fn new(backend: &'b B) -> Self {
        Self {
            backend,
            _conn: std::marker::PhantomData,
            cache: None,
        }
    }

    pub(crate) fn new_with_cache(backend: &'b B, cache: &'b DataCache) -> Self {
        Self {
            backend,
            _conn: std::marker::PhantomData,
            cache: Some(cache),
        }
    }
}

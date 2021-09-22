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
//! The resulting `merkle_state` can then be used via the [`Read`](crate::state::Read),
//! [`Write`](crate::state::Write) and [`MerkleRadixLeafReader`](super::MerkleRadixLeafReader)
//! traits.
//!
//! Available if the feature "state-merkle-sql" is enabled.

pub mod backend;
mod error;
pub mod migration;
mod models;
mod operations;
#[cfg(feature = "postgres")]
mod postgres;
mod schema;
#[cfg(feature = "sqlite")]
mod sqlite;

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use crate::error::InternalError;
use crate::state::error::StateWriteError;
use crate::state::StateChange;

use super::node::Node;

use backend::Backend;
pub use error::SqlMerkleStateBuildError;

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

struct MerkleRadixOverlay<'s, O> {
    tree_id: i64,
    state_root_hash: &'s str,
    inner: O,
}

// (Hash, packed bytes, path address)
type NodeChanges = Vec<(String, Node, String)>;

#[derive(Default)]
struct TreeUpdate {
    node_changes: NodeChanges,
    deletions: HashSet<String>,
}

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
        tree_update: TreeUpdate,
    ) -> Result<(), InternalError> {
        if tree_update.node_changes.is_empty() {
            return Ok(());
        }

        self.inner.write_changes(
            self.tree_id,
            new_state_root,
            self.state_root_hash,
            tree_update,
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
    ) -> Result<(String, TreeUpdate), StateWriteError> {
        if state_changes.is_empty() {
            return Ok((self.state_root_hash.to_string(), TreeUpdate::default()));
        }

        let mut path_map = HashMap::new();

        let mut deletions = HashSet::new();
        let mut additions = HashSet::new();

        let mut delete_items = vec![];
        for state_change in state_changes {
            match state_change {
                StateChange::Set { key, value } => {
                    let mut set_path_map = self
                        .get_path(key)
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
                        .get_path(key)
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

        Ok((
            key_hash_hex,
            TreeUpdate {
                node_changes: batch,
                deletions,
            },
        ))
    }

    fn get_path(&self, address: &str) -> Result<HashMap<String, Node>, InternalError> {
        // Build up the address along the path, starting with the empty address for the root, and
        // finishing with the complete address.
        let addresses_along_path = (0..address.len())
            .step_by(2)
            .map(|i| address[0..i].to_string())
            .chain(std::iter::once(address.to_string()));

        let node_path_iter = self
            .inner
            .get_path(self.tree_id, self.state_root_hash, address)?
            .into_iter()
            .map(|(_, node)| node)
            // include empty nodes after the queried path, to cover cases where the branch doesn't
            // exist.
            .chain(std::iter::repeat(Node::default()));

        Ok(addresses_along_path
            .zip(node_path_iter)
            .collect::<HashMap<_, _>>())
    }
}

struct MerkleRadixPruner<O> {
    tree_id: i64,
    inner: O,
}

impl<O> MerkleRadixPruner<O>
where
    O: OverlayWriter,
{
    fn new(tree_id: i64, inner: O) -> Self {
        Self { tree_id, inner }
    }

    fn prune(&self, state_ids: &[String]) -> Result<Vec<String>, InternalError> {
        let mut removed_hashes = vec![];
        for state_id in state_ids {
            let pruned = self.inner.prune(self.tree_id, state_id)?;
            removed_hashes.extend(pruned.into_iter());
        }
        Ok(removed_hashes)
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
        tree_update: TreeUpdate,
    ) -> Result<(), InternalError>;

    fn prune(&self, tree_id: i64, state_root: &str) -> Result<Vec<String>, InternalError>;
}

struct SqlOverlay<'b, B: Backend> {
    backend: &'b B,
}

impl<'b, B: Backend> SqlOverlay<'b, B> {
    fn new(backend: &'b B) -> Self {
        Self { backend }
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

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

use std::collections::HashMap;

use diesel::Connection as _;

use crate::error::{InternalError, InvalidStateError};
use crate::state::merkle::{node::Node, MerkleRadixLeafReadError, MerkleRadixLeafReader};
use crate::state::{
    Prune, Read, StateChange, StatePruneError, StateReadError, StateWriteError, Write,
};

use super::backend::{self, Backend, Connection};
use super::encode_and_hash;
use super::operations::get_leaves::MerkleRadixGetLeavesOperation as _;
use super::operations::get_or_create_tree::MerkleRadixGetOrCreateTreeOperation as _;
use super::operations::get_path::MerkleRadixGetPathOperation as _;
use super::operations::get_tree_by_name::MerkleRadixGetTreeByNameOperation as _;
use super::operations::has_root::MerkleRadixHasRootOperation as _;
use super::operations::insert_nodes::MerkleRadixInsertNodesOperation as _;
use super::operations::list_leaves::MerkleRadixListLeavesOperation as _;
use super::operations::prune_entries::MerkleRadixPruneEntriesOperation as _;
use super::operations::update_change_log::MerkleRadixUpdateUpdateChangeLogOperation as _;
use super::operations::MerkleRadixOperations;
use super::{
    MerkleRadixOverlay, MerkleRadixPruner, OverlayReader, OverlayWriter, SqlMerkleState,
    SqlMerkleStateBuildError, SqlMerkleStateBuilder, SqlOverlay, TreeUpdate,
};

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

        let (initial_state_root_hash, _) = encode_and_hash(Node::default())?;
        let tree_id: i64 = if self.create_tree {
            operations.get_or_create_tree(&tree_name, &hex::encode(&initial_state_root_hash))?
        } else {
            operations.get_tree_id_by_name(&tree_name)?.ok_or_else(|| {
                InvalidStateError::with_message("must provide the name of an existing tree".into())
            })?
        };

        Ok(SqlMerkleState { backend, tree_id })
    }
}

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

        let (next_state_id, tree_update) = overlay
            .generate_updates(state_changes)
            .map_err(|e| StateWriteError::StorageError(Box::new(e)))?;

        overlay
            .write_updates(&next_state_id, tree_update)
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

impl Prune for SqlMerkleState<backend::PostgresBackend> {
    type StateId = String;
    type Key = String;
    type Value = Vec<u8>;

    fn prune(&self, state_ids: Vec<Self::StateId>) -> Result<Vec<Self::Key>, StatePruneError> {
        let overlay = MerkleRadixPruner::new(self.tree_id, SqlOverlay::new(&self.backend));

        overlay
            .prune(&state_ids)
            .map_err(|e| StatePruneError::StorageError(Box::new(e)))
    }
}

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

type IterResult<T> = Result<T, MerkleRadixLeafReadError>;
type LeafIter<T> = Box<dyn Iterator<Item = IterResult<T>>>;

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

impl<'b> OverlayWriter for SqlOverlay<'b, backend::PostgresBackend> {
    fn write_changes(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        parent_state_root_hash: &str,
        tree_update: TreeUpdate,
    ) -> Result<(), InternalError> {
        let conn = self.backend.connection()?;

        conn.as_inner().transaction(|| {
            let operations = MerkleRadixOperations::new(conn.as_inner());

            let TreeUpdate {
                node_changes,
                deletions,
            } = tree_update;

            let insertable_changes = node_changes
                .into_iter()
                .map(
                    |(hash, node, address)| super::operations::insert_nodes::InsertableNode {
                        hash,
                        node,
                        address,
                    },
                )
                .collect::<Vec<_>>();

            operations.insert_nodes(tree_id, &insertable_changes)?;

            let additions = insertable_changes
                .iter()
                .map(|insertable| insertable.hash.as_ref())
                .collect::<Vec<_>>();
            let deletions = deletions.iter().map(|s| s.as_ref()).collect::<Vec<_>>();
            operations.update_change_log(
                tree_id,
                state_root_hash,
                parent_state_root_hash,
                &additions,
                &deletions,
            )?;

            Ok(())
        })
    }

    fn prune(&self, tree_id: i64, state_root: &str) -> Result<Vec<String>, InternalError> {
        let conn = self.backend.connection()?;
        let operations = MerkleRadixOperations::new(conn.as_inner());
        operations.prune_entries(tree_id, state_root)
    }
}

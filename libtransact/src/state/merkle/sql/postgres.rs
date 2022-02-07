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

use crate::error::{InternalError, InvalidStateError};
use crate::state::merkle::{node::Node, MerkleRadixLeafReadError, MerkleRadixLeafReader};
use crate::state::{
    Prune, Read, StateChange, StatePruneError, StateReadError, StateWriteError, Write,
};

use super::backend;
use super::encode_and_hash;
use super::{
    store::{MerkleRadixStore, SqlMerkleRadixStore},
    MerkleRadixOverlay, MerkleRadixPruner, SqlMerkleState, SqlMerkleStateBuildError,
    SqlMerkleStateBuilder,
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

        let store = SqlMerkleRadixStore::new(&backend);

        let (initial_state_root_hash, _) = encode_and_hash(Node::default())?;
        let tree_id: i64 = if self.create_tree {
            store.get_or_create_tree(&tree_name, &hex::encode(&initial_state_root_hash))?
        } else {
            store.get_tree_id_by_name(&tree_name)?.ok_or_else(|| {
                InvalidStateError::with_message("must provide the name of an existing tree".into())
            })?
        };

        Ok(SqlMerkleState { backend, tree_id })
    }
}

impl SqlMerkleState<backend::PostgresBackend> {
    /// Deletes the complete tree
    ///
    /// After calling this method, no data associated with the tree name will remain in the
    /// database.
    pub fn delete_tree(self) -> Result<(), InternalError> {
        let store = self.new_store();
        store.delete_tree(self.tree_id)?;
        Ok(())
    }

    fn new_store(&self) -> SqlMerkleRadixStore<backend::PostgresBackend> {
        SqlMerkleRadixStore::new(&self.backend)
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
        let overlay = MerkleRadixOverlay::new(self.tree_id, &*state_id, self.new_store());

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
        let overlay = MerkleRadixOverlay::new(self.tree_id, &*state_id, self.new_store());

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
        let overlay = MerkleRadixPruner::new(self.tree_id, self.new_store());

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
        let overlay = MerkleRadixOverlay::new(self.tree_id, &*state_id, self.new_store());

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
        if &self.initial_state_root_hash()? == state_id {
            return Ok(Box::new(std::iter::empty()));
        }

        let leaves = self
            .new_store()
            .list_entries(self.tree_id, state_id, subtree)?;

        Ok(Box::new(leaves.into_iter().map(Ok)))
    }
}

#[cfg(feature = "state-merkle-sql-postgres-tests")]
#[cfg(test)]
mod test {
    use super::*;

    use crate::state::merkle::sql::backend::{run_postgres_test, PostgresBackendBuilder};

    /// This test creates multiple trees in the same backend/db instance and verifies that values
    /// added to one are not added to the other.
    #[test]
    fn test_multiple_trees() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|db_url| {
            let backend = PostgresBackendBuilder::new().with_url(db_url).build()?;

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
        })
    }

    #[test]
    fn test_build_fails_without_explicit_create() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|db_url| {
            let backend = PostgresBackendBuilder::new().with_url(db_url).build()?;

            assert!(SqlMerkleStateBuilder::new()
                .with_backend(backend.clone())
                .with_tree("test-1")
                .build()
                .is_err());

            Ok(())
        })
    }

    /// This test creates state on a given  tree, adds some state, verifies that it can be read
    /// back with a new state instance on that same tree.  It then deletes the tree, and verifies
    /// that the tree no longer exists.
    #[test]
    fn test_delete_tree() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|db_url| {
            let backend = PostgresBackendBuilder::new().with_url(db_url).build()?;
            let state = SqlMerkleStateBuilder::new()
                .with_backend(backend.clone())
                .with_tree("test-1")
                .create_tree_if_necessary()
                .build()?;

            let initial_state_root_hash = state.initial_state_root_hash()?;

            let state_change_set = StateChange::Set {
                key: "1234".to_string(),
                value: "state_value".as_bytes().to_vec(),
            };

            let new_root = state.commit(&initial_state_root_hash, &[state_change_set])?;

            assert_read_value_at_address(&state, &new_root, "1234", Some("state_value"));
            drop(state);

            // re-use the same tree, but fail if it doesn't exist.
            let state = SqlMerkleStateBuilder::new()
                .with_backend(backend.clone())
                .with_tree("test-1")
                .build()?;

            assert_read_value_at_address(&state, &new_root, "1234", Some("state_value"));

            state.delete_tree()?;

            // verify it doesn't exist bu not creating the tree if it doesn't exist. That is, the
            // build will fail in this case.
            assert!(
                SqlMerkleStateBuilder::new()
                    .with_backend(backend)
                    .with_tree("test-1")
                    .build()
                    .is_err(),
                "The tree should no longer exist"
            );

            Ok(())
        })
    }

    #[test]
    fn test_list_leaves() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|db_url| {
            let backend = PostgresBackendBuilder::new().with_url(db_url).build()?;

            let tree_1 = SqlMerkleStateBuilder::new()
                .with_backend(backend.clone())
                .with_tree("test-1")
                .create_tree_if_necessary()
                .build()?;

            let initial_state_root_hash = tree_1.initial_state_root_hash()?;

            let state_change_set = StateChange::Set {
                key: "012345".to_string(),
                value: "state_value".as_bytes().to_vec(),
            };

            let new_root = tree_1
                .commit(&initial_state_root_hash, &[state_change_set])
                .unwrap();
            assert_read_value_at_address(&tree_1, &new_root, "012345", Some("state_value"));

            let entry = tree_1
                .leaves(&new_root, None)?
                .next()
                .expect("A value should have been listed")?;

            assert_eq!(("012345".to_string(), b"state_value".to_vec()), entry);

            Ok(())
        })
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

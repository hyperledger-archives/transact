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
    Committer, DryRunCommitter, Prune, Pruner, Read, Reader, StateChange, StateError,
    StatePruneError, StateReadError, StateWriteError, ValueIter, ValueIterResult, Write,
};

use super::backend::InTransactionSqliteBackend;
use super::backend::{Backend, Connection, SqliteBackend, WriteExclusiveExecute};
use super::encode_and_hash;
use super::{
    store::{MerkleRadixStore, SqlMerkleRadixStore},
    MerkleRadixOverlay, MerkleRadixPruner, SqlMerkleState, SqlMerkleStateBuildError,
    SqlMerkleStateBuilder,
};

const DEFAULT_MIN_CACHED_DATA_SIZE: usize = 100 * 1024; // 100KB
const DEFAULT_CACHE_SIZE: u16 = 512; // number of entries in cache

impl SqlMerkleStateBuilder<SqliteBackend> {
    /// Construct the final SqlMerkleState instance
    ///
    /// # Errors
    ///
    /// An error may be returned under the following circumstances:
    /// * If a Backend has not been provided
    /// * If a tree name has not been provided
    /// * If an internal error occurs while trying to create or lookup the tree
    pub fn build(self) -> Result<SqlMerkleState<SqliteBackend>, SqlMerkleStateBuildError> {
        do_build(self)
    }
}

impl<'a> SqlMerkleStateBuilder<InTransactionSqliteBackend<'a>> {
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
    ) -> Result<SqlMerkleState<InTransactionSqliteBackend<'a>>, SqlMerkleStateBuildError> {
        do_build(self)
    }
}

fn do_build<B>(
    builder: SqlMerkleStateBuilder<B>,
) -> Result<SqlMerkleState<B>, SqlMerkleStateBuildError>
where
    B: Backend + WriteExclusiveExecute,
    <B as Backend>::Connection: Connection<ConnectionType = diesel::SqliteConnection>,
{
    let backend = builder
        .backend
        .ok_or_else(|| InvalidStateError::with_message("must provide a backend".into()))?;

    let tree_name = builder
        .tree_name
        .ok_or_else(|| InvalidStateError::with_message("must provide a tree name".into()))?;

    let cache = {
        super::cache::DataCache::new(
            builder
                .min_cached_data_size
                .unwrap_or(DEFAULT_MIN_CACHED_DATA_SIZE),
            builder.cache_size.unwrap_or(DEFAULT_CACHE_SIZE),
        )
    };

    let store = SqlMerkleRadixStore::new(&backend);

    let (initial_state_root_hash, _) = encode_and_hash(Node::default())?;
    let tree_id: i64 = if builder.create_tree {
        store.get_or_create_tree(&tree_name, &hex::encode(&initial_state_root_hash))?
    } else {
        store.get_tree_id_by_name(&tree_name)?.ok_or_else(|| {
            InvalidStateError::with_message("must provide the name of an existing tree".into())
        })?
    };

    Ok(SqlMerkleState {
        backend,
        tree_id,
        cache,
    })
}

impl SqlMerkleState<SqliteBackend> {
    /// Deletes the complete tree
    ///
    /// After calling this method, no data associated with the tree name will remain in the
    /// database.
    pub fn delete_tree(self) -> Result<(), InternalError> {
        let store = self.new_store();
        store.delete_tree(self.tree_id)?;
        Ok(())
    }

    /// Removes all entries that have been marked as pruned.
    ///
    /// After calling this method, any records that have been marked as pruned will have been
    /// deleted from the database.
    pub fn remove_pruned_entries(&self) -> Result<(), InternalError> {
        let store = self.new_store();
        store.remove_pruned_entries(self.tree_id)?;
        Ok(())
    }

    fn new_store(&self) -> SqlMerkleRadixStore<SqliteBackend, diesel::SqliteConnection> {
        SqlMerkleRadixStore::new_with_cache(&self.backend, &self.cache)
    }
}

impl Write for SqlMerkleState<SqliteBackend> {
    type StateId = String;
    type Key = String;
    type Value = Vec<u8>;

    fn commit(
        &self,
        state_id: &Self::StateId,
        state_changes: &[StateChange],
    ) -> Result<Self::StateId, StateWriteError> {
        Committer::commit(self, state_id, state_changes).map_err(StateWriteError::from)
    }

    fn compute_state_id(
        &self,
        state_id: &Self::StateId,
        state_changes: &[StateChange],
    ) -> Result<Self::StateId, StateWriteError> {
        DryRunCommitter::dry_run_commit(self, state_id, state_changes)
            .map_err(StateWriteError::from)
    }
}

impl Read for SqlMerkleState<SqliteBackend> {
    type StateId = String;
    type Key = String;
    type Value = Vec<u8>;

    fn get(
        &self,
        state_id: &Self::StateId,
        keys: &[Self::Key],
    ) -> Result<HashMap<Self::Key, Self::Value>, StateReadError> {
        Reader::get(self, state_id, keys).map_err(StateReadError::from)
    }

    fn clone_box(
        &self,
    ) -> Box<dyn Read<StateId = Self::StateId, Key = Self::Key, Value = Self::Value>> {
        Box::new(self.clone())
    }
}

impl Prune for SqlMerkleState<SqliteBackend> {
    type StateId = String;
    type Key = String;
    type Value = Vec<u8>;

    fn prune(&self, state_ids: Vec<Self::StateId>) -> Result<Vec<Self::Key>, StatePruneError> {
        Pruner::prune(self, state_ids).map_err(StatePruneError::from)
    }
}

impl Reader for SqlMerkleState<SqliteBackend> {
    type Filter = str;

    fn get(
        &self,
        state_id: &Self::StateId,
        keys: &[Self::Key],
    ) -> Result<HashMap<Self::Key, Self::Value>, StateError> {
        let overlay = MerkleRadixOverlay::new(self.tree_id, &*state_id, self.new_store());

        if !overlay.has_root()? {
            return Err(InvalidStateError::with_message(state_id.into()).into());
        }

        Ok(overlay.get_entries(keys)?)
    }

    fn filter_iter(
        &self,
        state_id: &Self::StateId,
        filter: Option<&Self::Filter>,
    ) -> ValueIterResult<ValueIter<(Self::Key, Self::Value)>> {
        if &self.initial_state_root_hash()? == state_id {
            return Ok(Box::new(std::iter::empty()));
        }

        let leaves = self
            .new_store()
            .list_entries(self.tree_id, state_id, filter)?;

        Ok(Box::new(leaves.into_iter().map(Ok)))
    }
}

impl Committer for SqlMerkleState<SqliteBackend> {
    type StateChange = StateChange;

    fn commit(
        &self,
        state_id: &Self::StateId,
        state_changes: &[Self::StateChange],
    ) -> Result<Self::StateId, StateError> {
        let overlay = MerkleRadixOverlay::new(self.tree_id, &*state_id, self.new_store());

        let (next_state_id, tree_update) = overlay
            .generate_updates(state_changes)
            .map_err(|e| InternalError::from_source(Box::new(e)))?;

        overlay.write_updates(&next_state_id, tree_update)?;

        Ok(next_state_id)
    }
}

impl DryRunCommitter for SqlMerkleState<SqliteBackend> {
    type StateChange = StateChange;

    fn dry_run_commit(
        &self,
        state_id: &Self::StateId,
        state_changes: &[Self::StateChange],
    ) -> Result<Self::StateId, StateError> {
        let overlay = MerkleRadixOverlay::new(self.tree_id, &*state_id, self.new_store());

        let (next_state_id, _) = overlay
            .generate_updates(state_changes)
            .map_err(|e| InternalError::from_source(Box::new(e)))?;

        Ok(next_state_id)
    }
}

impl Pruner for SqlMerkleState<SqliteBackend> {
    fn prune(&self, state_ids: Vec<Self::StateId>) -> Result<Vec<Self::Key>, StateError> {
        let overlay = MerkleRadixPruner::new(self.tree_id, self.new_store());

        overlay.prune(&state_ids).map_err(StateError::from)
    }
}

impl<'a> SqlMerkleState<InTransactionSqliteBackend<'a>> {
    /// Deletes the complete tree
    ///
    /// After calling this method, no data associated with the tree name will remain in the
    /// database.
    pub fn delete_tree(self) -> Result<(), InternalError> {
        let store = self.new_store();
        store.delete_tree(self.tree_id)?;
        Ok(())
    }

    /// Removes all entries that have been marked as pruned.
    ///
    /// After calling this method, any records that have been marked as pruned will have been
    /// deleted from the database.
    pub fn remove_pruned_entries(&self) -> Result<(), InternalError> {
        let store = self.new_store();
        store.remove_pruned_entries(self.tree_id)?;
        Ok(())
    }

    fn new_store(
        &self,
    ) -> SqlMerkleRadixStore<InTransactionSqliteBackend<'a>, diesel::SqliteConnection> {
        SqlMerkleRadixStore::new_with_cache(&self.backend, &self.cache)
    }
}

impl<'a> Reader for SqlMerkleState<InTransactionSqliteBackend<'a>> {
    type Filter = str;

    fn get(
        &self,
        state_id: &Self::StateId,
        keys: &[Self::Key],
    ) -> Result<HashMap<Self::Key, Self::Value>, StateError> {
        let overlay = MerkleRadixOverlay::new(self.tree_id, &*state_id, self.new_store());

        if !overlay.has_root()? {
            return Err(InvalidStateError::with_message(state_id.into()).into());
        }

        Ok(overlay.get_entries(keys)?)
    }

    fn filter_iter(
        &self,
        state_id: &Self::StateId,
        filter: Option<&Self::Filter>,
    ) -> ValueIterResult<ValueIter<(Self::Key, Self::Value)>> {
        if &self.initial_state_root_hash()? == state_id {
            return Ok(Box::new(std::iter::empty()));
        }

        let leaves = self
            .new_store()
            .list_entries(self.tree_id, state_id, filter)?;

        Ok(Box::new(leaves.into_iter().map(Ok)))
    }
}

impl<'a> Committer for SqlMerkleState<InTransactionSqliteBackend<'a>> {
    type StateChange = StateChange;

    fn commit(
        &self,
        state_id: &Self::StateId,
        state_changes: &[Self::StateChange],
    ) -> Result<Self::StateId, StateError> {
        let overlay = MerkleRadixOverlay::new(self.tree_id, &*state_id, self.new_store());

        let (next_state_id, tree_update) = overlay
            .generate_updates(state_changes)
            .map_err(|e| InternalError::from_source(Box::new(e)))?;

        overlay.write_updates(&next_state_id, tree_update)?;

        Ok(next_state_id)
    }
}

impl<'a> DryRunCommitter for SqlMerkleState<InTransactionSqliteBackend<'a>> {
    type StateChange = StateChange;

    fn dry_run_commit(
        &self,
        state_id: &Self::StateId,
        state_changes: &[Self::StateChange],
    ) -> Result<Self::StateId, StateError> {
        let overlay = MerkleRadixOverlay::new(self.tree_id, &*state_id, self.new_store());

        let (next_state_id, _) = overlay
            .generate_updates(state_changes)
            .map_err(|e| InternalError::from_source(Box::new(e)))?;

        Ok(next_state_id)
    }
}

impl<'a> Pruner for SqlMerkleState<InTransactionSqliteBackend<'a>> {
    fn prune(&self, state_ids: Vec<Self::StateId>) -> Result<Vec<Self::Key>, StateError> {
        let overlay = MerkleRadixPruner::new(self.tree_id, self.new_store());

        overlay.prune(&state_ids).map_err(StateError::from)
    }
}

type IterResult<T> = Result<T, MerkleRadixLeafReadError>;
type LeafIter<T> = Box<dyn Iterator<Item = IterResult<T>>>;

impl MerkleRadixLeafReader for SqlMerkleState<SqliteBackend> {
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

#[cfg(test)]
mod test {
    use super::*;

    use std::sync::{Arc, RwLock};

    use diesel::{
        dsl::sql_query,
        prelude::*,
        r2d2::{ConnectionManager, Pool},
        sqlite,
    };

    use crate::state::merkle::sql::backend;
    use crate::state::merkle::sql::backend::SqliteBackendBuilder;
    use crate::state::merkle::sql::migration::MigrationManager;
    use crate::state::Committer;

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

        let new_root =
            Write::commit(&tree_1, &initial_state_root_hash, &[state_change_set]).unwrap();
        assert_read_value_at_address(&tree_1, &new_root, "1234", Some("state_value"));

        let tree_2 = SqlMerkleStateBuilder::new()
            .with_backend(backend)
            .with_tree("test-2")
            .create_tree_if_necessary()
            .build()?;

        assert!(Read::get(&tree_2, &new_root, &["1234".to_string()]).is_err());

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

    /// This test creates state on a given  tree, adds some state, verifies that it can be read
    /// back with a new state instance on that same tree.  It then deletes the tree, and verifies
    /// that the tree no longer exists.
    #[test]
    fn test_delete_tree() -> Result<(), Box<dyn std::error::Error>> {
        let backend = SqliteBackendBuilder::new().with_memory_database().build()?;

        backend.run_migrations()?;
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

        let new_root = Write::commit(&state, &initial_state_root_hash, &[state_change_set])?;

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
    }

    #[test]
    fn test_list_leaves() -> Result<(), Box<dyn std::error::Error>> {
        let backend = SqliteBackendBuilder::new().with_memory_database().build()?;

        backend.run_migrations()?;

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

        let new_root =
            Write::commit(&tree_1, &initial_state_root_hash, &[state_change_set]).unwrap();
        assert_read_value_at_address(&tree_1, &new_root, "012345", Some("state_value"));

        let entry = tree_1
            .leaves(&new_root, None)?
            .next()
            .expect("A value should have been listed")?;

        assert_eq!(("012345".to_string(), b"state_value".to_vec()), entry);

        Ok(())
    }

    #[test]
    fn test_in_transaction() -> Result<(), Box<dyn std::error::Error>> {
        let backend = SqliteBackendBuilder::new().with_memory_database().build()?;

        backend.run_migrations()?;

        let new_root = backend.execute_write(|conn| {
            let in_txn_backend: backend::InTransactionSqliteBackend = conn.as_inner().into();

            let tree = SqlMerkleStateBuilder::new()
                .with_backend(in_txn_backend)
                .with_tree("test-1")
                .create_tree_if_necessary()
                .build()
                .map_err(|e| InternalError::from_source(Box::new(e)))?;

            let initial_state_root_hash = tree.initial_state_root_hash()?;

            let state_change_set = StateChange::Set {
                key: "012345".to_string(),
                value: "state_value".as_bytes().to_vec(),
            };

            tree.commit(&initial_state_root_hash, &[state_change_set])
                .map_err(|e| InternalError::from_source(Box::new(e)))
        })?;

        let tree = SqlMerkleStateBuilder::new()
            .with_backend(backend.clone())
            .with_tree("test-1")
            .build()?;

        assert_read_value_at_address(&tree, &new_root, "012345", Some("state_value"));

        Ok(())
    }

    /// Test that pruned entries are successfully removed
    /// 1. Create a merkle tree
    /// 2. Commit a single value
    /// 3. Commit a value and delete the previous value
    /// 4. Prune the first state root
    /// 5. Verify that the records still exist
    /// 6. Remove the pruned values
    /// 7. Verify that the records no longer exist
    #[test]
    fn test_remove_pruned_entries() -> Result<(), Box<dyn std::error::Error>> {
        let backend = SqliteBackendBuilder::new().with_memory_database().build()?;

        backend.run_migrations()?;

        let tree = SqlMerkleStateBuilder::new()
            .with_backend(backend.clone())
            .with_tree("test-1")
            .create_tree_if_necessary()
            .build()?;

        let initial_state_root_hash = tree.initial_state_root_hash()?;

        let state_change_set = StateChange::Set {
            key: "012345".to_string(),
            value: "state_value".as_bytes().to_vec(),
        };

        let first_root = Write::commit(&tree, &initial_state_root_hash, &[state_change_set])?;
        assert_read_value_at_address(&tree, &first_root, "012345", Some("state_value"));

        let new_state_changes = [
            StateChange::Set {
                key: "ab0000".to_string(),
                value: "second_value".as_bytes().to_vec(),
            },
            StateChange::Delete {
                key: "012345".to_string(),
            },
        ];

        let second_root = Write::commit(&tree, &first_root, &new_state_changes)?;
        assert_read_value_at_address(&tree, &second_root, "ab0000", Some("second_value"));
        assert_read_value_at_address(&tree, &second_root, "012345", None);

        Prune::prune(&tree, vec![first_root])?;

        #[derive(Debug, QueryableByName)]
        struct Entries {
            #[column_name = "entries"]
            #[sql_type = "diesel::sql_types::BigInt"]
            pub entries: i64,
        }

        let pool: Arc<RwLock<Pool<ConnectionManager<sqlite::SqliteConnection>>>> = backend.into();

        {
            let conn = pool.read().unwrap().get()?;

            let count: Entries =
                sql_query("SELECT count(id) as entries FROM merkle_radix_leaf WHERE address = ?")
                    .bind::<diesel::sql_types::Text, _>("012345")
                    .get_result(&*conn)?;

            assert_eq!(count.entries, 1);
        }

        tree.remove_pruned_entries()?;

        {
            let conn = pool.read().unwrap().get()?;

            let count: Entries =
                sql_query("SELECT count(id) as entries FROM merkle_radix_leaf WHERE address = ?")
                    .bind::<diesel::sql_types::Text, _>("012345")
                    .get_result(&*conn)?;

            assert_eq!(count.entries, 0);
        }

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

// Copyright 2021 Cargill Incorporated
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SQL-backed tests for the merkle state implementation

use std::error::Error;

use transact::state::merkle::sql::{
    backend::{run_postgres_test, PostgresBackend, PostgresBackendBuilder},
    SqlMerkleState, SqlMerkleStateBuilder,
};
use transact::{database::btree::BTreeDatabase, state::merkle::INDEXES};

use super::*;

fn new_sql_merkle_state_and_root(
    db_url: &str,
) -> Result<(SqlMerkleState<PostgresBackend>, String), Box<dyn Error>> {
    let backend = PostgresBackendBuilder::new().with_url(db_url).build()?;

    let state = SqlMerkleStateBuilder::new()
        .with_backend(backend)
        .with_tree("test-tree")
        .create_tree_if_necessary()
        .build()?;

    let initial_state_root_hash = state.initial_state_root_hash()?;

    Ok((state, initial_state_root_hash))
}

#[test]
fn merkle_trie_empty_changes() -> Result<(), Box<dyn Error>> {
    run_postgres_test(|db_url| {
        let (state, orig_root) = new_sql_merkle_state_and_root(db_url)?;
        test_merkle_trie_empty_changes(orig_root, state);
        Ok(())
    })
}

#[test]
fn merkle_trie_delete() -> Result<(), Box<dyn Error>> {
    run_postgres_test(|db_url| {
        let (state, orig_root) = new_sql_merkle_state_and_root(db_url)?;
        test_merkle_trie_delete(orig_root, state);
        Ok(())
    })
}

#[test]
fn merkle_trie_update() -> Result<(), Box<dyn Error>> {
    run_postgres_test(|db_url| {
        let (state, orig_root) = new_sql_merkle_state_and_root(db_url)?;
        test_merkle_trie_update(orig_root, state);
        Ok(())
    })
}

#[test]
fn merkle_trie_update_same_address_space() -> Result<(), Box<dyn Error>> {
    run_postgres_test(|db_url| {
        let (state, orig_root) = new_sql_merkle_state_and_root(db_url)?;
        test_merkle_trie_update_same_address_space(orig_root, state);
        Ok(())
    })
}

#[test]
fn merkle_trie_update_same_address_space_with_no_children() -> Result<(), Box<dyn Error>> {
    run_postgres_test(|db_url| {
        let (state, orig_root) = new_sql_merkle_state_and_root(db_url)?;
        test_merkle_trie_update_same_address_space_with_no_children(orig_root, state);
        Ok(())
    })
}

#[cfg(feature = "state-merkle-leaf-reader")]
#[test]
fn leaf_iteration() -> Result<(), Box<dyn Error>> {
    run_postgres_test(|db_url| {
        let (state, orig_root) = new_sql_merkle_state_and_root(db_url)?;
        test_leaf_iteration(orig_root, state);
        Ok(())
    })
}

#[test]
fn merkle_produce_same_state_as_btree() -> Result<(), Box<dyn Error>> {
    run_postgres_test(|db_url| {
        let (sql_state, sql_orig_root) = new_sql_merkle_state_and_root(db_url)?;

        let btree_db = Box::new(BTreeDatabase::new(&INDEXES));
        let btree_state = MerkleState::new(btree_db.clone());

        let merkle_db = MerkleRadixTree::new(btree_db, None)
            .expect("Could not overlay the merkle tree on the database");

        let btree_orig_root = merkle_db.get_merkle_root();

        test_produce_same_state(btree_orig_root, btree_state, sql_orig_root, sql_state);

        Ok(())
    })
}

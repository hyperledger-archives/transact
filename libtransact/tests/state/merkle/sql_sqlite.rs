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
    backend::{JournalMode, SqliteBackend, SqliteBackendBuilder},
    migration::MigrationManager,
    SqlMerkleState, SqlMerkleStateBuilder,
};
use transact::{database::btree::BTreeDatabase, state::merkle::INDEXES};

use super::*;

fn new_sql_merkle_state_and_root(
    dbpath: &str,
) -> Result<(SqlMerkleState<SqliteBackend>, String), Box<dyn Error>> {
    let backend = SqliteBackendBuilder::new()
        .with_connection_path(dbpath)
        .with_journal_mode(JournalMode::Wal)
        .with_create()
        .build()?;
    backend.run_migrations()?;

    let state = SqlMerkleStateBuilder::new()
        .with_backend(backend)
        .with_tree(format!(
            "test-{}",
            GLOBAL_THREAD_COUNT.load(Ordering::SeqCst)
        ))
        .create_tree_if_necessary()
        .build()?;

    let initial_state_root_hash = state.initial_state_root_hash()?;

    Ok((state, initial_state_root_hash))
}

#[test]
fn merkle_trie_empty_changes() -> Result<(), Box<dyn Error>> {
    run_test(|db_path| {
        let (state, orig_root) = new_sql_merkle_state_and_root(db_path)?;
        test_merkle_trie_empty_changes(orig_root, state);
        Ok(())
    })
}

#[test]
fn merkle_trie_delete() -> Result<(), Box<dyn Error>> {
    run_test(|db_path| {
        let (state, orig_root) = new_sql_merkle_state_and_root(db_path)?;
        test_merkle_trie_delete(orig_root, state);
        Ok(())
    })
}

#[test]
fn merkle_trie_update() -> Result<(), Box<dyn Error>> {
    run_test(|db_path| {
        let (state, orig_root) = new_sql_merkle_state_and_root(db_path)?;
        test_merkle_trie_update(orig_root, state);
        Ok(())
    })
}

#[test]
fn merkle_trie_update_same_address_space() -> Result<(), Box<dyn Error>> {
    run_test(|db_path| {
        let (state, orig_root) = new_sql_merkle_state_and_root(db_path)?;
        test_merkle_trie_update_same_address_space(orig_root, state);
        Ok(())
    })
}

#[test]
fn merkle_trie_update_same_address_space_with_no_children() -> Result<(), Box<dyn Error>> {
    run_test(|db_path| {
        let (state, orig_root) = new_sql_merkle_state_and_root(db_path)?;
        test_merkle_trie_update_same_address_space_with_no_children(orig_root, state);
        Ok(())
    })
}

#[test]
fn merkle_trie_prune_parent() -> Result<(), Box<dyn Error>> {
    run_test(|db_path| {
        let (state, orig_root) = new_sql_merkle_state_and_root(db_path)?;
        test_merkle_trie_prune_parent(orig_root, state);
        Ok(())
    })
}

#[test]
fn merkle_trie_prune_successors() -> Result<(), Box<dyn Error>> {
    run_test(|db_path| {
        let (state, orig_root) = new_sql_merkle_state_and_root(db_path)?;
        test_merkle_trie_prune_successors(orig_root, state);
        Ok(())
    })
}

#[test]
fn merkle_trie_prune_duplicate_leaves() -> Result<(), Box<dyn Error>> {
    run_test(|db_path| {
        let (state, orig_root) = new_sql_merkle_state_and_root(db_path)?;
        test_merkle_trie_prune_duplicate_leaves(orig_root, state);
        Ok(())
    })
}

#[test]
fn merkle_trie_prune_successor_duplicate_leaves() -> Result<(), Box<dyn Error>> {
    run_test(|db_path| {
        let (state, orig_root) = new_sql_merkle_state_and_root(db_path)?;
        test_merkle_trie_prune_successor_duplicate_leaves(orig_root, state);
        Ok(())
    })
}

#[test]
fn leaf_iteration() -> Result<(), Box<dyn Error>> {
    run_test(|db_path| {
        let (state, orig_root) = new_sql_merkle_state_and_root(db_path)?;
        test_leaf_iteration(orig_root, state);
        Ok(())
    })
}

#[test]
fn merkle_produce_same_state_as_btree() -> Result<(), Box<dyn Error>> {
    run_test(|db_path| {
        let (sql_state, sql_orig_root) = new_sql_merkle_state_and_root(db_path)?;

        let btree_db = Box::new(BTreeDatabase::new(&INDEXES));
        let btree_state = MerkleState::new(btree_db.clone());

        let merkle_db = MerkleRadixTree::new(btree_db, None)
            .expect("Could not overlay the merkle tree on the database");

        let btree_orig_root = merkle_db.get_merkle_root();

        test_produce_same_state(btree_orig_root, btree_state, sql_orig_root, sql_state);

        Ok(())
    })
}

use std::sync::atomic::{AtomicUsize, Ordering};

fn run_test<T>(test: T) -> Result<(), Box<dyn Error>>
where
    T: FnOnce(&str) -> Result<(), Box<dyn Error>> + std::panic::UnwindSafe,
{
    let dbpath = temp_db_path();

    let testpath = dbpath.clone();
    let result = std::panic::catch_unwind(move || test(&testpath));

    std::fs::remove_file(dbpath).unwrap();

    match result {
        Ok(res) => res,
        Err(err) => {
            std::panic::resume_unwind(err);
        }
    }
}

static GLOBAL_THREAD_COUNT: AtomicUsize = AtomicUsize::new(1);

fn temp_db_path() -> String {
    let mut temp_dir = std::env::temp_dir();

    let thread_id = GLOBAL_THREAD_COUNT.fetch_add(1, Ordering::SeqCst);
    temp_dir.push(format!("sql_sqlite-test-{:?}.db", thread_id));
    temp_dir.to_str().unwrap().to_string()
}

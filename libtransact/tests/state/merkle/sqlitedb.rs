// Copyright 2019 Cargill Incorporated
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

use std::sync::atomic::{AtomicUsize, Ordering};

use transact::{
    database::{btree::BTreeDatabase, sqlite::SqliteDatabase},
    state::merkle::INDEXES,
};

use super::*;

fn new_sqlite_state_and_root(db_path: &str) -> (MerkleState, String) {
    let db = Box::new(
        SqliteDatabase::new(&db_path, &INDEXES).expect("Unable to create Sqlite database"),
    );
    let merkle_state = MerkleState::new(db.clone());

    let merkle_db =
        MerkleRadixTree::new(db, None).expect("Could not overlay the merkle tree on the database");

    let orig_root = merkle_db.get_merkle_root();

    (merkle_state, orig_root)
}

#[test]
fn merkle_trie_empty_changes() {
    run_test(|db_path| {
        let (state, orig_root) = new_sqlite_state_and_root(db_path);
        test_merkle_trie_empty_changes(orig_root, state);
    })
}

#[test]
fn merkle_trie_root_advance() {
    run_test(|db_path| {
        let db = Box::new(
            SqliteDatabase::new(&db_path, &INDEXES).expect("Unable to create Sqlite database"),
        );
        test_merkle_trie_root_advance(db);
    })
}

#[test]
fn merkle_trie_delete() {
    run_test(|db_path| {
        let (state, orig_root) = new_sqlite_state_and_root(db_path);
        test_merkle_trie_delete(orig_root, state);
    })
}

/// Atomic Commit/Rollback is the default journal model.
#[test]
fn merkle_trie_update_atomic_commit_rollback() {
    run_test(|db_path| {
        let (state, orig_root) = new_sqlite_state_and_root(db_path);
        test_merkle_trie_update(orig_root, state);
    })
}

#[test]
fn merkle_trie_update_with_wal_mode() {
    run_test(|db_path| {
        let db = Box::new(
            SqliteDatabase::builder()
                .with_path(db_path)
                .with_indexes(&INDEXES)
                .with_journal_mode(transact::database::sqlite::JournalMode::Wal)
                .build()
                .expect("Unable to create Sqlite database"),
        );
        let merkle_state = MerkleState::new(db.clone());

        let merkle_db = MerkleRadixTree::new(db, None)
            .expect("Could not overlay the merkle tree on the database");

        let orig_root = merkle_db.get_merkle_root();
        test_merkle_trie_update(orig_root, merkle_state);
    })
}

#[test]
fn merkle_trie_update_with_sync_full_wal_mode() {
    run_test(|db_path| {
        let db = Box::new(
            SqliteDatabase::builder()
                .with_path(db_path)
                .with_indexes(&INDEXES)
                .with_journal_mode(transact::database::sqlite::JournalMode::Wal)
                .with_synchronous(transact::database::sqlite::Synchronous::Full)
                .build()
                .expect("Unable to create Sqlite database"),
        );
        let merkle_state = MerkleState::new(db.clone());

        let merkle_db = MerkleRadixTree::new(db, None)
            .expect("Could not overlay the merkle tree on the database");

        let orig_root = merkle_db.get_merkle_root();
        test_merkle_trie_update(orig_root, merkle_state);
    })
}

#[test]
fn merkle_trie_update_same_address_space() {
    run_test(|db_path| {
        let (state, orig_root) = new_sqlite_state_and_root(db_path);
        test_merkle_trie_update_same_address_space(orig_root, state);
    })
}

#[test]
fn merkle_trie_update_same_address_space_with_no_children() {
    run_test(|db_path| {
        let (state, orig_root) = new_sqlite_state_and_root(db_path);
        test_merkle_trie_update_same_address_space_with_no_children(orig_root, state);
    })
}

#[test]
fn merkle_trie_prune_parent() {
    run_test(|db_path| {
        let (state, orig_root) = new_sqlite_state_and_root(db_path);
        test_merkle_trie_prune_parent(orig_root, state);
    })
}

#[test]
fn merkle_trie_pruning_parent() {
    run_test(|db_path| {
        let db = Box::new(
            SqliteDatabase::new(&db_path, &INDEXES).expect("Unable to create Sqlite database"),
        );
        test_merkle_trie_pruning_parent(db);
    })
}

#[test]
fn merkle_trie_prune_successors() {
    run_test(|db_path| {
        let (state, orig_root) = new_sqlite_state_and_root(db_path);
        test_merkle_trie_prune_successors(orig_root, state);
    })
}

#[test]
fn merkle_trie_pruning_successors() {
    run_test(|db_path| {
        let db = Box::new(
            SqliteDatabase::new(&db_path, &INDEXES).expect("Unable to create Sqlite database"),
        );
        test_merkle_trie_pruning_successors(db);
    })
}

#[test]
fn merkle_trie_prune_duplicate_leaves() {
    run_test(|db_path| {
        let (state, orig_root) = new_sqlite_state_and_root(db_path);
        test_merkle_trie_prune_duplicate_leaves(orig_root, state);
    })
}

#[test]
fn merkle_trie_pruning_duplicate_leaves() {
    run_test(|db_path| {
        let db = Box::new(
            SqliteDatabase::new(&db_path, &INDEXES).expect("Unable to create Sqlite database"),
        );
        test_merkle_trie_pruning_duplicate_leaves(db);
    })
}

#[test]
fn merkle_trie_prune_successor_duplicate_leaves() {
    run_test(|db_path| {
        let (state, orig_root) = new_sqlite_state_and_root(db_path);
        test_merkle_trie_prune_successor_duplicate_leaves(orig_root, state);
    })
}

#[test]
fn merkle_trie_pruning_successor_duplicate_leaves() {
    run_test(|db_path| {
        let db = Box::new(
            SqliteDatabase::new(&db_path, &INDEXES).expect("Unable to create Sqlite database"),
        );
        test_merkle_trie_pruning_successor_duplicate_leaves(db);
    })
}

#[test]
fn leaf_iteration() {
    run_test(|db_path| {
        let (state, orig_root) = new_sqlite_state_and_root(db_path);
        test_leaf_iteration(orig_root, state);
    })
}

/// Verifies that a state tree backed by lmdb and btree give the same root hashes
#[test]
fn sqlite_btree_comparison() {
    run_test(|db_path| {
        let sqlite_db = Box::new(
            SqliteDatabase::new(&db_path, &INDEXES).expect("Unable to create Sqlite database"),
        );
        let btree_db = Box::new(BTreeDatabase::new(&INDEXES));

        test_same_results(sqlite_db, btree_db);
    })
}

#[test]
fn btree_sqlite_comparison() {
    run_test(|db_path| {
        let sqlite_db = Box::new(
            SqliteDatabase::new(&db_path, &INDEXES).expect("Unable to create Sqlite database"),
        );
        let btree_db = Box::new(BTreeDatabase::new(&INDEXES));

        test_same_results(btree_db, sqlite_db);
    })
}

fn run_test<T>(test: T) -> ()
where
    T: FnOnce(&str) -> () + std::panic::UnwindSafe,
{
    let dbpath = temp_db_path();

    let testpath = dbpath.clone();
    let result = std::panic::catch_unwind(move || test(&testpath));

    std::fs::remove_file(dbpath).unwrap();

    if let Err(err) = result {
        std::panic::resume_unwind(err);
    }
}
static GLOBAL_THREAD_COUNT: AtomicUsize = AtomicUsize::new(1);

fn temp_db_path() -> String {
    let mut temp_dir = std::env::temp_dir();

    let thread_id = GLOBAL_THREAD_COUNT.fetch_add(1, Ordering::SeqCst);
    temp_dir.push(format!("sqlite-test-{:?}.db", thread_id));
    temp_dir.to_str().unwrap().to_string()
}

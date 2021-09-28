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

//! LMDB-backed tests for the merkle state implementation.

use std::env;
use std::fs::remove_file;
use std::panic;
use std::path::Path;
use std::thread;

use transact::{
    database::{
        btree::BTreeDatabase,
        lmdb::{LmdbContext, LmdbDatabase},
    },
    state::merkle::INDEXES,
};

use super::*;

fn new_lmdb_state_and_root(lmdb_path: &str) -> (MerkleState, String) {
    let lmdb_db = make_lmdb(lmdb_path);
    let merkle_state = MerkleState::new(lmdb_db.clone());

    let merkle_db = MerkleRadixTree::new(lmdb_db, None)
        .expect("Could not overlay the merkle tree on the database");

    let orig_root = merkle_db.get_merkle_root();

    (merkle_state, orig_root)
}

#[test]
fn merkle_trie_empty_changes() {
    run_test(|merkle_path| {
        let (state, orig_root) = new_lmdb_state_and_root(merkle_path);
        test_merkle_trie_empty_changes(orig_root, state);
    })
}

#[test]
fn merkle_trie_root_advance() {
    run_test(|merkle_path| {
        let db = make_lmdb(&merkle_path);
        test_merkle_trie_root_advance(db);
    })
}

#[test]
fn merkle_trie_delete() {
    run_test(|merkle_path| {
        let (state, orig_root) = new_lmdb_state_and_root(merkle_path);
        test_merkle_trie_delete(orig_root, state);
    })
}

#[test]
fn merkle_trie_update_multiple_entries() {
    run_test(|merkle_path| {
        let (state, orig_root) = new_lmdb_state_and_root(merkle_path);
        test_merkle_trie_update(orig_root, state);
    })
}

#[test]
fn merkle_trie_update_same_address_space() {
    run_test(|merkle_path| {
        let (state, orig_root) = new_lmdb_state_and_root(merkle_path);
        test_merkle_trie_update_same_address_space(orig_root, state);
    })
}

#[test]
fn merkle_trie_update_same_address_space_with_no_children() {
    run_test(|merkle_path| {
        let (state, orig_root) = new_lmdb_state_and_root(merkle_path);
        test_merkle_trie_update_same_address_space_with_no_children(orig_root, state);
    })
}

#[test]
fn merkle_trie_prune_parent() {
    run_test(|merkle_path| {
        let (state, orig_root) = new_lmdb_state_and_root(merkle_path);
        test_merkle_trie_prune_parent(orig_root, state);
    })
}

#[test]
fn merkle_trie_pruning_parent() {
    run_test(|merkle_path| {
        let db = make_lmdb(&merkle_path);
        test_merkle_trie_pruning_parent(db);
    })
}

#[test]
fn merkle_trie_prune_successors() {
    run_test(|merkle_path| {
        let (state, orig_root) = new_lmdb_state_and_root(merkle_path);
        test_merkle_trie_prune_successors(orig_root, state);
    })
}

#[test]
fn merkle_trie_pruning_successors() {
    run_test(|merkle_path| {
        let db = make_lmdb(&merkle_path);
        test_merkle_trie_pruning_successors(db);
    })
}

#[test]
fn merkle_trie_prune_duplicate_leaves() {
    run_test(|merkle_path| {
        let (state, orig_root) = new_lmdb_state_and_root(merkle_path);
        test_merkle_trie_prune_duplicate_leaves(orig_root, state);
    })
}

#[test]
fn merkle_trie_pruning_duplicate_leaves() {
    run_test(|merkle_path| {
        let db = make_lmdb(&merkle_path);
        test_merkle_trie_pruning_duplicate_leaves(db);
    })
}

#[test]
fn merkle_trie_prune_successor_duplicate_leaves() {
    run_test(|merkle_path| {
        let (state, orig_root) = new_lmdb_state_and_root(merkle_path);
        test_merkle_trie_prune_successor_duplicate_leaves(orig_root, state);
    })
}

#[test]
fn merkle_trie_pruning_successor_duplicate_leaves() {
    run_test(|merkle_path| {
        let db = make_lmdb(&merkle_path);
        test_merkle_trie_pruning_successor_duplicate_leaves(db);
    })
}

#[test]
fn leaf_iteration() {
    run_test(|merkle_path| {
        let (state, orig_root) = new_lmdb_state_and_root(merkle_path);
        test_leaf_iteration(orig_root, state);
    })
}

/// Verifies that a state tree backed by lmdb and btree give the same root hashes
#[test]
fn lmdb_btree_comparison() {
    run_test(|merkle_path| {
        let lmdb = make_lmdb(merkle_path);
        let btree_db = Box::new(BTreeDatabase::new(&INDEXES));

        test_same_results(lmdb, btree_db);
    })
}

#[test]
fn btree_lmdb_comparison() {
    run_test(|merkle_path| {
        let lmdb = make_lmdb(merkle_path);
        let btree_db = Box::new(BTreeDatabase::new(&INDEXES));

        test_same_results(btree_db, lmdb);
    })
}

fn run_test<T>(test: T) -> ()
where
    T: FnOnce(&str) -> () + panic::UnwindSafe,
{
    let dbpath = temp_db_path();

    let testpath = dbpath.clone();
    let result = panic::catch_unwind(move || test(&testpath));

    remove_file(dbpath).unwrap();

    if let Err(err) = result {
        panic::resume_unwind(err);
    }
}

fn make_lmdb(merkle_path: &str) -> Box<dyn Database> {
    let ctx = LmdbContext::new(
        Path::new(merkle_path),
        INDEXES.len(),
        Some(120 * 1024 * 1024),
    )
    .map_err(|err| DatabaseError::InitError(format!("{}", err)))
    .unwrap();
    Box::new(
        LmdbDatabase::new(ctx, &INDEXES)
            .map_err(|err| DatabaseError::InitError(format!("{}", err)))
            .unwrap(),
    )
}

fn temp_db_path() -> String {
    let mut temp_dir = env::temp_dir();

    let thread_id = thread::current().id();
    temp_dir.push(format!("merkle-{:?}.lmdb", thread_id));
    temp_dir.to_str().unwrap().to_string()
}

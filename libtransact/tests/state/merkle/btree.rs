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

//! B-Tree-backed tests for the merkle state implementation.

use transact::{database::btree::BTreeDatabase, state::merkle::INDEXES};

use super::*;

fn new_btree_state_and_root() -> (MerkleState, String) {
    let btree_db = Box::new(BTreeDatabase::new(&INDEXES));
    let merkle_state = MerkleState::new(btree_db.clone());

    let merkle_db = MerkleRadixTree::new(btree_db, None)
        .expect("Could not overlay the merkle tree on the database");

    let orig_root = merkle_db.get_merkle_root();

    (merkle_state, orig_root)
}

#[test]
fn merkle_trie_empty_changes() {
    let (state, orig_root) = new_btree_state_and_root();
    test_merkle_trie_empty_changes(orig_root, state);
}

#[test]
fn merkle_trie_root_advance() {
    let btree_db = Box::new(BTreeDatabase::new(&INDEXES));
    test_merkle_trie_root_advance(btree_db);
}

#[test]
fn merkle_trie_delete() {
    let (state, orig_root) = new_btree_state_and_root();
    test_merkle_trie_delete(orig_root, state);
}

#[test]
fn merkle_trie_update() {
    let (state, orig_root) = new_btree_state_and_root();
    test_merkle_trie_update(orig_root, state);
}

#[test]
fn merkle_trie_update_same_address_space() {
    let (state, orig_root) = new_btree_state_and_root();
    test_merkle_trie_update_same_address_space(orig_root, state);
}

#[test]
fn merkle_trie_update_same_address_space_with_no_children() {
    let (state, orig_root) = new_btree_state_and_root();
    test_merkle_trie_update_same_address_space_with_no_children(orig_root, state);
}

#[test]
fn merkle_trie_prune_parent() {
    let (state, orig_root) = new_btree_state_and_root();
    test_merkle_trie_prune_parent(orig_root, state);
}

#[test]
fn merkle_trie_pruning_parent() {
    let btree_db = Box::new(BTreeDatabase::new(&INDEXES));
    test_merkle_trie_pruning_parent(btree_db);
}

#[test]
fn merkle_trie_prune_successors() {
    let (state, orig_root) = new_btree_state_and_root();
    test_merkle_trie_prune_successors(orig_root, state);
}

#[test]
fn merkle_trie_pruning_successors() {
    let btree_db = Box::new(BTreeDatabase::new(&INDEXES));
    test_merkle_trie_pruning_successors(btree_db);
}

#[test]
fn merkle_trie_prune_duplicate_leaves() {
    let (state, orig_root) = new_btree_state_and_root();
    test_merkle_trie_prune_duplicate_leaves(orig_root, state);
}

#[test]
fn merkle_trie_pruning_duplicate_leaves() {
    let btree_db = Box::new(BTreeDatabase::new(&INDEXES));
    test_merkle_trie_pruning_duplicate_leaves(btree_db);
}

#[test]
fn merkle_trie_pruning_successor_duplicate_leaves() {
    let btree_db = Box::new(BTreeDatabase::new(&INDEXES));
    test_merkle_trie_pruning_successor_duplicate_leaves(btree_db);
}

#[test]
fn merkle_trie_prune_successor_duplicate_leaves() {
    let (state, orig_root) = new_btree_state_and_root();
    test_merkle_trie_prune_successor_duplicate_leaves(orig_root, state);
}

#[test]
fn leaf_iteration() {
    let (state, orig_root) = new_btree_state_and_root();
    test_leaf_iteration(orig_root, state);
}

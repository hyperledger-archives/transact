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

use std::iter;
use std::panic;

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use redis;

use transact::{
    database::{btree::BTreeDatabase, redis::RedisDatabase},
    state::merkle::INDEXES,
};

use super::*;

const DEFAULT_REDIS_URL: &str = "redis://localhost:6379/";

fn new_redis_state_and_root(redis_url: &str, primary: &str) -> (MerkleState, String) {
    let db = Box::new(
        RedisDatabase::new(redis_url, primary.to_string(), &INDEXES)
            .expect("Unable to create redis database"),
    );
    let merkle_state = MerkleState::new(db.clone());

    let merkle_db =
        MerkleRadixTree::new(db, None).expect("Could not overlay the merkle tree on the database");

    let orig_root = merkle_db.get_merkle_root();

    (merkle_state, orig_root)
}

#[test]
fn merkle_trie_empty_changes() {
    run_test(|redis_url, primary| {
        let (state, orig_root) = new_redis_state_and_root(redis_url, primary);
        test_merkle_trie_empty_changes(orig_root, state);
    })
}

#[test]
fn merkle_trie_root_advance() {
    run_test(|redis_url, primary| {
        let db = Box::new(
            RedisDatabase::new(redis_url, primary.to_string(), &INDEXES)
                .expect("Unable to create redis database"),
        );
        test_merkle_trie_root_advance(db);
    })
}

#[test]
fn merkle_trie_delete() {
    run_test(|redis_url, primary| {
        let (state, orig_root) = new_redis_state_and_root(redis_url, primary);
        test_merkle_trie_delete(orig_root, state);
    })
}

#[test]
fn merkle_trie_update() {
    run_test(|redis_url, primary| {
        let (state, orig_root) = new_redis_state_and_root(redis_url, primary);
        test_merkle_trie_update(orig_root, state);
    })
}

#[test]
fn merkle_trie_update_same_address_space() {
    run_test(|redis_url, primary| {
        let (state, orig_root) = new_redis_state_and_root(redis_url, primary);
        test_merkle_trie_update_same_address_space(orig_root, state);
    })
}

#[test]
fn merkle_trie_update_same_address_space_with_no_children() {
    run_test(|redis_url, primary| {
        let (state, orig_root) = new_redis_state_and_root(redis_url, primary);
        test_merkle_trie_update_same_address_space_with_no_children(orig_root, state);
    })
}

#[test]
fn merkle_trie_pruning_parent() {
    run_test(|redis_url, primary| {
        let db = Box::new(
            RedisDatabase::new(redis_url, primary.to_string(), &INDEXES)
                .expect("Unable to create redis database"),
        );
        test_merkle_trie_pruning_parent(db);
    })
}

#[test]
fn merkle_trie_pruning_successors() {
    run_test(|redis_url, primary| {
        let db = Box::new(
            RedisDatabase::new(redis_url, primary.to_string(), &INDEXES)
                .expect("Unable to create redis database"),
        );
        test_merkle_trie_pruning_successors(db);
    })
}

#[test]
fn merkle_trie_pruning_duplicate_leaves() {
    run_test(|redis_url, primary| {
        let db = Box::new(
            RedisDatabase::new(redis_url, primary.to_string(), &INDEXES)
                .expect("Unable to create redis database"),
        );
        test_merkle_trie_pruning_duplicate_leaves(db);
    })
}

#[test]
fn merkle_trie_pruning_successor_duplicate_leaves() {
    run_test(|redis_url, primary| {
        let db = Box::new(
            RedisDatabase::new(redis_url, primary.to_string(), &INDEXES)
                .expect("Unable to create redis database"),
        );
        test_merkle_trie_pruning_successor_duplicate_leaves(db);
    })
}
///
/// Verifies that a state tree backed by redis and btree give the same root hashes
#[test]
fn redis_btree_comparison() {
    run_test(|redis_url, primary| {
        let redis_db = Box::new(
            RedisDatabase::new(redis_url, primary.to_string(), &INDEXES)
                .expect("Unable to create redis database"),
        );
        let btree_db = Box::new(BTreeDatabase::new(&INDEXES));

        test_same_results(redis_db, btree_db);
    });
    run_test(|redis_url, primary| {
        let redis_db = Box::new(
            RedisDatabase::new(redis_url, primary.to_string(), &INDEXES)
                .expect("Unable to create redis database"),
        );
        let btree_db = Box::new(BTreeDatabase::new(&INDEXES));

        test_same_results(btree_db, redis_db);
    });
}

fn run_test<T>(test: T) -> ()
where
    T: FnOnce(&str, &str) -> () + panic::UnwindSafe,
{
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_string());
    let mut rng = thread_rng();
    let db_name = String::from("test-db-")
        + &iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(7)
            .collect::<String>();

    let test_redis_url = redis_url.clone();
    let test_db_name = db_name.clone();
    let result = panic::catch_unwind(move || test(&test_redis_url, &test_db_name));

    clean_up(&redis_url, &db_name).unwrap();

    assert!(result.is_ok())
}

fn clean_up(redis_url: &str, db_name: &str) -> Result<(), String> {
    let client = redis::Client::open(redis_url).map_err(|e| e.to_string())?;
    let mut con = client.get_connection().map_err(|e| e.to_string())?;

    redis::pipe()
        .atomic()
        .del(db_name)
        .ignore()
        .del(
            INDEXES
                .iter()
                .map(|s| format!("{}_{}", db_name, s))
                .collect::<Vec<_>>(),
        )
        .ignore()
        .query(&mut con)
        .map_err(|e| e.to_string())?;

    Ok(())
}

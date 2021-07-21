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

mod btree;
#[cfg(feature = "database-lmdb")]
mod lmdb;
#[cfg(feature = "state-merkle-redis-db-tests")]
mod redisdb;
#[cfg(feature = "state-merkle-sql-postgres-tests")]
mod sql_postgres;
#[cfg(all(feature = "state-merkle-sql", feature = "sqlite"))]
mod sql_sqlite;
#[cfg(feature = "database-sqlite")]
mod sqlitedb;

use std::collections::HashMap;
use std::str::from_utf8;

use rand::seq::IteratorRandom;
use rand::thread_rng;

#[cfg(feature = "state-merkle-leaf-reader")]
use transact::state::merkle::MerkleRadixLeafReader;
use transact::{
    database::{error::DatabaseError, Database},
    protos::merkle::ChangeLogEntry,
    state::{
        merkle::{MerkleRadixTree, MerkleState, CHANGE_LOG_INDEX},
        Prune, Read, StateChange, StateReadError, Write,
    },
};

/// 1. Compute the state root hash for an empty change list to the original root
/// 2. Validate the computed root is the same as the original root
fn test_merkle_trie_empty_changes<M>(initial_state_root: String, merkle_state: M)
where
    M: Read<StateId = String, Key = String, Value = Vec<u8>>
        + Write<StateId = String, Key = String, Value = Vec<u8>>,
{
    let new_state_id = merkle_state
        .compute_state_id(&initial_state_root, &[])
        .expect("Did not supply a state id");

    assert_eq!(initial_state_root, new_state_id);
}

/// 1. Layer a MerkleRadixTree over the given database
/// 2. Apply a Set state change to the original root
/// 3. Validate that the state has not changed under the original root
/// 4. Validate that the change log entries have been set
/// 5. Set the current root
/// 6. Validate that the value set is in the trie
fn test_merkle_trie_root_advance(db: Box<dyn Database>) {
    let merkle_state = MerkleState::new(db.clone());
    let mut merkle_db = MerkleRadixTree::new(db.clone(), None)
        .expect("Could not overlay the merkle tree on the database");

    let orig_root = merkle_db.get_merkle_root();
    let orig_root_bytes = &::hex::decode(orig_root.clone()).unwrap();

    {
        // check that there is no ChangeLogEntry for the initial root
        let reader = db.get_reader().unwrap();
        assert!(reader
            .index_get(CHANGE_LOG_INDEX, orig_root_bytes)
            .expect("A database error occurred")
            .is_none());
    }

    let state_change = StateChange::Set {
        key: "abcd".to_string(),
        value: "data_value".as_bytes().to_vec(),
    };
    let new_root = merkle_state.commit(&orig_root, &[state_change]).unwrap();
    let new_root_bytes = &::hex::decode(new_root.clone()).unwrap();

    assert_eq!(merkle_db.get_merkle_root(), orig_root, "Incorrect root");
    assert_ne!(orig_root, new_root, "root was not changed");

    let change_log: ChangeLogEntry = {
        // check that we have a change log entry for the new root
        let reader = db.get_reader().unwrap();
        let entry_bytes = &reader
            .index_get(CHANGE_LOG_INDEX, new_root_bytes)
            .expect("A database error occurred")
            .expect("Did not return a change log entry");
        protobuf::Message::parse_from_bytes(entry_bytes).expect("Failed to parse change log entry")
    };

    assert_eq!(orig_root_bytes, &change_log.parent);
    assert_eq!(3, change_log.additions.len());
    assert_eq!(0, change_log.successors.len());

    merkle_db.set_merkle_root(new_root.clone()).unwrap();
    assert_eq!(merkle_db.get_merkle_root(), new_root, "Incorrect root");

    assert_value_at_address(&merkle_db, "abcd", "data_value");
}

/// 1. Layer a MerkleRadixTree over the given database
/// 2. Commit a Set state change to the original root
/// 3. Set the merkle root to the committed root and validate that the value exists
/// 4. Commit a Delete state change against the previous state root
/// 5. Check that the value still exists under the previous state root
/// 6. Set the merkle root to the committed root and validate that the value has been deleted
fn test_merkle_trie_delete<M>(initial_state_root: String, merkle_state: M)
where
    M: Read<StateId = String, Key = String, Value = Vec<u8>>
        + Write<StateId = String, Key = String, Value = Vec<u8>>,
{
    let state_change_set = StateChange::Set {
        key: "1234".to_string(),
        value: "deletable".as_bytes().to_vec(),
    };

    let new_root = merkle_state
        .commit(&initial_state_root, &[state_change_set])
        .unwrap();
    assert_read_value_at_address(&merkle_state, &new_root, "1234", Some("deletable"));

    let state_change_del_1 = StateChange::Delete {
        key: "barf".to_string(),
    };

    // deleting an unknown key should return an error
    assert!(merkle_state
        .commit(&new_root, &[state_change_del_1])
        .is_err());

    let state_change_del_2 = StateChange::Delete {
        key: "1234".to_string(),
    };

    let del_root = merkle_state
        .commit(&new_root, &[state_change_del_2])
        .unwrap();

    assert_read_value_at_address(&merkle_state, &new_root, "1234", Some("deletable"));
    assert_read_value_at_address(&merkle_state, &del_root, "1234", None);
}

fn test_merkle_trie_update<M>(initial_state_root: String, merkle_state: M)
where
    M: Read<StateId = String, Key = String, Value = Vec<u8>>
        + Write<StateId = String, Key = String, Value = Vec<u8>>,
{
    let key_hashes = (0..1000)
        .map(|i| {
            let key = format!("{:016x}", i);
            let hash = hex_hash(key.as_bytes());
            (key, hash)
        })
        .collect::<Vec<_>>();

    let mut values = HashMap::new();
    let mut new_root = initial_state_root.clone();
    for &(ref key, ref hashed) in key_hashes.iter() {
        let state_change_set = StateChange::Set {
            key: hashed.to_string(),
            value: key.as_bytes().to_vec(),
        };
        new_root = merkle_state.commit(&new_root, &[state_change_set]).unwrap();
        values.insert(hashed.clone(), key.to_string());
    }

    let mut rng = thread_rng();
    let mut state_changes = vec![];
    // Perform some updates on the lower keys
    for i in (0..500_u32).choose_multiple(&mut rng, 50) {
        let hash_key = hex_hash(format!("{:016x}", i).as_bytes());
        state_changes.push(StateChange::Set {
            key: hash_key.clone(),
            value: "5.0".as_bytes().to_vec(),
        });
        values.insert(hash_key.clone(), "5.0".to_string());
    }

    let mut delete_items = vec![];
    // perform some deletions on the upper keys
    for i in (500..1000_u32).choose_multiple(&mut rng, 50) {
        let hash = hex_hash(format!("{:016x}", i).as_bytes());
        delete_items.push(StateChange::Delete { key: hash.clone() });
        values.remove(&hash);
    }

    state_changes.extend_from_slice(&delete_items);

    let virtual_root = merkle_state
        .compute_state_id(&new_root, &state_changes)
        .unwrap();

    // virtual root shouldn't match actual contents of tree
    let res = merkle_state.get(&virtual_root, &[state_changes[0].key().to_string()]);
    assert!(
        matches!(res, Err(StateReadError::InvalidStateId(_))),
        "expected {:?} but was {:?}",
        StateReadError::InvalidStateId(virtual_root.clone()),
        res
    );

    let actual_root = merkle_state.commit(&new_root, &state_changes).unwrap();

    // the virtual root should be the same as the actual root
    assert_eq!(virtual_root, actual_root);

    for (address, value) in values {
        assert_read_value_at_address(&merkle_state, &actual_root, &address, Some(&value));
    }

    for delete_change in delete_items {
        assert_read_value_at_address(&merkle_state, &actual_root, delete_change.key(), None);
    }
}

/// This test is similar to the update test except that it will ensure that
/// there are no index errors in path_map within update function in case
/// there are addresses within set_items & delete_items which have a common
/// prefix (of any length).
///
/// A Merkle trie is created with some initial values which is then updated
/// (set & delete).
fn test_merkle_trie_update_same_address_space<M>(initial_state_root: String, merkle_state: M)
where
    M: Read<StateId = String, Key = String, Value = Vec<u8>>
        + Write<StateId = String, Key = String, Value = Vec<u8>>,
{
    let key_hashes = vec![
        // matching prefix e55420
        (
            "asdfg",
            "e5542002d3e2892516fa461cde69e05880609fbad3d38ab69435a189e126de672b620c",
        ),
        (
            "qwert",
            "c946ee72d38b8c51328f1a5f31eb5bd3300362ad0ca69dab54eff996775c7069216bda",
        ),
        (
            "zxcvb",
            "487a6a63c71c9b7b63146ef68858e5d010b4978fd70dda0404d4fad5e298ccc9a560eb",
        ),
        // matching prefix e55420
        (
            "yuiop",
            "e55420c026596ee643e26fd93927249ea28fb5f359ddbd18bc02562dc7e8dbc93e89b9",
        ),
        (
            "hjklk",
            "cc1370ce67aa16c89721ee947e9733b2a3d2460db5b0ea6410288f426ad8d8040ea641",
        ),
        (
            "bnmvc",
            "d07e69664286712c3d268ca71464f2b3b2604346f833106f3e0f6a72276e57a16f3e0f",
        ),
    ];
    let mut values = HashMap::new();
    let mut new_root = initial_state_root.clone();
    for &(ref key, ref hashed) in key_hashes.iter() {
        let state_change_set = StateChange::Set {
            key: hashed.to_string(),
            value: key.as_bytes().to_vec(),
        };
        new_root = merkle_state.commit(&new_root, &[state_change_set]).unwrap();
        values.insert(hashed.to_string(), key.to_string());
    }

    assert_ne!(initial_state_root, new_root);
    let mut state_changes = vec![];
    // Perform some updates on the lower keys
    for &(_, ref key_hash) in key_hashes.iter() {
        state_changes.push(StateChange::Set {
            key: key_hash.to_string(),
            value: "2.0".as_bytes().to_vec(),
        });
        values.insert(key_hash.clone().to_string(), "2.0".to_string());
    }

    // The first item below(e55420...89b9) shares a common prefix
    // with the first in set_items(e55420...620c)
    let delete_items = vec![
        StateChange::Delete {
            key: "e55420c026596ee643e26fd93927249ea28fb5f359ddbd18bc02562dc7e8dbc93e89b9"
                .to_string(),
        },
        StateChange::Delete {
            key: "cc1370ce67aa16c89721ee947e9733b2a3d2460db5b0ea6410288f426ad8d8040ea641"
                .to_string(),
        },
        StateChange::Delete {
            key: "d07e69664286712c3d268ca71464f2b3b2604346f833106f3e0f6a72276e57a16f3e0f"
                .to_string(),
        },
    ];

    for delete_change in delete_items.iter() {
        match delete_change {
            StateChange::Delete { key } => {
                values.remove(key);
            }
            _ => (),
        }
    }

    state_changes.extend_from_slice(&delete_items);

    let virtual_root = merkle_state
        .compute_state_id(&new_root, &state_changes)
        .unwrap();
    // virtual root shouldn't match actual contents of tree
    let res = merkle_state.get(&virtual_root, &[state_changes[0].key().to_string()]);
    assert!(
        matches!(res, Err(StateReadError::InvalidStateId(_))),
        "expected {:?} but was {:?}",
        StateReadError::InvalidStateId(virtual_root.clone()),
        res
    );

    let actual_root = merkle_state.commit(&new_root, &state_changes).unwrap();
    // the virtual root should be the same as the actual root
    assert_eq!(virtual_root, actual_root);

    for (address, value) in values {
        assert_read_value_at_address(&merkle_state, &actual_root, &address, Some(&value));
    }

    for delete_change in delete_items {
        assert_read_value_at_address(&merkle_state, &actual_root, delete_change.key(), None);
    }
}

/// This test is similar to the update_same_address_space except that it will ensure that
/// there are no index errors in path_map within update function in case
/// there are addresses within set_items & delete_items which have a common
/// prefix (of any length), when trie doesn't have children to the parent node getting deleted.
///
/// A Merkle trie is created with some initial values which is then updated
/// (set & delete).
fn test_merkle_trie_update_same_address_space_with_no_children<M>(
    initial_state_root: String,
    merkle_state: M,
) where
    M: Read<StateId = String, Key = String, Value = Vec<u8>>
        + Write<StateId = String, Key = String, Value = Vec<u8>>,
{
    let key_hashes = vec![
        (
            "qwert",
            "c946ee72d38b8c51328f1a5f31eb5bd3300362ad0ca69dab54eff996775c7069216bda",
        ),
        (
            "zxcvb",
            "487a6a63c71c9b7b63146ef68858e5d010b4978fd70dda0404d4fad5e298ccc9a560eb",
        ),
        // matching prefix e55420, this will be deleted
        (
            "yuiop",
            "e55420c026596ee643e26fd93927249ea28fb5f359ddbd18bc02562dc7e8dbc93e89b9",
        ),
        (
            "hjklk",
            "cc1370ce67aa16c89721ee947e9733b2a3d2460db5b0ea6410288f426ad8d8040ea641",
        ),
        (
            "bnmvc",
            "d07e69664286712c3d268ca71464f2b3b2604346f833106f3e0f6a72276e57a16f3e0f",
        ),
    ];
    let mut values = HashMap::new();
    let mut new_root = initial_state_root.clone();
    for &(ref key, ref hashed) in key_hashes.iter() {
        let state_change_set = StateChange::Set {
            key: hashed.to_string(),
            value: key.as_bytes().to_vec(),
        };
        new_root = merkle_state.commit(&new_root, &[state_change_set]).unwrap();
        values.insert(hashed.to_string(), key.to_string());
    }

    assert_ne!(initial_state_root, new_root);

    // matching prefix e55420, however this will be newly added and not set already in trie
    let key_hash_to_be_inserted = vec![(
        "asdfg",
        "e5542002d3e2892516fa461cde69e05880609fbad3d38ab69435a189e126de672b620c",
    )];

    let mut state_changes = vec![];
    // Perform some updates on the lower keys
    for &(_, ref key_hash) in key_hash_to_be_inserted.iter() {
        state_changes.push(StateChange::Set {
            key: key_hash.clone().to_string(),
            value: "2.0".as_bytes().to_vec(),
        });
        values.insert(key_hash.clone().to_string(), "2.0".to_string());
    }

    // The first item below(e55420...89b9) shares a common prefix
    // with the first in set_items(e55420...620c)
    let delete_items = vec![
        StateChange::Delete {
            key: "e55420c026596ee643e26fd93927249ea28fb5f359ddbd18bc02562dc7e8dbc93e89b9"
                .to_string(),
        },
        StateChange::Delete {
            key: "cc1370ce67aa16c89721ee947e9733b2a3d2460db5b0ea6410288f426ad8d8040ea641"
                .to_string(),
        },
        StateChange::Delete {
            key: "d07e69664286712c3d268ca71464f2b3b2604346f833106f3e0f6a72276e57a16f3e0f"
                .to_string(),
        },
    ];

    for delete_change in delete_items.iter() {
        match delete_change {
            StateChange::Delete { key } => {
                values.remove(key);
            }
            _ => (),
        }
    }

    state_changes.extend_from_slice(&delete_items);

    let virtual_root = merkle_state
        .compute_state_id(&new_root, &state_changes)
        .unwrap();

    // virtual root shouldn't match actual contents of tree
    let res = merkle_state.get(&virtual_root, &[state_changes[0].key().to_string()]);
    assert!(
        matches!(res, Err(StateReadError::InvalidStateId(_))),
        "expected {:?} but was {:?}",
        StateReadError::InvalidStateId(virtual_root.clone()),
        res
    );

    let actual_root = merkle_state.commit(&new_root, &state_changes).unwrap();

    // the virtual root should be the same as the actual root
    assert_eq!(virtual_root, actual_root);

    for (address, value) in values {
        assert_read_value_at_address(&merkle_state, &actual_root, &address, Some(&value));
    }

    for delete_change in delete_items {
        assert_read_value_at_address(&merkle_state, &actual_root, delete_change.key(), None);
    }
}

/// This test creates a merkle trie with multiple entries, and produces a
/// second trie based on the first where an entry is change.
///
/// - It verifies that both tries have a ChangeLogEntry
/// - Prunes the parent trie
/// - Verifies that the nodes written are gone
/// - verifies that the parent trie's ChangeLogEntry is deleted
fn test_merkle_trie_pruning_parent(db: Box<dyn Database>) {
    let merkle_state = MerkleState::new(db.clone());
    let mut merkle_db = MerkleRadixTree::new(db.clone(), None).expect("No db errors");
    let mut updates: Vec<StateChange> = Vec::with_capacity(3);

    updates.push(StateChange::Set {
        key: "ab0000".to_string(),
        value: "0001".as_bytes().to_vec(),
    });
    updates.push(StateChange::Set {
        key: "ab0a01".to_string(),
        value: "0002".as_bytes().to_vec(),
    });
    updates.push(StateChange::Set {
        key: "abff00".to_string(),
        value: "0003".as_bytes().to_vec(),
    });

    let parent_root = merkle_state
        .commit(&merkle_db.get_merkle_root(), &updates)
        .expect("Update failed to work");
    merkle_db.set_merkle_root(parent_root.clone()).unwrap();

    let parent_root_bytes = ::hex::decode(parent_root.clone()).expect("Proper hex");
    // check that we have a change log entry for the new root
    let mut parent_change_log = expect_change_log(&*db, &parent_root_bytes);
    assert!(parent_change_log.successors.is_empty());

    assert_value_at_address(&merkle_db, "ab0000", "0001");
    assert_value_at_address(&merkle_db, "ab0a01", "0002");
    assert_value_at_address(&merkle_db, "abff00", "0003");

    let successor_root = merkle_state
        .commit(
            &parent_root,
            &[StateChange::Set {
                key: "ab0000".to_string(),
                value: "test".as_bytes().to_vec(),
            }],
        )
        .expect("Set failed to work");
    let successor_root_bytes = ::hex::decode(successor_root.clone()).expect("proper hex");

    // Load the parent change log after the change.
    parent_change_log = expect_change_log(&*db, &parent_root_bytes);
    let successor_change_log = expect_change_log(&*db, &successor_root_bytes);

    assert_has_successors(&parent_change_log, &[&successor_root_bytes]);
    assert_eq!(parent_root_bytes, successor_change_log.parent);

    merkle_db
        .set_merkle_root(successor_root)
        .expect("Unable to apply the new merkle root");

    let mut deletions = parent_change_log
        .successors
        .first()
        .unwrap()
        .deletions
        .clone();
    deletions.push(parent_root_bytes.clone());

    assert_eq!(
        deletions.len(),
        merkle_state
            .prune(vec!(parent_root.clone()))
            .expect("Prune should have no errors")
            .len()
    );
    {
        let reader = db.get_reader().unwrap();
        for deletion in parent_change_log
            .get_successors()
            .to_vec()
            .first()
            .unwrap()
            .get_deletions()
            .to_vec()
        {
            assert!(reader
                .get(&deletion)
                .expect("Could not query for deletion")
                .is_none());
        }

        assert!(reader
            .index_get(CHANGE_LOG_INDEX, &parent_root_bytes)
            .expect("DB query should succeed")
            .is_none());
    }
    assert!(merkle_db.set_merkle_root(parent_root).is_err());
}

/// This test creates a merkle trie with multiple entries and produces two
/// distinct successor tries from that first.
///
/// - it verifies that all the tries have a ChangeLogEntry
/// - it prunes one of the successors
/// - it verifies the nodes from that successor are removed
/// - it verifies that the pruned successor's ChangeLogEntry is removed
/// - it verifies the original and the remaining successor still are
///   persisted
fn test_merkle_trie_pruning_successors(db: Box<dyn Database>) {
    let merkle_state = MerkleState::new(db.clone());
    let mut merkle_db = MerkleRadixTree::new(db.clone(), None).expect("No db errors");
    let mut updates: Vec<StateChange> = Vec::with_capacity(3);

    updates.push(StateChange::Set {
        key: "ab0000".to_string(),
        value: "0001".as_bytes().to_vec(),
    });
    updates.push(StateChange::Set {
        key: "ab0a01".to_string(),
        value: "0002".as_bytes().to_vec(),
    });
    updates.push(StateChange::Set {
        key: "abff00".to_string(),
        value: "0003".as_bytes().to_vec(),
    });

    let parent_root = merkle_state
        .commit(&merkle_db.get_merkle_root(), &updates)
        .expect("Update failed to work");
    let parent_root_bytes = ::hex::decode(parent_root.clone()).expect("Proper hex");

    merkle_db.set_merkle_root(parent_root.clone()).unwrap();
    assert_value_at_address(&merkle_db, "ab0000", "0001");
    assert_value_at_address(&merkle_db, "ab0a01", "0002");
    assert_value_at_address(&merkle_db, "abff00", "0003");

    let successor_root_left = merkle_state
        .commit(
            &parent_root,
            &[StateChange::Set {
                key: "ab0000".to_string(),
                value: "left".as_bytes().to_vec(),
            }],
        )
        .expect("Set failed to work");

    let successor_root_left_bytes = ::hex::decode(successor_root_left.clone()).expect("proper hex");

    let successor_root_right = merkle_state
        .commit(
            &parent_root,
            &[StateChange::Set {
                key: "ab0a01".to_string(),
                value: "right".as_bytes().to_vec(),
            }],
        )
        .expect("Set failed to work");

    let successor_root_right_bytes =
        ::hex::decode(successor_root_right.clone()).expect("proper hex");

    let mut parent_change_log = expect_change_log(&*db, &parent_root_bytes);
    let successor_left_change_log = expect_change_log(&*db, &successor_root_left_bytes);
    expect_change_log(&*db, &successor_root_right_bytes);

    assert_has_successors(
        &parent_change_log,
        &[&successor_root_left_bytes, &successor_root_right_bytes],
    );

    // Let's prune the left successor:
    let res = merkle_state
        .prune(vec![successor_root_left.clone()])
        .expect("Prune should have no errors");
    assert_eq!(successor_left_change_log.additions.len(), res.len());

    parent_change_log = expect_change_log(&*db, &parent_root_bytes);
    assert_has_successors(&parent_change_log, &[&successor_root_right_bytes]);

    assert!(merkle_db.set_merkle_root(successor_root_left).is_err());
}

/// This test creates a merkle trie with multiple entries and produces a
/// successor with duplicate That changes one new leaf, followed by a second
/// successor that produces a leaf with the same hash.  When the pruning the
/// initial root, the duplicate leaf node is not pruned as well.
fn test_merkle_trie_pruning_duplicate_leaves(db: Box<dyn Database>) {
    let merkle_state = MerkleState::new(db.clone());
    let mut merkle_db = MerkleRadixTree::new(db.clone(), None).expect("No db errors");
    let mut updates: Vec<StateChange> = Vec::with_capacity(3);
    updates.push(StateChange::Set {
        key: "ab0000".to_string(),
        value: "0001".as_bytes().to_vec(),
    });
    updates.push(StateChange::Set {
        key: "ab0a01".to_string(),
        value: "0002".as_bytes().to_vec(),
    });
    updates.push(StateChange::Set {
        key: "abff00".to_string(),
        value: "0003".as_bytes().to_vec(),
    });

    let parent_root = merkle_state
        .commit(&merkle_db.get_merkle_root(), &updates)
        .expect("Update failed to work");
    let parent_root_bytes = ::hex::decode(parent_root.clone()).expect("Proper hex");

    // create the middle root
    merkle_db.set_merkle_root(parent_root.clone()).unwrap();
    updates.clear();
    updates.push(StateChange::Set {
        key: "ab0000".to_string(),
        value: "change0".as_bytes().to_vec(),
    });
    updates.push(StateChange::Set {
        key: "ab0001".to_string(),
        value: "change1".as_bytes().to_vec(),
    });

    let successor_root_middle = merkle_state
        .commit(&parent_root, &updates)
        .expect("Update failed to work");

    // Set the value back to the original
    let successor_root_last = merkle_state
        .commit(
            &successor_root_middle,
            &[StateChange::Set {
                key: "ab0000".to_string(),
                value: "0001".as_bytes().to_vec(),
            }],
        )
        .expect("Set failed to work");

    merkle_db.set_merkle_root(successor_root_last).unwrap();
    let parent_change_log = expect_change_log(&*db, &parent_root_bytes);
    assert_eq!(
        parent_change_log
            .successors
            .clone()
            .first()
            .unwrap()
            .deletions
            .len(),
        merkle_state
            .prune(vec!(parent_root.clone()))
            .expect("Prune should have no errors")
            .len()
    );

    assert_value_at_address(&merkle_db, "ab0000", "0001");
}

/// This test creates a merkle trie with multiple entries and produces a
/// successor with duplicate That changes one new leaf, followed by a second
/// successor that produces a leaf with the same hash.  When the pruning the
/// last root, the duplicate leaf node is not pruned as well.
fn test_merkle_trie_pruning_successor_duplicate_leaves(db: Box<dyn Database>) {
    let merkle_state = MerkleState::new(db.clone());
    let mut merkle_db = MerkleRadixTree::new(db.clone(), None).expect("No db errors");
    let mut updates: Vec<StateChange> = Vec::with_capacity(3);

    updates.push(StateChange::Set {
        key: "ab0000".to_string(),
        value: "0001".as_bytes().to_vec(),
    });
    updates.push(StateChange::Set {
        key: "ab0a01".to_string(),
        value: "0002".as_bytes().to_vec(),
    });
    updates.push(StateChange::Set {
        key: "abff00".to_string(),
        value: "0003".as_bytes().to_vec(),
    });

    let parent_root = merkle_state
        .commit(&merkle_db.get_merkle_root(), &updates)
        .expect("Update failed to work");

    updates.clear();
    updates.push(StateChange::Set {
        key: "ab0000".to_string(),
        value: "change0".as_bytes().to_vec(),
    });
    updates.push(StateChange::Set {
        key: "ab0001".to_string(),
        value: "change1".as_bytes().to_vec(),
    });
    let successor_root_middle = merkle_state
        .commit(&parent_root, &updates)
        .expect("Update failed to work");

    // Set the value back to the original
    let successor_root_last = merkle_state
        .commit(
            &successor_root_middle,
            &[StateChange::Set {
                key: "ab0000".to_string(),
                value: "0001".as_bytes().to_vec(),
            }],
        )
        .expect("Set failed to work");
    let successor_root_bytes = ::hex::decode(successor_root_last.clone()).expect("Proper hex");

    // set back to the parent root
    merkle_db.set_merkle_root(parent_root).unwrap();

    let last_change_log = expect_change_log(&*db, &successor_root_bytes);
    assert_eq!(
        last_change_log.additions.len() - 1,
        merkle_state
            .prune(vec!(successor_root_last.clone()))
            .expect("Prune should have no errors")
            .len()
    );

    assert_value_at_address(&merkle_db, "ab0000", "0001");
}

#[cfg(feature = "state-merkle-leaf-reader")]
/// Test iteration over leaves.
fn test_leaf_iteration<M>(initial_state_root: String, merkle_state: M)
where
    M: Write<StateId = String, Key = String, Value = Vec<u8>> + MerkleRadixLeafReader,
{
    {
        let mut leaf_iter = merkle_state.leaves(&initial_state_root, None).unwrap();
        assert!(
            leaf_iter.next().is_none(),
            "Empty tree should return no leaves"
        );
    }

    let addresses = vec!["ab0000", "aba001", "abff02"];
    let mut new_root = initial_state_root.clone();
    for (i, key) in addresses.iter().enumerate() {
        let state_change_set = StateChange::Set {
            key: key.to_string(),
            value: format!("{:04x}", i * 10).as_bytes().to_vec(),
        };
        new_root = merkle_state.commit(&new_root, &[state_change_set]).unwrap();
    }

    assert_read_value_at_address(&merkle_state, &new_root, "ab0000", Some("0000"));
    assert_read_value_at_address(&merkle_state, &new_root, "aba001", Some("000a"));
    assert_read_value_at_address(&merkle_state, &new_root, "abff02", Some("0014"));

    let mut leaf_iter = merkle_state.leaves(&new_root, None).unwrap();

    assert_eq!(
        ("ab0000".into(), "0000".as_bytes().to_vec()),
        leaf_iter.next().unwrap().unwrap()
    );
    assert_eq!(
        ("aba001".into(), "000a".as_bytes().to_vec()),
        leaf_iter.next().unwrap().unwrap()
    );
    assert_eq!(
        ("abff02".into(), "0014".as_bytes().to_vec()),
        leaf_iter.next().unwrap().unwrap()
    );
    assert!(leaf_iter.next().is_none(), "Iterator should be Exhausted");

    // test that we can start from an prefix:
    let mut leaf_iter = merkle_state.leaves(&new_root, Some("abff")).unwrap();
    assert_eq!(
        ("abff02".into(), "0014".as_bytes().to_vec()),
        leaf_iter.next().unwrap().unwrap()
    );
    assert!(leaf_iter.next().is_none(), "Iterator should be Exhausted");
}

/// Check that two database implementations will produce the same results when overlayed by a
/// MerkleRadixTree.
///
/// 1. Perform set operations and verify the same result root
/// 2. Perform a delete operation and verify the same result root
/// 3. Perform a prune operation and verify the same set of entries removed.
fn test_same_results(left: Box<dyn Database>, right: Box<dyn Database>) {
    let mut merkle_left = MerkleRadixTree::new(left.clone(), None).unwrap();
    let mut merkle_right = MerkleRadixTree::new(right.clone(), None).unwrap();

    let mut updates: Vec<StateChange> = Vec::with_capacity(3);

    updates.push(StateChange::Set {
        key: "ab0000".to_string(),
        value: "0001".as_bytes().to_vec(),
    });
    updates.push(StateChange::Set {
        key: "ab0a01".to_string(),
        value: "0002".as_bytes().to_vec(),
    });
    updates.push(StateChange::Set {
        key: "abff00".to_string(),
        value: "0003".as_bytes().to_vec(),
    });
    updates.push(StateChange::Set {
        key: "abff01".to_string(),
        value: "0004".as_bytes().to_vec(),
    });

    let merkle_left_root = merkle_left.update(&updates, false).unwrap();
    let merkle_right_root = merkle_right.update(&updates, false).unwrap();

    assert_eq!(merkle_left_root, merkle_right_root);

    merkle_left
        .set_merkle_root(merkle_left_root.clone())
        .unwrap();
    merkle_right
        .set_merkle_root(merkle_right_root.clone())
        .unwrap();

    let state_change_delete = StateChange::Delete {
        key: "abff01".to_string(),
    };

    let merkle_left_root_del = merkle_left
        .update(&[state_change_delete.clone()], false)
        .unwrap();
    let merkle_right_root_del = merkle_right
        .update(&[state_change_delete.clone()], false)
        .unwrap();

    assert_eq!(merkle_left_root_del, merkle_right_root_del);

    let mut prune_result_left =
        MerkleRadixTree::prune(&*left, &merkle_left_root).expect("Prune should have no errors");

    let mut prune_result_right =
        MerkleRadixTree::prune(&*right, &merkle_right_root).expect("Prune should have no errors");

    assert_eq!(
        prune_result_left.sort_unstable(),
        prune_result_right.sort_unstable()
    );
}

/// Check that two merkle state implementations will produce the same results.
///
/// 1. Perform set operations and verify the same result root
/// 2. Perform a delete operation and verify the same result root
#[cfg(all(feature = "state-merkle-sql", feature = "sqlite"))]
fn test_produce_same_state<L, R>(
    left_initial_state_root: String,
    left: L,
    right_initial_state_root: String,
    right: R,
) where
    L: Read<StateId = String, Key = String, Value = Vec<u8>>
        + Write<StateId = String, Key = String, Value = Vec<u8>>,
    R: Read<StateId = String, Key = String, Value = Vec<u8>>
        + Write<StateId = String, Key = String, Value = Vec<u8>>,
{
    assert_eq!(
        left_initial_state_root, right_initial_state_root,
        "State not starting from the same initial state root hash"
    );
    let mut updates: Vec<StateChange> = Vec::with_capacity(3);

    updates.push(StateChange::Set {
        key: "ab0000".to_string(),
        value: "0001".as_bytes().to_vec(),
    });
    updates.push(StateChange::Set {
        key: "ab0a01".to_string(),
        value: "0002".as_bytes().to_vec(),
    });
    updates.push(StateChange::Set {
        key: "abff00".to_string(),
        value: "0003".as_bytes().to_vec(),
    });
    updates.push(StateChange::Set {
        key: "abff01".to_string(),
        value: "0004".as_bytes().to_vec(),
    });

    let merkle_left_root = left.commit(&left_initial_state_root, &updates).unwrap();
    let merkle_right_root = right.commit(&right_initial_state_root, &updates).unwrap();

    assert_eq!(merkle_left_root, merkle_right_root);

    let state_change_delete = vec![StateChange::Delete {
        key: "abff01".to_string(),
    }];

    let merkle_left_root_del = left
        .commit(&merkle_left_root, &state_change_delete)
        .unwrap();
    let merkle_right_root_del = right
        .commit(&merkle_right_root, &state_change_delete)
        .unwrap();

    assert_eq!(merkle_left_root_del, merkle_right_root_del);
}

fn assert_value_at_address(merkle_db: &MerkleRadixTree, address: &str, expected_value: &str) {
    let value = merkle_db.get_value(address);
    match value {
        Ok(Some(value)) => assert_eq!(
            expected_value,
            from_utf8(&value).expect("could not convert bytes to string")
        ),
        Ok(None) => panic!("value at address {} was not found", address),
        Err(err) => panic!("value at address {} produced an error: {}", address, err),
    }
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
            Ok(values
                .remove(address)
                .map(|value| String::from_utf8(value).expect("could not convert bytes to string")))
        });

    match value {
        Ok(value) => assert_eq!(expected_value, value.as_deref()),
        Err(err) => panic!("value at address {} produced an error: {}", address, err),
    }
}

fn expect_change_log(db: &dyn Database, root_hash: &[u8]) -> ChangeLogEntry {
    let reader = db.get_reader().unwrap();
    protobuf::Message::parse_from_bytes(
        &reader
            .index_get(CHANGE_LOG_INDEX, root_hash)
            .expect("No db errors")
            .expect("A change log entry"),
    )
    .expect("The change log entry to have bytes")
}

fn assert_has_successors(change_log: &ChangeLogEntry, successor_roots: &[&[u8]]) {
    assert_eq!(successor_roots.len(), change_log.successors.len());
    for successor_root in successor_roots {
        let mut has_root = false;
        for successor in change_log.get_successors().to_vec() {
            if &successor.successor == successor_root {
                has_root = true;
                break;
            }
        }
        if !has_root {
            panic!(
                "Root {} not found in change log {:?}",
                ::hex::encode(successor_root),
                change_log
            );
        }
    }
}

fn hex_hash(input: &[u8]) -> String {
    let mut bytes: Vec<u8> = Vec::new();
    bytes.extend(openssl::sha::sha512(input).iter());
    let (hash, _rest) = bytes.split_at(bytes.len() / 2);
    hex::encode(hash.to_vec())
}

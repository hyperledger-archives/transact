/*
 * Copyright 2018 Intel Corporation
 * Copyright 2019 Bitwise IO, Inc.
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
 * ------------------------------------------------------------------------------
 */

use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Cursor;

use cbor;
use cbor::decoder::GenericDecoder;
use cbor::encoder::GenericEncoder;
use cbor::value::{Bytes, Key, Text, Value};

use openssl;

use crate::database::error::DatabaseError;
use crate::database::lmdb::{DatabaseReader, LmdbDatabase, LmdbDatabaseWriter};

use super::change_log::{ChangeLogEntry, Successor};
use super::error::{StatePruneError, StateReadError, StateWriteError};
use super::{Prune, Read, StateChange, Write};

pub use super::merkle_error::StateDatabaseError;

const TOKEN_SIZE: usize = 2;

pub const CHANGE_LOG_INDEX: &str = "change_log";
pub const DUPLICATE_LOG_INDEX: &str = "duplicate_log";
pub const INDEXES: [&str; 2] = [CHANGE_LOG_INDEX, DUPLICATE_LOG_INDEX];

type StateIter = Iterator<Item = Result<(String, Vec<u8>), StateDatabaseError>>;
type StateHash = Vec<u8>;

#[derive(Clone)]
pub struct MerkleState {
    db: LmdbDatabase,
}

impl Write for MerkleState {
    type StateId = String;
    type Key = String;
    type Value = Vec<u8>;

    fn commit(
        &self,
        state_id: &Self::StateId,
        state_changes: &[StateChange<Self::Key, Self::Value>],
    ) -> Result<Self::StateId, StateWriteError> {
        let mut merkle_tree = MerkleRadixTree::new(self.db.clone(), Some(state_id))
            .map_err(|err| StateWriteError::StorageError(Box::new(err)))?;
        merkle_tree
            .set_merkle_root(state_id.to_string())
            .map_err(|err| match err {
                StateDatabaseError::NotFound(msg) => StateWriteError::InvalidStateId(msg),
                _ => StateWriteError::StorageError(Box::new(err)),
            })?;
        merkle_tree
            .update(state_changes, false)
            .map_err(|err| StateWriteError::StorageError(Box::new(err)))
    }

    fn compute_state_id(
        &self,
        state_id: &Self::StateId,
        state_changes: &[StateChange<Self::Key, Self::Value>],
    ) -> Result<Self::StateId, StateWriteError> {
        let mut merkle_tree = MerkleRadixTree::new(self.db.clone(), Some(state_id))
            .map_err(|err| StateWriteError::StorageError(Box::new(err)))?;

        merkle_tree
            .set_merkle_root(state_id.to_string())
            .map_err(|err| match err {
                StateDatabaseError::NotFound(msg) => StateWriteError::InvalidStateId(msg),
                _ => StateWriteError::StorageError(Box::new(err)),
            })?;
        merkle_tree
            .update(state_changes, true)
            .map_err(|err| StateWriteError::StorageError(Box::new(err)))
    }
}

impl Read for MerkleState {
    type StateId = String;
    type Key = String;
    type Value = Vec<u8>;

    fn get(
        &self,
        state_id: &Self::StateId,
        keys: &[Self::Key],
    ) -> Result<HashMap<Self::Key, Self::Value>, StateReadError> {
        let mut merkle_tree = MerkleRadixTree::new(self.db.clone(), Some(state_id))
            .map_err(|err| StateReadError::StorageError(Box::new(err)))?;

        merkle_tree
            .set_merkle_root(state_id.to_string())
            .map_err(|err| match err {
                StateDatabaseError::NotFound(msg) => StateReadError::InvalidStateId(msg),
                _ => StateReadError::StorageError(Box::new(err)),
            })?;
        keys.iter().try_fold(HashMap::new(), |mut result, key| {
            let value = match merkle_tree.get_by_address(key) {
                Ok(value) => Ok(value.value),
                Err(err) => match err {
                    StateDatabaseError::NotFound(_) => Ok(None),
                    _ => Err(StateReadError::StorageError(Box::new(err))),
                },
            }?;
            if value.is_some() {
                result.insert(key.to_string(), value.unwrap());
            }
            Ok(result)
        })
    }
}

impl Prune for MerkleState {
    type StateId = String;
    type Key = String;
    type Value = Vec<u8>;

    fn prune(&self, state_ids: Vec<Self::StateId>) -> Result<Vec<Self::Key>, StatePruneError> {
        state_ids
            .iter()
            .try_fold(Vec::new(), |mut result, state_id| {
                result.extend(MerkleRadixTree::prune(&self.db, state_id).map_err(
                    |err| match err {
                        StateDatabaseError::NotFound(msg) => StatePruneError::InvalidStateId(msg),
                        _ => StatePruneError::StorageError(Box::new(err)),
                    },
                )?);
                Ok(result)
            })
    }
}

/// Merkle Database
#[derive(Clone)]
pub struct MerkleRadixTree {
    root_hash: String,
    db: LmdbDatabase,
    root_node: Node,
}

impl MerkleRadixTree {
    /// Constructs a new MerkleRadixTree, backed by a given Database
    ///
    /// An optional starting merkle root may be provided.
    pub fn new(db: LmdbDatabase, merkle_root: Option<&str>) -> Result<Self, StateDatabaseError> {
        let root_hash = merkle_root.map_or_else(|| initialize_db(&db), |s| Ok(s.into()))?;
        let root_node = get_node_by_hash(&db, &root_hash)?;

        Ok(MerkleRadixTree {
            root_hash,
            db,
            root_node,
        })
    }

    /// Prunes nodes that are no longer needed under a given state root
    /// Returns a list of addresses that were deleted
    pub fn prune(db: &LmdbDatabase, merkle_root: &str) -> Result<Vec<String>, StateDatabaseError> {
        let root_bytes = ::hex::decode(merkle_root).map_err(|_| {
            StateDatabaseError::InvalidHash(format!("{} is not a valid hash", merkle_root))
        })?;
        let mut db_writer = db.writer()?;
        let change_log = get_change_log(&db_writer, &root_bytes)?;

        if change_log.is_none() {
            // There's no change log for this entry
            return Ok(vec![]);
        }

        let mut change_log = change_log.unwrap();
        let removed_addresses = if change_log.successors.len() > 1 {
            // Currently, we don't clean up a parent with multiple successors
            vec![]
        } else if change_log.successors.is_empty() {
            // deleting the tip of a trie lineage

            let (deletion_candidates, duplicates) =
                MerkleRadixTree::remove_duplicate_hashes(&mut db_writer, change_log.additions)?;

            for hash in &deletion_candidates {
                let hash_hex = ::hex::encode(hash);
                delete_ignore_missing(&mut db_writer, hash_hex.as_bytes())?
            }

            for hash in &duplicates {
                decrement_ref_count(&mut db_writer, hash)?;
            }

            db_writer.index_delete(CHANGE_LOG_INDEX, &root_bytes)?;
            let parent_root_bytes = &change_log.parent;

            if let Some(ref mut parent_change_log) =
                get_change_log(&db_writer, parent_root_bytes)?.as_mut()
            {
                let successors = parent_change_log.take_successors();
                let new_successors = successors
                    .into_iter()
                    .filter(|successor| root_bytes != successor.successor)
                    .collect::<Vec<_>>();
                parent_change_log.successors = new_successors;

                write_change_log(&mut db_writer, parent_root_bytes, &parent_change_log)?;
            }

            deletion_candidates.into_iter().collect()
        } else {
            // deleting a parent
            let mut successor = change_log.successors.pop().unwrap();
            successor.deletions.push(root_bytes.clone());

            let (deletion_candidates, duplicates): (Vec<Vec<u8>>, Vec<Vec<u8>>) =
                MerkleRadixTree::remove_duplicate_hashes(&mut db_writer, successor.deletions)?;

            for hash in &deletion_candidates {
                let hash_hex = ::hex::encode(hash);
                delete_ignore_missing(&mut db_writer, hash_hex.as_bytes())?
            }

            for hash in &duplicates {
                decrement_ref_count(&mut db_writer, hash)?;
            }

            db_writer.index_delete(CHANGE_LOG_INDEX, &root_bytes)?;

            deletion_candidates.into_iter().collect()
        };

        db_writer.commit()?;
        Ok(removed_addresses.iter().map(::hex::encode).collect())
    }

    fn remove_duplicate_hashes(
        db_writer: &mut LmdbDatabaseWriter,
        deletions: Vec<Vec<u8>>,
    ) -> Result<(Vec<StateHash>, Vec<StateHash>), StateDatabaseError> {
        Ok(deletions.into_iter().partition(|key| {
            if let Ok(count) = get_ref_count(db_writer, &key) {
                count == 0
            } else {
                false
            }
        }))
    }
    /// Returns the current merkle root for this MerkleRadixTree
    pub fn get_merkle_root(&self) -> String {
        self.root_hash.clone()
    }

    /// Sets the current merkle root for this MerkleRadixTree
    pub fn set_merkle_root<S: Into<String>>(
        &mut self,
        merkle_root: S,
    ) -> Result<(), StateDatabaseError> {
        let new_root = merkle_root.into();
        self.root_node = get_node_by_hash(&self.db, &new_root)?;
        self.root_hash = new_root;
        Ok(())
    }

    /// Updates the tree with multiple changes.  Applies both set and deletes,
    /// as given.
    ///
    /// If the flag `is_virtual` is set, the values are not written to the
    /// underlying database.
    ///
    /// Returns a Result with the new root hash.
    pub fn update(
        &self,
        state_changes: &[StateChange<String, Vec<u8>>],
        is_virtual: bool,
    ) -> Result<String, StateDatabaseError> {
        let mut path_map = HashMap::new();

        let mut deletions = HashSet::new();
        let mut additions = HashSet::new();

        let mut delete_items = vec![];
        for state_change in state_changes {
            match state_change {
                StateChange::Set { key, value } => {
                    let tokens = tokenize_address(key);
                    let mut set_path_map = self.get_path_by_tokens(&tokens, false)?;
                    {
                        let node = set_path_map
                            .get_mut(key)
                            .expect("Path map not correctly generated");
                        node.value = Some(value.to_vec());
                    }
                    for pkey in set_path_map.keys() {
                        additions.insert(pkey.clone());
                    }
                    path_map.extend(set_path_map);
                }
                StateChange::Delete { key } => {
                    let tokens = tokenize_address(key);
                    let del_path_map = self.get_path_by_tokens(&tokens, true)?;
                    path_map.extend(del_path_map);
                    delete_items.push(key);
                }
            }
        }

        for del_address in delete_items.iter() {
            path_map.remove(*del_address);
            let (mut parent_address, mut path_branch) = parent_and_branch(del_address);
            while parent_address != "" {
                let remove_parent = {
                    let parent_node = path_map
                        .get_mut(parent_address)
                        .expect("Path map not correctly generated or entry is deleted");

                    if let Some(old_hash_key) = parent_node.children.remove(path_branch) {
                        deletions.insert(old_hash_key);
                    }

                    parent_node.children.is_empty()
                };

                // there's no children to the parent node already in the tree, remove it from
                // path_map if a new node doesn't have this as its parent
                if remove_parent && !additions.contains(parent_address) {
                    // empty node delete it.
                    path_map.remove(parent_address);
                } else {
                    // found a node that is not empty no need to continue
                    break;
                }

                let (next_parent, next_branch) = parent_and_branch(parent_address);
                parent_address = next_parent;
                path_branch = next_branch;

                if parent_address == "" {
                    let parent_node = path_map
                        .get_mut(parent_address)
                        .expect("Path map not correctly generated");

                    if let Some(old_hash_key) = parent_node.children.remove(path_branch) {
                        deletions.insert(old_hash_key);
                    }
                }
            }
        }

        let mut sorted_paths: Vec<_> = path_map.keys().cloned().collect();
        // Sort by longest to shortest
        sorted_paths.sort_by(|a, b| b.len().cmp(&a.len()));

        // initializing this to empty, to make the compiler happy
        let mut key_hash = Vec::with_capacity(0);
        let mut batch = Vec::with_capacity(sorted_paths.len());
        for path in sorted_paths {
            let node = path_map
                .remove(&path)
                .expect("Path map keys are out of sink");
            let (hash_key, packed) = encode_and_hash(node)?;
            key_hash = hash_key.clone();

            if path != "" {
                let (parent_address, path_branch) = parent_and_branch(&path);
                let parent = path_map
                    .get_mut(parent_address)
                    .expect("Path map not correctly generated");
                if let Some(old_hash_key) = parent
                    .children
                    .insert(path_branch.to_string(), ::hex::encode(hash_key.clone()))
                {
                    deletions.insert(old_hash_key);
                }
            }

            batch.push((hash_key, packed));
        }

        if !is_virtual {
            let deletions: Vec<Vec<u8>> = deletions
                .iter()
                // We expect this to be hex, since we generated it
                .map(|s| ::hex::decode(s).expect("Improper hex"))
                .collect();
            self.store_changes(&key_hash, &batch, &deletions)?;
        }

        Ok(::hex::encode(key_hash))
    }

    /// Puts all the items into the database.
    fn store_changes(
        &self,
        successor_root_hash: &[u8],
        batch: &[(Vec<u8>, Vec<u8>)],
        deletions: &[Vec<u8>],
    ) -> Result<(), StateDatabaseError> {
        let mut db_writer = self.db.writer()?;

        // We expect this to be hex, since we generated it
        let root_hash_bytes = ::hex::decode(&self.root_hash).expect("Improper hex");

        for &(ref key, ref value) in batch {
            match db_writer.put(::hex::encode(key).as_bytes(), &value) {
                Ok(_) => continue,
                Err(DatabaseError::DuplicateEntry) => {
                    increment_ref_count(&mut db_writer, key)?;
                }
                Err(err) => return Err(StateDatabaseError::from(err)),
            }
        }

        let mut current_change_log = get_change_log(&db_writer, &root_hash_bytes)?;
        if let Some(change_log) = current_change_log.as_mut() {
            let successor = Successor {
                successor: Vec::from(successor_root_hash),
                deletions: deletions.to_vec(),
            };
            change_log.successors.push(successor);
        }

        let next_change_log = ChangeLogEntry {
            parent: root_hash_bytes.clone(),
            additions: batch
                .iter()
                .map(|&(ref hash, _)| hash.clone())
                .collect::<Vec<Vec<u8>>>(),
            successors: vec![],
        };

        if current_change_log.is_some() {
            write_change_log(
                &mut db_writer,
                &root_hash_bytes,
                &current_change_log.unwrap(),
            )?;
        }
        write_change_log(&mut db_writer, successor_root_hash, &next_change_log)?;

        db_writer.commit()?;
        Ok(())
    }

    pub fn get_value(&self, address: &str) -> Result<Option<Vec<u8>>, StateDatabaseError> {
        match self.get_by_address(address) {
            Ok(value) => Ok(value.value),
            Err(StateDatabaseError::NotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn get_by_address(&self, address: &str) -> Result<Node, StateDatabaseError> {
        let tokens = tokenize_address(address);

        // There's probably a better way to do this than a clone
        let mut node = self.root_node.clone();

        for token in tokens.iter() {
            node = match node.children.get(&token.to_string()) {
                None => {
                    return Err(StateDatabaseError::NotFound(format!(
                        "invalid address {} from root {}",
                        address,
                        self.root_hash.clone()
                    )));
                }
                Some(child_hash) => get_node_by_hash(&self.db, child_hash)?,
            }
        }
        Ok(node)
    }

    pub fn contains(&self, address: &str) -> Result<bool, StateDatabaseError> {
        match self.get_by_address(address) {
            Ok(_) => Ok(true),
            Err(StateDatabaseError::NotFound(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    pub fn leaves(&self, prefix: Option<&str>) -> Result<Box<StateIter>, StateDatabaseError> {
        Ok(Box::new(MerkleLeafIterator::new(self.clone(), prefix)?))
    }

    fn get_path_by_tokens(
        &self,
        tokens: &[&str],
        strict: bool,
    ) -> Result<HashMap<String, Node>, StateDatabaseError> {
        let mut nodes = HashMap::new();

        let mut path = String::new();
        nodes.insert(path.clone(), self.root_node.clone());

        let mut new_branch = false;

        for token in tokens {
            let node = {
                // this is safe to unwrap, because we've just inserted the path in the previous loop
                let child_address = &nodes[&path].children.get(&token.to_string());

                match (!new_branch && child_address.is_some(), strict) {
                    (true, _) => get_node_by_hash(&self.db, child_address.unwrap())?,
                    (false, true) => {
                        return Err(StateDatabaseError::NotFound(format!(
                            "invalid address {} from root {}",
                            tokens.join(""),
                            self.root_hash.clone()
                        )));
                    }
                    (false, false) => {
                        new_branch = true;
                        Node::default()
                    }
                }
            };

            path.push_str(token);
            nodes.insert(path.clone(), node);
        }
        Ok(nodes)
    }
}

// A MerkleLeafIterator is fixed to iterate over the state address/value pairs
// the merkle root hash at the time of its creation.
pub struct MerkleLeafIterator {
    merkle_db: MerkleRadixTree,
    visited: VecDeque<(String, Node)>,
}

impl MerkleLeafIterator {
    fn new(merkle_db: MerkleRadixTree, prefix: Option<&str>) -> Result<Self, StateDatabaseError> {
        let path = prefix.unwrap_or("");

        let mut visited = VecDeque::new();
        let initial_node = merkle_db.get_by_address(path)?;
        visited.push_front((path.to_string(), initial_node));

        Ok(MerkleLeafIterator { merkle_db, visited })
    }
}

impl Iterator for MerkleLeafIterator {
    type Item = Result<(String, Vec<u8>), StateDatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.visited.is_empty() {
            return None;
        }

        loop {
            if let Some((path, node)) = self.visited.pop_front() {
                if node.value.is_some() {
                    return Some(Ok((path, node.value.unwrap())));
                }

                // Reverse the list, such that we have an in-order traversal of the
                // children, based on the natural path order.
                for (child_path, hash_key) in node.children.iter().rev() {
                    let child = match get_node_by_hash(&self.merkle_db.db, hash_key) {
                        Ok(node) => node,
                        Err(err) => return Some(Err(err)),
                    };
                    let mut child_address = path.clone();
                    child_address.push_str(child_path);
                    self.visited.push_front((child_address, child));
                }
            } else {
                return None;
            }
        }
    }
}

/// Initializes a database with an empty Trie
fn initialize_db(db: &LmdbDatabase) -> Result<String, StateDatabaseError> {
    let (hash, packed) = encode_and_hash(Node::default())?;

    let mut db_writer = db.writer()?;
    let hex_hash = ::hex::encode(hash);
    // Ignore ref counts for the default, empty tree
    db_writer.overwrite(hex_hash.as_bytes(), &packed)?;
    db_writer.commit()?;

    Ok(hex_hash)
}

/// Returns the change log entry for a given root hash.
fn get_change_log<R>(
    db_reader: &R,
    root_hash: &[u8],
) -> Result<Option<ChangeLogEntry>, StateDatabaseError>
where
    R: DatabaseReader,
{
    let log_bytes = db_reader.index_get(CHANGE_LOG_INDEX, root_hash)?;

    Ok(match log_bytes {
        Some(bytes) => Some(ChangeLogEntry::from_bytes(&bytes)?),

        None => None,
    })
    //TODO maybe check hex::decode here ? ??
}

/// Writes the given change log entry to the database
fn write_change_log(
    db_writer: &mut LmdbDatabaseWriter,
    root_hash: &[u8],
    change_log: &ChangeLogEntry,
) -> Result<(), StateDatabaseError> {
    db_writer.index_put(CHANGE_LOG_INDEX, root_hash, &change_log.to_bytes()?)?;
    Ok(())
}

fn increment_ref_count(
    db_writer: &mut LmdbDatabaseWriter,
    key: &[u8],
) -> Result<u64, StateDatabaseError> {
    let ref_count = get_ref_count(db_writer, key)?;

    db_writer.index_put(DUPLICATE_LOG_INDEX, key, &to_bytes(ref_count + 1))?;

    Ok(ref_count)
}

fn decrement_ref_count(
    db_writer: &mut LmdbDatabaseWriter,
    key: &[u8],
) -> Result<u64, StateDatabaseError> {
    let count = get_ref_count(db_writer, key)?;
    Ok(if count == 1 {
        db_writer.index_delete(DUPLICATE_LOG_INDEX, key)?;
        0
    } else {
        db_writer.index_put(DUPLICATE_LOG_INDEX, key, &to_bytes(count - 1))?;
        count - 1
    })
}

fn get_ref_count(
    db_writer: &mut LmdbDatabaseWriter,
    key: &[u8],
) -> Result<u64, StateDatabaseError> {
    Ok(
        if let Some(ref_count) = db_writer.index_get(DUPLICATE_LOG_INDEX, key)? {
            from_bytes(&ref_count)
        } else {
            0
        },
    )
}

fn to_bytes(num: u64) -> [u8; 8] {
    unsafe { ::std::mem::transmute(num.to_le()) }
}

fn from_bytes(bytes: &[u8]) -> u64 {
    let mut num_bytes = [0u8; 8];
    num_bytes.copy_from_slice(&bytes);
    u64::from_le(unsafe { ::std::mem::transmute(num_bytes) })
}

/// This delete ignores any MDB_NOTFOUND errors
fn delete_ignore_missing(
    db_writer: &mut LmdbDatabaseWriter,
    key: &[u8],
) -> Result<(), StateDatabaseError> {
    match db_writer.delete(key) {
        Err(DatabaseError::WriterError(ref s))
            if s == "MDB_NOTFOUND: No matching key/data pair found" =>
        {
            // This can be ignored, as the record doesn't exist
            debug!(
                "Attempting to delete a missing entry: {}",
                ::hex::encode(key)
            );
            Ok(())
        }
        Err(err) => Err(StateDatabaseError::DatabaseError(err)),
        Ok(_) => Ok(()),
    }
}
/// Encodes the given node, and returns the hash of the bytes.
fn encode_and_hash(node: Node) -> Result<(Vec<u8>, Vec<u8>), StateDatabaseError> {
    let packed = node.into_bytes()?;
    let hash = hash(&packed);
    Ok((hash, packed))
}

/// Given a path, split it into its parent's path and the specific branch for
/// this path, such that the following assertion is true:
fn parent_and_branch(path: &str) -> (&str, &str) {
    let parent_address = if !path.is_empty() {
        &path[..path.len() - TOKEN_SIZE]
    } else {
        ""
    };

    let path_branch = if !path.is_empty() {
        &path[(path.len() - TOKEN_SIZE)..]
    } else {
        ""
    };

    (parent_address, path_branch)
}

/// Splits an address into tokens
fn tokenize_address(address: &str) -> Box<[&str]> {
    let mut tokens: Vec<&str> = Vec::with_capacity(address.len() / TOKEN_SIZE);
    let mut i = 0;
    while i < address.len() {
        tokens.push(&address[i..i + TOKEN_SIZE]);
        i += TOKEN_SIZE;
    }
    tokens.into_boxed_slice()
}

/// Fetch a node by its hash
fn get_node_by_hash(db: &LmdbDatabase, hash: &str) -> Result<Node, StateDatabaseError> {
    match db.reader()?.get(hash.as_bytes()) {
        Some(bytes) => Node::from_bytes(&bytes),
        None => Err(StateDatabaseError::NotFound(hash.to_string())),
    }
}

/// Internal Node structure of the Radix tree
#[derive(Default, Debug, PartialEq, Clone)]
struct Node {
    value: Option<Vec<u8>>,
    children: BTreeMap<String, String>,
}

impl Node {
    /// Consumes this node and serializes it to bytes
    fn into_bytes(self) -> Result<Vec<u8>, StateDatabaseError> {
        let mut e = GenericEncoder::new(Cursor::new(Vec::new()));

        let mut map = BTreeMap::new();
        map.insert(
            Key::Text(Text::Text("v".to_string())),
            match self.value {
                Some(bytes) => Value::Bytes(Bytes::Bytes(bytes)),
                None => Value::Null,
            },
        );

        let children = self
            .children
            .into_iter()
            .map(|(k, v)| {
                (
                    Key::Text(Text::Text(k.to_string())),
                    Value::Text(Text::Text(v.to_string())),
                )
            })
            .collect();

        map.insert(Key::Text(Text::Text("c".to_string())), Value::Map(children));

        e.value(&Value::Map(map))?;

        Ok(e.into_inner().into_writer().into_inner())
    }

    /// Deserializes the given bytes to a Node
    fn from_bytes(bytes: &[u8]) -> Result<Node, StateDatabaseError> {
        let input = Cursor::new(bytes);
        let mut decoder = GenericDecoder::new(cbor::Config::default(), input);
        let decoder_value = decoder.value()?;
        let (val, children_raw) = match decoder_value {
            Value::Map(mut root_map) => (
                root_map.remove(&Key::Text(Text::Text("v".to_string()))),
                root_map.remove(&Key::Text(Text::Text("c".to_string()))),
            ),
            _ => return Err(StateDatabaseError::InvalidRecord),
        };

        let value = match val {
            Some(Value::Bytes(Bytes::Bytes(bytes))) => Some(bytes),
            Some(Value::Null) => None,
            _ => return Err(StateDatabaseError::InvalidRecord),
        };

        let children = match children_raw {
            Some(Value::Map(child_map)) => {
                let mut result = BTreeMap::new();
                for (k, v) in child_map {
                    result.insert(key_to_string(k)?, text_to_string(v)?);
                }
                result
            }
            None => BTreeMap::new(),
            _ => return Err(StateDatabaseError::InvalidRecord),
        };

        Ok(Node { value, children })
    }
}

/// Converts a CBOR Key to its String content
fn key_to_string(key_val: Key) -> Result<String, StateDatabaseError> {
    match key_val {
        Key::Text(Text::Text(s)) => Ok(s),
        _ => Err(StateDatabaseError::InvalidRecord),
    }
}

/// Converts a CBOR Text Value to its String content
fn text_to_string(text_val: Value) -> Result<String, StateDatabaseError> {
    match text_val {
        Value::Text(Text::Text(s)) => Ok(s),
        _ => Err(StateDatabaseError::InvalidRecord),
    }
}

/// Creates a hash of the given bytes
fn hash(input: &[u8]) -> Vec<u8> {
    let mut bytes: Vec<u8> = Vec::new();
    bytes.extend(openssl::sha::sha512(input).iter());
    let (hash, _rest) = bytes.split_at(bytes.len() / 2);
    hash.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::error::DatabaseError;
    use crate::database::lmdb::{DatabaseReader, LmdbContext, LmdbDatabase};

    use super::{Read, StateChange, Write};
    use crate::state::change_log::ChangeLogEntry;

    use rand::seq::IteratorRandom;
    use rand::thread_rng;
    use std::env;
    use std::fs::remove_file;
    use std::panic;
    use std::path::Path;
    use std::str::from_utf8;
    use std::thread;

    #[test]
    fn node_serialize() {
        let n = Node {
            value: Some(b"hello".to_vec()),
            children: vec![("ab".to_string(), "123".to_string())]
                .into_iter()
                .collect(),
        };

        let packed = n
            .into_bytes()
            .unwrap()
            .iter()
            .map(|b| format!("{:x}", b))
            .collect::<Vec<_>>()
            .join("");
        // This expected output was generated using the python structures
        let output = "a26163a16261626331323361764568656c6c6f";

        assert_eq!(output, packed);
    }

    #[test]
    fn node_deserialize() {
        let packed =
            ::hex::decode("a26163a162303063616263617647676f6f64627965").expect("improper hex");

        let unpacked = Node::from_bytes(&packed).unwrap();
        assert_eq!(
            Node {
                value: Some(b"goodbye".to_vec()),
                children: vec![("00".to_string(), "abc".to_string())]
                    .into_iter()
                    .collect(),
            },
            unpacked
        );
    }

    #[test]
    fn node_roundtrip() {
        let n = Node {
            value: Some(b"hello".to_vec()),
            children: vec![("ab".to_string(), "123".to_string())]
                .into_iter()
                .collect(),
        };

        let packed = n.into_bytes().unwrap();
        let unpacked = Node::from_bytes(&packed).unwrap();

        assert_eq!(
            Node {
                value: Some(b"hello".to_vec()),
                children: vec![("ab".to_string(), "123".to_string())]
                    .into_iter()
                    .collect(),
            },
            unpacked
        )
    }

    #[test]
    fn merkle_trie_root_advance() {
        run_test(|merkle_path| {
            let db = make_lmdb(&merkle_path);
            let mut merkle_db = MerkleRadixTree::new(db.clone(), None).unwrap();

            let orig_root = merkle_db.get_merkle_root();
            let orig_root_bytes = &::hex::decode(orig_root.clone()).unwrap();

            {
                // check that there is no ChangeLogEntry for the initial root
                let reader = db.reader().unwrap();
                assert!(reader
                    .index_get(CHANGE_LOG_INDEX, orig_root_bytes)
                    .expect("A database error occurred")
                    .is_none());
            }

            let state_change = StateChange::Set {
                key: "abcd".to_string(),
                value: "data_value".as_bytes().to_vec(),
            };
            merkle_db.set_merkle_root(orig_root.clone()).unwrap();
            let new_root = merkle_db.update(&[state_change], false).unwrap();
            let new_root_bytes = &::hex::decode(new_root.clone()).unwrap();

            assert_eq!(merkle_db.get_merkle_root(), orig_root, "Incorrect root");
            assert_ne!(orig_root, new_root, "root was not changed");

            let change_log: ChangeLogEntry = {
                // check that we have a change log entry for the new root
                let reader = db.reader().unwrap();
                let entry_bytes = &reader
                    .index_get(CHANGE_LOG_INDEX, new_root_bytes)
                    .expect("A database error occurred")
                    .expect("Did not return a change log entry");
                ChangeLogEntry::from_bytes(entry_bytes).expect("Failed to parse change log entry")
            };

            assert_eq!(orig_root_bytes, &change_log.parent);
            assert_eq!(3, change_log.additions.len());
            assert_eq!(0, change_log.successors.len());

            merkle_db.set_merkle_root(new_root.clone()).unwrap();
            assert_eq!(merkle_db.get_merkle_root(), new_root, "Incorrect root");

            assert_value_at_address(&merkle_db, "abcd", "data_value");
        })
    }

    #[test]
    fn merkle_trie_delete() {
        run_test(|merkle_path| {
            let mut merkle_db = make_db(&merkle_path);

            let state_change_set = StateChange::Set {
                key: "1234".to_string(),
                value: "deletable".as_bytes().to_vec(),
            };

            let new_root = merkle_db.update(&[state_change_set], false).unwrap();
            merkle_db.set_merkle_root(new_root).unwrap();
            assert_value_at_address(&merkle_db, "1234", "deletable");

            let state_change_del_1 = StateChange::Delete {
                key: "barf".to_string(),
            };

            // deleting an unknown key should return an error
            assert!(merkle_db.update(&[state_change_del_1], false).is_err());

            let state_change_del_2 = StateChange::Delete {
                key: "1234".to_string(),
            };

            let del_root = merkle_db.update(&[state_change_del_2], false).unwrap();

            // del_root hasn't been set yet, so address should still have value
            assert_value_at_address(&merkle_db, "1234", "deletable");
            merkle_db.set_merkle_root(del_root).unwrap();
            assert!(!merkle_db.contains("1234").unwrap());
        })
    }

    #[test]
    fn merkle_trie_update() {
        run_test(|merkle_path| {
            let mut merkle_db = make_db(&merkle_path);
            let init_root = merkle_db.get_merkle_root();

            let key_hashes = (0..1000)
                .map(|i| {
                    let key = format!("{:016x}", i);
                    let hash = hex_hash(key.as_bytes());
                    (key, hash)
                })
                .collect::<Vec<_>>();

            let mut values = HashMap::new();
            for &(ref key, ref hashed) in key_hashes.iter() {
                let state_change_set = StateChange::Set {
                    key: hashed.to_string(),
                    value: key.as_bytes().to_vec(),
                };
                let new_root = merkle_db.update(&[state_change_set], false).unwrap();
                merkle_db.set_merkle_root(new_root.clone()).unwrap();
                values.insert(hashed.clone(), key.to_string());
            }

            assert_ne!(init_root, merkle_db.get_merkle_root());

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

            let virtual_root = merkle_db.update(&state_changes, true).unwrap();

            // virtual root shouldn't match actual contents of tree
            assert!(merkle_db.set_merkle_root(virtual_root.clone()).is_err());

            let actual_root = merkle_db.update(&state_changes, false).unwrap();
            // the virtual root should be the same as the actual root
            assert_eq!(virtual_root, actual_root);
            assert_ne!(actual_root, merkle_db.get_merkle_root());

            merkle_db.set_merkle_root(actual_root).unwrap();

            for (address, value) in values {
                assert_value_at_address(&merkle_db, &address, &value);
            }

            for delete_change in delete_items {
                match delete_change {
                    StateChange::Delete { key } => {
                        assert!(!merkle_db.get_by_address(&key.clone()).is_ok());
                    }
                    _ => (),
                }
            }
        })
    }

    #[test]
    /// This test is similar to the update test except that it will ensure that
    /// there are no index errors in path_map within update function in case
    /// there are addresses within set_items & delete_items which have a common
    /// prefix (of any length).
    ///
    /// A Merkle trie is created with some initial values which is then updated
    /// (set & delete).
    fn merkle_trie_update_same_address_space() {
        run_test(|merkle_path| {
            let mut merkle_db = make_db(merkle_path);

            let init_root = merkle_db.get_merkle_root();
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
            //let mut new_root = init_root.clone();
            for &(ref key, ref hashed) in key_hashes.iter() {
                let state_change_set = StateChange::Set {
                    key: hashed.to_string(),
                    value: key.as_bytes().to_vec(),
                };
                let new_root = merkle_db.update(&[state_change_set], false).unwrap();
                merkle_db.set_merkle_root(new_root).unwrap();
                values.insert(hashed.to_string(), key.to_string());
            }

            assert_ne!(init_root, merkle_db.get_merkle_root());
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

            let virtual_root = merkle_db.update(&state_changes, true).unwrap();

            // virtual root shouldn't match actual contents of tree
            assert!(merkle_db.set_merkle_root(virtual_root.clone()).is_err());

            let actual_root = merkle_db.update(&state_changes, false).unwrap();
            // the virtual root should be the same as the actual root
            assert_eq!(virtual_root, actual_root);
            assert_ne!(actual_root, merkle_db.get_merkle_root());

            merkle_db.set_merkle_root(actual_root).unwrap();

            for (address, value) in values {
                assert_value_at_address(&merkle_db, &address, &value);
            }

            for delete_change in delete_items {
                match delete_change {
                    StateChange::Delete { key } => assert!(!merkle_db.get_by_address(&key).is_ok()),
                    _ => (),
                }
            }
        })
    }

    #[test]
    /// This test is similar to the update_same_address_space except that it will ensure that
    /// there are no index errors in path_map within update function in case
    /// there are addresses within set_items & delete_items which have a common
    /// prefix (of any length), when trie doesn't have children to the parent node getting deleted.
    ///
    /// A Merkle trie is created with some initial values which is then updated
    /// (set & delete).
    fn merkle_trie_update_same_address_space_with_no_children() {
        run_test(|merkle_path| {
            let mut merkle_db = make_db(merkle_path);

            let init_root = merkle_db.get_merkle_root();
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
            for &(ref key, ref hashed) in key_hashes.iter() {
                let state_change_set = StateChange::Set {
                    key: hashed.to_string(),
                    value: key.as_bytes().to_vec(),
                };
                let new_root = merkle_db.update(&[state_change_set], false).unwrap();
                merkle_db.set_merkle_root(new_root).unwrap();
                values.insert(hashed.to_string(), key.to_string());
            }

            assert_ne!(init_root, merkle_db.get_merkle_root());

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

            let virtual_root = merkle_db.update(&state_changes, true).unwrap();

            // virtual root shouldn't match actual contents of tree
            assert!(merkle_db.set_merkle_root(virtual_root.clone()).is_err());

            let actual_root = merkle_db.update(&state_changes, false).unwrap();

            // the virtual root should be the same as the actual root
            assert_eq!(virtual_root, actual_root);
            assert_ne!(actual_root, merkle_db.get_merkle_root());

            merkle_db.set_merkle_root(actual_root).unwrap();

            for (address, value) in values {
                assert_value_at_address(&merkle_db, &address, &value);
            }

            for delete_change in delete_items {
                match delete_change {
                    StateChange::Delete { key } => {
                        assert!(!merkle_db.get_by_address(&key).is_ok());
                    }
                    _ => (),
                }
            }
        })
    }

    #[test]
    /// This test creates a merkle trie with multiple entries, and produces a
    /// second trie based on the first where an entry is change.
    ///
    /// - It verifies that both tries have a ChangeLogEntry
    /// - Prunes the parent trie
    /// - Verifies that the nodes written are gone
    /// - verifies that the parent trie's ChangeLogEntry is deleted
    fn merkle_trie_pruning_parent() {
        run_test(|merkle_path| {
            let db = make_lmdb(&merkle_path);
            let mut merkle_db = MerkleRadixTree::new(db.clone(), None).expect("No db errors");
            let mut updates: Vec<StateChange<String, Vec<u8>>> = Vec::with_capacity(3);

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

            let parent_root = merkle_db
                .update(&updates, false)
                .expect("Update failed to work");
            merkle_db.set_merkle_root(parent_root.clone()).unwrap();

            let parent_root_bytes = ::hex::decode(parent_root.clone()).expect("Proper hex");
            // check that we have a change log entry for the new root
            let mut parent_change_log = expect_change_log(&db, &parent_root_bytes);
            assert!(parent_change_log.successors.is_empty());

            assert_value_at_address(&merkle_db, "ab0000", "0001");
            assert_value_at_address(&merkle_db, "ab0a01", "0002");
            assert_value_at_address(&merkle_db, "abff00", "0003");

            let successor_root = merkle_db
                .update(
                    &[StateChange::Set {
                        key: "ab0000".to_string(),
                        value: "test".as_bytes().to_vec(),
                    }],
                    false,
                )
                .expect("Set failed to work");
            let successor_root_bytes = ::hex::decode(successor_root.clone()).expect("proper hex");

            // Load the parent change log after the change.
            parent_change_log = expect_change_log(&db, &parent_root_bytes);
            let successor_change_log = expect_change_log(&db, &successor_root_bytes);

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
                MerkleRadixTree::prune(&db, &parent_root)
                    .expect("Prune should have no errors")
                    .len()
            );
            {
                let reader = db.reader().unwrap();
                for addition in parent_change_log
                    .successors
                    .clone()
                    .first()
                    .unwrap()
                    .deletions
                    .clone()
                {
                    assert!(reader.get(&addition).is_none());
                }

                assert!(reader
                    .index_get(CHANGE_LOG_INDEX, &parent_root_bytes)
                    .expect("DB query should succeed")
                    .is_none());
            }
            assert!(merkle_db.set_merkle_root(parent_root).is_err());
        })
    }

    #[test]
    /// This test creates a merkle trie with multiple entries and produces two
    /// distinct successor tries from that first.
    ///
    /// - it verifies that all the tries have a ChangeLogEntry
    /// - it prunes one of the successors
    /// - it verifies the nodes from that successor are removed
    /// - it verifies that the pruned successor's ChangeLogEntry is removed
    /// - it verifies the original and the remaining successor still are
    ///   persisted
    fn merkle_trie_pruning_successors() {
        run_test(|merkle_path| {
            let db = make_lmdb(&merkle_path);
            let mut merkle_db = MerkleRadixTree::new(db.clone(), None).expect("No db errors");
            let mut updates: Vec<StateChange<String, Vec<u8>>> = Vec::with_capacity(3);

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

            let parent_root = merkle_db
                .update(&updates, false)
                .expect("Update failed to work");
            let parent_root_bytes = ::hex::decode(parent_root.clone()).expect("Proper hex");

            merkle_db.set_merkle_root(parent_root.clone()).unwrap();
            assert_value_at_address(&merkle_db, "ab0000", "0001");
            assert_value_at_address(&merkle_db, "ab0a01", "0002");
            assert_value_at_address(&merkle_db, "abff00", "0003");

            let successor_root_left = merkle_db
                .update(
                    &[StateChange::Set {
                        key: "ab0000".to_string(),
                        value: "left".as_bytes().to_vec(),
                    }],
                    false,
                )
                .expect("Set failed to work");

            let successor_root_left_bytes =
                ::hex::decode(successor_root_left.clone()).expect("proper hex");

            let successor_root_right = merkle_db
                .update(
                    &[StateChange::Set {
                        key: "ab0a01".to_string(),
                        value: "right".as_bytes().to_vec(),
                    }],
                    false,
                )
                .expect("Set failed to work");

            let successor_root_right_bytes =
                ::hex::decode(successor_root_right.clone()).expect("proper hex");

            let mut parent_change_log = expect_change_log(&db, &parent_root_bytes);
            let successor_left_change_log = expect_change_log(&db, &successor_root_left_bytes);
            expect_change_log(&db, &successor_root_right_bytes);

            assert_has_successors(
                &parent_change_log,
                &[&successor_root_left_bytes, &successor_root_right_bytes],
            );

            // Let's prune the left successor:

            let res = MerkleRadixTree::prune(&db, &successor_root_left)
                .expect("Prune should have no errors");
            assert_eq!(successor_left_change_log.additions.len(), res.len());

            parent_change_log = expect_change_log(&db, &parent_root_bytes);
            assert_has_successors(&parent_change_log, &[&successor_root_right_bytes]);

            assert!(merkle_db.set_merkle_root(successor_root_left).is_err());
        })
    }

    #[test]
    /// This test creates a merkle trie with multiple entries and produces a
    /// successor with duplicate That changes one new leaf, followed by a second
    /// successor that produces a leaf with the same hash.  When the pruning the
    /// initial root, the duplicate leaf node is not pruned as well.
    fn merkle_trie_pruning_duplicate_leaves() {
        run_test(|merkle_path| {
            let db = make_lmdb(&merkle_path);
            let mut merkle_db = MerkleRadixTree::new(db.clone(), None).expect("No db errors");
            let mut updates: Vec<StateChange<String, Vec<u8>>> = Vec::with_capacity(3);
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

            let parent_root = merkle_db
                .update(&updates, false)
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

            let successor_root_middle = merkle_db
                .update(&updates, false)
                .expect("Update failed to work");

            // create the last root
            merkle_db
                .set_merkle_root(successor_root_middle.clone())
                .unwrap();
            // Set the value back to the original
            let successor_root_last = merkle_db
                .update(
                    &[StateChange::Set {
                        key: "ab0000".to_string(),
                        value: "0001".as_bytes().to_vec(),
                    }],
                    false,
                )
                .expect("Set failed to work");

            merkle_db.set_merkle_root(successor_root_last).unwrap();
            let parent_change_log = expect_change_log(&db, &parent_root_bytes);
            assert_eq!(
                parent_change_log
                    .successors
                    .clone()
                    .first()
                    .unwrap()
                    .deletions
                    .len(),
                MerkleRadixTree::prune(&db, &parent_root)
                    .expect("Prune should have no errors")
                    .len()
            );

            assert_value_at_address(&merkle_db, "ab0000", "0001");
        })
    }

    #[test]
    /// This test creates a merkle trie with multiple entries and produces a
    /// successor with duplicate That changes one new leaf, followed by a second
    /// successor that produces a leaf with the same hash.  When the pruning the
    /// last root, the duplicate leaf node is not pruned as well.
    fn merkle_trie_pruning_successor_duplicate_leaves() {
        run_test(|merkle_path| {
            let db = make_lmdb(&merkle_path);
            let mut merkle_db = MerkleRadixTree::new(db.clone(), None).expect("No db errors");
            let mut updates: Vec<StateChange<String, Vec<u8>>> = Vec::with_capacity(3);

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

            let parent_root = merkle_db
                .update(&updates, false)
                .expect("Update failed to work");

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
            let successor_root_middle = merkle_db
                .update(&updates, false)
                .expect("Update failed to work");

            // create the last root
            merkle_db
                .set_merkle_root(successor_root_middle.clone())
                .unwrap();
            // Set the value back to the original
            let successor_root_last = merkle_db
                .update(
                    &[StateChange::Set {
                        key: "ab0000".to_string(),
                        value: "0001".as_bytes().to_vec(),
                    }],
                    false,
                )
                .expect("Set failed to work");
            let successor_root_bytes =
                ::hex::decode(successor_root_last.clone()).expect("Proper hex");

            // set back to the parent root
            merkle_db.set_merkle_root(parent_root).unwrap();
            let last_change_log = expect_change_log(&db, &successor_root_bytes);
            assert_eq!(
                last_change_log.additions.len() - 1,
                MerkleRadixTree::prune(&db, &successor_root_last)
                    .expect("Prune should have no errors")
                    .len()
            );

            assert_value_at_address(&merkle_db, "ab0000", "0001");
        })
    }

    #[test]
    fn leaf_iteration() {
        run_test(|merkle_path| {
            let mut merkle_db = make_db(merkle_path);

            {
                let mut leaf_iter = merkle_db.leaves(None).unwrap();
                assert!(
                    leaf_iter.next().is_none(),
                    "Empty tree should return no leaves"
                );
            }

            let addresses = vec!["ab0000", "aba001", "abff02"];
            for (i, key) in addresses.iter().enumerate() {
                let state_change_set = StateChange::Set {
                    key: key.to_string(),
                    value: format!("{:04x}", i * 10).as_bytes().to_vec(),
                };
                let new_root = merkle_db.update(&[state_change_set], false).unwrap();
                merkle_db.set_merkle_root(new_root).unwrap();
            }
            assert_value_at_address(&merkle_db, "ab0000", "0000");
            assert_value_at_address(&merkle_db, "aba001", "000a");
            assert_value_at_address(&merkle_db, "abff02", "0014");

            let mut leaf_iter = merkle_db.leaves(None).unwrap();

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
            let mut leaf_iter = merkle_db.leaves(Some("abff")).unwrap();
            assert_eq!(
                ("abff02".into(), "0014".as_bytes().to_vec()),
                leaf_iter.next().unwrap().unwrap()
            );
            assert!(leaf_iter.next().is_none(), "Iterator should be Exhausted");
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

        assert!(result.is_ok())
    }

    fn assert_value_at_address(merkle_db: &MerkleRadixTree, address: &str, expected_value: &str) {
        let value = merkle_db.get_by_address(&address.to_string());
        assert!(value.is_ok(), format!("Value not returned: {:?}", value));
        assert_eq!(
            Ok(expected_value),
            from_utf8(&value.unwrap().value.unwrap())
        );
    }

    fn expect_change_log(db: &LmdbDatabase, root_hash: &[u8]) -> ChangeLogEntry {
        let reader = db.reader().unwrap();
        ChangeLogEntry::from_bytes(
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
            for successor in change_log.successors.clone() {
                if &successor.successor == successor_root {
                    has_root = true;
                    break;
                }
            }
            if !has_root {
                panic!(format!(
                    "Root {} not found in change log {:?}",
                    ::hex::encode(successor_root),
                    change_log
                ));
            }
        }
    }

    fn make_lmdb(merkle_path: &str) -> LmdbDatabase {
        let ctx = LmdbContext::new(
            Path::new(merkle_path),
            INDEXES.len(),
            Some(120 * 1024 * 1024),
        )
        .map_err(|err| DatabaseError::InitError(format!("{}", err)))
        .unwrap();
        LmdbDatabase::new(ctx, &INDEXES)
            .map_err(|err| DatabaseError::InitError(format!("{}", err)))
            .unwrap()
    }

    fn make_db(merkle_path: &str) -> MerkleRadixTree {
        MerkleRadixTree::new(make_lmdb(merkle_path), None).unwrap()
    }

    fn temp_db_path() -> String {
        let mut temp_dir = env::temp_dir();

        let thread_id = thread::current().id();
        temp_dir.push(format!("merkle-{:?}.lmdb", thread_id));
        temp_dir.to_str().unwrap().to_string()
    }

    fn hex_hash(b: &[u8]) -> String {
        ::hex::encode(hash(b))
    }

}

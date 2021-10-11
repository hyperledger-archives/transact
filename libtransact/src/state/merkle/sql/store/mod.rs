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

//! Provides a trait for the low-level store operations required to read and write changes to the
//! merkle radix tree.

mod models;
mod operations;
#[cfg(feature = "postgres")]
mod postgres;
mod schema;
#[cfg(feature = "sqlite")]
mod sqlite;

use std::collections::HashSet;

use crate::error::InternalError;
use crate::state::merkle::{node::Node, sql::backend::Backend};

// (Hash, packed bytes, path address)
type NodeChanges = Vec<(String, Node, String)>;

#[derive(Default)]
pub(in crate::state::merkle::sql) struct TreeUpdate {
    pub node_changes: NodeChanges,
    pub deletions: HashSet<String>,
}

pub(in crate::state::merkle::sql) trait MerkleRadixStore {
    fn get_or_create_tree(
        &self,
        tree_name: &str,
        initial_state_root_hash: &str,
    ) -> Result<i64, InternalError>;

    fn get_tree_id_by_name(&self, tree_name: &str) -> Result<Option<i64>, InternalError>;

    fn has_root(&self, tree_id: i64, state_root_hash: &str) -> Result<bool, InternalError>;

    fn get_path(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        address: &str,
    ) -> Result<Vec<(String, Node)>, InternalError>;

    fn get_entries(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        keys: Vec<&str>,
    ) -> Result<Vec<(String, Vec<u8>)>, InternalError>;

    fn list_entries(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<(String, Vec<u8>)>, InternalError>;

    fn write_changes(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        parent_state_root_hash: &str,
        tree_update: TreeUpdate,
    ) -> Result<(), InternalError>;

    fn prune(&self, tree_id: i64, state_root: &str) -> Result<Vec<String>, InternalError>;
}

pub(in crate::state::merkle::sql) struct SqlMerkleRadixStore<'b, B: Backend> {
    pub backend: &'b B,
}

impl<'b, B: Backend> SqlMerkleRadixStore<'b, B> {
    pub(in crate::state::merkle::sql) fn new(backend: &'b B) -> Self {
        Self { backend }
    }
}

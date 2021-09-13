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

#[cfg(feature = "postgres")]
pub(in crate::state::merkle::sql) mod postgres;
#[cfg(feature = "sqlite")]
pub(in crate::state::merkle::sql) mod sqlite;

use super::schema::*;

#[derive(Insertable, Queryable, Identifiable)]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[table_name = "merkle_radix_tree"]
#[primary_key(id)]
pub struct MerkleRadixTree {
    pub id: i64,
    pub name: String,
}

#[derive(Insertable)]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[table_name = "merkle_radix_tree"]
pub struct NewMerkleRadixTree<'a> {
    pub name: &'a str,
}

#[derive(Insertable, Queryable, Identifiable)]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[table_name = "merkle_radix_leaf"]
#[primary_key(id)]
pub struct MerkleRadixLeaf {
    pub id: i64,
    pub tree_id: i64,
    pub address: String,
    pub data: Vec<u8>,
}

#[derive(Insertable)]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[table_name = "merkle_radix_leaf"]
pub struct NewMerkleRadixLeaf<'a> {
    pub id: i64,
    pub tree_id: i64,
    pub address: &'a str,
    pub data: &'a [u8],
}

#[derive(Insertable)]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[table_name = "merkle_radix_change_log_addition"]
pub struct NewMerkleRadixChangeLogAddition<'a> {
    pub tree_id: i64,
    pub state_root: &'a str,
    pub parent_state_root: Option<&'a str>,
    pub addition: &'a str,
}

#[derive(Queryable, QueryableByName, Identifiable)]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[table_name = "merkle_radix_change_log_addition"]
#[primary_key(id)]
pub struct MerkleRadixChangeLogAddition {
    pub id: i64,
    pub tree_id: i64,
    pub state_root: String,
    pub parent_state_root: Option<String>,
    pub addition: String,
}

#[derive(Insertable)]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[table_name = "merkle_radix_change_log_deletion"]
pub struct NewMerkleRadixChangeLogDeletion<'a> {
    pub tree_id: i64,
    pub successor_state_root: &'a str,
    pub state_root: &'a str,
    pub deletion: &'a str,
}

#[derive(Queryable, QueryableByName, Identifiable)]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[table_name = "merkle_radix_change_log_deletion"]
#[primary_key(id)]
pub struct MerkleRadixChangeLogDeletion {
    pub id: i64,
    pub tree_id: i64,
    pub successor_state_root: String,
    pub state_root: String,
    pub deletion: String,
}

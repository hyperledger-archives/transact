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

#[cfg(feature = "sqlite")]
mod sqlite;

use super::schema::*;

#[derive(Insertable, Queryable, Identifiable)]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[table_name = "merkle_radix_leaf"]
#[primary_key(id)]
pub struct MerkleRadixLeaf {
    pub id: i64,
    pub address: String,
    pub data: Vec<u8>,
}

#[derive(Insertable)]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[table_name = "merkle_radix_leaf"]
pub struct NewMerkleRadixLeaf<'a> {
    pub id: i64,
    pub address: &'a str,
    pub data: &'a [u8],
}

#[derive(Insertable, Queryable, QueryableByName, Identifiable)]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[table_name = "merkle_radix_tree_node"]
#[primary_key(hash)]
pub struct MerkleRadixTreeNode {
    pub hash: String,
    pub leaf_id: Option<i64>,
    pub children: Children,
}

#[derive(AsExpression, Debug, FromSqlRow)]
#[cfg_attr(test, derive(PartialEq))]
#[cfg_attr(feature = "sqlite", derive(Deserialize, Serialize))]
#[cfg_attr(feature = "sqlite", sql_type = "diesel::sql_types::Text")]
pub struct Children(pub Vec<Option<String>>);

#[derive(Insertable, Queryable, Identifiable)]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[table_name = "merkle_radix_state_root"]
#[primary_key(id)]
pub struct MerkleRadixStateRoot {
    pub id: i64,
    pub state_root: String,
    pub parent_state_root: String,
}

#[derive(Insertable)]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[table_name = "merkle_radix_state_root"]
pub struct NewMerkleRadixStateRoot<'a> {
    pub state_root: &'a str,
    pub parent_state_root: &'a str,
}

#[derive(Insertable, Queryable, Identifiable)]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[table_name = "merkle_radix_state_root_leaf_index"]
#[primary_key(id)]
pub struct MerkleRadixStateRootLeafIndexEntry {
    pub id: i64,
    pub leaf_id: i64,
    pub from_state_root_id: i64,
    pub to_state_root_id: Option<i64>,
}

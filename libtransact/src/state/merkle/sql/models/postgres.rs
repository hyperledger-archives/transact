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

use crate::state::merkle::sql::schema::postgres_merkle_radix_tree_node;

#[derive(Insertable, Queryable, QueryableByName, Identifiable)]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[table_name = "postgres_merkle_radix_tree_node"]
#[primary_key(hash, tree_id)]
pub struct MerkleRadixTreeNode {
    pub hash: String,
    pub tree_id: i64,
    pub leaf_id: Option<i64>,
    pub children: Vec<Option<String>>,
}

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

use diesel::prelude::*;
use diesel::sql_query;

use crate::error::InternalError;

use diesel::sql_types::{BigInt, Binary, VarChar};

use super::MerkleRadixOperations;

pub trait MerkleRadixListLeavesOperation {
    fn list_leaves(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<(String, Vec<u8>)>, InternalError>;
}

#[derive(QueryableByName)]
struct Leaf {
    #[column_name = "address"]
    #[sql_type = "VarChar"]
    address: String,
    #[column_name = "data"]
    #[sql_type = "Binary"]
    data: Vec<u8>,
}

#[cfg(feature = "sqlite")]
impl<'a> MerkleRadixListLeavesOperation for MerkleRadixOperations<'a, SqliteConnection> {
    fn list_leaves(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<(String, Vec<u8>)>, InternalError> {
        let results = sql_query(
            r#"
            WITH RECURSIVE tree_path AS
            (
                -- This is the initial node
                SELECT hash, tree_id, leaf_id, children, 0 as depth
                FROM merkle_radix_tree_node
                WHERE hash = ? AND tree_id = ?

                UNION ALL

                -- Recurse through the tree
                SELECT c.hash, c.tree_id, c.leaf_id, c.children, p.depth + 1
                FROM merkle_radix_tree_node c, tree_path p, json_each(p.children)
                WHERE c.hash = json_each.value
            )
            SELECT l.address, l.data
            FROM tree_path t, merkle_radix_leaf l
            WHERE t.tree_id = ? AND t.leaf_id = l.id AND l.address LIKE ?
            ORDER BY l.address
            "#,
        )
        .bind::<VarChar, _>(state_root_hash)
        .bind::<BigInt, _>(tree_id)
        .bind::<BigInt, _>(tree_id)
        .bind::<VarChar, _>(format!("{}%", prefix.unwrap_or("")))
        .load::<Leaf>(self.conn)
        .map_err(|err| InternalError::from_source(Box::new(err)))?
        .into_iter()
        .map(|leaf| (leaf.address, leaf.data))
        .collect::<Vec<_>>();

        Ok(results)
    }
}

#[cfg(feature = "postgres")]
impl<'a> MerkleRadixListLeavesOperation for MerkleRadixOperations<'a, PgConnection> {
    fn list_leaves(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<(String, Vec<u8>)>, InternalError> {
        let results = sql_query(
            r#"
            WITH RECURSIVE tree_path AS
            (
                -- This is the initial node
                SELECT hash, tree_id, leaf_id, children, 0 as depth
                FROM merkle_radix_tree_node
                WHERE hash = $2 AND tree_id = $1

                UNION ALL

                -- Recurse through the tree
                SELECT c.hash, c.tree_id, c.leaf_id, c.children, p.depth + 1
                FROM merkle_radix_tree_node c, tree_path p
                WHERE c.hash = ANY(p.children)
            )
            SELECT l.address, l.data
            FROM tree_path t, merkle_radix_leaf l
            WHERE t.tree_id = $1 AND t.leaf_id = l.id AND l.address LIKE $3
            ORDER BY l.address
            "#,
        )
        .bind::<BigInt, _>(tree_id)
        .bind::<VarChar, _>(state_root_hash)
        .bind::<VarChar, _>(format!("{}%", prefix.unwrap_or("")))
        .load::<Leaf>(self.conn)
        .map_err(|err| InternalError::from_source(Box::new(err)))?
        .into_iter()
        .map(|leaf| (leaf.address, leaf.data))
        .collect::<Vec<_>>();

        Ok(results)
    }
}

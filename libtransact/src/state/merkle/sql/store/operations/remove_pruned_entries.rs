/*
 * Copyright 2022 Cargill Incorporated
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

use diesel::dsl::{delete, not};
use diesel::prelude::*;
use diesel::sql_types::BigInt;

use crate::error::InternalError;
#[cfg(feature = "postgres")]
use crate::state::merkle::sql::store::schema::postgres_merkle_radix_tree_node;
#[cfg(feature = "sqlite")]
use crate::state::merkle::sql::store::schema::sqlite_merkle_radix_tree_node;
use crate::state::merkle::sql::store::schema::{
    merkle_radix_change_log_addition, merkle_radix_change_log_deletion, merkle_radix_leaf,
};

use super::prepared_stmt::prepare_stmt;
use super::MerkleRadixOperations;

pub trait MerkleRadixRemovePrunedEntriesOperation {
    /// Deletes all items marked as deleted.  This includes change log entries and nodes with a
    /// reference count of 0.
    ///
    /// # Returns
    ///
    /// The total count of deleted records.
    ///
    /// # Errors
    ///
    /// Returns an [`InternalError`]
    fn remove_pruned_entries(&self, tree_id: i64) -> Result<u64, InternalError>;
}

#[cfg(feature = "sqlite")]
impl<'a> MerkleRadixRemovePrunedEntriesOperation for MerkleRadixOperations<'a, SqliteConnection> {
    fn remove_pruned_entries(&self, tree_id: i64) -> Result<u64, InternalError> {
        self.conn.transaction(|| {
            let mut removed_records: u64 = 0;

            let deleted = delete(
                merkle_radix_change_log_addition::table.filter(
                    merkle_radix_change_log_addition::tree_id
                        .eq(tree_id)
                        .and(not(merkle_radix_change_log_addition::pruned_at.is_null())),
                ),
            )
            .execute(self.conn)?;

            removed_records += deleted as u64;

            let deleted = delete(
                merkle_radix_change_log_deletion::table.filter(
                    merkle_radix_change_log_deletion::tree_id
                        .eq(tree_id)
                        .and(not(merkle_radix_change_log_deletion::pruned_at.is_null())),
                ),
            )
            .execute(self.conn)?;

            removed_records += deleted as u64;

            // This join isn't supported in diesel given that leaf id in the node table is
            // nullable.  This makes the rust types incompatible, even though they are in SQL.
            let leaves: Vec<LeafId> = prepare_stmt(
                r#"
                  SELECT leaf_id FROM merkle_radix_tree_node n
                  INNER JOIN merkle_radix_leaf l
                    ON (l.id = n.leaf_id)
                  WHERE n.tree_id = $1 AND n.reference = 0
                  "#,
            )
            .bind::<BigInt, _>(tree_id)
            .get_results(self.conn)?;

            let deleted = delete(
                sqlite_merkle_radix_tree_node::table.filter(
                    sqlite_merkle_radix_tree_node::tree_id
                        .eq(tree_id)
                        .and(sqlite_merkle_radix_tree_node::reference.eq(0)),
                ),
            )
            .execute(self.conn)?;

            removed_records += deleted as u64;

            for leaf in &leaves {
                delete(merkle_radix_leaf::table.find(leaf.id)).execute(self.conn)?;
            }
            removed_records += leaves.len() as u64;

            Ok(removed_records)
        })
    }
}

#[cfg(feature = "postgres")]
impl<'a> MerkleRadixRemovePrunedEntriesOperation
    for MerkleRadixOperations<'a, diesel::pg::PgConnection>
{
    fn remove_pruned_entries(&self, tree_id: i64) -> Result<u64, InternalError> {
        self.conn.transaction(|| {
            let mut removed_records: u64 = 0;

            let deleted = delete(
                merkle_radix_change_log_addition::table.filter(
                    merkle_radix_change_log_addition::tree_id
                        .eq(tree_id)
                        .and(not(merkle_radix_change_log_addition::pruned_at.is_null())),
                ),
            )
            .execute(self.conn)?;

            removed_records += deleted as u64;

            let deleted = delete(
                merkle_radix_change_log_deletion::table.filter(
                    merkle_radix_change_log_deletion::tree_id
                        .eq(tree_id)
                        .and(not(merkle_radix_change_log_deletion::pruned_at.is_null())),
                ),
            )
            .execute(self.conn)?;

            removed_records += deleted as u64;

            // This join isn't supported in diesel given that leaf id in the node table is
            // nullable.  This makes the rust types incompatible, even though they are in SQL.
            let leaves: Vec<LeafId> = prepare_stmt(
                r#"
                  SELECT leaf_id FROM merkle_radix_tree_node n
                  INNER JOIN merkle_radix_leaf l
                    ON (l.id = n.leaf_id)
                  WHERE n.tree_id = $1 AND n.reference = 0
                  "#,
            )
            .bind::<BigInt, _>(tree_id)
            .get_results(self.conn)?;

            let deleted = delete(
                postgres_merkle_radix_tree_node::table.filter(
                    postgres_merkle_radix_tree_node::tree_id
                        .eq(tree_id)
                        .and(postgres_merkle_radix_tree_node::reference.eq(0)),
                ),
            )
            .execute(self.conn)?;
            removed_records += deleted as u64;

            for leaf in &leaves {
                delete(merkle_radix_leaf::table.find(leaf.id)).execute(self.conn)?;
            }
            removed_records += leaves.len() as u64;

            Ok(removed_records)
        })
    }
}

#[derive(Debug, QueryableByName)]
struct LeafId {
    #[column_name = "leaf_id"]
    #[sql_type = "BigInt"]
    pub id: i64,
}

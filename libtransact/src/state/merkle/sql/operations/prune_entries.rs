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

use std::collections::HashMap;

use diesel::dsl::{delete, update};
use diesel::prelude::*;

use crate::error::InternalError;
use crate::state::merkle::sql::models::{
    MerkleRadixChangeLogAddition, MerkleRadixChangeLogDeletion,
};
#[cfg(feature = "postgres")]
use crate::state::merkle::sql::schema::postgres_merkle_radix_tree_node;
#[cfg(feature = "sqlite")]
use crate::state::merkle::sql::schema::sqlite_merkle_radix_tree_node;
use crate::state::merkle::sql::schema::{
    merkle_radix_change_log_addition, merkle_radix_change_log_deletion,
};

use super::MerkleRadixOperations;

const NULL_PARENT: Option<String> = None;

pub trait MerkleRadixPruneEntriesOperation {
    fn prune_entries(&self, tree_id: i64, state_root: &str) -> Result<Vec<String>, InternalError>;
}

#[cfg(feature = "sqlite")]
impl<'a> MerkleRadixPruneEntriesOperation for MerkleRadixOperations<'a, SqliteConnection> {
    fn prune_entries(&self, tree_id: i64, state_root: &str) -> Result<Vec<String>, InternalError> {
        self.conn.transaction(|| {
            let deletion_candidates = get_deletion_candidates(self.conn, tree_id, state_root)?;

            // Remove the change logs for this root
            // delete its additions entry
            delete(
                merkle_radix_change_log_addition::table.filter(
                    merkle_radix_change_log_addition::tree_id
                        .eq(tree_id)
                        .and(merkle_radix_change_log_addition::state_root.eq(state_root)),
                ),
            )
            .execute(self.conn)?;
            // Unlink any successors it might have
            delete(
                merkle_radix_change_log_deletion::table.filter(
                    merkle_radix_change_log_deletion::tree_id
                        .eq(tree_id)
                        .and(merkle_radix_change_log_deletion::state_root.eq(state_root)),
                ),
            )
            .execute(self.conn)?;
            // Delete its successor entry
            delete(
                merkle_radix_change_log_deletion::table.filter(
                    merkle_radix_change_log_deletion::tree_id
                        .eq(tree_id)
                        .and(merkle_radix_change_log_deletion::successor_state_root.eq(state_root)),
                ),
            )
            .execute(self.conn)?;
            // Remove the parent relation ship on its successors
            update(
                merkle_radix_change_log_addition::table
                    .filter(merkle_radix_change_log_addition::parent_state_root.eq(state_root)),
            )
            .set(merkle_radix_change_log_addition::parent_state_root.eq(NULL_PARENT))
            .execute(self.conn)?;

            let mut deleted_values = vec![];
            for hash in deletion_candidates.into_iter() {
                match delete(
                    sqlite_merkle_radix_tree_node::table.filter(
                        sqlite_merkle_radix_tree_node::tree_id
                            .eq(tree_id)
                            .and(sqlite_merkle_radix_tree_node::hash.eq(&hash)),
                    ),
                )
                .execute(self.conn)
                {
                    Ok(_) => deleted_values.push(hash),
                    Err(diesel::result::Error::DatabaseError(
                        diesel::result::DatabaseErrorKind::ForeignKeyViolation,
                        _,
                    )) => (),
                    Err(err) => return Err(InternalError::from(err)),
                }
            }
            Ok(deleted_values)
        })
    }
}

#[cfg(feature = "postgres")]
impl<'a> MerkleRadixPruneEntriesOperation for MerkleRadixOperations<'a, PgConnection> {
    fn prune_entries(&self, tree_id: i64, state_root: &str) -> Result<Vec<String>, InternalError> {
        self.conn.transaction(|| {
            let deletion_candidates = get_deletion_candidates(self.conn, tree_id, state_root)?;

            // Remove the change logs for this root
            // delete its additions entry
            delete(
                merkle_radix_change_log_addition::table.filter(
                    merkle_radix_change_log_addition::tree_id
                        .eq(tree_id)
                        .and(merkle_radix_change_log_addition::state_root.eq(state_root)),
                ),
            )
            .execute(self.conn)?;
            // Unlink any successors it might have
            delete(
                merkle_radix_change_log_deletion::table.filter(
                    merkle_radix_change_log_deletion::tree_id
                        .eq(tree_id)
                        .and(merkle_radix_change_log_deletion::state_root.eq(state_root)),
                ),
            )
            .execute(self.conn)?;
            // Delete its successor entry
            delete(
                merkle_radix_change_log_deletion::table.filter(
                    merkle_radix_change_log_deletion::tree_id
                        .eq(tree_id)
                        .and(merkle_radix_change_log_deletion::successor_state_root.eq(state_root)),
                ),
            )
            .execute(self.conn)?;
            // Remove the parent relation ship on its successors
            update(
                merkle_radix_change_log_addition::table
                    .filter(merkle_radix_change_log_addition::parent_state_root.eq(state_root)),
            )
            .set(merkle_radix_change_log_addition::parent_state_root.eq(NULL_PARENT))
            .execute(self.conn)?;

            let mut deleted_values = vec![];
            for hash in deletion_candidates.into_iter().rev() {
                // Put this in a new save-point.
                match self.conn.transaction(|| {
                    delete(
                        postgres_merkle_radix_tree_node::table.filter(
                            postgres_merkle_radix_tree_node::tree_id
                                .eq(tree_id)
                                .and(postgres_merkle_radix_tree_node::hash.eq(&hash)),
                        ),
                    )
                    .execute(self.conn)
                }) {
                    Ok(_) => deleted_values.push(hash),
                    Err(diesel::result::Error::DatabaseError(
                        diesel::result::DatabaseErrorKind::ForeignKeyViolation,
                        _,
                    )) => (),
                    Err(err) => return Err(InternalError::from(err)),
                }
            }
            Ok(deleted_values)
        })
    }
}

fn get_deletion_candidates<C>(
    conn: &C,
    tree_id: i64,
    state_root: &str,
) -> Result<Vec<String>, InternalError>
where
    C: diesel::Connection,
    i64: diesel::deserialize::FromSql<diesel::sql_types::BigInt, C::Backend>,
    String: diesel::deserialize::FromSql<diesel::sql_types::Text, C::Backend>,
{
    let change_additions = merkle_radix_change_log_addition::table
        .filter(
            merkle_radix_change_log_addition::tree_id
                .eq(tree_id)
                .and(merkle_radix_change_log_addition::state_root.eq(state_root)),
        )
        .get_results::<MerkleRadixChangeLogAddition>(conn)?
        .into_iter()
        .map(|addition| addition.addition)
        .collect::<Vec<_>>();

    if change_additions.is_empty() {
        return Ok(Vec::new());
    }

    // Find all successors
    let successors = merkle_radix_change_log_deletion::table
        .filter(
            merkle_radix_change_log_deletion::tree_id
                .eq(tree_id)
                .and(merkle_radix_change_log_deletion::state_root.eq(state_root)),
        )
        .load::<MerkleRadixChangeLogDeletion>(conn)?
        .into_iter()
        .fold(HashMap::new(), |mut acc, successor| {
            let hashes = acc
                .entry(successor.successor_state_root)
                .or_insert_with(Vec::new);
            hashes.push(successor.deletion);
            acc
        });

    // Currently, don't clean up a parent with multiple successors
    if successors.len() > 1 {
        return Ok(vec![]);
    }

    let deletion_candidates: Vec<String> = if successors.is_empty() {
        // this root is the tip of the trie history
        change_additions
    } else {
        // we have one successor, based on our criteria, so we can safely unwrap
        let (_successor_state_root, mut deletions) = successors.into_iter().next().unwrap();
        deletions.push(state_root.into());
        deletions
    };

    Ok(deletion_candidates)
}

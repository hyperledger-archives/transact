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

use diesel::dsl::{not, sql, update};
use diesel::prelude::*;

use crate::error::InternalError;
use crate::state::merkle::sql::store::models::{
    MerkleRadixChangeLogAddition, MerkleRadixChangeLogDeletion,
};
use crate::state::merkle::sql::store::schema::{
    merkle_radix_change_log_addition, merkle_radix_change_log_deletion, merkle_radix_leaf,
};
#[cfg(feature = "postgres")]
use crate::state::merkle::sql::store::{models::postgres, schema::postgres_merkle_radix_tree_node};
#[cfg(feature = "sqlite")]
use crate::state::merkle::sql::store::{models::sqlite, schema::sqlite_merkle_radix_tree_node};

use super::MerkleRadixOperations;

pub trait MerkleRadixPruneEntriesOperation {
    fn prune_entries(&self, tree_id: i64, state_root: &str) -> Result<Vec<String>, InternalError>;
}

#[cfg(feature = "sqlite")]
const SQLITE_NOW_MILLIS: &str = "strftime('%s') * 1000";

#[cfg(feature = "sqlite")]
impl<'a> MerkleRadixPruneEntriesOperation for MerkleRadixOperations<'a, SqliteConnection> {
    fn prune_entries(&self, tree_id: i64, state_root: &str) -> Result<Vec<String>, InternalError> {
        self.conn.transaction(|| {
            let Candidates {
                deletions,
                parent,
                successor,
            } = get_deletion_candidates(self.conn, tree_id, state_root)?;

            if deletions.is_empty() {
                return Ok(vec![]);
            }

            update_changelogs(
                self.conn,
                tree_id,
                state_root,
                parent.as_deref(),
                successor.as_deref(),
                &deletions,
                SQLITE_NOW_MILLIS,
            )?;

            let mut deleted_values = vec![];
            for hash in deletions.into_iter() {
                update(
                    sqlite_merkle_radix_tree_node::table.filter(
                        sqlite_merkle_radix_tree_node::tree_id
                            .eq(tree_id)
                            .and(sqlite_merkle_radix_tree_node::hash.eq(&hash))
                            .and(sqlite_merkle_radix_tree_node::reference.gt(0)),
                    ),
                )
                .set(
                    sqlite_merkle_radix_tree_node::reference
                        .eq(sqlite_merkle_radix_tree_node::reference - 1),
                )
                .execute(self.conn)?;

                let node: sqlite::MerkleRadixTreeNode = sqlite_merkle_radix_tree_node::table
                    .find((&hash, tree_id))
                    .get_result(self.conn)?;

                if node.reference == 0 {
                    deleted_values.push(hash)
                }
            }

            sqlite_mark_leaves_pruned(self.conn, tree_id, &deleted_values)?;

            Ok(deleted_values)
        })
    }
}

#[cfg(feature = "postgres")]
const POSTGRES_NOW_MILLIS: &str =
    "TRUNC(EXTRACT(EPOCH FROM (SELECT NOW() AT TIME ZONE 'UTC')) * 1000)";

#[cfg(feature = "postgres")]
impl<'a> MerkleRadixPruneEntriesOperation for MerkleRadixOperations<'a, PgConnection> {
    fn prune_entries(&self, tree_id: i64, state_root: &str) -> Result<Vec<String>, InternalError> {
        self.conn.transaction(|| {
            let Candidates {
                deletions,
                parent,
                successor,
            } = get_deletion_candidates(self.conn, tree_id, state_root)?;

            if deletions.is_empty() {
                return Ok(vec![]);
            }

            update_changelogs(
                self.conn,
                tree_id,
                state_root,
                parent.as_deref(),
                successor.as_deref(),
                &deletions,
                POSTGRES_NOW_MILLIS,
            )?;

            let mut deleted_values = vec![];
            for hash in deletions.into_iter().rev() {
                let node: postgres::MerkleRadixTreeNode = update(
                    postgres_merkle_radix_tree_node::table.filter(
                        postgres_merkle_radix_tree_node::tree_id
                            .eq(tree_id)
                            .and(postgres_merkle_radix_tree_node::hash.eq(&hash)),
                    ),
                )
                .set(
                    postgres_merkle_radix_tree_node::reference
                        .eq(postgres_merkle_radix_tree_node::reference - 1),
                )
                .get_result(self.conn)?;

                if node.reference == 0 {
                    deleted_values.push(node.hash);
                }
            }

            postgres_mark_leaves_pruned(self.conn, tree_id, &deleted_values)?;

            Ok(deleted_values)
        })
    }
}

#[derive(Default)]
struct Candidates {
    deletions: Vec<String>,
    parent: Option<String>,
    successor: Option<String>,
}

fn get_deletion_candidates<C>(
    conn: &C,
    tree_id: i64,
    state_root: &str,
) -> Result<Candidates, InternalError>
where
    C: diesel::Connection,
    i64: diesel::deserialize::FromSql<diesel::sql_types::BigInt, C::Backend>,
    String: diesel::deserialize::FromSql<diesel::sql_types::Text, C::Backend>,
{
    let addition_changelog = merkle_radix_change_log_addition::table
        .filter(
            merkle_radix_change_log_addition::tree_id
                .eq(tree_id)
                .and(merkle_radix_change_log_addition::state_root.eq(state_root))
                .and(merkle_radix_change_log_addition::pruned_at.is_null()),
        )
        .get_results::<MerkleRadixChangeLogAddition>(conn)?;

    if addition_changelog.is_empty() {
        return Ok(Candidates::default());
    }

    let parent = addition_changelog
        .get(0)
        .and_then(|entry| entry.parent_state_root.clone());

    let change_additions = addition_changelog
        .into_iter()
        .map(|addition| addition.addition)
        .collect::<Vec<_>>();

    // Find all successors
    let successors = merkle_radix_change_log_deletion::table
        .filter(
            merkle_radix_change_log_deletion::tree_id
                .eq(tree_id)
                .and(merkle_radix_change_log_deletion::state_root.eq(state_root))
                .and(merkle_radix_change_log_deletion::pruned_at.is_null()),
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
        return Ok(Candidates::default());
    }

    let (deletions, successor) = if successors.is_empty() {
        // this root is the tip of the trie history
        (change_additions, None)
    } else {
        // we have one successor, based on our criteria, so we can safely unwrap
        let (successor_state_root, deletions) = successors.into_iter().next().unwrap();
        (deletions, Some(successor_state_root))
    };

    Ok(Candidates {
        deletions,
        parent,
        successor,
    })
}

fn update_changelogs<C>(
    conn: &C,
    tree_id: i64,
    state_root: &str,
    parent: Option<&str>,
    successor: Option<&str>,
    deletions: &[String],
    now_as_millis: &'static str,
) -> Result<(), InternalError>
where
    C: diesel::Connection,
    i64: diesel::deserialize::FromSql<diesel::sql_types::BigInt, C::Backend>,
    String: diesel::deserialize::FromSql<diesel::sql_types::Text, C::Backend>,
{
    // delete the deletions for this root.
    update(
        merkle_radix_change_log_deletion::table.filter(
            merkle_radix_change_log_deletion::tree_id
                .eq(tree_id)
                .and(merkle_radix_change_log_deletion::state_root.eq(state_root)),
        ),
    )
    .set(merkle_radix_change_log_deletion::pruned_at.eq(sql(now_as_millis)))
    .execute(conn)?;

    // prune any addition that was included in the deletions
    update(
        merkle_radix_change_log_addition::table.filter(
            merkle_radix_change_log_addition::tree_id
                .eq(tree_id)
                // .and(merkle_radix_change_log_addition::state_root.eq(state_root))
                .and(merkle_radix_change_log_addition::addition.eq_any(deletions)),
        ),
    )
    .set(merkle_radix_change_log_addition::pruned_at.eq(sql(now_as_millis)))
    .execute(conn)?;

    // If there is a successor, re-link the relationsships using the parent and the successor node
    if let Some(successor) = successor {
        // Re-link the parent's deletions to its successor, if any
        update(
            merkle_radix_change_log_deletion::table.filter(
                merkle_radix_change_log_deletion::tree_id
                    .eq(tree_id)
                    .and(merkle_radix_change_log_deletion::successor_state_root.eq(state_root)),
            ),
        )
        .set(merkle_radix_change_log_deletion::successor_state_root.eq(successor))
        .execute(conn)?;

        // Move the remainder of the additions by the pruned root to the successor root
        update(
            merkle_radix_change_log_addition::table.filter(
                merkle_radix_change_log_addition::tree_id
                    .eq(tree_id)
                    .and(merkle_radix_change_log_addition::state_root.eq(state_root))
                    .and(not(
                        merkle_radix_change_log_addition::addition.eq_any(deletions)
                    )),
            ),
        )
        .set(merkle_radix_change_log_addition::state_root.eq(successor))
        .execute(conn)?;
    }

    // Remove the parent relationship on its successors
    update(
        merkle_radix_change_log_addition::table
            .filter(merkle_radix_change_log_addition::parent_state_root.eq(state_root)),
    )
    .set(merkle_radix_change_log_addition::parent_state_root.eq(parent))
    .execute(conn)?;

    Ok(())
}

#[cfg(feature = "sqlite")]
fn sqlite_mark_leaves_pruned(
    conn: &SqliteConnection,
    tree_id: i64,
    node_hashs: &[String],
) -> Result<(), InternalError> {
    let leaf_ids: Vec<Option<i64>> = sqlite_merkle_radix_tree_node::table
        .select(sqlite_merkle_radix_tree_node::leaf_id)
        .filter(
            sqlite_merkle_radix_tree_node::tree_id
                .eq(tree_id)
                .and(sqlite_merkle_radix_tree_node::hash.eq_any(node_hashs))
                .and(sqlite_merkle_radix_tree_node::leaf_id.is_not_null()),
        )
        .get_results(conn)?;

    let leaf_ids: Vec<i64> = leaf_ids.into_iter().flatten().collect();

    update(merkle_radix_leaf::table)
        .set(merkle_radix_leaf::pruned_at.eq(1))
        .filter(merkle_radix_leaf::id.eq_any(leaf_ids))
        .execute(conn)?;

    Ok(())
}

#[cfg(feature = "postgres")]
fn postgres_mark_leaves_pruned(
    conn: &PgConnection,
    tree_id: i64,
    node_hashs: &[String],
) -> Result<(), InternalError> {
    let leaf_ids: Vec<Option<i64>> = postgres_merkle_radix_tree_node::table
        .select(postgres_merkle_radix_tree_node::leaf_id)
        .filter(
            postgres_merkle_radix_tree_node::tree_id
                .eq(tree_id)
                .and(postgres_merkle_radix_tree_node::hash.eq_any(node_hashs))
                .and(postgres_merkle_radix_tree_node::leaf_id.is_not_null()),
        )
        .get_results(conn)?;

    let leaf_ids: Vec<i64> = leaf_ids.into_iter().flatten().collect();

    update(merkle_radix_leaf::table)
        .set(merkle_radix_leaf::pruned_at.eq(1))
        .filter(merkle_radix_leaf::id.eq_any(leaf_ids))
        .execute(conn)?;

    Ok(())
}

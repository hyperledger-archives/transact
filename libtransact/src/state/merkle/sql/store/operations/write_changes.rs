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

use diesel::dsl::{insert_into, max};
use diesel::prelude::*;
#[cfg(feature = "sqlite")]
use diesel::sql_types::{BigInt, Nullable, Text};

use crate::error::InternalError;
use crate::state::merkle::node::Node;
#[cfg(feature = "sqlite")]
use crate::state::merkle::sql::store::models::sqlite;
#[cfg(feature = "postgres")]
use crate::state::merkle::sql::store::models::{postgres, MerkleRadixLeaf};
use crate::state::merkle::sql::store::models::{
    NewMerkleRadixChangeLogAddition, NewMerkleRadixChangeLogDeletion, NewMerkleRadixLeaf,
};
use crate::state::merkle::sql::store::schema::{
    merkle_radix_change_log_addition, merkle_radix_change_log_deletion, merkle_radix_leaf,
};
use crate::state::merkle::sql::store::TreeUpdate;

use super::prepared_stmt::prepare_stmt;
use super::MerkleRadixOperations;

struct InsertableNode<'a> {
    pub hash: &'a str,
    pub node: &'a Node,
    pub address: &'a str,
}

pub(in crate::state::merkle::sql) trait MerkleRadixWriteChangesOperation {
    fn write_changes(
        &self,
        tree_id: i64,
        state_root: &str,
        parent_state_root: &str,
        update: &TreeUpdate,
    ) -> Result<(), InternalError>;
}

#[cfg(feature = "sqlite")]
impl<'a> MerkleRadixWriteChangesOperation for MerkleRadixOperations<'a, SqliteConnection> {
    fn write_changes(
        &self,
        tree_id: i64,
        state_root: &str,
        parent_state_root: &str,
        update: &TreeUpdate,
    ) -> Result<(), InternalError> {
        self.conn.transaction::<_, InternalError, _>(|| {
            // We manually increment the id, so we don't have to insert one at a time and fetch
            // back the resulting id.
            let initial_id: i64 = merkle_radix_leaf::table
                .select(max(merkle_radix_leaf::id))
                .first::<Option<i64>>(self.conn)?
                .unwrap_or(0);

            let nodes = update
                .node_changes
                .iter()
                .map(|(hash, node, address)| InsertableNode {
                    hash,
                    node,
                    address,
                })
                .collect::<Vec<_>>();

            let leaves = nodes
                .iter()
                .enumerate()
                .filter_map(|(i, insertable_node)| {
                    insertable_node.node.value.as_deref().map(|data| {
                        Ok(NewMerkleRadixLeaf {
                            id: Some(initial_id.checked_add(1 + i as i64).ok_or_else(|| {
                                InternalError::with_message("exceeded id space".into())
                            })?),
                            tree_id,
                            address: insertable_node.address,
                            data,
                        })
                    })
                })
                .collect::<Result<Vec<NewMerkleRadixLeaf>, InternalError>>()?;

            let leaf_ids: HashMap<&str, i64> = leaves
                .iter()
                .filter_map(|new_leaf| new_leaf.id.map(|id| (new_leaf.address, id)))
                .collect();

            insert_into(merkle_radix_leaf::table)
                .values(leaves)
                .execute(self.conn)?;

            let node_models = nodes
                .iter()
                .map::<Result<sqlite::MerkleRadixTreeNode, InternalError>, _>(|insertable_node| {
                    Ok(
                        sqlite::MerkleRadixTreeNode::new(insertable_node.hash, tree_id)
                            .with_leaf_id(leaf_ids.get(insertable_node.address).copied())
                            .with_children(node_to_children(insertable_node.node)?),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;

            let node_models = dedup_sqlite(node_models);

            // As diesel 1.4.x doesn't currently support the on_conflict function for the Sqlite
            // DB, we manually have to perform the upsert on each row.
            for node in node_models {
                prepare_stmt(
                    r#"
                    INSERT INTO merkle_radix_tree_node
                        (hash, tree_id, leaf_id, children, reference)
                        VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (hash, tree_id)
                        DO UPDATE SET reference = reference + ?
                    "#,
                )
                .bind::<Text, _>(node.hash)
                .bind::<BigInt, _>(node.tree_id)
                .bind::<Nullable<BigInt>, _>(node.leaf_id)
                .bind::<Text, _>(node.children)
                .bind::<BigInt, _>(node.reference)
                .bind::<BigInt, _>(node.reference)
                .execute(self.conn)
                .map_err(|err| InternalError::from_source(Box::new(err)))?;
            }

            // Update the change log
            let additions = update
                .node_changes
                .iter()
                .map(|(hash, _, _)| hash.as_ref())
                .collect::<Vec<_>>();
            let deletions = update
                .deletions
                .iter()
                .map(|s| s.as_ref())
                .collect::<Vec<_>>();

            let change_log_additions = additions
                .iter()
                .map(|hash| NewMerkleRadixChangeLogAddition {
                    state_root,
                    tree_id,
                    parent_state_root: Some(parent_state_root),
                    addition: hash,
                    pruned_at: None,
                })
                .collect::<Vec<_>>();

            insert_into(merkle_radix_change_log_addition::table)
                .values(change_log_additions)
                .execute(self.conn)?;

            let change_log_deletions = deletions
                .iter()
                .map(|hash| NewMerkleRadixChangeLogDeletion {
                    state_root: parent_state_root,
                    tree_id,
                    successor_state_root: state_root,
                    deletion: hash,
                    pruned_at: None,
                })
                .collect::<Vec<_>>();

            insert_into(merkle_radix_change_log_deletion::table)
                .values(change_log_deletions)
                .execute(self.conn)?;

            Ok(())
        })
    }
}

#[cfg(feature = "postgres")]
impl<'a> MerkleRadixWriteChangesOperation for MerkleRadixOperations<'a, PgConnection> {
    fn write_changes(
        &self,
        tree_id: i64,
        state_root: &str,
        parent_state_root: &str,
        update: &TreeUpdate,
    ) -> Result<(), InternalError> {
        self.conn.transaction::<_, InternalError, _>(|| {
            let nodes = update
                .node_changes
                .iter()
                .map(|(hash, node, address)| InsertableNode {
                    hash,
                    node,
                    address,
                })
                .collect::<Vec<_>>();

            let leaves = nodes
                .iter()
                .filter_map(|insertable_node| {
                    insertable_node.node.value.as_deref().map(|data| {
                        Ok(NewMerkleRadixLeaf {
                            id: None,
                            tree_id,
                            address: insertable_node.address,
                            data,
                        })
                    })
                })
                .collect::<Result<Vec<NewMerkleRadixLeaf>, InternalError>>()?;

            let inserted: Vec<MerkleRadixLeaf> = insert_into(merkle_radix_leaf::table)
                .values(leaves)
                .get_results(self.conn)?;

            let leaf_ids: HashMap<&str, i64> = inserted
                .iter()
                .map(|new_leaf| (&*new_leaf.address, new_leaf.id))
                .collect();

            let node_models: Vec<postgres::MerkleRadixTreeNode> = nodes
                .iter()
                .map::<Result<postgres::MerkleRadixTreeNode, InternalError>, _>(|insertable_node| {
                    Ok(
                        postgres::MerkleRadixTreeNode::new(insertable_node.hash, tree_id)
                            .with_leaf_id(leaf_ids.get(insertable_node.address).copied())
                            .with_children(node_to_children(insertable_node.node)?),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;

            let node_models = dedup_postgres(node_models);

            for node in node_models {
                prepare_stmt(
                    r#"
                    INSERT INTO merkle_radix_tree_node
                        (hash, tree_id, leaf_id, children, reference)
                        VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (hash, tree_id)
                        DO UPDATE
                        SET reference = EXCLUDED.reference + $5
                    "#,
                )
                .bind::<diesel::sql_types::VarChar, _>(node.hash)
                .bind::<BigInt, _>(node.tree_id)
                .bind::<Nullable<BigInt>, _>(node.leaf_id)
                .bind::<diesel::sql_types::Array<Nullable<diesel::sql_types::VarChar>>, _>(
                    node.children,
                )
                .bind::<BigInt, _>(node.reference)
                .execute(self.conn)
                .map_err(|err| InternalError::from_source(Box::new(err)))?;
            }

            // Update the change log
            let additions = update
                .node_changes
                .iter()
                .map(|(hash, _, _)| hash.as_ref())
                .collect::<Vec<_>>();
            let deletions = update
                .deletions
                .iter()
                .map(|s| s.as_ref())
                .collect::<Vec<_>>();

            let change_log_additions = additions
                .iter()
                .map(|hash| NewMerkleRadixChangeLogAddition {
                    state_root,
                    tree_id,
                    parent_state_root: Some(parent_state_root),
                    addition: hash,
                    pruned_at: None,
                })
                .collect::<Vec<_>>();

            insert_into(merkle_radix_change_log_addition::table)
                .values(change_log_additions)
                .execute(self.conn)?;

            let change_log_deletions = deletions
                .iter()
                .map(|hash| NewMerkleRadixChangeLogDeletion {
                    state_root: parent_state_root,
                    tree_id,
                    successor_state_root: state_root,
                    deletion: hash,
                    pruned_at: None,
                })
                .collect::<Vec<_>>();

            insert_into(merkle_radix_change_log_deletion::table)
                .values(change_log_deletions)
                .execute(self.conn)?;

            Ok(())
        })
    }
}

#[cfg(feature = "postgres")]
fn dedup_postgres(nodes: Vec<postgres::MerkleRadixTreeNode>) -> Vec<postgres::MerkleRadixTreeNode> {
    let mut deduped_nodes: Vec<postgres::MerkleRadixTreeNode> = vec![];
    let mut node_map = HashMap::new();
    for node in nodes.into_iter() {
        if let Some(index) = node_map.get(&node.hash) {
            let node: &mut postgres::MerkleRadixTreeNode = &mut deduped_nodes[*index];
            node.reference += 1;
        } else {
            node_map.insert(node.hash.clone(), deduped_nodes.len());
            deduped_nodes.push(node);
        }
    }

    deduped_nodes
}

#[cfg(feature = "sqlite")]
fn dedup_sqlite(nodes: Vec<sqlite::MerkleRadixTreeNode>) -> Vec<sqlite::MerkleRadixTreeNode> {
    let mut deduped_nodes: Vec<sqlite::MerkleRadixTreeNode> = vec![];
    let mut node_map = HashMap::new();
    for node in nodes.into_iter() {
        if let Some(index) = node_map.get(&node.hash) {
            let node: &mut sqlite::MerkleRadixTreeNode = &mut deduped_nodes[*index];
            node.reference += 1;
        } else {
            node_map.insert(node.hash.clone(), deduped_nodes.len());
            deduped_nodes.push(node);
        }
    }

    deduped_nodes
}

fn node_to_children(node: &Node) -> Result<Vec<Option<String>>, InternalError> {
    let mut children = vec![None; 256];
    for (location, hash) in node.children.iter() {
        let pos = u8::from_str_radix(location, 16)
            .map_err(|err| InternalError::from_source(Box::new(err)))?;

        children[pos as usize] = Some(hash.to_string());
    }
    Ok(children)
}

impl From<diesel::result::Error> for InternalError {
    fn from(err: diesel::result::Error) -> Self {
        InternalError::from_source(Box::new(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use diesel::dsl::count;

    #[cfg(feature = "sqlite")]
    use crate::state::merkle::sql::migration;
    use crate::state::merkle::sql::store::models::MerkleRadixLeaf;
    #[cfg(feature = "sqlite")]
    use crate::state::merkle::sql::store::schema::sqlite_merkle_radix_tree_node;
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    use crate::state::merkle::sql::{
        backend::postgres::test::run_postgres_test, store::schema::postgres_merkle_radix_tree_node,
    };

    /// This test inserts a single node (that of the initial state root) and verifies that it is
    /// correctly inserted into the node table.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_insert_single_node() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        migration::sqlite::run_migrations(&conn)?;

        let operations = MerkleRadixOperations::new(&conn);

        operations.write_changes(
            1,
            "initial-state-root",
            "initial-state-root",
            &TreeUpdate {
                node_changes: vec![(
                    "initial-state-root".into(),
                    Node {
                        value: None,
                        children: Default::default(),
                    },
                    String::new(),
                )],
                ..Default::default()
            },
        )?;

        assert_eq!(
            merkle_radix_leaf::table
                .select(count(merkle_radix_leaf::id))
                .get_result::<i64>(&conn)?,
            0
        );

        let nodes = sqlite_merkle_radix_tree_node::table
            .get_results::<sqlite::MerkleRadixTreeNode>(&conn)?;

        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes[0],
            sqlite::MerkleRadixTreeNode::new("initial-state-root", 1),
        );

        Ok(())
    }

    /// This test inserts a single node (that of the initial state root) and verifies that it is
    /// correctly inserted into the node table.
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_insert_single_node() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;

            let operations = MerkleRadixOperations::new(&conn);

            operations.write_changes(
                1,
                "initial-state-root",
                "initial-state-root",
                &TreeUpdate {
                    node_changes: vec![(
                        "initial-state-root".into(),
                        Node {
                            value: None,
                            children: Default::default(),
                        },
                        String::new(),
                    )],
                    ..Default::default()
                },
            )?;

            assert_eq!(
                merkle_radix_leaf::table
                    .select(count(merkle_radix_leaf::id))
                    .get_result::<i64>(&conn)?,
                0
            );

            let nodes = postgres_merkle_radix_tree_node::table
                .get_results::<postgres::MerkleRadixTreeNode>(&conn)?;

            assert_eq!(nodes.len(), 1);
            assert_eq!(
                nodes[0],
                postgres::MerkleRadixTreeNode::new("initial-state-root", 1)
            );

            Ok(())
        })
    }

    /// This test inserts a set of nodes where the deepest node references a leaf.  It verifies
    /// that the nodes have been inserted into the node table, and the leave has been inserted into
    /// the leaf table.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_write_changes_with_leaf() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        migration::sqlite::run_migrations(&conn)?;

        let operations = MerkleRadixOperations::new(&conn);

        let update = TreeUpdate {
            node_changes: vec![
                (
                    "state-root".into(),
                    Node {
                        value: None,
                        children: single_child_btree("0a", "first-node-hash"),
                    },
                    String::new(),
                ),
                (
                    "first-node-hash".into(),
                    Node {
                        value: None,
                        children: single_child_btree("01", "second-node-hash"),
                    },
                    "0a".into(),
                ),
                (
                    "second-node-hash".into(),
                    Node {
                        value: None,
                        children: single_child_btree("ff", "leaf-node-hash"),
                    },
                    "0a01".into(),
                ),
                (
                    "leaf-node-hash".into(),
                    Node {
                        value: Some(b"hello".to_vec()),
                        children: BTreeMap::default(),
                    },
                    "0a01ff".into(),
                ),
            ],
            ..Default::default()
        };

        operations.write_changes(1, "state-root", "state-root", &update)?;

        let leaves = merkle_radix_leaf::table.get_results::<MerkleRadixLeaf>(&conn)?;
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].address, "0a01ff");
        assert_eq!(leaves[0].data, b"hello");

        let nodes = sqlite_merkle_radix_tree_node::table
            .get_results::<sqlite::MerkleRadixTreeNode>(&conn)?;

        assert_eq!(
            nodes,
            vec![
                sqlite::MerkleRadixTreeNode::new("state-root", 1)
                    .with_children(single_db_child(10, "first-node-hash")),
                sqlite::MerkleRadixTreeNode::new("first-node-hash", 1)
                    .with_children(single_db_child(1, "second-node-hash")),
                sqlite::MerkleRadixTreeNode::new("second-node-hash", 1)
                    .with_children(single_db_child(255, "leaf-node-hash")),
                sqlite::MerkleRadixTreeNode::new("leaf-node-hash", 1)
                    .with_leaf_id(Some(leaves[0].id)),
            ]
        );

        Ok(())
    }

    /// This test inserts a set of nodes where the deepest node references a leaf.  It verifies
    /// that the nodes have been inserted into the node table, and the leave has been inserted into
    /// the leaf table.
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_write_changes_with_leaf() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;

            let operations = MerkleRadixOperations::new(&conn);

            let update = TreeUpdate {
                node_changes: vec![
                    (
                        "state-root".into(),
                        Node {
                            value: None,
                            children: single_child_btree("0a", "first-node-hash"),
                        },
                        String::new(),
                    ),
                    (
                        "first-node-hash".into(),
                        Node {
                            value: None,
                            children: single_child_btree("01", "second-node-hash"),
                        },
                        "0a".into(),
                    ),
                    (
                        "second-node-hash".into(),
                        Node {
                            value: None,
                            children: single_child_btree("ff", "leaf-node-hash"),
                        },
                        "0a01".into(),
                    ),
                    (
                        "leaf-node-hash".into(),
                        Node {
                            value: Some(b"hello".to_vec()),
                            children: BTreeMap::default(),
                        },
                        "0a01ff".into(),
                    ),
                ],
                ..Default::default()
            };

            operations.write_changes(1, "state-root", "state-root", &update)?;

            let leaves = merkle_radix_leaf::table.get_results::<MerkleRadixLeaf>(&conn)?;
            assert_eq!(leaves.len(), 1);
            assert_eq!(leaves[0].address, "0a01ff");
            assert_eq!(leaves[0].data, b"hello");

            let nodes = postgres_merkle_radix_tree_node::table
                .get_results::<postgres::MerkleRadixTreeNode>(&conn)?;

            assert_eq!(
                nodes,
                vec![
                    postgres::MerkleRadixTreeNode::new("state-root", 1)
                        .with_children(single_db_child(10, "first-node-hash")),
                    postgres::MerkleRadixTreeNode::new("first-node-hash", 1)
                        .with_children(single_db_child(1, "second-node-hash")),
                    postgres::MerkleRadixTreeNode::new("second-node-hash", 1)
                        .with_children(single_db_child(255, "leaf-node-hash")),
                    postgres::MerkleRadixTreeNode::new("leaf-node-hash", 1)
                        .with_leaf_id(Some(leaves[0].id)),
                ]
            );

            Ok(())
        })
    }

    fn single_db_child(pos: usize, hash: &str) -> Vec<Option<String>> {
        let mut children = vec![None; 256];

        children[pos] = Some(hash.into());

        children
    }

    fn single_child_btree(addr_part: &str, hash: &str) -> BTreeMap<String, String> {
        let mut children = BTreeMap::new();
        children.insert(addr_part.to_string(), hash.to_string());
        children
    }
}

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

#[cfg(feature = "sqlite")]
use diesel::dsl::insert_or_ignore_into;
use diesel::dsl::{insert_into, max};
use diesel::prelude::*;

use crate::error::InternalError;
use crate::state::merkle::node::Node;
#[cfg(feature = "postgres")]
use crate::state::merkle::sql::models::postgres;
#[cfg(feature = "sqlite")]
use crate::state::merkle::sql::models::sqlite;
use crate::state::merkle::sql::models::NewMerkleRadixLeaf;
use crate::state::merkle::sql::schema::merkle_radix_leaf;
#[cfg(feature = "postgres")]
use crate::state::merkle::sql::schema::postgres_merkle_radix_tree_node;
#[cfg(feature = "sqlite")]
use crate::state::merkle::sql::schema::sqlite_merkle_radix_tree_node;

use super::MerkleRadixOperations;

pub struct InsertableNode {
    pub hash: String,
    pub node: Node,
    pub address: String,
}

pub trait MerkleRadixInsertNodesOperation {
    fn insert_nodes(&self, tree_id: i64, nodes: &[InsertableNode]) -> Result<(), InternalError>;
}

#[cfg(feature = "sqlite")]
impl<'a> MerkleRadixInsertNodesOperation for MerkleRadixOperations<'a, SqliteConnection> {
    fn insert_nodes(&self, tree_id: i64, nodes: &[InsertableNode]) -> Result<(), InternalError> {
        self.conn.transaction::<_, InternalError, _>(|| {
            // We manually increment the id, so we don't have to insert one at a time and fetch
            // back the resulting id.
            let initial_id: i64 = merkle_radix_leaf::table
                .select(max(merkle_radix_leaf::id))
                .first::<Option<i64>>(self.conn)?
                .unwrap_or(0);

            let leaves = nodes
                .iter()
                .enumerate()
                .filter_map(|(i, insertable_node)| {
                    insertable_node.node.value.as_deref().map(|data| {
                        Ok(NewMerkleRadixLeaf {
                            id: initial_id.checked_add(1 + i as i64).ok_or_else(|| {
                                InternalError::with_message("exceeded id space".into())
                            })?,
                            tree_id,
                            address: &insertable_node.address,
                            data,
                        })
                    })
                })
                .collect::<Result<Vec<NewMerkleRadixLeaf>, InternalError>>()?;

            let leaf_ids: HashMap<&str, i64> = leaves
                .iter()
                .map(|new_leaf| (new_leaf.address, new_leaf.id))
                .collect();

            insert_into(merkle_radix_leaf::table)
                .values(leaves)
                .execute(self.conn)?;

            let node_models = nodes
                .iter()
                .map::<Result<sqlite::MerkleRadixTreeNode, InternalError>, _>(|insertable_node| {
                    Ok(sqlite::MerkleRadixTreeNode {
                        hash: insertable_node.hash.clone(),
                        tree_id,
                        leaf_id: leaf_ids.get(insertable_node.address.as_str()).copied(),
                        children: node_to_sqlite_children(&insertable_node.node)?,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            insert_or_ignore_into(sqlite_merkle_radix_tree_node::table)
                .values(node_models)
                .execute(self.conn)?;

            Ok(())
        })
    }
}

#[cfg(feature = "sqlite")]
fn node_to_sqlite_children(node: &Node) -> Result<sqlite::Children, InternalError> {
    let mut children = vec![None; 256];
    for (location, hash) in node.children.iter() {
        let pos = u8::from_str_radix(location, 16)
            .map_err(|err| InternalError::from_source(Box::new(err)))?;

        children[pos as usize] = Some(hash.to_string());
    }
    Ok(sqlite::Children(children))
}

#[cfg(feature = "postgres")]
impl<'a> MerkleRadixInsertNodesOperation for MerkleRadixOperations<'a, PgConnection> {
    fn insert_nodes(&self, tree_id: i64, nodes: &[InsertableNode]) -> Result<(), InternalError> {
        self.conn.transaction::<_, InternalError, _>(|| {
            // We manually increment the id, so we don't have to insert one at a time and fetch
            // back the resulting id.
            let initial_id: i64 = merkle_radix_leaf::table
                .select(max(merkle_radix_leaf::id))
                .first::<Option<i64>>(self.conn)?
                .unwrap_or(0);

            let leaves = nodes
                .iter()
                .enumerate()
                .filter_map(|(i, insertable_node)| {
                    insertable_node.node.value.as_deref().map(|data| {
                        Ok(NewMerkleRadixLeaf {
                            id: initial_id.checked_add(1 + i as i64).ok_or_else(|| {
                                InternalError::with_message("exceeded id space".into())
                            })?,
                            tree_id,
                            address: &insertable_node.address,
                            data,
                        })
                    })
                })
                .collect::<Result<Vec<NewMerkleRadixLeaf>, InternalError>>()?;

            let leaf_ids: HashMap<&str, i64> = leaves
                .iter()
                .map(|new_leaf| (new_leaf.address, new_leaf.id))
                .collect();

            insert_into(merkle_radix_leaf::table)
                .values(leaves)
                .execute(self.conn)?;

            let node_models: Vec<postgres::MerkleRadixTreeNode> = nodes
                .iter()
                .map::<Result<postgres::MerkleRadixTreeNode, InternalError>, _>(|insertable_node| {
                    Ok(postgres::MerkleRadixTreeNode {
                        hash: insertable_node.hash.clone(),
                        tree_id,
                        leaf_id: leaf_ids.get(insertable_node.address.as_str()).copied(),
                        children: node_to_postgres_children(&insertable_node.node)?,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            insert_into(postgres_merkle_radix_tree_node::table)
                .values(&node_models)
                .on_conflict_do_nothing()
                .execute(self.conn)?;

            Ok(())
        })
    }
}

#[cfg(feature = "postgres")]
fn node_to_postgres_children(node: &Node) -> Result<Vec<Option<String>>, InternalError> {
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

    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    use crate::state::merkle::sql::backend::postgres::test::run_postgres_test;
    #[cfg(feature = "sqlite")]
    use crate::state::merkle::sql::migration;
    use crate::state::merkle::sql::models::MerkleRadixLeaf;

    /// This test inserts a single node (that of the initial state root) and verifies that it is
    /// correctly inserted into the node table.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_insert_single_node() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        migration::sqlite::run_migrations(&conn)?;

        let operations = MerkleRadixOperations::new(&conn);

        operations.insert_nodes(
            1,
            &vec![InsertableNode {
                hash: "initial-state-root".into(),
                node: Node {
                    value: None,
                    children: Default::default(),
                },
                address: String::new(),
            }],
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
            sqlite::MerkleRadixTreeNode {
                hash: "initial-state-root".into(),
                tree_id: 1,
                leaf_id: None,
                children: sqlite::Children(vec![None; 256]),
            }
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

            operations.insert_nodes(
                1,
                &vec![InsertableNode {
                    hash: "initial-state-root".into(),
                    node: Node {
                        value: None,
                        children: Default::default(),
                    },
                    address: String::new(),
                }],
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
                postgres::MerkleRadixTreeNode {
                    hash: "initial-state-root".into(),
                    tree_id: 1,
                    leaf_id: None,
                    children: vec![None; 256],
                }
            );

            Ok(())
        })
    }

    /// This test inserts a set of nodes where the deepest node references a leaf.  It verifies
    /// that the nodes have been inserted into the node table, and the leave has been inserted into
    /// the leaf table.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_insert_nodes_with_leaf() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        migration::sqlite::run_migrations(&conn)?;

        let operations = MerkleRadixOperations::new(&conn);

        let nodes = vec![
            InsertableNode {
                hash: "state-root".into(),
                node: Node {
                    value: None,
                    children: single_child_btree("0a", "first-node-hash"),
                },
                address: String::new(),
            },
            InsertableNode {
                hash: "first-node-hash".into(),
                node: Node {
                    value: None,
                    children: single_child_btree("01", "second-node-hash"),
                },
                address: "0a".into(),
            },
            InsertableNode {
                hash: "second-node-hash".into(),
                node: Node {
                    value: None,
                    children: single_child_btree("ff", "leaf-node-hash"),
                },
                address: "0a01".into(),
            },
            InsertableNode {
                hash: "leaf-node-hash".into(),
                node: Node {
                    value: Some(b"hello".to_vec()),
                    children: BTreeMap::default(),
                },
                address: "0a01ff".into(),
            },
        ];

        operations.insert_nodes(1, &nodes)?;

        let leaves = merkle_radix_leaf::table.get_results::<MerkleRadixLeaf>(&conn)?;
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].address, "0a01ff");
        assert_eq!(leaves[0].data, b"hello");

        let nodes = sqlite_merkle_radix_tree_node::table
            .get_results::<sqlite::MerkleRadixTreeNode>(&conn)?;

        assert_eq!(
            nodes,
            vec![
                sqlite::MerkleRadixTreeNode {
                    hash: "state-root".into(),
                    tree_id: 1,
                    leaf_id: None,
                    children: single_sqlite_child(10, "first-node-hash"),
                },
                sqlite::MerkleRadixTreeNode {
                    hash: "first-node-hash".into(),
                    tree_id: 1,
                    leaf_id: None,
                    children: single_sqlite_child(1, "second-node-hash"),
                },
                sqlite::MerkleRadixTreeNode {
                    hash: "second-node-hash".into(),
                    tree_id: 1,
                    leaf_id: None,
                    children: single_sqlite_child(255, "leaf-node-hash"),
                },
                sqlite::MerkleRadixTreeNode {
                    hash: "leaf-node-hash".into(),
                    tree_id: 1,
                    leaf_id: Some(leaves[0].id),
                    children: sqlite::Children(vec![None; 256])
                },
            ]
        );

        Ok(())
    }

    /// This test inserts a set of nodes where the deepest node references a leaf.  It verifies
    /// that the nodes have been inserted into the node table, and the leave has been inserted into
    /// the leaf table.
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_insert_nodes_with_leaf() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;

            let operations = MerkleRadixOperations::new(&conn);

            let nodes = vec![
                InsertableNode {
                    hash: "state-root".into(),
                    node: Node {
                        value: None,
                        children: single_child_btree("0a", "first-node-hash"),
                    },
                    address: String::new(),
                },
                InsertableNode {
                    hash: "first-node-hash".into(),
                    node: Node {
                        value: None,
                        children: single_child_btree("01", "second-node-hash"),
                    },
                    address: "0a".into(),
                },
                InsertableNode {
                    hash: "second-node-hash".into(),
                    node: Node {
                        value: None,
                        children: single_child_btree("ff", "leaf-node-hash"),
                    },
                    address: "0a01".into(),
                },
                InsertableNode {
                    hash: "leaf-node-hash".into(),
                    node: Node {
                        value: Some(b"hello".to_vec()),
                        children: BTreeMap::default(),
                    },
                    address: "0a01ff".into(),
                },
            ];

            operations.insert_nodes(1, &nodes)?;

            let leaves = merkle_radix_leaf::table.get_results::<MerkleRadixLeaf>(&conn)?;
            assert_eq!(leaves.len(), 1);
            assert_eq!(leaves[0].address, "0a01ff");
            assert_eq!(leaves[0].data, b"hello");

            let nodes = postgres_merkle_radix_tree_node::table
                .get_results::<postgres::MerkleRadixTreeNode>(&conn)?;

            assert_eq!(
                nodes,
                vec![
                    postgres::MerkleRadixTreeNode {
                        hash: "state-root".into(),
                        tree_id: 1,
                        leaf_id: None,
                        children: single_db_child(10, "first-node-hash"),
                    },
                    postgres::MerkleRadixTreeNode {
                        hash: "first-node-hash".into(),
                        tree_id: 1,
                        leaf_id: None,
                        children: single_db_child(1, "second-node-hash"),
                    },
                    postgres::MerkleRadixTreeNode {
                        hash: "second-node-hash".into(),
                        tree_id: 1,
                        leaf_id: None,
                        children: single_db_child(255, "leaf-node-hash"),
                    },
                    postgres::MerkleRadixTreeNode {
                        hash: "leaf-node-hash".into(),
                        tree_id: 1,
                        leaf_id: Some(leaves[0].id),
                        children: vec![None; 256]
                    },
                ]
            );

            Ok(())
        })
    }

    #[cfg(feature = "sqlite")]
    fn single_sqlite_child(pos: usize, hash: &str) -> sqlite::Children {
        sqlite::Children(single_db_child(pos, hash))
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

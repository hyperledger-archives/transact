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

use diesel::dsl::{insert_into, insert_or_ignore_into, max};
use diesel::prelude::*;

use crate::error::InternalError;
use crate::state::merkle::node::Node;
use crate::state::merkle::sql::models::{Children, MerkleRadixTreeNode, NewMerkleRadixLeaf};
use crate::state::merkle::sql::schema::{merkle_radix_leaf, merkle_radix_tree_node};

use super::MerkleRadixOperations;

pub struct InsertableNode {
    pub hash: String,
    pub node: Node,
    pub address: String,
}

#[cfg_attr(test, derive(Debug, PartialEq))]
pub struct IndexableLeafInfo {
    pub address: String,
    pub leaf_id: i64,
}

pub trait MerkleRadixInsertNodesOperation {
    fn insert_nodes(
        &self,
        nodes: &[InsertableNode],
    ) -> Result<Vec<IndexableLeafInfo>, InternalError>;
}

#[cfg(feature = "sqlite")]
impl<'a> MerkleRadixInsertNodesOperation for MerkleRadixOperations<'a, SqliteConnection> {
    fn insert_nodes(
        &self,
        nodes: &[InsertableNode],
    ) -> Result<Vec<IndexableLeafInfo>, InternalError> {
        self.conn.transaction::<_, InternalError, _>(|| {
            // We manually increment the id, so we don't have to insert one at a time and fetch
            // back the resulting id.
            let initial_id: i64 = merkle_radix_leaf::table
                .select(max(merkle_radix_leaf::id))
                .first::<Option<i64>>(self.conn)?
                .unwrap_or(1);

            let leaves = nodes
                .iter()
                .filter(|insertable_node| insertable_node.node.value.is_some())
                .enumerate()
                .map(|(i, insertable_node)| {
                    if let Some(data) = insertable_node.node.value.as_deref() {
                        Ok(NewMerkleRadixLeaf {
                            id: initial_id.checked_add(1 + i as i64).ok_or_else(|| {
                                InternalError::with_message("exceeded id space".into())
                            })?,
                            address: &insertable_node.address,
                            data,
                        })
                    } else {
                        // we already filtered out the None values
                        unreachable!()
                    }
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
                .map::<Result<MerkleRadixTreeNode, InternalError>, _>(|insertable_node| {
                    Ok(MerkleRadixTreeNode {
                        hash: insertable_node.hash.clone(),
                        leaf_id: leaf_ids.get(insertable_node.address.as_str()).copied(),
                        children: node_to_children(&insertable_node.node)?,
                    })
                })
                .collect::<Result<Vec<MerkleRadixTreeNode>, _>>()?;

            insert_or_ignore_into(merkle_radix_tree_node::table)
                .values(node_models)
                .execute(self.conn)?;

            Ok(leaf_ids
                .into_iter()
                .map(|(address, leaf_id)| IndexableLeafInfo {
                    address: address.into(),
                    leaf_id,
                })
                .collect())
        })
    }
}

fn node_to_children(node: &Node) -> Result<Children, InternalError> {
    let mut children = vec![None; 256];
    for (location, hash) in node.children.iter() {
        let pos = u8::from_str_radix(&location, 16)
            .map_err(|err| InternalError::from_source(Box::new(err)))?;

        children[pos as usize] = Some(hash.to_string());
    }
    Ok(Children(children))
}

impl From<diesel::result::Error> for InternalError {
    fn from(err: diesel::result::Error) -> Self {
        InternalError::from_source(Box::new(err))
    }
}

#[cfg(feature = "sqlite")]
#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use diesel::dsl::count;

    use crate::state::merkle::sql::migration::sqlite::run_migrations;
    use crate::state::merkle::sql::models::MerkleRadixLeaf;

    /// This test inserts a single node (that of the initial state root) and verifies that it is
    /// correctly inserted into the node table.
    #[test]
    fn test_insert_single_node() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        run_migrations(&conn)?;

        let operations = MerkleRadixOperations::new(&conn);

        let indexable_info = operations.insert_nodes(&vec![InsertableNode {
            hash: "initial-state-root".into(),
            node: Node {
                value: None,
                children: Default::default(),
            },
            address: String::new(),
        }])?;

        assert!(
            indexable_info.is_empty(),
            "Expected no indexable information, but received some"
        );

        assert_eq!(
            merkle_radix_leaf::table
                .select(count(merkle_radix_leaf::id))
                .get_result::<i64>(&conn)?,
            0
        );

        let nodes = merkle_radix_tree_node::table.get_results::<MerkleRadixTreeNode>(&conn)?;

        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes[0],
            MerkleRadixTreeNode {
                hash: "initial-state-root".into(),
                leaf_id: None,
                children: Children(vec![None; 256]),
            }
        );

        Ok(())
    }

    /// This test inserts a set of nodes where the deepest node references a leaf.  It verifies
    /// that the nodes have been inserted into the node table, and the leave has been inserted into
    /// the leaf table.
    #[test]
    fn test_insert_nodes_with_leaf() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        run_migrations(&conn)?;

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

        let indexable_info = operations.insert_nodes(&nodes)?;

        let leaves = merkle_radix_leaf::table.get_results::<MerkleRadixLeaf>(&conn)?;
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].address, "0a01ff");
        assert_eq!(leaves[0].data, b"hello");

        assert_eq!(
            indexable_info,
            vec![IndexableLeafInfo {
                address: "0a01ff".into(),
                leaf_id: leaves[0].id
            }]
        );

        let nodes = merkle_radix_tree_node::table.get_results::<MerkleRadixTreeNode>(&conn)?;

        assert_eq!(
            nodes,
            vec![
                MerkleRadixTreeNode {
                    hash: "state-root".into(),
                    leaf_id: None,
                    children: single_child(10, "first-node-hash"),
                },
                MerkleRadixTreeNode {
                    hash: "first-node-hash".into(),
                    leaf_id: None,
                    children: single_child(1, "second-node-hash"),
                },
                MerkleRadixTreeNode {
                    hash: "second-node-hash".into(),
                    leaf_id: None,
                    children: single_child(255, "leaf-node-hash"),
                },
                MerkleRadixTreeNode {
                    hash: "leaf-node-hash".into(),
                    leaf_id: Some(leaves[0].id),
                    children: Children(vec![None; 256])
                },
            ]
        );

        Ok(())
    }

    fn single_child(pos: usize, hash: &str) -> Children {
        let mut children = vec![None; 256];

        children[pos] = Some(hash.into());

        Children(children)
    }

    fn single_child_btree(addr_part: &str, hash: &str) -> BTreeMap<String, String> {
        let mut children = BTreeMap::new();
        children.insert(addr_part.to_string(), hash.to_string());
        children
    }
}

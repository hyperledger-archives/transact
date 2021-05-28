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

use std::collections::BTreeMap;

use diesel::prelude::*;
use diesel::sql_query;
#[cfg(feature = "sqlite")]
use diesel::sql_types::{BigInt, Blob, Nullable, Text};

use crate::error::InternalError;
use crate::state::merkle::node::Node;
use crate::state::merkle::sql::models::Children;

use super::MerkleRadixOperations;

#[cfg(feature = "sqlite")]
#[derive(QueryableByName)]
struct ExtendedMerkleRadixTreeNode {
    #[column_name = "hash"]
    #[sql_type = "Text"]
    pub hash: String,

    #[column_name = "leaf_id"]
    #[sql_type = "Nullable<BigInt>"]
    pub leaf_id: Option<i64>,

    #[column_name = "children"]
    #[sql_type = "Text"]
    pub children: Children,

    #[column_name = "data"]
    #[sql_type = "Nullable<Blob>"]
    pub data: Option<Vec<u8>>,
}

pub trait MerkleRadixGetPathOperation {
    fn get_path(
        &self,
        state_root_hash: &str,
        address: &str,
    ) -> Result<Vec<(String, Node)>, InternalError>;
}

#[cfg(feature = "sqlite")]
impl<'a> MerkleRadixGetPathOperation for MerkleRadixOperations<'a, SqliteConnection> {
    fn get_path(
        &self,
        state_root_hash: &str,
        address: &str,
    ) -> Result<Vec<(String, Node)>, InternalError> {
        let address_bytes =
            hex::decode(address).map_err(|e| InternalError::from_source(Box::new(e)))?;
        let path = sql_query(
            r#"
            WITH RECURSIVE tree_path AS
            (
                -- This is the initial node
                SELECT hash, leaf_id, children, 0 as depth
                FROM merkle_radix_tree_node
                WHERE hash = ?

                UNION ALL

                -- Recurse through the tree
                SELECT c.hash, c.leaf_id, c.children, p.depth + 1
                FROM merkle_radix_tree_node c, tree_path p
                WHERE c.hash = json_extract(
                  p.children,
                  '$[' || json_extract(?, '$[' || p.depth || ']') || ']'
                )
            )
            SELECT t.hash, t.leaf_id, t.children, l.data FROM tree_path t
            LEFT OUTER JOIN merkle_radix_leaf l ON t.leaf_id = l.id
            "#,
        )
        .bind::<Text, _>(state_root_hash)
        .bind::<Text, _>(
            serde_json::to_string(&address_bytes)
                .map_err(|err| InternalError::from_source(Box::new(err)))?,
        )
        .load::<ExtendedMerkleRadixTreeNode>(self.conn)
        .map_err(|err| InternalError::from_source(Box::new(err)))?;

        Ok(path
            .into_iter()
            .map(|extended_node| {
                (
                    extended_node.hash,
                    Node {
                        value: extended_node.data,
                        children: extended_node.children.into(),
                    },
                )
            })
            .collect())
    }
}

impl From<Children> for BTreeMap<String, String> {
    fn from(children: Children) -> Self {
        let mut btree = BTreeMap::new();

        for (i, hash_opt) in children
            .0
            .into_iter()
            .enumerate()
            .filter(|(_, opt)| opt.is_some())
        {
            if let Some(hash) = hash_opt {
                btree.insert(format!("{:02x}", i), hash);
            }
        }
        btree
    }
}

#[cfg(feature = "sqlite")]
#[cfg(test)]
mod sqlite_tests {
    use super::*;

    use diesel::dsl::{insert_into, select};

    use crate::state::merkle::sql::migration::sqlite::run_migrations;
    use crate::state::merkle::sql::models::{Children, MerkleRadixTreeNode, NewMerkleRadixLeaf};
    use crate::state::merkle::sql::operations::last_insert_rowid;
    use crate::state::merkle::sql::schema::{merkle_radix_leaf, merkle_radix_tree_node};

    /// Test that the get path on a non-existent root returns an empty path.
    #[test]
    fn test_get_path_empty_tree() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        run_migrations(&conn)?;

        let path = MerkleRadixOperations::new(&conn).get_path("state-root", "aabbcc")?;

        assert!(path.is_empty());

        Ok(())
    }

    /// Test that a single leaf, with intermediate nodes will return the correct path, transformed
    /// into the merkle Node representation. Additionally, verify that partial paths are returned
    /// for addresses that do not have leaves in the tree.
    #[test]
    fn test_get_path_single_entry() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        run_migrations(&conn)?;

        insert_into(merkle_radix_leaf::table)
            .values(NewMerkleRadixLeaf {
                id: 1,
                address: "000000",
                data: b"hello",
            })
            .execute(&conn)?;

        let inserted_id = select(last_insert_rowid).get_result::<i64>(&conn)?;

        insert_into(merkle_radix_tree_node::table)
            .values(vec![
                MerkleRadixTreeNode {
                    hash: "000000-hash".into(),
                    leaf_id: Some(inserted_id),
                    children: Children(vec![]),
                },
                MerkleRadixTreeNode {
                    hash: "0000-hash".into(),
                    leaf_id: None,
                    children: Children(vec![Some("000000-hash".to_string())]),
                },
                MerkleRadixTreeNode {
                    hash: "00-hash".into(),
                    leaf_id: None,
                    children: Children(vec![Some("0000-hash".to_string())]),
                },
                MerkleRadixTreeNode {
                    hash: "root-hash".into(),
                    leaf_id: None,
                    children: Children(vec![Some("00-hash".to_string())]),
                },
            ])
            .execute(&conn)?;

        let path = MerkleRadixOperations::new(&conn).get_path("root-hash", "000000")?;

        assert_eq!(
            path,
            vec![
                (
                    "root-hash".to_string(),
                    Node {
                        value: None,
                        children: single_child_btree("00", "00-hash"),
                    }
                ),
                (
                    "00-hash".into(),
                    Node {
                        value: None,
                        children: single_child_btree("00", "0000-hash"),
                    }
                ),
                (
                    "0000-hash".into(),
                    Node {
                        value: None,
                        children: single_child_btree("00", "000000-hash"),
                    }
                ),
                (
                    "000000-hash".into(),
                    Node {
                        value: Some(b"hello".to_vec()),
                        children: BTreeMap::new()
                    }
                ),
            ]
        );

        // verify that a path that doesn't exist returns just the intermediate nodes.
        let path = MerkleRadixOperations::new(&conn).get_path("root-hash", "000001")?;

        assert_eq!(
            path,
            vec![
                (
                    "root-hash".to_string(),
                    Node {
                        value: None,
                        children: single_child_btree("00", "00-hash"),
                    }
                ),
                (
                    "00-hash".into(),
                    Node {
                        value: None,
                        children: single_child_btree("00", "0000-hash"),
                    }
                ),
                (
                    "0000-hash".into(),
                    Node {
                        value: None,
                        children: single_child_btree("00", "000000-hash"),
                    }
                ),
            ]
        );

        // verify that a path that is completely non-existent only returns the root node.
        let path = MerkleRadixOperations::new(&conn).get_path("root-hash", "aabbcc")?;

        assert_eq!(
            path,
            vec![(
                "root-hash".to_string(),
                Node {
                    value: None,
                    children: single_child_btree("00", "00-hash"),
                }
            ),]
        );

        Ok(())
    }

    fn single_child_btree(addr_part: &str, hash: &str) -> BTreeMap<String, String> {
        let mut children = BTreeMap::new();
        children.insert(addr_part.to_string(), hash.to_string());
        children
    }
}

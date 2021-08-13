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
use diesel::sql_types::{BigInt, Binary, Nullable, Text};
#[cfg(feature = "postgres")]
use diesel::{pg::types::sql_types::Array, sql_types::SmallInt};

use crate::error::InternalError;
use crate::state::merkle::node::Node;
#[cfg(feature = "sqlite")]
use crate::state::merkle::sql::models::sqlite;

use super::MerkleRadixOperations;

#[cfg(feature = "sqlite")]
#[derive(QueryableByName)]
struct SqliteExtendedMerkleRadixTreeNode {
    #[column_name = "hash"]
    #[sql_type = "Text"]
    pub hash: String,

    #[column_name = "children"]
    #[sql_type = "Text"]
    pub children: sqlite::Children,

    #[column_name = "data"]
    #[sql_type = "Nullable<Binary>"]
    pub data: Option<Vec<u8>>,
}

#[cfg(feature = "postgres")]
#[derive(QueryableByName)]
struct PostgresExtendedMerkleRadixTreeNode {
    #[column_name = "hash"]
    #[sql_type = "Text"]
    pub hash: String,

    #[column_name = "children"]
    #[sql_type = "Array<Nullable<Text>>"]
    pub children: Vec<Option<String>>,

    #[column_name = "data"]
    #[sql_type = "Nullable<Binary>"]
    pub data: Option<Vec<u8>>,
}

pub trait MerkleRadixGetPathOperation {
    fn get_path(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        address: &str,
    ) -> Result<Vec<(String, Node)>, InternalError>;
}

#[cfg(feature = "sqlite")]
impl<'a> MerkleRadixGetPathOperation for MerkleRadixOperations<'a, SqliteConnection> {
    fn get_path(
        &self,
        tree_id: i64,
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
                SELECT hash, tree_id, leaf_id, children, 0 as depth
                FROM merkle_radix_tree_node
                WHERE hash = ? AND tree_id = ?

                UNION ALL

                -- Recurse through the tree
                SELECT c.hash, c.tree_id, c.leaf_id, c.children, p.depth + 1
                FROM merkle_radix_tree_node c, tree_path p
                WHERE c.hash = json_extract(
                  p.children,
                  '$[' || json_extract(?, '$[' || p.depth || ']') || ']'
                )
            )
            SELECT t.hash, t.children, l.data, t.depth FROM tree_path t
            LEFT OUTER JOIN merkle_radix_leaf l ON t.leaf_id = l.id
            WHERE t.tree_id = ?
            ORDER BY t.depth
            "#,
        )
        .bind::<Text, _>(state_root_hash)
        .bind::<BigInt, _>(tree_id)
        .bind::<Text, _>(
            serde_json::to_string(&address_bytes)
                .map_err(|err| InternalError::from_source(Box::new(err)))?,
        )
        .bind::<BigInt, _>(tree_id)
        .load::<SqliteExtendedMerkleRadixTreeNode>(self.conn)
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

#[cfg(feature = "sqlite")]
impl From<sqlite::Children> for BTreeMap<String, String> {
    fn from(children: sqlite::Children) -> Self {
        vec_to_btree(children.0)
    }
}

#[cfg(feature = "postgres")]
impl<'a> MerkleRadixGetPathOperation for MerkleRadixOperations<'a, PgConnection> {
    fn get_path(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        address: &str,
    ) -> Result<Vec<(String, Node)>, InternalError> {
        // both the indexes in the array and the depth in the SQL statement are set to start at 1,
        // as the SQL arrays are 1-indexed.
        let address_branches: Vec<i16> = hex::decode(address)
            .map_err(|e| InternalError::from_source(Box::new(e)))?
            .into_iter()
            .map(|b| i16::from(b) + 1)
            .collect();

        let path = sql_query(
            r#"
            WITH RECURSIVE tree_path AS
            (
                -- This is the initial node
                SELECT hash, tree_id, leaf_id, children, 1 as depth
                FROM merkle_radix_tree_node
                WHERE hash = $1 AND tree_id = $2

                UNION ALL

                -- Recurse through the tree
                SELECT c.hash, c.tree_id, c.leaf_id, c.children, p.depth + 1
                FROM merkle_radix_tree_node c, tree_path p
                WHERE c.hash = p.children[$3[p.depth]]
            )
            SELECT t.hash, t.children, l.data, t.depth FROM tree_path t
            LEFT OUTER JOIN merkle_radix_leaf l ON t.leaf_id = l.id
            WHERE t.tree_id = $2
            ORDER BY t.depth
            "#,
        )
        .bind::<Text, _>(state_root_hash)
        .bind::<BigInt, _>(tree_id)
        .bind::<Array<SmallInt>, _>(&address_branches)
        .load::<PostgresExtendedMerkleRadixTreeNode>(self.conn)
        .map_err(|err| InternalError::from_source(Box::new(err)))?;

        Ok(path
            .into_iter()
            .map(|extended_node| {
                (
                    extended_node.hash,
                    Node {
                        value: extended_node.data,
                        children: vec_to_btree(extended_node.children),
                    },
                )
            })
            .collect())
    }
}

fn vec_to_btree(hashes: Vec<Option<String>>) -> BTreeMap<String, String> {
    hashes
        .into_iter()
        .enumerate()
        .filter_map(|(i, hash_opt)| hash_opt.map(|hash| (format!("{:02x}", i), hash)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use diesel::dsl::insert_into;
    #[cfg(feature = "sqlite")]
    use diesel::dsl::select;

    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    use crate::state::merkle::sql::{
        backend::postgres::test::run_postgres_test, models::postgres,
        schema::postgres_merkle_radix_tree_node,
    };
    #[cfg(feature = "sqlite")]
    use crate::state::merkle::sql::{
        migration, models::sqlite, schema::sqlite_merkle_radix_tree_node,
    };

    use crate::state::merkle::sql::models::NewMerkleRadixLeaf;
    #[cfg(feature = "sqlite")]
    use crate::state::merkle::sql::operations::last_insert_rowid;
    use crate::state::merkle::sql::schema::merkle_radix_leaf;

    /// Test that the get path on a non-existent root returns an empty path.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_get_path_empty_tree() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        migration::sqlite::run_migrations(&conn)?;

        let path = MerkleRadixOperations::new(&conn).get_path(1, "state-root", "aabbcc")?;

        assert!(path.is_empty());

        Ok(())
    }

    /// Test that the get path on a non-existent root returns an empty path.
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_get_path_empty_tree() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;

            let path = MerkleRadixOperations::new(&conn).get_path(1, "state-root", "aabbcc")?;

            assert!(path.is_empty());

            Ok(())
        })
    }

    /// Test that a single leaf, with intermediate nodes will return the correct path, transformed
    /// into the merkle Node representation. Additionally, verify that partial paths are returned
    /// for addresses that do not have leaves in the tree.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_get_path_single_entry() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        migration::sqlite::run_migrations(&conn)?;

        insert_into(merkle_radix_leaf::table)
            .values(NewMerkleRadixLeaf {
                id: 1,
                tree_id: 1,
                address: "000000",
                data: b"hello",
            })
            .execute(&conn)?;

        let inserted_id = select(last_insert_rowid).get_result::<i64>(&conn)?;

        insert_into(sqlite_merkle_radix_tree_node::table)
            .values(vec![
                sqlite::MerkleRadixTreeNode {
                    hash: "000000-hash".into(),
                    tree_id: 1,
                    leaf_id: Some(inserted_id),
                    children: sqlite::Children(vec![]),
                },
                sqlite::MerkleRadixTreeNode {
                    hash: "0000-hash".into(),
                    tree_id: 1,
                    leaf_id: None,
                    children: sqlite::Children(vec![Some("000000-hash".to_string())]),
                },
                sqlite::MerkleRadixTreeNode {
                    hash: "00-hash".into(),
                    tree_id: 1,
                    leaf_id: None,
                    children: sqlite::Children(vec![Some("0000-hash".to_string())]),
                },
                sqlite::MerkleRadixTreeNode {
                    hash: "root-hash".into(),
                    tree_id: 1,
                    leaf_id: None,
                    children: sqlite::Children(vec![Some("00-hash".to_string())]),
                },
            ])
            .execute(&conn)?;

        let path = MerkleRadixOperations::new(&conn).get_path(1, "root-hash", "000000")?;

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
        let path = MerkleRadixOperations::new(&conn).get_path(1, "root-hash", "000001")?;

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
        let path = MerkleRadixOperations::new(&conn).get_path(1, "root-hash", "aabbcc")?;

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

    /// Test that a single leaf, with intermediate nodes will return the correct path, transformed
    /// into the merkle Node representation. Additionally, verify that partial paths are returned
    /// for addresses that do not have leaves in the tree.
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_get_path_single_entry() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;

            let leaf_id = 1;
            insert_into(merkle_radix_leaf::table)
                .values(NewMerkleRadixLeaf {
                    id: leaf_id,
                    tree_id: 1,
                    address: "000000",
                    data: b"hello",
                })
                .execute(&conn)?;

            insert_into(postgres_merkle_radix_tree_node::table)
                .values(vec![
                    postgres::MerkleRadixTreeNode {
                        hash: "000000-hash".into(),
                        tree_id: 1,
                        leaf_id: Some(leaf_id),
                        children: vec![],
                    },
                    postgres::MerkleRadixTreeNode {
                        hash: "0000-hash".into(),
                        tree_id: 1,
                        leaf_id: None,
                        children: vec![Some("000000-hash".to_string())],
                    },
                    postgres::MerkleRadixTreeNode {
                        hash: "00-hash".into(),
                        tree_id: 1,
                        leaf_id: None,
                        children: vec![Some("0000-hash".to_string())],
                    },
                    postgres::MerkleRadixTreeNode {
                        hash: "root-hash".into(),
                        tree_id: 1,
                        leaf_id: None,
                        children: vec![Some("00-hash".to_string())],
                    },
                ])
                .execute(&conn)?;

            let path = MerkleRadixOperations::new(&conn).get_path(1, "root-hash", "000000")?;

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
            let path = MerkleRadixOperations::new(&conn).get_path(1, "root-hash", "000001")?;

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
            let path = MerkleRadixOperations::new(&conn).get_path(1, "root-hash", "aabbcc")?;

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
        })
    }

    fn single_child_btree(addr_part: &str, hash: &str) -> BTreeMap<String, String> {
        let mut children = BTreeMap::new();
        children.insert(addr_part.to_string(), hash.to_string());
        children
    }
}

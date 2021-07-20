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

use crate::error::InternalError;

use crate::state::merkle::sql::schema::{
    merkle_radix_leaf, merkle_radix_state_root, merkle_radix_state_root_leaf_index,
};

use super::MerkleRadixOperations;

pub trait MerkleRadixListLeavesOperation {
    fn list_leaves(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<(String, Vec<u8>)>, InternalError>;
}

impl<'a, C> MerkleRadixListLeavesOperation for MerkleRadixOperations<'a, C>
where
    C: diesel::Connection,
    i64: diesel::deserialize::FromSql<diesel::sql_types::BigInt, C::Backend>,
    String: diesel::deserialize::FromSql<diesel::sql_types::Text, C::Backend>,
    Vec<u8>: diesel::deserialize::FromSql<diesel::sql_types::Blob, C::Backend>,
{
    fn list_leaves(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<(String, Vec<u8>)>, InternalError> {
        self.conn
            .transaction::<_, diesel::result::Error, _>(|| {
                let state_root_id = merkle_radix_state_root::table
                    .filter(
                        merkle_radix_state_root::state_root
                            .eq(state_root_hash)
                            .and(merkle_radix_state_root::tree_id.eq(tree_id)),
                    )
                    .select(merkle_radix_state_root::id)
                    .get_result::<i64>(self.conn)?;

                let mut query = merkle_radix_leaf::table
                    .inner_join(merkle_radix_state_root_leaf_index::table)
                    .filter(
                        merkle_radix_state_root_leaf_index::from_state_root_id
                            .le(state_root_id)
                            .and(merkle_radix_state_root_leaf_index::tree_id.eq(tree_id))
                            .and(
                                merkle_radix_state_root_leaf_index::to_state_root_id
                                    .is_null()
                                    .or(merkle_radix_state_root_leaf_index::to_state_root_id
                                        .gt(state_root_id)),
                            ),
                    )
                    .select((
                        merkle_radix_leaf::id,
                        merkle_radix_leaf::address,
                        merkle_radix_leaf::data,
                        merkle_radix_state_root_leaf_index::to_state_root_id,
                    ))
                    .into_boxed();

                if let Some(prefix) = prefix {
                    query = query.filter(merkle_radix_leaf::address.like(format!("{}%", prefix)));
                }

                query = query
                    .order(merkle_radix_leaf::address.asc())
                    .then_order_by(merkle_radix_state_root_leaf_index::to_state_root_id.desc());

                let leaves = query
                    .get_results::<(i64, String, Vec<u8>, Option<i64>)>(self.conn)?
                    .into_iter()
                    .map(|(_, addr, data, _)| (addr, data))
                    .collect();

                Ok(leaves)
            })
            .map_err(|e| InternalError::from_source(Box::new(e)))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use diesel::dsl::insert_into;

    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    use crate::state::merkle::sql::backend::postgres::test::run_postgres_test;
    use crate::state::merkle::sql::migration;
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    use crate::state::merkle::sql::models::postgres;
    use crate::state::merkle::sql::models::MerkleRadixLeaf;
    use crate::state::merkle::sql::operations::update_index::{
        ChangedLeaf, MerkleRadixUpdateIndexOperation,
    };
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    use crate::state::merkle::sql::schema::postgres_merkle_radix_tree_node;

    /// This tests that a leaf changed across several state root hashes is included in the list
    /// at the given state root hash. It verifies that:
    /// 1. No leaves are returned for the initial state root hash
    /// 2. The first leaf change is returned for first state root.
    /// 3. The second leaf change is returned for the second state root.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_list_leaves_at_state_root() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        migration::sqlite::run_migrations(&conn)?;

        let operations = MerkleRadixOperations::new(&conn);

        // insert the initial root:
        insert_into(merkle_radix_state_root::table)
            .values((
                merkle_radix_state_root::tree_id.eq(1),
                merkle_radix_state_root::state_root.eq("initial-state-root"),
                merkle_radix_state_root::parent_state_root.eq(""),
            ))
            .execute(&conn)?;

        // insert the initial leaf
        insert_into(merkle_radix_leaf::table)
            .values(MerkleRadixLeaf {
                id: 1,
                tree_id: 1,
                address: "aabbcc".into(),
                data: b"hello".to_vec(),
            })
            .execute(&conn)?;

        // update the index
        operations.update_index(
            1,
            "first-state-root",
            "initial-state-root",
            vec![ChangedLeaf::AddedOrUpdated {
                address: "aabbcc",
                leaf_id: 1,
            }],
        )?;

        // insert the changed leaf
        insert_into(merkle_radix_leaf::table)
            .values(MerkleRadixLeaf {
                id: 2,
                tree_id: 1,
                address: "aabbcc".into(),
                data: b"goodbye".to_vec(),
            })
            .execute(&conn)?;

        operations.update_index(
            1,
            "second-state-root",
            "first-state-root",
            vec![ChangedLeaf::AddedOrUpdated {
                address: "aabbcc",
                leaf_id: 2,
            }],
        )?;

        let leaves = operations.list_leaves(1, "initial-state-root", None)?;
        assert!(leaves.is_empty());

        let leaves = operations.list_leaves(1, "first-state-root", None)?;
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].0, "aabbcc");
        assert_eq!(leaves[0].1, b"hello");

        let leaves = operations.list_leaves(1, "second-state-root", None)?;
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].0, "aabbcc");
        assert_eq!(leaves[0].1, b"goodbye");

        Ok(())
    }

    /// This tests that a leaf changed across several state root hashes is included in the list
    /// at the given state root hash. It verifies that:
    /// 1. No leaves are returned for the initial state root hash
    /// 2. The first leaf change is returned for first state root.
    /// 3. The second leaf change is returned for the second state root.
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_list_leaves_at_state_root() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;

            insert_state_root_nodes(&conn)?;

            let operations = MerkleRadixOperations::new(&conn);

            // insert the initial root:
            insert_into(merkle_radix_state_root::table)
                .values((
                    merkle_radix_state_root::tree_id.eq(1),
                    merkle_radix_state_root::state_root.eq("initial-state-root"),
                    merkle_radix_state_root::parent_state_root.eq(""),
                ))
                .execute(&conn)?;

            // insert the initial leaf
            insert_into(merkle_radix_leaf::table)
                .values(MerkleRadixLeaf {
                    id: 1,
                    tree_id: 1,
                    address: "aabbcc".into(),
                    data: b"hello".to_vec(),
                })
                .execute(&conn)?;

            // update the index
            operations.update_index(
                1,
                "first-state-root",
                "initial-state-root",
                vec![ChangedLeaf::AddedOrUpdated {
                    address: "aabbcc",
                    leaf_id: 1,
                }],
            )?;

            // insert the changed leaf
            insert_into(merkle_radix_leaf::table)
                .values(MerkleRadixLeaf {
                    id: 2,
                    tree_id: 1,
                    address: "aabbcc".into(),
                    data: b"goodbye".to_vec(),
                })
                .execute(&conn)?;

            operations.update_index(
                1,
                "second-state-root",
                "first-state-root",
                vec![ChangedLeaf::AddedOrUpdated {
                    address: "aabbcc",
                    leaf_id: 2,
                }],
            )?;

            let leaves = operations.list_leaves(1, "initial-state-root", None)?;
            assert!(leaves.is_empty());

            let leaves = operations.list_leaves(1, "first-state-root", None)?;
            assert_eq!(leaves.len(), 1);
            assert_eq!(leaves[0].0, "aabbcc");
            assert_eq!(leaves[0].1, b"hello");

            let leaves = operations.list_leaves(1, "second-state-root", None)?;
            assert_eq!(leaves.len(), 1);
            assert_eq!(leaves[0].0, "aabbcc");
            assert_eq!(leaves[0].1, b"goodbye");

            Ok(())
        })
    }

    /// This tests that a leaf inserted then deleted across several state root hashes is included
    /// in the list only on the state root hash where it exists.  It verifies that:
    /// 1. No leaves are returned for the initial state root hash
    /// 2. The leaf is returned for first state root.
    /// 3. The leaf is no longer returned for the second state root.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_list_leaves_with_deletion() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        migration::sqlite::run_migrations(&conn)?;

        let operations = MerkleRadixOperations::new(&conn);

        // insert the initial root:
        insert_into(merkle_radix_state_root::table)
            .values((
                merkle_radix_state_root::tree_id.eq(1),
                merkle_radix_state_root::state_root.eq("initial-state-root"),
                merkle_radix_state_root::parent_state_root.eq(""),
            ))
            .execute(&conn)?;

        // insert the initial leaf
        insert_into(merkle_radix_leaf::table)
            .values(MerkleRadixLeaf {
                id: 1,
                tree_id: 1,
                address: "aabbcc".into(),
                data: b"hello".to_vec(),
            })
            .execute(&conn)?;

        // update the index
        operations.update_index(
            1,
            "first-state-root",
            "initial-state-root",
            vec![ChangedLeaf::AddedOrUpdated {
                address: "aabbcc",
                leaf_id: 1,
            }],
        )?;

        // insert the changed leaf
        operations.update_index(
            1,
            "second-state-root",
            "first-state-root",
            vec![ChangedLeaf::Deleted("aabbcc")],
        )?;

        let leaves = operations.list_leaves(1, "initial-state-root", None)?;
        assert!(leaves.is_empty());

        let leaves = operations.list_leaves(1, "first-state-root", None)?;
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].0, "aabbcc");
        assert_eq!(leaves[0].1, b"hello");

        let leaves = operations.list_leaves(1, "second-state-root", None)?;
        assert!(leaves.is_empty());

        Ok(())
    }

    /// This tests that a leaf inserted then deleted across several state root hashes is included
    /// in the list only on the state root hash where it exists.  It verifies that:
    /// 1. No leaves are returned for the initial state root hash
    /// 2. The leaf is returned for first state root.
    /// 3. The leaf is no longer returned for the second state root.
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_list_leaves_with_deletion() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;

            insert_state_root_nodes(&conn)?;

            let operations = MerkleRadixOperations::new(&conn);

            // insert the initial root:
            insert_into(merkle_radix_state_root::table)
                .values((
                    merkle_radix_state_root::tree_id.eq(1),
                    merkle_radix_state_root::state_root.eq("initial-state-root"),
                    merkle_radix_state_root::parent_state_root.eq(""),
                ))
                .execute(&conn)?;

            // insert the initial leaf
            insert_into(merkle_radix_leaf::table)
                .values(MerkleRadixLeaf {
                    id: 1,
                    tree_id: 1,
                    address: "aabbcc".into(),
                    data: b"hello".to_vec(),
                })
                .execute(&conn)?;

            // update the index
            operations.update_index(
                1,
                "first-state-root",
                "initial-state-root",
                vec![ChangedLeaf::AddedOrUpdated {
                    address: "aabbcc",
                    leaf_id: 1,
                }],
            )?;

            // insert the changed leaf
            operations.update_index(
                1,
                "second-state-root",
                "first-state-root",
                vec![ChangedLeaf::Deleted("aabbcc")],
            )?;

            let leaves = operations.list_leaves(1, "initial-state-root", None)?;
            assert!(leaves.is_empty());

            let leaves = operations.list_leaves(1, "first-state-root", None)?;
            assert_eq!(leaves.len(), 1);
            assert_eq!(leaves[0].0, "aabbcc");
            assert_eq!(leaves[0].1, b"hello");

            let leaves = operations.list_leaves(1, "second-state-root", None)?;
            assert!(leaves.is_empty());

            Ok(())
        })
    }

    /// This tests that several leaves, added against successive state root hashes.
    /// 1. Add a leaf and nodes to the tree at state root 1
    /// 2. Add a second leaf and nodes to the tree at state root 2
    /// 3. Verify that only the first leaf is returned with state root 1
    /// 4. Verify that both leaves are returned with state root 2
    /// 5. Verify that only one of the leaves is included in the list when a subtree is specified
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_list_leaves_at_state_root_larger_tree() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        migration::sqlite::run_migrations(&conn)?;

        let operations = MerkleRadixOperations::new(&conn);

        // insert the initial root:
        insert_into(merkle_radix_state_root::table)
            .values((
                merkle_radix_state_root::tree_id.eq(1),
                merkle_radix_state_root::state_root.eq("initial-state-root"),
                merkle_radix_state_root::parent_state_root.eq(""),
            ))
            .execute(&conn)?;

        // insert the initial leaf
        insert_into(merkle_radix_leaf::table)
            .values(MerkleRadixLeaf {
                id: 1,
                tree_id: 1,
                address: "aabbcc".into(),
                data: b"hello".to_vec(),
            })
            .execute(&conn)?;

        // update the index
        operations.update_index(
            1,
            "first-state-root",
            "initial-state-root",
            vec![ChangedLeaf::AddedOrUpdated {
                address: "aabbcc",
                leaf_id: 1,
            }],
        )?;

        // insert a new leaf
        insert_into(merkle_radix_leaf::table)
            .values(MerkleRadixLeaf {
                id: 2,
                tree_id: 1,
                address: "112233".into(),
                data: b"goodbye".to_vec(),
            })
            .execute(&conn)?;

        operations.update_index(
            1,
            "second-state-root",
            "first-state-root",
            vec![ChangedLeaf::AddedOrUpdated {
                address: "112233",
                leaf_id: 2,
            }],
        )?;

        let leaves = operations.list_leaves(1, "initial-state-root", None)?;
        assert!(leaves.is_empty());

        let leaves = operations.list_leaves(1, "first-state-root", None)?;
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].0, "aabbcc");
        assert_eq!(leaves[0].1, b"hello");

        let leaves = operations.list_leaves(1, "second-state-root", None)?;
        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0].0, "112233");
        assert_eq!(leaves[0].1, b"goodbye");
        assert_eq!(leaves[1].0, "aabbcc");
        assert_eq!(leaves[1].1, b"hello");

        // Test with a prefix
        let leaves = operations.list_leaves(1, "second-state-root", Some("aa"))?;
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].0, "aabbcc");
        assert_eq!(leaves[0].1, b"hello");

        Ok(())
    }

    /// This tests that several leaves, added against successive state root hashes.
    /// 1. Add a leaf and nodes to the tree at state root 1
    /// 2. Add a second leaf and nodes to the tree at state root 2
    /// 3. Verify that only the first leaf is returned with state root 1
    /// 4. Verify that both leaves are returned with state root 2
    /// 5. Verify that only one of the leaves is included in the list when a subtree is specified
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_list_leaves_at_state_root_larger_tree() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;

            insert_state_root_nodes(&conn)?;

            let operations = MerkleRadixOperations::new(&conn);

            // insert the initial root:
            insert_into(merkle_radix_state_root::table)
                .values((
                    merkle_radix_state_root::tree_id.eq(1),
                    merkle_radix_state_root::state_root.eq("initial-state-root"),
                    merkle_radix_state_root::parent_state_root.eq(""),
                ))
                .execute(&conn)?;

            // insert the initial leaf
            insert_into(merkle_radix_leaf::table)
                .values(MerkleRadixLeaf {
                    id: 1,
                    tree_id: 1,
                    address: "aabbcc".into(),
                    data: b"hello".to_vec(),
                })
                .execute(&conn)?;

            // update the index
            operations.update_index(
                1,
                "first-state-root",
                "initial-state-root",
                vec![ChangedLeaf::AddedOrUpdated {
                    address: "aabbcc",
                    leaf_id: 1,
                }],
            )?;

            // insert a new leaf
            insert_into(merkle_radix_leaf::table)
                .values(MerkleRadixLeaf {
                    id: 2,
                    tree_id: 1,
                    address: "112233".into(),
                    data: b"goodbye".to_vec(),
                })
                .execute(&conn)?;

            operations.update_index(
                1,
                "second-state-root",
                "first-state-root",
                vec![ChangedLeaf::AddedOrUpdated {
                    address: "112233",
                    leaf_id: 2,
                }],
            )?;

            let leaves = operations.list_leaves(1, "initial-state-root", None)?;
            assert!(leaves.is_empty());

            let leaves = operations.list_leaves(1, "first-state-root", None)?;
            assert_eq!(leaves.len(), 1);
            assert_eq!(leaves[0].0, "aabbcc");
            assert_eq!(leaves[0].1, b"hello");

            let leaves = operations.list_leaves(1, "second-state-root", None)?;
            assert_eq!(leaves.len(), 2);
            assert_eq!(leaves[0].0, "112233");
            assert_eq!(leaves[0].1, b"goodbye");
            assert_eq!(leaves[1].0, "aabbcc");
            assert_eq!(leaves[1].1, b"hello");

            // Test with a prefix
            let leaves = operations.list_leaves(1, "second-state-root", Some("aa"))?;
            assert_eq!(leaves.len(), 1);
            assert_eq!(leaves[0].0, "aabbcc");
            assert_eq!(leaves[0].1, b"hello");

            Ok(())
        })
    }

    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    fn insert_state_root_nodes(conn: &PgConnection) -> Result<(), Box<dyn std::error::Error>> {
        insert_into(postgres_merkle_radix_tree_node::table)
            .values(vec![
                postgres::MerkleRadixTreeNode {
                    hash: "initial-state-root".into(),
                    tree_id: 1,
                    leaf_id: None,
                    children: vec![],
                },
                postgres::MerkleRadixTreeNode {
                    hash: "new-state-root".into(),
                    tree_id: 1,
                    leaf_id: None,
                    children: vec![],
                },
                postgres::MerkleRadixTreeNode {
                    hash: "first-state-root".into(),
                    tree_id: 1,
                    leaf_id: None,
                    children: vec![],
                },
                postgres::MerkleRadixTreeNode {
                    hash: "second-state-root".into(),
                    tree_id: 1,
                    leaf_id: None,
                    children: vec![],
                },
            ])
            .execute(conn)?;

        Ok(())
    }
}

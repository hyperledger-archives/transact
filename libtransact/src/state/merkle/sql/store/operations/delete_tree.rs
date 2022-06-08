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

use diesel::delete;
use diesel::prelude::*;

use crate::error::InternalError;

#[cfg(feature = "postgres")]
use crate::state::merkle::sql::store::schema::postgres_merkle_radix_tree_node;
#[cfg(feature = "sqlite")]
use crate::state::merkle::sql::store::schema::sqlite_merkle_radix_tree_node;
use crate::state::merkle::sql::store::schema::{
    merkle_radix_change_log_addition, merkle_radix_change_log_deletion, merkle_radix_leaf,
    merkle_radix_tree,
};

use super::MerkleRadixOperations;

pub trait MerkleRadixDeleteTreeOperation {
    fn delete_tree(&self, tree_id: i64) -> Result<(), InternalError>;
}

#[cfg(feature = "sqlite")]
impl<'a> MerkleRadixDeleteTreeOperation for MerkleRadixOperations<'a, SqliteConnection> {
    fn delete_tree(&self, tree_id: i64) -> Result<(), InternalError> {
        self.conn.transaction(|| {
            delete(
                merkle_radix_change_log_addition::table
                    .filter(merkle_radix_change_log_addition::tree_id.eq(tree_id)),
            )
            .execute(self.conn)
            .map_err(|err| InternalError::from_source(Box::new(err)))?;

            delete(
                merkle_radix_change_log_deletion::table
                    .filter(merkle_radix_change_log_deletion::tree_id.eq(tree_id)),
            )
            .execute(self.conn)
            .map_err(|err| InternalError::from_source(Box::new(err)))?;

            delete(
                sqlite_merkle_radix_tree_node::table
                    .filter(sqlite_merkle_radix_tree_node::tree_id.eq(tree_id)),
            )
            .execute(self.conn)
            .map_err(|err| InternalError::from_source(Box::new(err)))?;

            delete(merkle_radix_leaf::table.filter(merkle_radix_leaf::tree_id.eq(tree_id)))
                .execute(self.conn)
                .map_err(|err| InternalError::from_source(Box::new(err)))?;

            delete(merkle_radix_tree::table.find(tree_id))
                .execute(self.conn)
                .map_err(|err| InternalError::from_source(Box::new(err)))?;

            Ok(())
        })
    }
}

#[cfg(feature = "postgres")]
impl<'a> MerkleRadixDeleteTreeOperation for MerkleRadixOperations<'a, PgConnection> {
    fn delete_tree(&self, tree_id: i64) -> Result<(), InternalError> {
        self.conn.transaction(|| {
            delete(
                merkle_radix_change_log_addition::table
                    .filter(merkle_radix_change_log_addition::tree_id.eq(tree_id)),
            )
            .execute(self.conn)
            .map_err(|err| InternalError::from_source(Box::new(err)))?;

            delete(
                merkle_radix_change_log_deletion::table
                    .filter(merkle_radix_change_log_deletion::tree_id.eq(tree_id)),
            )
            .execute(self.conn)
            .map_err(|err| InternalError::from_source(Box::new(err)))?;

            delete(
                postgres_merkle_radix_tree_node::table
                    .filter(postgres_merkle_radix_tree_node::tree_id.eq(tree_id)),
            )
            .execute(self.conn)
            .map_err(|err| InternalError::from_source(Box::new(err)))?;

            delete(merkle_radix_leaf::table.filter(merkle_radix_leaf::tree_id.eq(tree_id)))
                .execute(self.conn)
                .map_err(|err| InternalError::from_source(Box::new(err)))?;

            delete(merkle_radix_tree::table.find(tree_id))
                .execute(self.conn)
                .map_err(|err| InternalError::from_source(Box::new(err)))?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use diesel::{connection::SimpleConnection, dsl::insert_into};

    #[cfg(feature = "sqlite")]
    use diesel::dsl::select;

    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    use crate::state::merkle::sql::{
        backend::postgres::test::run_postgres_test,
        store::{models::postgres, schema::postgres_merkle_radix_tree_node},
    };
    #[cfg(feature = "sqlite")]
    use crate::state::merkle::sql::{
        migration,
        store::{models::sqlite, schema::sqlite_merkle_radix_tree_node},
    };

    use crate::state::merkle::sql::store::models::{
        MerkleRadixLeaf, MerkleRadixTree, NewMerkleRadixLeaf,
    };
    #[cfg(feature = "sqlite")]
    use crate::state::merkle::sql::store::operations::last_insert_rowid;
    use crate::state::merkle::sql::store::schema::merkle_radix_leaf;

    /// Given a tree with a single leaf, validate that the delete operation deletes all values in
    /// the tables with a foreign key on `tree.id`.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_delete_tree() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;
        conn.batch_execute("PRAGMA foreign_keys = ON;")?;

        migration::sqlite::run_migrations(&conn)?;

        insert_into(merkle_radix_tree::table)
            .values(MerkleRadixTree {
                id: 2,
                name: "sqlite_delete_tree".into(),
            })
            .execute(&conn)?;

        insert_into(merkle_radix_leaf::table)
            .values(NewMerkleRadixLeaf {
                id: Some(1),
                tree_id: 2,
                address: "000000",
                data: b"hello",
            })
            .execute(&conn)?;

        let inserted_id = select(last_insert_rowid).get_result::<i64>(&conn)?;

        insert_into(sqlite_merkle_radix_tree_node::table)
            .values(vec![
                sqlite::MerkleRadixTreeNode::new("000000-hash", 2).with_leaf_id(Some(inserted_id)),
                sqlite::MerkleRadixTreeNode::new("0000-hash", 2)
                    .with_children(vec![Some("000000-hash".to_string())]),
                sqlite::MerkleRadixTreeNode::new("00-hash", 2)
                    .with_children(vec![Some("0000-hash".to_string())]),
                sqlite::MerkleRadixTreeNode::new("root-hash", 2)
                    .with_children(vec![Some("00-hash".to_string())]),
            ])
            .execute(&conn)?;

        MerkleRadixOperations::new(&conn).delete_tree(2)?;

        let tree = merkle_radix_tree::table
            .find(2)
            .get_result::<MerkleRadixTree>(&conn)
            .optional()?;

        assert!(tree.is_none());

        let tree = merkle_radix_tree::table
            .find(1)
            .get_result::<MerkleRadixTree>(&conn)?;

        assert_eq!("default", &tree.name);

        let nodes = sqlite_merkle_radix_tree_node::table
            .filter(sqlite_merkle_radix_tree_node::tree_id.eq(2))
            .get_results::<sqlite::MerkleRadixTreeNode>(&conn)?;

        assert!(nodes.is_empty());

        let leaves = merkle_radix_leaf::table
            .filter(merkle_radix_leaf::tree_id.eq(2))
            .get_results::<MerkleRadixLeaf>(&conn)?;

        assert!(leaves.is_empty());

        Ok(())
    }

    /// Given a tree with a single leaf, validate that the delete operation deletes all values in
    /// the tables with a foreign key on `tree.id`.
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_delete_tree() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;

            insert_into(merkle_radix_tree::table)
                .values(MerkleRadixTree {
                    id: 2,
                    name: "sqlite_delete_tree".into(),
                })
                .execute(&conn)?;

            insert_into(merkle_radix_leaf::table)
                .values(NewMerkleRadixLeaf {
                    id: Some(1),
                    tree_id: 2,
                    address: "000000",
                    data: b"hello",
                })
                .execute(&conn)?;

            insert_into(postgres_merkle_radix_tree_node::table)
                .values(vec![
                    postgres::MerkleRadixTreeNode::new("000000-hash", 2).with_leaf_id(Some(1)),
                    postgres::MerkleRadixTreeNode::new("0000-hash", 2)
                        .with_children(vec![Some("000000-hash".to_string())]),
                    postgres::MerkleRadixTreeNode::new("00-hash", 2)
                        .with_children(vec![Some("0000-hash".to_string())]),
                    postgres::MerkleRadixTreeNode::new("root-hash", 2)
                        .with_children(vec![Some("00-hash".to_string())]),
                ])
                .execute(&conn)?;

            MerkleRadixOperations::new(&conn).delete_tree(2)?;

            let tree = merkle_radix_tree::table
                .find(2)
                .get_result::<MerkleRadixTree>(&conn)
                .optional()?;

            assert!(tree.is_none());

            let tree = merkle_radix_tree::table
                .find(1)
                .get_result::<MerkleRadixTree>(&conn)?;

            assert_eq!("default", &tree.name);

            let nodes = postgres_merkle_radix_tree_node::table
                .filter(postgres_merkle_radix_tree_node::tree_id.eq(2))
                .get_results::<postgres::MerkleRadixTreeNode>(&conn)?;

            assert!(nodes.is_empty());

            let leaves = merkle_radix_leaf::table
                .filter(merkle_radix_leaf::tree_id.eq(2))
                .get_results::<MerkleRadixLeaf>(&conn)?;

            assert!(leaves.is_empty());

            Ok(())
        })
    }
}

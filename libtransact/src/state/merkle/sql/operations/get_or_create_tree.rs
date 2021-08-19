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

use diesel::dsl::insert_into;
#[cfg(feature = "sqlite")]
use diesel::dsl::select;
use diesel::prelude::*;

use crate::error::InternalError;
use crate::state::merkle::sql::models::NewMerkleRadixTree;
use crate::state::merkle::sql::schema::merkle_radix_tree;
#[cfg(feature = "sqlite")]
use crate::state::merkle::sql::{models::sqlite, schema::sqlite_merkle_radix_tree_node};
#[cfg(feature = "postgres")]
use crate::state::merkle::sql::{
    models::{postgres, MerkleRadixTree},
    schema::postgres_merkle_radix_tree_node,
};

#[cfg(feature = "sqlite")]
use super::last_insert_rowid;
use super::MerkleRadixOperations;

pub trait MerkleRadixGetOrCreateTreeOperation {
    fn get_or_create_tree(
        &self,
        tree_name: &str,
        initial_state_root: &str,
    ) -> Result<i64, InternalError>;
}

#[cfg(feature = "sqlite")]
impl<'a> MerkleRadixGetOrCreateTreeOperation for MerkleRadixOperations<'a, SqliteConnection> {
    fn get_or_create_tree(
        &self,
        tree_name: &str,
        initial_state_root: &str,
    ) -> Result<i64, InternalError> {
        self.conn.transaction::<_, InternalError, _>(|| {
            if let Some(tree_id) = merkle_radix_tree::table
                .filter(merkle_radix_tree::name.eq(tree_name))
                .select(merkle_radix_tree::id)
                .get_result(self.conn)
                .optional()
                .map_err(|e| InternalError::from_source(Box::new(e)))?
            {
                return Ok(tree_id);
            }

            insert_into(merkle_radix_tree::table)
                .values(NewMerkleRadixTree { name: tree_name })
                .execute(self.conn)?;

            let tree_id = select(last_insert_rowid)
                .get_result::<i64>(self.conn)
                .map_err(|e| InternalError::from_source(Box::new(e)))?;

            insert_into(sqlite_merkle_radix_tree_node::table)
                .values(sqlite::MerkleRadixTreeNode {
                    hash: initial_state_root.to_string(),
                    tree_id,
                    leaf_id: None,
                    children: sqlite::Children(vec![None; 256]),
                })
                .execute(self.conn)?;

            Ok(tree_id)
        })
    }
}

#[cfg(feature = "postgres")]
impl<'a> MerkleRadixGetOrCreateTreeOperation for MerkleRadixOperations<'a, PgConnection> {
    fn get_or_create_tree(
        &self,
        tree_name: &str,
        initial_state_root: &str,
    ) -> Result<i64, InternalError> {
        self.conn.transaction::<_, InternalError, _>(|| {
            if let Some(tree_id) = merkle_radix_tree::table
                .filter(merkle_radix_tree::name.eq(tree_name))
                .select(merkle_radix_tree::id)
                .get_result(self.conn)
                .optional()
                .map_err(|e| InternalError::from_source(Box::new(e)))?
            {
                return Ok(tree_id);
            }

            let tree_id = insert_into(merkle_radix_tree::table)
                .values(NewMerkleRadixTree { name: tree_name })
                .get_result::<MerkleRadixTree>(self.conn)
                .map(|tree| tree.id)
                .map_err(|e| InternalError::from_source(Box::new(e)))?;

            insert_into(postgres_merkle_radix_tree_node::table)
                .values(postgres::MerkleRadixTreeNode {
                    hash: initial_state_root.to_string(),
                    tree_id,
                    leaf_id: None,
                    children: vec![None; 256],
                })
                .execute(self.conn)?;

            Ok(tree_id)
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    use crate::state::merkle::sql::backend::postgres::test::run_postgres_test;
    #[cfg(feature = "sqlite")]
    use crate::state::merkle::sql::migration::sqlite::run_migrations;

    /// This tests that a tree id can be returned from its name.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_test_get_or_create_tree() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;
        run_migrations(&conn)?;

        let operations = MerkleRadixOperations::new(&conn);

        let id = operations.get_or_create_tree("test", "test-root")?;
        assert_eq!(2, id);

        let nodes = sqlite_merkle_radix_tree_node::table
            .get_results::<sqlite::MerkleRadixTreeNode>(&conn)?;

        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes[0],
            sqlite::MerkleRadixTreeNode {
                hash: "test-root".into(),
                tree_id: 2,
                leaf_id: None,
                children: sqlite::Children(vec![None; 256]),
            }
        );

        let id = operations.get_or_create_tree("default", "test-root")?;
        assert_eq!(1, id);

        Ok(())
    }

    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_test_get_or_create_tree() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;

            let operations = MerkleRadixOperations::new(&conn);

            let id = operations.get_or_create_tree("test", "test_root")?;
            assert_eq!(2, id);

            let nodes = postgres_merkle_radix_tree_node::table
                .get_results::<postgres::MerkleRadixTreeNode>(&conn)?;

            assert_eq!(nodes.len(), 1);
            assert_eq!(
                nodes[0],
                postgres::MerkleRadixTreeNode {
                    hash: "test_root".into(),
                    tree_id: 2,
                    leaf_id: None,
                    children: vec![None; 256],
                }
            );

            let id = operations.get_or_create_tree("default", "test_root")?;
            assert_eq!(1, id);

            Ok(())
        })
    }
}

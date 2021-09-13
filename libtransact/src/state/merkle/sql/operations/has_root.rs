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

use diesel::{dsl::exists, select};

use crate::error::InternalError;
#[cfg(feature = "postgres")]
use crate::state::merkle::sql::schema::postgres_merkle_radix_tree_node;
#[cfg(feature = "sqlite")]
use crate::state::merkle::sql::schema::sqlite_merkle_radix_tree_node;

use super::MerkleRadixOperations;

pub trait MerkleRadixHasRootOperation {
    fn has_root(&self, tree_id: i64, state_root_hash: &str) -> Result<bool, InternalError>;
}

#[cfg(feature = "sqlite")]
impl<'a> MerkleRadixHasRootOperation for MerkleRadixOperations<'a, SqliteConnection> {
    fn has_root(&self, tree_id: i64, state_root_hash: &str) -> Result<bool, InternalError> {
        select(exists(
            sqlite_merkle_radix_tree_node::table.find((state_root_hash, tree_id)),
        ))
        .get_result::<bool>(self.conn)
        .map_err(|e| InternalError::from_source(Box::new(e)))
    }
}

#[cfg(feature = "postgres")]
impl<'a> MerkleRadixHasRootOperation for MerkleRadixOperations<'a, PgConnection> {
    fn has_root(&self, tree_id: i64, state_root_hash: &str) -> Result<bool, InternalError> {
        select(exists(
            postgres_merkle_radix_tree_node::table.find((state_root_hash, tree_id)),
        ))
        .get_result::<bool>(self.conn)
        .map_err(|e| InternalError::from_source(Box::new(e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use diesel::dsl::insert_into;

    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    use crate::state::merkle::sql::{backend::postgres::test::run_postgres_test, models::postgres};
    #[cfg(feature = "sqlite")]
    use crate::state::merkle::sql::{migration, models::sqlite};

    /// Test that has_node succeeds if the root hash is in the tree table.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_has_root_from_tree_node() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        migration::sqlite::run_migrations(&conn)?;

        // Does not exist
        assert!(!MerkleRadixOperations::new(&conn).has_root(1, "initial-state-root")?);

        // insert the root only into the tree:
        insert_into(sqlite_merkle_radix_tree_node::table)
            .values(sqlite::MerkleRadixTreeNode {
                hash: "initial-state-root".into(),
                tree_id: 1,
                leaf_id: None,
                children: sqlite::Children(vec![]),
            })
            .execute(&conn)?;

        assert!(MerkleRadixOperations::new(&conn).has_root(1, "initial-state-root")?);

        Ok(())
    }

    /// Test that has_node succeeds if the root hash is in the tree table.
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_has_root_from_tree_node() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;

            // Does not exist
            assert!(!MerkleRadixOperations::new(&conn).has_root(1, "initial-state-root")?);

            // insert the root only into the tree:
            insert_into(postgres_merkle_radix_tree_node::table)
                .values(postgres::MerkleRadixTreeNode {
                    hash: "initial-state-root".into(),
                    tree_id: 1,
                    leaf_id: None,
                    children: vec![],
                })
                .execute(&conn)?;

            assert!(MerkleRadixOperations::new(&conn).has_root(1, "initial-state-root")?);

            Ok(())
        })
    }
}

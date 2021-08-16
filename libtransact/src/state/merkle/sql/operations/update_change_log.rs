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
use diesel::prelude::*;

use crate::error::InternalError;
use crate::state::merkle::sql::{
    models::{NewMerkleRadixChangeLogAddition, NewMerkleRadixChangeLogDeletion},
    schema::merkle_radix_change_log_addition,
    schema::merkle_radix_change_log_deletion,
};

use super::MerkleRadixOperations;

pub trait MerkleRadixUpdateUpdateChangeLogOperation {
    fn update_change_log(
        &self,
        tree_id: i64,
        state_root: &str,
        parent_state_root: &str,
        additions: &[&str],
        deletions: &[&str],
    ) -> Result<(), InternalError>;
}

#[cfg(feature = "sqlite")]
impl<'a> MerkleRadixUpdateUpdateChangeLogOperation for MerkleRadixOperations<'a, SqliteConnection> {
    fn update_change_log(
        &self,
        tree_id: i64,
        state_root: &str,
        parent_state_root: &str,
        additions: &[&str],
        deletions: &[&str],
    ) -> Result<(), InternalError> {
        self.conn.transaction::<_, InternalError, _>(|| {
            let change_log_additions = additions
                .iter()
                .map(|hash| NewMerkleRadixChangeLogAddition {
                    state_root,
                    tree_id,
                    parent_state_root: Some(parent_state_root),
                    addition: hash,
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
impl<'a> MerkleRadixUpdateUpdateChangeLogOperation for MerkleRadixOperations<'a, PgConnection> {
    fn update_change_log(
        &self,
        tree_id: i64,
        state_root: &str,
        parent_state_root: &str,
        additions: &[&str],
        deletions: &[&str],
    ) -> Result<(), InternalError> {
        self.conn.transaction::<_, InternalError, _>(|| {
            let change_log_additions = additions
                .iter()
                .map(|hash| NewMerkleRadixChangeLogAddition {
                    state_root,
                    tree_id,
                    parent_state_root: Some(parent_state_root),
                    addition: hash,
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
                })
                .collect::<Vec<_>>();

            insert_into(merkle_radix_change_log_deletion::table)
                .values(change_log_deletions)
                .execute(self.conn)?;

            Ok(())
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    use crate::state::merkle::sql::backend::postgres::test::run_postgres_test;
    #[cfg(feature = "sqlite")]
    use crate::state::merkle::sql::{
        migration, models::sqlite, schema::sqlite_merkle_radix_tree_node,
    };
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    use crate::state::merkle::sql::{models::postgres, schema::postgres_merkle_radix_tree_node};

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_update_change_log() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        migration::sqlite::run_migrations(&conn)?;

        sqlite_insert_state_root_nodes(&conn)?;

        MerkleRadixOperations::new(&conn).update_change_log(
            1,
            "new-state-root",
            "initial-state-root",
            &[],
            &[],
        )?;

        Ok(())
    }

    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_update_change_log() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;

            postgres_insert_state_root_nodes(&conn)?;

            MerkleRadixOperations::new(&conn).update_change_log(
                1,
                "new-state-root",
                "initial-state-root",
                &[],
                &[],
            )?;

            Ok(())
        })
    }

    #[cfg(feature = "sqlite")]
    fn sqlite_insert_state_root_nodes(
        conn: &SqliteConnection,
    ) -> Result<(), Box<dyn std::error::Error>> {
        insert_into(sqlite_merkle_radix_tree_node::table)
            .values(vec![
                sqlite::MerkleRadixTreeNode {
                    hash: "initial-state-root".into(),
                    tree_id: 1,
                    leaf_id: None,
                    children: sqlite::Children(vec![]),
                },
                sqlite::MerkleRadixTreeNode {
                    hash: "new-state-root".into(),
                    tree_id: 1,
                    leaf_id: None,
                    children: sqlite::Children(vec![]),
                },
                sqlite::MerkleRadixTreeNode {
                    hash: "first-state-root".into(),
                    tree_id: 1,
                    leaf_id: None,
                    children: sqlite::Children(vec![]),
                },
                sqlite::MerkleRadixTreeNode {
                    hash: "second-state-root".into(),
                    tree_id: 1,
                    leaf_id: None,
                    children: sqlite::Children(vec![]),
                },
            ])
            .execute(conn)?;

        Ok(())
    }

    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    fn postgres_insert_state_root_nodes(
        conn: &PgConnection,
    ) -> Result<(), Box<dyn std::error::Error>> {
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

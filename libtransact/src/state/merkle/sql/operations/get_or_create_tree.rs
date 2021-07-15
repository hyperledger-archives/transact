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

use diesel::dsl::{insert_into, select};
use diesel::prelude::*;

use crate::error::InternalError;
use crate::state::merkle::sql::models::NewMerkleRadixTree;
use crate::state::merkle::sql::schema::merkle_radix_tree;

use super::{last_insert_rowid, MerkleRadixOperations};

pub trait MerkleRadixGetOrCreateTreeOperation {
    fn get_or_create_tree(&self, tree_name: &str) -> Result<i64, InternalError>;
}

#[cfg(feature = "sqlite")]
impl<'a> MerkleRadixGetOrCreateTreeOperation for MerkleRadixOperations<'a, SqliteConnection> {
    fn get_or_create_tree(&self, tree_name: &str) -> Result<i64, InternalError> {
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

            select(last_insert_rowid)
                .get_result::<i64>(self.conn)
                .map_err(|e| InternalError::from_source(Box::new(e)))
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(feature = "sqlite")]
    use crate::state::merkle::sql::migration::sqlite::run_migrations;

    /// This tests that a tree id can be returned from its name.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_test_get_or_create_tree() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;
        run_migrations(&conn)?;

        let operations = MerkleRadixOperations::new(&conn);

        let id = operations.get_or_create_tree("test")?;
        assert_eq!(2, id);

        let id = operations.get_or_create_tree("default")?;
        assert_eq!(1, id);

        Ok(())
    }
}

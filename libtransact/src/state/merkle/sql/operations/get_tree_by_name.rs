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
use crate::state::merkle::sql::schema::merkle_radix_tree;

use super::MerkleRadixOperations;

pub trait MerkleRadixGetTreeByNameOperation {
    fn get_tree_id_by_name(&self, tree_name: &str) -> Result<Option<i64>, InternalError>;
}

impl<'a, C> MerkleRadixGetTreeByNameOperation for MerkleRadixOperations<'a, C>
where
    C: diesel::Connection,
    i64: diesel::deserialize::FromSql<diesel::sql_types::BigInt, C::Backend>,
    String: diesel::deserialize::FromSql<diesel::sql_types::Text, C::Backend>,
{
    fn get_tree_id_by_name(&self, tree_name: &str) -> Result<Option<i64>, InternalError> {
        merkle_radix_tree::table
            .filter(merkle_radix_tree::name.eq(tree_name))
            .select(merkle_radix_tree::id)
            .get_result(self.conn)
            .optional()
            .map_err(|e| InternalError::from_source(Box::new(e)))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use diesel::dsl::insert_into;

    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    use crate::state::merkle::sql::backend::postgres::test::run_postgres_test;
    #[cfg(feature = "sqlite")]
    use crate::state::merkle::sql::migration;
    use crate::state::merkle::sql::models::NewMerkleRadixTree;

    /// This tests that a tree id can be returned from its name.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_get_tree_id_by_name() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;
        migration::sqlite::run_migrations(&conn)?;

        let operations = MerkleRadixOperations::new(&conn);

        let id_opt = operations.get_tree_id_by_name("test")?;
        assert_eq!(None, id_opt);

        let id_opt = operations.get_tree_id_by_name("default")?;
        assert_eq!(Some(1), id_opt);

        insert_into(merkle_radix_tree::table)
            .values(NewMerkleRadixTree { name: "test" })
            .execute(&conn)?;

        let id_opt = operations.get_tree_id_by_name("test")?;
        assert_eq!(Some(2), id_opt);

        Ok(())
    }

    /// This tests that a tree id can be returned from its name.
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_get_tree_id_by_name() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;

            let operations = MerkleRadixOperations::new(&conn);

            let id_opt = operations.get_tree_id_by_name("test")?;
            assert_eq!(None, id_opt);

            let id_opt = operations.get_tree_id_by_name("default")?;
            assert_eq!(Some(1), id_opt);

            insert_into(merkle_radix_tree::table)
                .values(NewMerkleRadixTree { name: "test" })
                .execute(&conn)?;

            let id_opt = operations.get_tree_id_by_name("test")?;
            assert_eq!(Some(2), id_opt);

            Ok(())
        })
    }
}

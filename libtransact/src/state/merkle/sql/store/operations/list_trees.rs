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
use diesel::sql_types::{BigInt, Text};

use crate::error::InternalError;
use crate::state::merkle::sql::store::models::MerkleRadixTree;
use crate::state::merkle::sql::store::schema::merkle_radix_tree;

use super::MerkleRadixOperations;

pub trait MerkleRadixListTreesOperation {
    fn list_trees(
        &self,
    ) -> Result<Box<dyn ExactSizeIterator<Item = Result<String, InternalError>>>, InternalError>;
}

impl<'a, C> MerkleRadixListTreesOperation for MerkleRadixOperations<'a, C>
where
    C: Connection,
    String: diesel::deserialize::FromSql<Text, C::Backend>,
    i64: diesel::deserialize::FromSql<BigInt, C::Backend>,
{
    fn list_trees(
        &self,
    ) -> Result<Box<dyn ExactSizeIterator<Item = Result<String, InternalError>>>, InternalError>
    {
        let trees: Vec<MerkleRadixTree> = merkle_radix_tree::table
            .get_results(self.conn)
            .map_err(|e| InternalError::from_source(Box::new(e)))?;

        Ok(Box::new(trees.into_iter().map(|tree| Ok(tree.name))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use diesel::dsl::insert_into;

    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    use crate::state::merkle::sql::backend::postgres::test::run_postgres_test;
    #[cfg(feature = "sqlite")]
    use crate::state::merkle::sql::migration;
    use crate::state::merkle::sql::store::models::NewMerkleRadixTree;

    /// This tests that trees names can be listed.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_list_trees() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;
        migration::sqlite::run_migrations(&conn)?;

        let operations = MerkleRadixOperations::new(&conn);

        let trees = operations.list_trees()?.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(vec!["default".to_string()], trees);

        insert_into(merkle_radix_tree::table)
            .values(NewMerkleRadixTree { name: "test" })
            .execute(&conn)?;

        let trees = operations.list_trees()?.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(vec!["default".to_string(), "test".to_string()], trees);

        Ok(())
    }

    /// This tests that trees names can be listed.
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_list_trees() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;

            let operations = MerkleRadixOperations::new(&conn);

            let trees = operations.list_trees()?.collect::<Result<Vec<_>, _>>()?;
            assert_eq!(vec!["default".to_string()], trees);

            insert_into(merkle_radix_tree::table)
                .values(NewMerkleRadixTree { name: "test" })
                .execute(&conn)?;

            let trees = operations.list_trees()?.collect::<Result<Vec<_>, _>>()?;
            assert_eq!(vec!["default".to_string(), "test".to_string()], trees);

            Ok(())
        })
    }
}

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
use crate::state::merkle::sql::schema::merkle_radix_tree_node;

use super::MerkleRadixOperations;

pub trait MerkleRadixHasRootOperation {
    fn has_root(&self, tree_id: i64, state_root_hash: &str) -> Result<bool, InternalError>;
}

impl<'a, C> MerkleRadixHasRootOperation for MerkleRadixOperations<'a, C>
where
    C: diesel::Connection,
    C::Backend: diesel::sql_types::HasSqlType<diesel::sql_types::Bool>,
    bool: diesel::deserialize::FromSql<diesel::sql_types::Bool, C::Backend>,
{
    fn has_root(&self, tree_id: i64, state_root_hash: &str) -> Result<bool, InternalError> {
        select(exists(
            merkle_radix_tree_node::table.find((state_root_hash, tree_id)),
        ))
        .get_result::<bool>(self.conn)
        .map_err(|e| InternalError::from_source(Box::new(e)))
    }
}

#[cfg(feature = "sqlite")]
#[cfg(test)]
mod tests {
    use super::*;

    use diesel::dsl::insert_into;

    use crate::state::merkle::sql::migration::sqlite::run_migrations;
    use crate::state::merkle::sql::models::{Children, MerkleRadixTreeNode};

    /// Test that has_node succeeds if the root hash is in the tree table.
    #[test]
    fn test_has_root_from_tree_node() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        run_migrations(&conn)?;

        // Does not exist
        assert!(!MerkleRadixOperations::new(&conn).has_root(1, "initial-state-root")?);

        // insert the root only into the tree:
        insert_into(merkle_radix_tree_node::table)
            .values(MerkleRadixTreeNode {
                hash: "initial-state-root".into(),
                tree_id: 1,
                leaf_id: None,
                children: Children(vec![]),
            })
            .execute(&conn)?;

        assert!(MerkleRadixOperations::new(&conn).has_root(1, "initial-state-root")?);

        Ok(())
    }
}

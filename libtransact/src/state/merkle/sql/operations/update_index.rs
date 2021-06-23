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

use diesel::dsl::{delete, insert_into, select, update};
use diesel::prelude::*;

use crate::error::InternalError;
use crate::state::merkle::sql::models::{MerkleRadixStateRoot, NewMerkleRadixStateRoot};
use crate::state::merkle::sql::schema::{
    merkle_radix_leaf, merkle_radix_state_root, merkle_radix_state_root_leaf_index,
};

#[cfg(feature = "sqlite")]
use super::last_insert_rowid;
use super::MerkleRadixOperations;

pub enum ChangedLeaf<'data> {
    AddedOrUpdated { address: &'data str, leaf_id: i64 },
    Deleted(&'data str),
}

impl<'data> ChangedLeaf<'data> {
    fn address(&self) -> &'data str {
        match self {
            ChangedLeaf::AddedOrUpdated { address, .. } => address,
            ChangedLeaf::Deleted(addr) => addr,
        }
    }
}

pub trait MerkleRadixUpdateIndexOperation {
    fn update_index(
        &self,
        state_root: &str,
        parent_state_root: &str,
        changed_addresses: Vec<ChangedLeaf>,
    ) -> Result<(), InternalError>;
}

#[cfg(feature = "sqlite")]
impl<'a> MerkleRadixUpdateIndexOperation for MerkleRadixOperations<'a, SqliteConnection> {
    fn update_index(
        &self,
        state_root: &str,
        parent_state_root: &str,
        changed_addresses: Vec<ChangedLeaf>,
    ) -> Result<(), InternalError> {
        let tree_id: i64 = 1;
        self.conn
            .transaction::<_, diesel::result::Error, _>(|| {
                let fork = merkle_radix_state_root::table
                    .filter(merkle_radix_state_root::parent_state_root.eq(parent_state_root))
                    .first::<MerkleRadixStateRoot>(self.conn)
                    .optional()?;

                if let Some(fork_state_root_id) = fork.map(|state_root| state_root.id) {
                    let null_id: Option<i64> = None;
                    // remove the fork changes from the index
                    update(merkle_radix_state_root_leaf_index::table.filter(
                        merkle_radix_state_root_leaf_index::to_state_root_id.ge(fork_state_root_id),
                    ))
                    .set(merkle_radix_state_root_leaf_index::to_state_root_id.eq(null_id))
                    .execute(self.conn)?;

                    delete(
                        merkle_radix_state_root_leaf_index::table.filter(
                            merkle_radix_state_root_leaf_index::from_state_root_id
                                .eq(fork_state_root_id),
                        ),
                    )
                    .execute(self.conn)?;

                    delete(merkle_radix_state_root::table.find(fork_state_root_id))
                        .execute(self.conn)?;
                }

                insert_into(merkle_radix_state_root::table)
                    .values(NewMerkleRadixStateRoot {
                        tree_id,
                        state_root,
                        parent_state_root,
                    })
                    .execute(self.conn)?;

                let state_root_id = select(last_insert_rowid).get_result::<i64>(self.conn)?;

                for change in changed_addresses.iter() {
                    update(
                        merkle_radix_state_root_leaf_index::table.filter(
                            merkle_radix_state_root_leaf_index::to_state_root_id
                                .is_null()
                                .and(
                                    merkle_radix_state_root_leaf_index::leaf_id.eq_any(
                                        merkle_radix_leaf::table
                                            .select(merkle_radix_leaf::id)
                                            .filter(
                                                merkle_radix_leaf::address.eq(change.address()),
                                            ),
                                    ),
                                ),
                        ),
                    )
                    .set(merkle_radix_state_root_leaf_index::to_state_root_id.eq(state_root_id))
                    .execute(self.conn)?;
                }

                // insert index records for all added or updated leaves.
                insert_into(merkle_radix_state_root_leaf_index::table)
                    .values(
                        changed_addresses
                            .iter()
                            .filter(|change| matches!(change, ChangedLeaf::AddedOrUpdated { .. }))
                            .map(|change| match change {
                                ChangedLeaf::AddedOrUpdated { leaf_id, .. } => (
                                    merkle_radix_state_root_leaf_index::tree_id.eq(tree_id),
                                    merkle_radix_state_root_leaf_index::leaf_id.eq(leaf_id),
                                    merkle_radix_state_root_leaf_index::from_state_root_id
                                        .eq(state_root_id),
                                ),
                                // we filtered only on AddedOrUpdated, so everything else is
                                // unreachable.
                                _ => unreachable!(),
                            })
                            .collect::<Vec<_>>(),
                    )
                    .execute(self.conn)?;

                Ok(())
            })
            .map_err(|e| InternalError::from_source(Box::new(e)))
    }
}

#[cfg(feature = "sqlite")]
#[cfg(test)]
mod sqlite_tests {
    use super::*;

    use crate::state::merkle::sql::migration::sqlite::run_migrations;
    use crate::state::merkle::sql::models::{MerkleRadixLeaf, MerkleRadixStateRootLeafIndexEntry};
    use crate::state::merkle::sql::schema::merkle_radix_leaf;

    /// This test indexes a leaf and its tree nodes into the index as in an initial entry into the
    /// merkle tree.  That is, there was no prior entry at that address. It
    /// 1. Verifies that the new state root hash specified is inserted into the state root table
    /// 2. Verifies that the leaf-state-root relationship is added to the index, with a null
    ///    to_state_root_id
    #[test]
    fn test_update_index_initial_change() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        run_migrations(&conn)?;

        // insert the leaf
        insert_into(merkle_radix_leaf::table)
            .values(MerkleRadixLeaf {
                id: 1,
                tree_id: 1,
                address: "aabbcc".into(),
                data: b"hello".to_vec(),
            })
            .execute(&conn)?;

        MerkleRadixOperations::new(&conn).update_index(
            "new-state-root",
            "initial-state-root",
            vec![ChangedLeaf::AddedOrUpdated {
                address: "aabbcc",
                leaf_id: 1,
            }],
        )?;

        let state_root_ord = merkle_radix_state_root::table
            .filter(merkle_radix_state_root::state_root.eq("new-state-root"))
            .first::<MerkleRadixStateRoot>(&conn)?;

        assert_eq!("initial-state-root", &state_root_ord.parent_state_root);

        let leaves = merkle_radix_state_root_leaf_index::table
            .filter(merkle_radix_state_root_leaf_index::leaf_id.eq(1))
            .get_results::<MerkleRadixStateRootLeafIndexEntry>(&conn)?;

        assert_eq!(leaves.len(), 1);

        assert_eq!(leaves[0].leaf_id, 1);
        assert_eq!(leaves[0].from_state_root_id, state_root_ord.id);
        assert_eq!(leaves[0].to_state_root_id, None);

        Ok(())
    }

    /// This test indexes an initial leaf and set of nodes, then updates the leaf at the same
    /// address.  It verifies that
    /// 1. Both state roots are in the state root table, with the correct parents
    /// 2. Verifies that the index contains two entries for the leaf, one that is valid only for
    ///    the first state root, and one that is valid for the current root, with a null
    ///    to_state_root_id (i.e. has not been changed yet).
    #[test]
    fn test_update_index_single_change() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        run_migrations(&conn)?;

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
        MerkleRadixOperations::new(&conn).update_index(
            "first-state-root",
            "initial-state-root",
            vec![ChangedLeaf::AddedOrUpdated {
                address: "aabbcc",
                leaf_id: 1,
            }],
        )?;
        let first_state_root_ord = merkle_radix_state_root::table
            .filter(merkle_radix_state_root::state_root.eq("first-state-root"))
            .first::<MerkleRadixStateRoot>(&conn)?;

        // insert the changed leaf
        insert_into(merkle_radix_leaf::table)
            .values(MerkleRadixLeaf {
                id: 2,
                tree_id: 1,
                address: "aabbcc".into(),
                data: b"goodbye".to_vec(),
            })
            .execute(&conn)?;

        MerkleRadixOperations::new(&conn).update_index(
            "second-state-root",
            "first-state-root",
            vec![ChangedLeaf::AddedOrUpdated {
                address: "aabbcc",
                leaf_id: 2,
            }],
        )?;

        let second_state_root_ord = merkle_radix_state_root::table
            .filter(merkle_radix_state_root::state_root.eq("second-state-root"))
            .first::<MerkleRadixStateRoot>(&conn)?;

        assert_eq!(second_state_root_ord.parent_state_root, "first-state-root");

        let leaves = merkle_radix_state_root_leaf_index::table
            .filter(
                merkle_radix_state_root_leaf_index::leaf_id.eq_any(
                    merkle_radix_leaf::table
                        .select(merkle_radix_leaf::id)
                        .filter(merkle_radix_leaf::address.eq("aabbcc")),
                ),
            )
            .get_results::<MerkleRadixStateRootLeafIndexEntry>(&conn)?;

        assert_eq!(leaves.len(), 2);

        assert_eq!(leaves[0].leaf_id, 1);
        assert_eq!(leaves[0].from_state_root_id, first_state_root_ord.id);
        assert_eq!(leaves[0].to_state_root_id, Some(second_state_root_ord.id));

        assert_eq!(leaves[1].leaf_id, 2);
        assert_eq!(leaves[1].from_state_root_id, second_state_root_ord.id);
        assert_eq!(leaves[1].to_state_root_id, None);

        Ok(())
    }

    /// This test indexes a leaf starting from the initial root, then indexes a second value at
    /// that address starting also starting from the initial root.  It will treat this as a fork in
    /// the tree's history, and remove the old branch in favor of the new one.  It should
    /// 1. Verify that the state root table only has one entry where the parent hash is the initial
    ///    state root hash.
    /// 2. Verify that there is only one index entry for the leaf with the second state root hash
    ///    as its from_state_root_id and null for its to_state_root_id
    #[test]
    fn test_update_index_single_change_fork() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;

        run_migrations(&conn)?;

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
        MerkleRadixOperations::new(&conn).update_index(
            "first-state-root",
            "initial-state-root",
            vec![ChangedLeaf::AddedOrUpdated {
                address: "aabbcc",
                leaf_id: 1,
            }],
        )?;
        let _first_state_root_ord = merkle_radix_state_root::table
            .filter(merkle_radix_state_root::state_root.eq("first-state-root"))
            .first::<MerkleRadixStateRoot>(&conn)?;

        // insert the changed leaf
        insert_into(merkle_radix_leaf::table)
            .values(MerkleRadixLeaf {
                id: 2,
                tree_id: 1,
                address: "aabbcc".into(),
                data: b"goodbye".to_vec(),
            })
            .execute(&conn)?;

        // update the index as if it's transition from the initial state root again (i.e.
        // a new forked tree)
        MerkleRadixOperations::new(&conn).update_index(
            "second-state-root",
            "initial-state-root",
            vec![ChangedLeaf::AddedOrUpdated {
                address: "aabbcc",
                leaf_id: 2,
            }],
        )?;

        let second_state_root_ord = merkle_radix_state_root::table
            .filter(merkle_radix_state_root::state_root.eq("second-state-root"))
            .first::<MerkleRadixStateRoot>(&conn)?;

        assert_eq!(
            second_state_root_ord.parent_state_root,
            "initial-state-root"
        );

        let leaves = merkle_radix_state_root_leaf_index::table
            .filter(
                merkle_radix_state_root_leaf_index::leaf_id.eq_any(
                    merkle_radix_leaf::table
                        .select(merkle_radix_leaf::id)
                        .filter(merkle_radix_leaf::address.eq("aabbcc")),
                ),
            )
            .get_results::<MerkleRadixStateRootLeafIndexEntry>(&conn)?;

        assert_eq!(leaves.len(), 1);

        assert_eq!(leaves[0].leaf_id, 2);
        assert_eq!(leaves[0].from_state_root_id, second_state_root_ord.id);
        assert_eq!(leaves[0].to_state_root_id, None);

        Ok(())
    }
}

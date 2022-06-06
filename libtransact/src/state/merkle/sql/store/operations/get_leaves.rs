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
use diesel::sql_query;
use diesel::sql_types::{BigInt, Binary, Bool, Nullable, Text};
#[cfg(feature = "postgres")]
use diesel::{pg::types::sql_types::Array, sql_types::SmallInt};

use crate::error::InternalError;
use crate::state::merkle::sql::cache::DataCache;

use super::MerkleRadixOperations;

pub trait MerkleRadixGetLeavesOperation {
    fn get_leaves(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        addresses: Vec<&str>,
        data_cache: Option<&DataCache>,
    ) -> Result<Vec<(String, Vec<u8>)>, InternalError>;
}

#[cfg(feature = "sqlite")]
impl<'a> MerkleRadixGetLeavesOperation for MerkleRadixOperations<'a, SqliteConnection> {
    fn get_leaves(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        addresses: Vec<&str>,
        data_cache: Option<&DataCache>,
    ) -> Result<Vec<(String, Vec<u8>)>, InternalError> {
        self.conn.transaction(|| {
            let mut results = vec![];
            for address in addresses {
                let cached_data: Option<(String, Vec<u8>)> = data_cache
                    .map(|cache| cache.peek_last_known_data_for_address(address))
                    .transpose()?
                    .flatten();

                let address_bytes =
                    hex::decode(address).map_err(|e| InternalError::from_source(Box::new(e)))?;
                let values = sql_query(
                    r#"
                    WITH RECURSIVE tree_path AS
                    (
                        -- This is the initial node
                        SELECT hash, tree_id, leaf_id, children, 0 as depth
                        FROM merkle_radix_tree_node
                        WHERE hash = ? AND tree_id = ? AND reference > 0

                        UNION ALL

                        -- Recurse through the tree
                        SELECT c.hash, c.tree_id, c.leaf_id, c.children, p.depth + 1
                        FROM merkle_radix_tree_node c, tree_path p
                        WHERE c.hash = json_extract(
                          p.children,
                          '$[' || json_extract(?, '$[' || p.depth || ']') || ']'
                        )
                        AND c.tree_id = ? AND reference > 0
                    )
                    SELECT
                        t.hash,
                        CASE t.hash
                            WHEN ? THEN NULL
                            ELSE l.data
                        END as data,
                        CASE t.hash
                            WHEN ? THEN 1
                            ELSE 0
                        END as cached
                    FROM tree_path t, merkle_radix_leaf l
                    WHERE t.tree_id = ? AND t.leaf_id = l.id
                    "#,
                )
                .bind::<Text, _>(state_root_hash)
                .bind::<BigInt, _>(tree_id)
                .bind::<Text, _>(
                    serde_json::to_string(&address_bytes)
                        .map_err(|err| InternalError::from_source(Box::new(err)))?,
                )
                .bind::<BigInt, _>(tree_id)
                .bind::<Nullable<Text>, _>(cached_data.as_ref().map(|(hash, _)| hash))
                .bind::<Nullable<Text>, _>(cached_data.as_ref().map(|(hash, _)| hash))
                .bind::<BigInt, _>(tree_id)
                .load::<LeafData>(self.conn)
                .map_err(|err| InternalError::from_source(Box::new(err)))?;

                match (values.into_iter().next(), data_cache, cached_data) {
                    (
                        Some(LeafData {
                            hash,
                            data: Some(data),
                            ..
                        }),
                        Some(cache),
                        _,
                    ) => {
                        if cache.cacheable(&data) {
                            cache.insert(address.to_string(), hash, data.clone())?;
                        }
                        results.push((address.to_string(), data));
                    }
                    (
                        Some(LeafData {
                            data: Some(data), ..
                        }),
                        None,
                        _,
                    ) => {
                        results.push((address.to_string(), data));
                    }
                    (Some(LeafData { hash, cached, .. }), Some(cache), Some((_, data)))
                        if cached =>
                    {
                        cache.touch_entry(&hash)?;
                        results.push((address.to_string(), data));
                    }
                    _ => (),
                }
            }

            Ok(results)
        })
    }
}

#[cfg(feature = "postgres")]
impl<'a> MerkleRadixGetLeavesOperation for MerkleRadixOperations<'a, PgConnection> {
    fn get_leaves(
        &self,
        tree_id: i64,
        state_root_hash: &str,
        addresses: Vec<&str>,
        data_cache: Option<&DataCache>,
    ) -> Result<Vec<(String, Vec<u8>)>, InternalError> {
        self.conn.transaction(|| {
            let mut results = vec![];
            for addr in addresses {
                let cached_data: Option<(String, Vec<u8>)> = data_cache
                    .map(|cache| cache.peek_last_known_data_for_address(addr))
                    .transpose()?
                    .flatten();

                // both the indexes in the array and the depth in the SQL statement are set to start at 1,
                // as the SQL arrays are 1-indexed.
                let address_branches: Vec<i16> = hex::decode(addr)
                    .map_err(|e| InternalError::from_source(Box::new(e)))?
                    .into_iter()
                    .map(|b| i16::from(b) + 1)
                    .collect();

                let values = sql_query(
                    r#"
                    WITH RECURSIVE tree_path AS
                    (
                        -- This is the initial node
                        SELECT hash, tree_id, leaf_id, children, 1 as depth
                        FROM merkle_radix_tree_node
                        WHERE hash = $1 AND tree_id = $2 AND reference > 0

                        UNION ALL

                        -- Recurse through the tree
                        SELECT c.hash, c.tree_id, c.leaf_id, c.children, p.depth + 1
                        FROM merkle_radix_tree_node c, tree_path p
                        WHERE c.hash = p.children[$3[p.depth]]
                        AND c.tree_id = $2 AND reference > 0
                    )
                    SELECT
                        t.hash,
                        CASE t.hash
                            WHEN $4 THEN NULL
                            ELSE l.data
                        END as data,
                        t.hash = $4 as cached
                    FROM tree_path t, merkle_radix_leaf l
                    WHERE t.tree_id = $2 AND t.leaf_id = l.id
                    "#,
                )
                .bind::<Text, _>(state_root_hash)
                .bind::<BigInt, _>(tree_id)
                .bind::<Array<SmallInt>, _>(&address_branches)
                .bind::<Nullable<Text>, _>(cached_data.as_ref().map(|(hash, _)| hash))
                .load::<LeafData>(self.conn)
                .map_err(|err| InternalError::from_source(Box::new(err)))?;

                match (values.into_iter().next(), data_cache, cached_data) {
                    (
                        Some(LeafData {
                            hash,
                            data: Some(data),
                            ..
                        }),
                        Some(cache),
                        _,
                    ) => {
                        if cache.cacheable(&data) {
                            cache.insert(addr.to_string(), hash, data.clone())?;
                        }
                        results.push((addr.to_string(), data));
                    }
                    (
                        Some(LeafData {
                            data: Some(data), ..
                        }),
                        None,
                        _,
                    ) => {
                        results.push((addr.to_string(), data));
                    }
                    (Some(LeafData { hash, cached, .. }), Some(cache), Some((_, data)))
                        if cached =>
                    {
                        cache.touch_entry(&hash)?;
                        results.push((addr.to_string(), data));
                    }
                    _ => (),
                }
            }

            Ok(results)
        })
    }
}

#[derive(Debug, QueryableByName)]
struct LeafData {
    #[column_name = "hash"]
    #[sql_type = "Text"]
    pub hash: String,

    #[column_name = "data"]
    #[sql_type = "Nullable<Binary>"]
    pub data: Option<Vec<u8>>,

    #[column_name = "cached"]
    #[sql_type = "Bool"]
    pub cached: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    use diesel::dsl::insert_into;
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

    use crate::state::merkle::sql::store::models::NewMerkleRadixLeaf;
    #[cfg(feature = "sqlite")]
    use crate::state::merkle::sql::store::operations::last_insert_rowid;
    use crate::state::merkle::sql::store::schema::merkle_radix_leaf;

    const MIN_CACHED_DATA_SIZE: usize = 10; // 10 bytes
    const CACHE_SIZE: u16 = 16; // number of entries in cache

    /// Test that the get entries on a non-existent root returns a empty entries.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_get_entries_empty_tree() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;
        let cache = DataCache::new(MIN_CACHED_DATA_SIZE, CACHE_SIZE);

        migration::sqlite::run_migrations(&conn)?;

        let entries = MerkleRadixOperations::new(&conn).get_leaves(
            1,
            "state-root",
            vec!["aabbcc"],
            Some(&cache),
        )?;

        assert!(entries.is_empty());

        Ok(())
    }

    /// Test that the get entries on a non-existent root returns a empty entries.
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_get_entries_empty_tree() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;
            let cache = DataCache::new(MIN_CACHED_DATA_SIZE, CACHE_SIZE);

            let entries = MerkleRadixOperations::new(&conn).get_leaves(
                1,
                "state-root",
                vec!["aabbcc"],
                Some(&cache),
            )?;

            assert!(entries.is_empty());

            Ok(())
        })
    }

    /// Test that a single leaf, with intermediate nodes will return the correct entry address and
    /// bytes.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_get_entries_single_entry() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;
        let cache = DataCache::new(MIN_CACHED_DATA_SIZE, CACHE_SIZE);

        migration::sqlite::run_migrations(&conn)?;

        insert_into(merkle_radix_leaf::table)
            .values(NewMerkleRadixLeaf {
                id: Some(1),
                tree_id: 1,
                address: "000000",
                data: b"hello",
            })
            .execute(&conn)?;

        let inserted_id = select(last_insert_rowid).get_result::<i64>(&conn)?;

        insert_into(sqlite_merkle_radix_tree_node::table)
            .values(vec![
                sqlite::MerkleRadixTreeNode::new("000000-hash", 1).with_leaf_id(Some(inserted_id)),
                sqlite::MerkleRadixTreeNode::new("0000-hash", 1)
                    .with_children(vec![Some("000000-hash".to_string())]),
                sqlite::MerkleRadixTreeNode::new("00-hash", 1)
                    .with_children(vec![Some("0000-hash".to_string())]),
                sqlite::MerkleRadixTreeNode::new("root-hash", 1)
                    .with_children(vec![Some("00-hash".to_string())]),
            ])
            .execute(&conn)?;

        let entries = MerkleRadixOperations::new(&conn).get_leaves(
            1,
            "root-hash",
            vec!["000000"],
            Some(&cache),
        )?;

        assert_eq!(entries, vec![("000000".to_string(), b"hello".to_vec(),),]);

        Ok(())
    }

    /// Test that a single leaf, with intermediate nodes will return the correct entry address and
    /// bytes.
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_get_entries_single_entry() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;
            let cache = DataCache::new(MIN_CACHED_DATA_SIZE, CACHE_SIZE);

            let leaf_id = 1;
            insert_into(merkle_radix_leaf::table)
                .values(NewMerkleRadixLeaf {
                    id: Some(leaf_id),
                    tree_id: 1,
                    address: "000000",
                    data: b"hello",
                })
                .execute(&conn)?;

            insert_into(postgres_merkle_radix_tree_node::table)
                .values(vec![
                    postgres::MerkleRadixTreeNode::new("000000-hash", 1)
                        .with_leaf_id(Some(leaf_id)),
                    postgres::MerkleRadixTreeNode::new("0000-hash", 1)
                        .with_children(vec![Some("000000-hash".to_string())]),
                    postgres::MerkleRadixTreeNode::new("00-hash", 1)
                        .with_children(vec![Some("0000-hash".to_string())]),
                    postgres::MerkleRadixTreeNode::new("root-hash", 1)
                        .with_children(vec![Some("00-hash".to_string())]),
                ])
                .execute(&conn)?;

            let entries = MerkleRadixOperations::new(&conn).get_leaves(
                1,
                "root-hash",
                vec!["000000"],
                Some(&cache),
            )?;

            assert_eq!(entries, vec![("000000".to_string(), b"hello".to_vec(),),]);

            Ok(())
        })
    }

    /// Test that a single leaf, with intermediate nodes will return the correct entry address and
    /// the bytes, where the bytes have been cached.
    ///
    /// Note, in this test, in order to verify that the cache has been hit, we set a different
    /// value than what is set in the database.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_get_entries_single_entry_cached() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;
        let cache = DataCache::new(MIN_CACHED_DATA_SIZE, CACHE_SIZE);
        cache.insert(
            "000000".into(),
            "000000-hash".into(),
            b"cached data!".to_vec(),
        )?;

        migration::sqlite::run_migrations(&conn)?;

        insert_into(merkle_radix_leaf::table)
            .values(NewMerkleRadixLeaf {
                id: Some(1),
                tree_id: 1,
                address: "000000",
                data: b"not cached",
            })
            .execute(&conn)?;

        let inserted_id = select(last_insert_rowid).get_result::<i64>(&conn)?;

        insert_into(sqlite_merkle_radix_tree_node::table)
            .values(vec![
                sqlite::MerkleRadixTreeNode::new("000000-hash", 1).with_leaf_id(Some(inserted_id)),
                sqlite::MerkleRadixTreeNode::new("0000-hash", 1)
                    .with_children(vec![Some("000000-hash".to_string())]),
                sqlite::MerkleRadixTreeNode::new("00-hash", 1)
                    .with_children(vec![Some("0000-hash".to_string())]),
                sqlite::MerkleRadixTreeNode::new("root-hash", 1)
                    .with_children(vec![Some("00-hash".to_string())]),
            ])
            .execute(&conn)?;

        let entries = MerkleRadixOperations::new(&conn).get_leaves(
            1,
            "root-hash",
            vec!["000000"],
            Some(&cache),
        )?;

        assert_eq!(
            entries,
            vec![("000000".to_string(), b"cached data!".to_vec(),),]
        );

        Ok(())
    }

    /// Test that a single leaf, with intermediate nodes will return the correct entry address and
    /// the bytes, where the bytes have been cached.
    ///
    /// Note, in this test, in order to verify that the cache has been hit, we set a different
    /// value than what is set in the database.
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_get_entries_single_entry_cached() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;
            let cache = DataCache::new(MIN_CACHED_DATA_SIZE, CACHE_SIZE);
            cache.insert(
                "000000".into(),
                "000000-hash".into(),
                b"cached data!".to_vec(),
            )?;

            let leaf_id = 1;
            insert_into(merkle_radix_leaf::table)
                .values(NewMerkleRadixLeaf {
                    id: Some(leaf_id),
                    tree_id: 1,
                    address: "000000",
                    data: b"not cached",
                })
                .execute(&conn)?;

            insert_into(postgres_merkle_radix_tree_node::table)
                .values(vec![
                    postgres::MerkleRadixTreeNode::new("000000-hash", 1)
                        .with_leaf_id(Some(leaf_id)),
                    postgres::MerkleRadixTreeNode::new("0000-hash", 1)
                        .with_children(vec![Some("000000-hash".to_string())]),
                    postgres::MerkleRadixTreeNode::new("00-hash", 1)
                        .with_children(vec![Some("0000-hash".to_string())]),
                    postgres::MerkleRadixTreeNode::new("root-hash", 1)
                        .with_children(vec![Some("00-hash".to_string())]),
                ])
                .execute(&conn)?;

            let entries = MerkleRadixOperations::new(&conn).get_leaves(
                1,
                "root-hash",
                vec!["000000"],
                Some(&cache),
            )?;

            assert_eq!(
                entries,
                vec![("000000".to_string(), b"cached data!".to_vec(),),]
            );

            Ok(())
        })
    }

    /// Test that a single leaf, with intermediate nodes will return the correct entry address and
    /// the bytes, and the value is large enough to be written to the cache.
    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_get_entries_single_large_entry_cached() -> Result<(), Box<dyn std::error::Error>> {
        let conn = SqliteConnection::establish(":memory:")?;
        let cache = DataCache::new(MIN_CACHED_DATA_SIZE, CACHE_SIZE);

        migration::sqlite::run_migrations(&conn)?;

        let leaf_id = 1;
        insert_into(merkle_radix_leaf::table)
            .values(NewMerkleRadixLeaf {
                id: Some(leaf_id),
                tree_id: 1,
                address: "000000",
                data: b"large enough to cache",
            })
            .execute(&conn)?;

        insert_into(sqlite_merkle_radix_tree_node::table)
            .values(vec![
                sqlite::MerkleRadixTreeNode::new("000000-hash", 1).with_leaf_id(Some(leaf_id)),
                sqlite::MerkleRadixTreeNode::new("0000-hash", 1)
                    .with_children(vec![Some("000000-hash".to_string())]),
                sqlite::MerkleRadixTreeNode::new("00-hash", 1)
                    .with_children(vec![Some("0000-hash".to_string())]),
                sqlite::MerkleRadixTreeNode::new("root-hash", 1)
                    .with_children(vec![Some("00-hash".to_string())]),
            ])
            .execute(&conn)?;

        let entries = MerkleRadixOperations::new(&conn).get_leaves(
            1,
            "root-hash",
            vec!["000000"],
            Some(&cache),
        )?;

        assert_eq!(
            entries,
            vec![("000000".to_string(), b"large enough to cache".to_vec(),),]
        );

        assert_eq!(
            Some(("000000-hash".to_string(), b"large enough to cache".to_vec())),
            cache.peek_last_known_data_for_address("000000")?
        );

        Ok(())
    }

    /// Test that a single leaf, with intermediate nodes will return the correct entry address and
    /// the bytes, and the value is large enough to be written to the cache.
    #[cfg(feature = "state-merkle-sql-postgres-tests")]
    #[test]
    fn postgres_get_entries_single_large_entry_cached() -> Result<(), Box<dyn std::error::Error>> {
        run_postgres_test(|url| {
            let conn = PgConnection::establish(&url)?;
            let cache = DataCache::new(MIN_CACHED_DATA_SIZE, CACHE_SIZE);

            let leaf_id = 1;
            insert_into(merkle_radix_leaf::table)
                .values(NewMerkleRadixLeaf {
                    id: Some(leaf_id),
                    tree_id: 1,
                    address: "000000",
                    data: b"large enough to cache",
                })
                .execute(&conn)?;

            insert_into(postgres_merkle_radix_tree_node::table)
                .values(vec![
                    postgres::MerkleRadixTreeNode::new("000000-hash", 1)
                        .with_leaf_id(Some(leaf_id)),
                    postgres::MerkleRadixTreeNode::new("0000-hash", 1)
                        .with_children(vec![Some("000000-hash".to_string())]),
                    postgres::MerkleRadixTreeNode::new("00-hash", 1)
                        .with_children(vec![Some("0000-hash".to_string())]),
                    postgres::MerkleRadixTreeNode::new("root-hash", 1)
                        .with_children(vec![Some("00-hash".to_string())]),
                ])
                .execute(&conn)?;

            let entries = MerkleRadixOperations::new(&conn).get_leaves(
                1,
                "root-hash",
                vec!["000000"],
                Some(&cache),
            )?;

            assert_eq!(
                entries,
                vec![("000000".to_string(), b"large enough to cache".to_vec(),),]
            );

            assert_eq!(
                Some(("000000-hash".to_string(), b"large enough to cache".to_vec())),
                cache.peek_last_known_data_for_address("000000")?
            );

            Ok(())
        })
    }
}

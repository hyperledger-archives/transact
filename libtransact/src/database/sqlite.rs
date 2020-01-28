/*
 * Copyright 2020 Cargill Incorporated
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
 * ------------------------------------------------------------------------------
 */

use std::cell::RefCell;
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::num::NonZeroU32;
use std::rc::Rc;

use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{named_params, params};

use super::{
    Database, DatabaseCursor, DatabaseError, DatabaseReader, DatabaseReaderCursor, DatabaseWriter,
};

type SqliteConnection = r2d2::PooledConnection<SqliteConnectionManager>;

/// The default size
pub const DEFAULT_MMAP_SIZE: i64 = 100 * 1024 * 1024;

/// A builder for generating SqliteDatabase instances.
pub struct SqliteDatabaseBuilder {
    path: Option<String>,
    indexes: Vec<&'static str>,
    pool_size: Option<u32>,
    memory_map_size: i64,
}

impl SqliteDatabaseBuilder {
    fn new() -> Self {
        Self {
            path: None,
            indexes: vec![],
            pool_size: None,
            memory_map_size: DEFAULT_MMAP_SIZE,
        }
    }

    /// Set the path for the Sqlite database.
    pub fn with_path<S: Into<String>>(mut self, path: S) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Set the names of the indexes to include in the database instance.
    pub fn with_indexes(mut self, indexes: &[&'static str]) -> Self {
        self.indexes = indexes.to_vec();
        self
    }

    /// Set the connection pool size.
    pub fn with_connection_pool_size(mut self, pool_size: NonZeroU32) -> Self {
        self.pool_size = Some(pool_size.get());
        self
    }

    /// Set the size used for Memory-Mapped I/O.
    ///
    /// This can be disabled by setting the value to `0`
    pub fn with_memory_map_size(mut self, memory_map_size: u64) -> Self {
        // The try from would fail if the u64 is greater than max i64, so we can unwrap this as
        // such.
        self.memory_map_size = i64::try_from(memory_map_size).unwrap_or(std::i64::MAX);
        self
    }

    /// Constructs the database instance.
    ///
    /// # Errors
    ///
    /// This may return a SqliteDatabaseError for a variety of reasons:
    ///
    /// * No path provided
    /// * Unable to connect to the database.
    /// * Unable to configure the provided memory map size
    /// * Unable to create tables, as required.
    pub fn build(self) -> Result<SqliteDatabase, SqliteDatabaseError> {
        let path = self.path.ok_or_else(|| SqliteDatabaseError {
            context: "must provide a sqlite database path".into(),
            source: None,
        })?;

        let manager = SqliteConnectionManager::file(&path);

        let mut pool_builder = r2d2::Pool::builder();
        if let Some(pool_size) = self.pool_size {
            pool_builder = pool_builder.max_size(pool_size);
        }
        let pool = pool_builder
            .build(manager)
            .map_err(|err| SqliteDatabaseError {
                context: format!("unable to create sqlite connection pool to {}", path),
                source: Some(Box::new(err)),
            })?;

        let conn = pool.get().map_err(|err| SqliteDatabaseError {
            context: "unable to connect to database".into(),
            source: Some(Box::new(err)),
        })?;

        conn.pragma_update(None, "mmap_size", &self.memory_map_size)
            .map_err(|err| SqliteDatabaseError {
                context: "unable to configure memory map I/O".into(),
                source: Some(Box::new(err)),
            })?;

        let create_table = "CREATE TABLE IF NOT EXISTS transact_primary (\
                            key BLOB PRIMARY KEY, \
                            value BLOB NOT NULL\
                            )";
        conn.execute(create_table, params![])
            .map_err(|err| SqliteDatabaseError {
                context: "unable to create primary table".into(),
                source: Some(Box::new(err)),
            })?;

        for index in self.indexes {
            let create_index_table = format!(
                "CREATE TABLE IF NOT EXISTS transact_index_{} (\
                 index_key BLOB PRIMARY KEY, \
                 value BLOB NOT NULL\
                 )",
                index,
            );

            conn.execute(&create_index_table, params![])
                .map_err(|err| SqliteDatabaseError {
                    context: format!("unable to create index {} table", index),
                    source: Some(Box::new(err)),
                })?;
        }

        Ok(SqliteDatabase { pool })
    }
}

#[derive(Clone)]
pub struct SqliteDatabase {
    pool: r2d2::Pool<SqliteConnectionManager>,
}

impl SqliteDatabase {
    pub fn new(path: &str, indexes: &[&'static str]) -> Result<Self, SqliteDatabaseError> {
        SqliteDatabaseBuilder::new()
            .with_path(path)
            .with_indexes(indexes)
            .build()
    }

    pub fn builder() -> SqliteDatabaseBuilder {
        SqliteDatabaseBuilder::new()
    }

    pub fn vacuum(&self) -> Result<(), SqliteDatabaseError> {
        let conn = self.pool.get().map_err(|err| SqliteDatabaseError {
            context: "unable to connect to database".into(),
            source: Some(Box::new(err)),
        })?;

        conn.execute("VACUUM", params![])
            .map_err(|err| SqliteDatabaseError {
                context: "unable to vacuum database".into(),
                source: Some(Box::new(err)),
            })?;

        Ok(())
    }
}

impl Database for SqliteDatabase {
    fn get_reader<'a>(&'a self) -> Result<Box<dyn DatabaseReader + 'a>, DatabaseError> {
        let conn = self.pool.get().map_err(|err| {
            DatabaseError::ReaderError(format!("Unable to connect to database: {}", err))
        })?;

        Ok(Box::new(SqliteDatabaseReader {
            conn: Rc::new(RefCell::new(conn)),
        }))
    }

    fn get_writer<'a>(&'a self) -> Result<Box<dyn DatabaseWriter + 'a>, DatabaseError> {
        let conn = self.pool.get().map_err(|err| {
            DatabaseError::WriterError(format!("Unable to connect to database: {}", err))
        })?;

        conn.execute_batch("BEGIN DEFERRED").map_err(|err| {
            DatabaseError::WriterError(format!("Unable to begin read transaction: {}", err))
        })?;

        Ok(Box::new(SqliteDatabaseWriter {
            conn: Rc::new(RefCell::new(conn)),
        }))
    }

    fn clone_box(&self) -> Box<dyn Database> {
        Box::new(self.clone())
    }
}

struct SqliteDatabaseReader {
    conn: Rc<RefCell<SqliteConnection>>,
}

impl DatabaseReader for SqliteDatabaseReader {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let mut conn = self.conn.borrow_mut();
        execute_get(&mut conn, key)
    }

    fn index_get(&self, index: &str, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let mut conn = self.conn.borrow_mut();
        execute_index_get(&mut conn, index, key)
    }

    fn cursor(&self) -> Result<DatabaseCursor, DatabaseError> {
        let total = {
            let mut conn = self.conn.borrow_mut();
            execute_count(&mut conn)?
        };

        Ok(Box::new(SqliteCursor::new(
            self.conn.clone(),
            "SELECT key, value from transact_primary LIMIT ? OFFSET ?",
            total,
        )?))
    }

    fn index_cursor(&self, index: &str) -> Result<DatabaseCursor, DatabaseError> {
        let total = {
            let mut conn = self.conn.borrow_mut();
            execute_index_count(&mut conn, index)?
        };

        Ok(Box::new(SqliteCursor::new(
            self.conn.clone(),
            &format!(
                "SELECT index_key, value from transact_index_{} LIMIT ? OFFSET ?",
                index
            ),
            total,
        )?))
    }

    fn count(&self) -> Result<usize, DatabaseError> {
        let mut conn = self.conn.borrow_mut();
        execute_count(&mut conn).map(|count| count as usize)
    }

    fn index_count(&self, index: &str) -> Result<usize, DatabaseError> {
        let mut conn = self.conn.borrow_mut();
        execute_index_count(&mut conn, index).map(|count| count as usize)
    }
}

struct SqliteDatabaseWriter {
    conn: Rc<RefCell<SqliteConnection>>,
}

impl Drop for SqliteDatabaseWriter {
    fn drop(&mut self) {
        if let Err(err) = self.conn.borrow_mut().execute_batch("ROLLBACK") {
            warn!("Unable to rollback writer transaction: {}", err);
        }
    }
}

impl DatabaseWriter for SqliteDatabaseWriter {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        let conn = self.conn.borrow_mut();
        let mut stmt = conn
            .prepare_cached("INSERT INTO transact_primary (key, value) VALUES (?, ?)")
            .map_err(|err| DatabaseError::WriterError(format!("unable to write value: {}", err)))?;

        match stmt.execute(params![key, value]) {
            Ok(_) => Ok(()),
            Err(ref err) if err.to_string().contains("UNIQUE constraint") => {
                Err(DatabaseError::DuplicateEntry)
            }
            Err(err) => Err(DatabaseError::WriterError(format!(
                "unable to write value: {}",
                err
            ))),
        }
    }

    fn overwrite(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        let conn = self.conn.borrow_mut();
        let mut stmt = conn
            .prepare_cached(
                "INSERT OR REPLACE INTO transact_primary (key, value) VALUES (:key, :value)",
            )
            .map_err(|err| DatabaseError::WriterError(format!("unable to write value: {}", err)))?;

        stmt.execute_named(named_params! {":key": key, ":value": value})
            .map_err(|err| DatabaseError::WriterError(format!("unable to write value: {}", err)))?;

        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        let conn = self.conn.borrow_mut();
        let mut stmt = conn
            .prepare_cached("DELETE FROM transact_primary WHERE key = ?")
            .map_err(|err| {
                DatabaseError::WriterError(format!("unable to delete value: {}", err))
            })?;
        stmt.execute(params![key]).map_err(|err| {
            DatabaseError::WriterError(format!("unable to delete value: {}", err))
        })?;

        Ok(())
    }

    fn index_put(&mut self, index: &str, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        let sql = format!(
            "INSERT OR REPLACE INTO transact_index_{} (index_key, value) VALUES (:key, :value)",
            index
        );
        let conn = self.conn.borrow_mut();
        let mut stmt = conn
            .prepare_cached(&sql)
            .map_err(|err| DatabaseError::WriterError(format!("unable to write value: {}", err)))?;

        stmt.execute_named(named_params! {":key": key, ":value": value})
            .map_err(|err| DatabaseError::WriterError(format!("unable to write value: {}", err)))?;

        Ok(())
    }
    fn index_delete(&mut self, index: &str, key: &[u8]) -> Result<(), DatabaseError> {
        let sql = format!("DELETE FROM transact_index_{} WHERE index_key = ?", index);
        let conn = self.conn.borrow_mut();
        let mut stmt = conn.prepare_cached(&sql).map_err(|err| {
            DatabaseError::WriterError(format!("unable to delete value: {}", err))
        })?;
        stmt.execute(params![key]).map_err(|err| {
            DatabaseError::WriterError(format!("unable to delete value: {}", err))
        })?;

        Ok(())
    }
    fn commit(self: Box<Self>) -> Result<(), DatabaseError> {
        let conn = self.conn.borrow_mut();
        conn.execute_batch("COMMIT").map_err(|err| {
            DatabaseError::WriterError(format!("Unable to commit changes: {}", err))
        })?;

        Ok(())
    }
    fn as_reader(&self) -> &dyn DatabaseReader {
        self
    }
}

impl DatabaseReader for SqliteDatabaseWriter {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let mut conn = self.conn.borrow_mut();
        execute_get(&mut conn, key)
    }

    fn index_get(&self, index: &str, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let mut conn = self.conn.borrow_mut();
        execute_index_get(&mut conn, index, key)
    }

    fn cursor(&self) -> Result<DatabaseCursor, DatabaseError> {
        unimplemented!()
    }

    fn index_cursor(&self, _index: &str) -> Result<DatabaseCursor, DatabaseError> {
        unimplemented!()
    }

    fn count(&self) -> Result<usize, DatabaseError> {
        let mut conn = self.conn.borrow_mut();
        execute_count(&mut conn).map(|count| count as usize)
    }

    fn index_count(&self, index: &str) -> Result<usize, DatabaseError> {
        let mut conn = self.conn.borrow_mut();
        execute_index_count(&mut conn, index).map(|count| count as usize)
    }
}

const PAGE_SIZE: i64 = 100;

struct SqliteCursor {
    conn: Rc<RefCell<SqliteConnection>>,
    sql: String,
    start: Option<i64>,
    total: i64,
    cache: VecDeque<(Vec<u8>, Vec<u8>)>,
}

impl SqliteCursor {
    fn new(
        conn: Rc<RefCell<SqliteConnection>>,
        prepared_stmt_sql: &str,
        total: i64,
    ) -> Result<Self, DatabaseError> {
        let mut new_instance = Self {
            conn,
            sql: prepared_stmt_sql.into(),
            total,
            start: Some(0),
            cache: VecDeque::new(),
        };

        new_instance.fill_cache()?;

        Ok(new_instance)
    }

    fn fill_cache(&mut self) -> Result<(), DatabaseError> {
        if let Some(start) = self.start.as_ref() {
            let conn = self.conn.borrow_mut();

            let mut stmt = conn.prepare_cached(&self.sql).map_err(|err| {
                DatabaseError::ReaderError(format!(
                    "unable to prepare statement for cursor: {}",
                    err
                ))
            })?;

            let value_iter = stmt
                .query_map(params![PAGE_SIZE, start], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .map_err(|err| {
                    DatabaseError::ReaderError(format!(
                        "unable to execute query for cursor: {}",
                        err
                    ))
                })?;

            let new_cache: Result<VecDeque<_>, _> = value_iter.collect();

            self.cache = new_cache.map_err(|err| {
                DatabaseError::ReaderError(format!("unable to read entries: {}", err))
            })?;

            let next_start = start + PAGE_SIZE;
            self.start = if next_start >= self.total {
                None
            } else {
                Some(next_start)
            };
        }
        Ok(())
    }
}

impl Iterator for SqliteCursor {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cache.is_empty() {
            if let Err(err) = self.fill_cache() {
                error!("Unable to fill cursor cache; aborting: {}", err);
            }
        }

        self.cache.pop_front()
    }
}

impl DatabaseReaderCursor for SqliteCursor {
    fn seek_first(&mut self) -> Option<Self::Item> {
        self.start = Some(0);
        if let Err(err) = self.fill_cache() {
            error!("Unable to fill cursor cache; aborting: {}", err);
            self.cache = VecDeque::new();
        }

        self.cache.pop_front()
    }

    fn seek_last(&mut self) -> Option<Self::Item> {
        if self.total > 0 {
            self.start = Some(self.total - 1);
            if let Err(err) = self.fill_cache() {
                error!("Unable to fill cursor cache; aborting: {}", err);
                self.cache = VecDeque::new();
            }

            self.cache.pop_front()
        } else {
            None
        }
    }
}

fn execute_get(conn: &mut SqliteConnection, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
    let mut stmt = conn
        .prepare_cached("SELECT value FROM transact_primary WHERE key = ?")
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))?;

    let mut value_iter = stmt
        .query_map(params![key], |row| row.get(0))
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))?;

    value_iter
        .next()
        .transpose()
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))
}

fn execute_count(conn: &mut SqliteConnection) -> Result<i64, DatabaseError> {
    let mut stmt = conn
        .prepare_cached("SELECT COUNT(key) FROM transact_primary")
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))?;

    stmt.query_row(params![], |row| row.get(0))
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))
}

fn execute_index_get(
    conn: &mut SqliteConnection,
    index: &str,
    key: &[u8],
) -> Result<Option<Vec<u8>>, DatabaseError> {
    let query = format!(
        "SELECT value FROM transact_index_{} WHERE index_key = ?",
        index
    );
    let mut stmt = conn
        .prepare_cached(&query)
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))?;

    let mut value_iter = stmt
        .query_map(params![key], |row| row.get(0))
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))?;

    value_iter
        .next()
        .transpose()
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))
}

fn execute_index_count(conn: &mut SqliteConnection, index: &str) -> Result<i64, DatabaseError> {
    let query = format!("SELECT COUNT(index_key) FROM transact_index_{}", index);
    let mut stmt = conn
        .prepare_cached(&query)
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))?;

    stmt.query_row(params![], |row| row.get(0))
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))
}

#[derive(Debug)]
pub struct SqliteDatabaseError {
    context: String,
    source: Option<Box<dyn std::error::Error + Send>>,
}

impl std::error::Error for SqliteDatabaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let Some(ref err) = self.source {
            Some(&**err)
        } else {
            None
        }
    }
}

impl std::fmt::Display for SqliteDatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(ref err) = self.source {
            write!(f, "{}: {}", self.context, err)
        } else {
            f.write_str(&self.context)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::convert::TryInto;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Asserts that there are COUNT many objects in DB.
    fn assert_database_count(count: usize, db: &dyn Database) {
        let reader = db.get_reader().expect("unable to get a database reader");

        assert_eq!(
            reader.count().expect("could not count database records"),
            count,
        );
    }

    /// Asserts that there are are COUNT many objects in DB's INDEX.
    fn assert_index_count(index: &str, count: usize, db: &dyn Database) {
        let reader = db.get_reader().expect("unable to get a database reader");

        assert_eq!(
            reader
                .index_count(index)
                .expect("unable to count index records"),
            count,
        );
    }

    /// Asserts that KEY is associated with VAL in DB.
    fn assert_key_value(key: u8, val: u8, db: &dyn Database) {
        let reader = db.get_reader().expect("unable to get a database reader");

        assert_eq!(
            reader.get(&[key]).expect("unable to get value"),
            Some(vec![val])
        );
    }

    /// Asserts that KEY is associated with VAL in DB's INDEX.
    fn assert_index_key_value(index: &str, key: u8, val: u8, db: &dyn Database) {
        let reader = db.get_reader().expect("unable to get a database reader");

        assert_eq!(
            reader
                .index_get(index, &[key])
                .expect("unable to get value by index"),
            Some(vec![val])
        );
    }

    /// Asserts that KEY is not in DB.
    fn assert_not_in_database(key: u8, db: &dyn Database) {
        let reader = db.get_reader().expect("unable to get a database reader");

        assert!(reader.get(&[key]).expect("unable to get value").is_none());
    }

    /// Asserts that KEY is not in DB's INDEX.
    fn assert_not_in_index(index: &str, key: u8, db: &dyn Database) {
        let reader = db.get_reader().expect("unable to get a database reader");

        assert!(reader
            .index_get(index, &[key])
            .expect("unable to get value by index")
            .is_none());
    }

    #[test]
    fn test_basic_db_operations() {
        run_test(|db_path| {
            let database =
                SqliteDatabase::new(&db_path, &["a", "b"]).expect("Could not instantiate database");

            assert_database_count(0, &database);
            assert_not_in_database(3, &database);
            assert_not_in_database(5, &database);

            // Add {3: 4}
            let mut writer = database.get_writer().expect("unable to get db writer");
            writer.put(&[3], &[4]).expect("unable to put value");

            assert_database_count(0, &database);
            assert_not_in_database(3, &database);

            writer.commit().expect("unable to commit");

            assert_database_count(1, &database);
            assert_key_value(3, 4, &database);

            // Add {5: 6}
            let mut writer = database.get_writer().expect("unable to get db writer");
            writer.put(&[5], &[6]).expect("unable to put value");
            writer.commit().expect("unable to commit");

            assert_database_count(2, &database);
            assert_key_value(5, 6, &database);
            assert_key_value(3, 4, &database);

            // Delete {3: 4}
            let mut writer = database.get_writer().expect("unable to get db writer");
            writer.delete(&[3]).expect("unable to delete value");

            assert_database_count(2, &database);

            writer.commit().expect("unable to commit");

            assert_database_count(1, &database);
            assert_key_value(5, 6, &database);
            assert_not_in_database(3, &database);

            // Add {55: 5} in "a"
            assert_index_count("a", 0, &database);
            assert_index_count("b", 0, &database);
            assert_not_in_index("a", 5, &database);
            assert_not_in_index("b", 5, &database);

            let mut writer = database.get_writer().expect("unable to get db writer");
            writer
                .index_put("a", &[55], &[5])
                .expect("unable to put in index value");

            assert_index_count("a", 0, &database);
            assert_index_count("b", 0, &database);
            assert_not_in_index("a", 5, &database);
            assert_not_in_index("b", 5, &database);

            writer.commit().expect("unable to commit");

            assert_index_count("a", 1, &database);
            assert_index_count("b", 0, &database);
            assert_index_key_value("a", 55, 5, &database);
            assert_not_in_index("b", 5, &database);
            assert_database_count(1, &database);
            assert_key_value(5, 6, &database);
            assert_not_in_database(3, &database);

            // Delete {55: 5} in "a"
            let mut writer = database.get_writer().expect("unable to get db writer");
            writer
                .index_delete("a", &[55])
                .expect("unable to delete value by index");

            assert_index_count("a", 1, &database);
            assert_index_count("b", 0, &database);
            assert_index_key_value("a", 55, 5, &database);
            assert_not_in_index("b", 5, &database);

            writer.commit().expect("unable to commit");

            assert_index_count("a", 0, &database);
            assert_index_count("b", 0, &database);
            assert_not_in_index("a", 5, &database);
            assert_not_in_index("b", 5, &database);
            assert_database_count(1, &database);
            assert_key_value(5, 6, &database);
            assert_not_in_database(3, &database);

            database
                .vacuum()
                .expect("should have successfully vacuumed");
        })
    }

    /// Tests the cursor operations from the reader and writer's perspective.
    #[test]
    fn test_cursor_operations() {
        run_test(|db_path| {
            let db =
                SqliteDatabase::new(&db_path, &["a", "b"]).expect("Could not instantiate database");

            {
                let reader = db.get_reader().expect("unable to get a database reader");
                let cursor = reader.cursor().expect("unable to open cursor");

                assert!(cursor.collect::<Vec<_>>().is_empty());
            }

            let mut writer = db.get_writer().expect("unable to get db writer");
            writer.put(&[1], b"hello").expect("unable to put value");
            writer.put(&[2], b"bonjour").expect("unable to put value");
            writer.put(&[3], b"guten tag").expect("unable to put value");

            writer.commit().expect("unable to commit");

            {
                let reader = db.get_reader().expect("unable to get a database reader");
                let cursor = reader.cursor().expect("unable to open cursor");

                assert_eq!(
                    vec![
                        (vec![1u8], b"hello".to_vec()),
                        (vec![2u8], b"bonjour".to_vec()),
                        (vec![3u8], b"guten tag".to_vec()),
                    ],
                    cursor.collect::<Vec<_>>()
                );
            }

            {
                let reader = db.get_reader().expect("unable to get a database reader");
                let mut cursor = reader.cursor().expect("unable to open cursor");

                assert_eq!(Some((vec![1u8], b"hello".to_vec())), cursor.next());
                assert_eq!(Some((vec![2u8], b"bonjour".to_vec())), cursor.next());

                assert_eq!(Some((vec![1u8], b"hello".to_vec())), cursor.seek_first());

                assert_eq!(Some((vec![3u8], b"guten tag".to_vec())), cursor.seek_last());

                assert_eq!(None, cursor.next());
            }
        })
    }

    /// Tests the index cursor operations from the reader and writer's perspective.
    #[test]
    fn test_index_cursor_operations() {
        run_test(|db_path| {
            let db =
                SqliteDatabase::new(&db_path, &["a", "b"]).expect("Could not instantiate database");

            {
                let reader = db.get_reader().expect("unable to get a database reader");
                let cursor = reader.index_cursor("a").expect("unable to open cursor");

                assert!(cursor.collect::<Vec<_>>().is_empty());
            }

            let mut writer = db.get_writer().expect("unable to get db writer");
            writer
                .index_put("a", &[1], b"hello")
                .expect("unable to put value");
            writer
                .index_put("a", &[2], b"bonjour")
                .expect("unable to put value");
            writer
                .index_put("a", &[3], b"guten tag")
                .expect("unable to put value");
            writer
                .index_put("b", &[44], b"goodbye")
                .expect("unable to put value");

            writer.commit().expect("unable to commit");

            {
                let reader = db.get_reader().expect("unable to get a database reader");
                let cursor = reader.index_cursor("a").expect("unable to open cursor");

                assert_eq!(
                    vec![
                        (vec![1u8], b"hello".to_vec()),
                        (vec![2u8], b"bonjour".to_vec()),
                        (vec![3u8], b"guten tag".to_vec()),
                    ],
                    cursor.collect::<Vec<_>>()
                );
            }

            {
                let reader = db.get_reader().expect("unable to get a database reader");
                let mut cursor = reader.index_cursor("a").expect("unable to open cursor");

                assert_eq!(Some((vec![1u8], b"hello".to_vec())), cursor.next());
                assert_eq!(Some((vec![2u8], b"bonjour".to_vec())), cursor.next());

                assert_eq!(Some((vec![1u8], b"hello".to_vec())), cursor.seek_first());

                assert_eq!(Some((vec![3u8], b"guten tag".to_vec())), cursor.seek_last());

                assert_eq!(None, cursor.next());
            }
        })
    }

    #[test]
    fn test_large_cursor() {
        run_test(|db_path| {
            let db =
                SqliteDatabase::new(&db_path, &["a", "b"]).expect("Could not instantiate database");
            let mut writer = db.get_writer().expect("unable to get db writer");

            for i in 0..PAGE_SIZE * 2 {
                let i_bytes = i.to_be_bytes();
                writer
                    .put(&i_bytes, b"record")
                    .expect("unable to write record");
            }

            writer.commit().expect("unable to commit records");

            let reader = db.get_reader().expect("unable to get a database reader");
            let cursor = reader.cursor().expect("unable to open cursor");

            let record_ids: Vec<i64> = cursor
                .map(|(key, _)| {
                    let (int_bytes, _) = key.split_at(std::mem::size_of::<i64>());
                    i64::from_be_bytes(
                        int_bytes
                            .try_into()
                            .expect("should be the correct number of bytes"),
                    )
                })
                .collect();

            assert_eq!(
                (0..PAGE_SIZE * 2).into_iter().collect::<Vec<i64>>(),
                record_ids
            );
        })
    }

    static GLOBAL_THREAD_COUNT: AtomicUsize = AtomicUsize::new(1);

    fn run_test<T>(test: T) -> ()
    where
        T: FnOnce(&str) -> () + std::panic::UnwindSafe,
    {
        let dbpath = temp_db_path();

        let testpath = dbpath.clone();
        let result = std::panic::catch_unwind(move || test(&testpath));

        std::fs::remove_file(dbpath).unwrap();

        assert!(result.is_ok())
    }

    fn temp_db_path() -> String {
        let mut temp_dir = std::env::temp_dir();

        let thread_id = GLOBAL_THREAD_COUNT.fetch_add(1, Ordering::SeqCst);
        temp_dir.push(format!("sqlite-test-{:?}.db", thread_id));
        temp_dir.to_str().unwrap().to_string()
    }
}

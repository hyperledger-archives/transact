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

//! An Sqlite-backed implementation of the database traits.
//!
//! # Note
//!
//! When using this feature with an in-memory sqlite instance, `DatabaseReader`s cannot be opened
//! at the same time as an existing `DatabaseWriter`. This is particularly true when using a
//! database path like `"file::memory:?cache=shared".  In this case, the underlying connection pool
//! will use the same database instance.
//!
//! For a single, non-shared memory instances, the database should be created with a connection
//! pool size of 1, such that creating readers or writers across threads will block until a
//! connection is available.
//!
//! This can be accomplished via
//!
//! ```
//! # use std::num::NonZeroU32;
//! # use transact::database::sqlite::SqliteDatabase;
//! let database = SqliteDatabase::builder()
//!     .with_path(":memory:")
//!     .with_connection_pool_size(NonZeroU32::new(1).unwrap())
//!     .build()
//!     .unwrap();
//! ```
//!
//! # Feature
//!
//! This is available via the optional feature "sqlite-db".
//!
//! _This is currently an experimental feature._
use std::cell::RefCell;
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::num::NonZeroU32;
use std::rc::Rc;

use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{
    named_params, params,
    types::{ToSql, ToSqlOutput, ValueRef},
};

use super::{
    Database, DatabaseCursor, DatabaseError, DatabaseReader, DatabaseReaderCursor, DatabaseWriter,
};

type SqliteConnection = r2d2::PooledConnection<SqliteConnectionManager>;

/// The default size
pub const DEFAULT_MMAP_SIZE: i64 = 100 * 1024 * 1024;

/// Synchronous setting values.
///
/// See the [PRAGMA "synchronous"](https://sqlite.org/pragma.html#pragma_journal_mode)
/// documentation for more details.
#[derive(Debug)]
pub enum Synchronous {
    /// With synchronous Off, SQLite continues without syncing as soon as it has handed data
    /// off to the operating system. If the application running SQLite crashes, the data will be
    /// safe, but the database might become corrupted if the operating system crashes or the
    /// computer loses power before that data has been written to the disk surface. On the other
    /// hand, commits can be orders of magnitude faster with synchronous Off.
    ///
    /// Not Recommended for production use.
    Off,
    /// When synchronous is Normal, the SQLite database engine will still sync at the most
    /// critical moments, but less often than in FULL mode.  WAL mode is safe from corruption with
    /// Normal. WAL mode is always consistent with Normal, but WAL mode does lose durability. A
    /// transaction committed in WAL mode with Normal might roll back following a power loss or
    /// system crash. Transactions are durable across application crashes regardless of the
    /// synchronous setting or journal mode. The Normal setting is a good choice for most
    /// applications running in WAL mode.
    Normal,
    /// When synchronous is Full, the SQLite database engine will use the xSync method of the VFS
    /// to ensure that all content is safely written to the disk surface prior to continuing. This
    /// ensures that an operating system crash or power failure will not corrupt the database. Full
    /// synchronous is very safe, but it is also slower. Full is the most commonly used synchronous
    /// setting when not in WAL mode.
    Full,
}

impl ToSql for Synchronous {
    fn to_sql(&self) -> Result<ToSqlOutput, rusqlite::Error> {
        match self {
            Synchronous::Off => Ok(ToSqlOutput::Borrowed(ValueRef::Text(b"OFF"))),
            Synchronous::Normal => Ok(ToSqlOutput::Borrowed(ValueRef::Text(b"NORMAL"))),
            Synchronous::Full => Ok(ToSqlOutput::Borrowed(ValueRef::Text(b"FULL"))),
        }
    }
}

/// A builder for generating SqliteDatabase instances.
pub struct SqliteDatabaseBuilder {
    path: Option<String>,
    prefix: Option<String>,
    indexes: Vec<&'static str>,
    pool_size: Option<u32>,
    memory_map_size: i64,
    write_ahead_log_mode: bool,
    synchronous: Option<Synchronous>,
}

impl SqliteDatabaseBuilder {
    fn new() -> Self {
        Self {
            path: None,
            prefix: None,
            indexes: vec![],
            pool_size: None,
            memory_map_size: DEFAULT_MMAP_SIZE,
            write_ahead_log_mode: false,
            synchronous: None,
        }
    }

    /// Set the path for the Sqlite database.
    pub fn with_path<S: Into<String>>(mut self, path: S) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Set the prefix for all table names in the database.
    ///
    /// This prefix results in table names such as `"<prefix>_primary"` and
    /// `"<prefix>_index_<index-name>"`.
    ///
    /// Defaults to "transact".
    pub fn with_prefix<S: Into<String>>(mut self, prefix: S) -> Self {
        self.prefix = Some(prefix.into());
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

    /// Enable "Write-Ahead Log" (WAL) journal mode.
    ///
    /// In SQLite, WAL journal mode provides considerable performance improvements in most
    /// scenarios.  See [https://sqlite.org/wal.html](https://sqlite.org/wal.html) for more
    /// details on this mode.
    ///
    /// While many of the disadvantages of this mode are unlikely to effect transact and its
    /// use-cases, WAL mode cannot be used with a database file on a networked file system.
    pub fn with_write_ahead_log_mode(mut self) -> Self {
        self.write_ahead_log_mode = true;
        self
    }

    /// Modify the "synchronous" setting for the SQLite connection.
    ///
    /// This setting changes the mode of how the transaction journal is synchronized to disk. See
    /// the [pragma synchronouse documentation](https://sqlite.org/pragma.html#pragma_synchronous)
    /// for more information.
    ///
    /// If unchanged, the default value is left to underlying SQLite installation.
    pub fn with_synchronous(mut self, synchronous: Synchronous) -> Self {
        self.synchronous = Some(synchronous);
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
    /// * Unable to configure the WAL journal mode, if requested
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

        if self.write_ahead_log_mode {
            conn.pragma_update(None, "journal_mode", &String::from("WAL"))
                .map_err(|err| SqliteDatabaseError {
                    context: "unable to configure WAL journal mode".into(),
                    source: Some(Box::new(err)),
                })?;
        }

        if let Some(synchronous) = self.synchronous {
            conn.pragma_update(None, "synchronous", &synchronous)
                .map_err(|err| SqliteDatabaseError {
                    context: "unable to modify synchronous setting".into(),
                    source: Some(Box::new(err)),
                })?;
        }

        let prefix = self
            .prefix
            .map(|s| s + "_")
            .unwrap_or_else(|| "transact_".into());

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {}primary (\
            key BLOB PRIMARY KEY, \
            value BLOB NOT NULL\
            )",
            &prefix,
        );
        conn.execute(&create_table, params![])
            .map_err(|err| SqliteDatabaseError {
                context: "unable to create primary table".into(),
                source: Some(Box::new(err)),
            })?;

        for index in self.indexes {
            let create_index_table = format!(
                "CREATE TABLE IF NOT EXISTS {}index_{} (\
                 index_key BLOB PRIMARY KEY, \
                 value BLOB NOT NULL\
                 )",
                &prefix, index,
            );

            conn.execute(&create_index_table, params![])
                .map_err(|err| SqliteDatabaseError {
                    context: format!("unable to create index {} table", index),
                    source: Some(Box::new(err)),
                })?;
        }

        Ok(SqliteDatabase { prefix, pool })
    }
}

/// A Database implementation backed by a Sqlite instance.
#[derive(Clone)]
pub struct SqliteDatabase {
    prefix: String,
    pool: r2d2::Pool<SqliteConnectionManager>,
}

impl SqliteDatabase {
    /// Constructs a new database with the given path to the Sqlite store (either in memory or on
    /// the filesystem) and a provided set of index tables.
    ///
    /// # Errors
    ///
    /// This will return an error for the same reasons as `SqliteDatabaseBuilder::build`
    pub fn new(path: &str, indexes: &[&'static str]) -> Result<Self, SqliteDatabaseError> {
        SqliteDatabaseBuilder::new()
            .with_path(path)
            .with_indexes(indexes)
            .build()
    }

    /// Constructs a builder with the default settings.
    ///
    /// This can be used to construct an instance with finer-grained control over underlying Sqlite
    /// connections.
    ///
    /// For example:
    ///
    /// ```
    /// # use std::num::NonZeroU32;
    /// # use transact::database::sqlite::SqliteDatabase;
    /// let database = SqliteDatabase::builder()
    ///     .with_path(":memory:")
    ///     .with_connection_pool_size(NonZeroU32::new(1).unwrap())
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn builder() -> SqliteDatabaseBuilder {
        SqliteDatabaseBuilder::new()
    }

    /// Executes the VACUUM operation on the underlying database instance.
    ///
    /// The VACUUM command rebuilds the database file, repacking it into a minimal amount of disk
    /// space.
    ///
    /// # Errors
    ///
    /// This will return an error if it cannot
    ///
    /// - connect to the database
    /// - execute the VACUUM command succesfully.
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
            prefix: &self.prefix,
            conn: Rc::new(RefCell::new(conn)),
        }))
    }

    fn get_writer<'a>(&'a self) -> Result<Box<dyn DatabaseWriter + 'a>, DatabaseError> {
        let conn = self.pool.get().map_err(|err| {
            DatabaseError::WriterError(format!("Unable to connect to database: {}", err))
        })?;

        conn.execute_batch("BEGIN DEFERRED").map_err(|err| {
            DatabaseError::WriterError(format!("Unable to begin write transaction: {}", err))
        })?;

        Ok(Box::new(SqliteDatabaseWriter {
            prefix: &self.prefix,
            conn: Rc::new(RefCell::new(conn)),
        }))
    }

    fn clone_box(&self) -> Box<dyn Database> {
        Box::new(self.clone())
    }
}

struct SqliteDatabaseReader<'db> {
    prefix: &'db str,
    conn: Rc<RefCell<SqliteConnection>>,
}

impl<'db> DatabaseReader for SqliteDatabaseReader<'db> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let mut conn = self.conn.borrow_mut();
        execute_get(&mut conn, self.prefix, key)
    }

    fn index_get(&self, index: &str, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let mut conn = self.conn.borrow_mut();
        execute_index_get(&mut conn, self.prefix, index, key)
    }

    fn cursor(&self) -> Result<DatabaseCursor, DatabaseError> {
        let total = {
            let mut conn = self.conn.borrow_mut();
            execute_count(&mut conn, self.prefix)?
        };

        Ok(Box::new(SqliteCursor::new(
            self.conn.clone(),
            &format!(
                "SELECT key, value from {}primary LIMIT ? OFFSET ?",
                self.prefix,
            ),
            total,
        )?))
    }

    fn index_cursor(&self, index: &str) -> Result<DatabaseCursor, DatabaseError> {
        let total = {
            let mut conn = self.conn.borrow_mut();
            execute_index_count(&mut conn, self.prefix, index)?
        };

        Ok(Box::new(SqliteCursor::new(
            self.conn.clone(),
            &format!(
                "SELECT index_key, value from {}index_{} LIMIT ? OFFSET ?",
                self.prefix, index,
            ),
            total,
        )?))
    }

    fn count(&self) -> Result<usize, DatabaseError> {
        let mut conn = self.conn.borrow_mut();
        execute_count(&mut conn, self.prefix).map(|count| count as usize)
    }

    fn index_count(&self, index: &str) -> Result<usize, DatabaseError> {
        let mut conn = self.conn.borrow_mut();
        execute_index_count(&mut conn, self.prefix, index).map(|count| count as usize)
    }
}

struct SqliteDatabaseWriter<'db> {
    prefix: &'db str,
    conn: Rc<RefCell<SqliteConnection>>,
}

impl<'db> Drop for SqliteDatabaseWriter<'db> {
    fn drop(&mut self) {
        if let Err(err) = self.conn.borrow_mut().execute_batch("ROLLBACK") {
            warn!("Unable to rollback writer transaction: {}", err);
        }
    }
}

impl<'db> DatabaseWriter for SqliteDatabaseWriter<'db> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        let conn = self.conn.borrow_mut();
        let mut stmt = conn
            .prepare_cached(&format!(
                "INSERT INTO {}primary (key, value) VALUES (?, ?)",
                self.prefix,
            ))
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
            .prepare_cached(&format!(
                "INSERT OR REPLACE INTO {}primary (key, value) VALUES (:key, :value)",
                self.prefix,
            ))
            .map_err(|err| DatabaseError::WriterError(format!("unable to write value: {}", err)))?;

        stmt.execute_named(named_params! {":key": key, ":value": value})
            .map_err(|err| DatabaseError::WriterError(format!("unable to write value: {}", err)))?;

        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        let conn = self.conn.borrow_mut();
        let mut stmt = conn
            .prepare_cached(&format!("DELETE FROM {}primary WHERE key = ?", self.prefix,))
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
            "INSERT OR REPLACE INTO {}index_{} (index_key, value) VALUES (:key, :value)",
            self.prefix, index,
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
        let sql = format!(
            "DELETE FROM {}index_{} WHERE index_key = ?",
            self.prefix, index
        );
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

impl<'db> DatabaseReader for SqliteDatabaseWriter<'db> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let mut conn = self.conn.borrow_mut();
        execute_get(&mut conn, self.prefix, key)
    }

    fn index_get(&self, index: &str, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let mut conn = self.conn.borrow_mut();
        execute_index_get(&mut conn, self.prefix, index, key)
    }

    fn cursor(&self) -> Result<DatabaseCursor, DatabaseError> {
        let total = {
            let mut conn = self.conn.borrow_mut();
            execute_count(&mut conn, self.prefix)?
        };

        Ok(Box::new(SqliteCursor::new(
            self.conn.clone(),
            &format!(
                "SELECT key, value from {}primary LIMIT ? OFFSET ?",
                self.prefix,
            ),
            total,
        )?))
    }

    fn index_cursor(&self, index: &str) -> Result<DatabaseCursor, DatabaseError> {
        let total = {
            let mut conn = self.conn.borrow_mut();
            execute_index_count(&mut conn, self.prefix, index)?
        };

        Ok(Box::new(SqliteCursor::new(
            self.conn.clone(),
            &format!(
                "SELECT index_key, value from {}index_{} LIMIT ? OFFSET ?",
                self.prefix, index,
            ),
            total,
        )?))
    }

    fn count(&self) -> Result<usize, DatabaseError> {
        let mut conn = self.conn.borrow_mut();
        execute_count(&mut conn, self.prefix).map(|count| count as usize)
    }

    fn index_count(&self, index: &str) -> Result<usize, DatabaseError> {
        let mut conn = self.conn.borrow_mut();
        execute_index_count(&mut conn, self.prefix, index).map(|count| count as usize)
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

fn execute_get(
    conn: &mut SqliteConnection,
    prefix: &str,
    key: &[u8],
) -> Result<Option<Vec<u8>>, DatabaseError> {
    let mut stmt = conn
        .prepare_cached(&format!(
            "SELECT value FROM {}primary WHERE key = ?",
            prefix
        ))
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))?;

    let mut value_iter = stmt
        .query_map(params![key], |row| row.get(0))
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))?;

    value_iter
        .next()
        .transpose()
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))
}

fn execute_count(conn: &mut SqliteConnection, prefix: &str) -> Result<i64, DatabaseError> {
    let mut stmt = conn
        .prepare_cached(&format!("SELECT COUNT(key) FROM {}primary", prefix))
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))?;

    stmt.query_row(params![], |row| row.get(0))
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))
}

fn execute_index_get(
    conn: &mut SqliteConnection,
    prefix: &str,
    index: &str,
    key: &[u8],
) -> Result<Option<Vec<u8>>, DatabaseError> {
    let query = format!(
        "SELECT value FROM {}index_{} WHERE index_key = ?",
        prefix, index
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

fn execute_index_count(
    conn: &mut SqliteConnection,
    prefix: &str,
    index: &str,
) -> Result<i64, DatabaseError> {
    let query = format!("SELECT COUNT(index_key) FROM {}index_{}", prefix, index);
    let mut stmt = conn
        .prepare_cached(&query)
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))?;

    stmt.query_row(params![], |row| row.get(0))
        .map_err(|err| DatabaseError::ReaderError(format!("unable to read value: {}", err)))
}

/// An error that may be returned during SqliteDatabase-specific operations.
///
/// This error type provides a context and the source error.
#[derive(Debug)]
pub struct SqliteDatabaseError {
    pub context: String,
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

    #[test]
    fn test_writer_cursors() {
        run_test(|db_path| {
            let db =
                SqliteDatabase::new(&db_path, &["a", "b"]).expect("Could not instantiate database");

            {
                let writer = db.get_writer().expect("unable to get a database writer");
                let cursor = writer.index_cursor("a").expect("unable to open cursor");

                assert!(cursor.collect::<Vec<_>>().is_empty());
            }

            let mut writer = db.get_writer().expect("unable to get db writer");
            writer.put(b"a", b"first").expect("unable to put value");
            writer.put(b"b", b"second").expect("unable to put value");

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
                let writer = db.get_writer().expect("unable to get a database writer");
                let mut cursor = writer.cursor().expect("unable to open cursor");
                assert_eq!(Some((b"a".to_vec(), b"first".to_vec())), cursor.next());
                assert_eq!(Some((b"b".to_vec(), b"second".to_vec())), cursor.next());

                assert_eq!(None, cursor.next());

                let mut index_cursor = writer
                    .index_cursor("a")
                    .expect("unable to open index_cursor");

                assert_eq!(Some((vec![1u8], b"hello".to_vec())), index_cursor.next());
                assert_eq!(Some((vec![2u8], b"bonjour".to_vec())), index_cursor.next());

                assert_eq!(
                    Some((vec![1u8], b"hello".to_vec())),
                    index_cursor.seek_first()
                );

                assert_eq!(
                    Some((vec![3u8], b"guten tag".to_vec())),
                    index_cursor.seek_last()
                );

                assert_eq!(None, index_cursor.next());
            }
        })
    }

    #[test]
    fn test_multi_prefix() {
        run_test(|db_path| {
            let db_alpha = SqliteDatabase::builder()
                .with_path(db_path)
                .with_prefix("alpha")
                .with_indexes(&["idx_one"])
                .build()
                .expect("Could not instantiate database");

            let db_beta = SqliteDatabase::builder()
                .with_path(db_path)
                .with_prefix("beta")
                .with_indexes(&["idx_one", "idx_two"])
                .build()
                .expect("Could not instantiate database");

            let mut writer = db_alpha
                .get_writer()
                .expect("Unable to create alpha writer");
            writer
                .put(b"rec-1", b"alpha value")
                .expect("unable to write to alpha primary");
            writer
                .index_put("idx_one", b"idx-1", b"alpha index value")
                .expect("Unable to write to alpha index");
            writer.commit().expect("Unable to commit to alpha");

            let mut writer = db_beta.get_writer().expect("Unable to create beta writer");
            writer
                .put(b"rec-1", b"beta value")
                .expect("unable to write to beta primary");
            writer
                .index_put("idx_two", b"idx-1", b"beta index value")
                .expect("Unable to write to alpha index");
            writer.commit().expect("Unable to commit to beta");

            let alpha_reader = db_alpha
                .get_reader()
                .expect("unable to get an alpha reader");
            let alpha_value = alpha_reader
                .get(b"rec-1")
                .expect("Unable to read value from alpha primary");
            assert_eq!(Some(b"alpha value".to_vec()), alpha_value);

            let alpha_idx_value = alpha_reader
                .index_get("idx_one", b"idx-1")
                .expect("unable to read value from alpha index");
            assert_eq!(Some(b"alpha index value".to_vec()), alpha_idx_value);

            let beta_reader = db_beta.get_reader().expect("unable to get an beta reader");
            let beta_value = beta_reader
                .get(b"rec-1")
                .expect("Unable to read value from beta primary");
            assert_eq!(Some(b"beta value".to_vec()), beta_value);

            let beta_idx_value = beta_reader
                .index_get("idx_one", b"idx-1")
                .expect("unable to read value from beta index");
            assert_eq!(None, beta_idx_value);

            let beta_idx_value = beta_reader
                .index_get("idx_two", b"idx-1")
                .expect("unable to read value from beta index");
            assert_eq!(Some(b"beta index value".to_vec()), beta_idx_value);
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

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

//! A Backend implementation for SQLite databases.
//!
//! Available if the feature "sqlite" is enabled.

use std::convert::TryFrom;
use std::fmt;
use std::num::NonZeroU32;

use diesel::{
    connection::SimpleConnection,
    r2d2::{ConnectionManager, CustomizeConnection, Pool, PooledConnection},
    sqlite,
};

use crate::error::{InternalError, InvalidStateError};

use super::{Backend, Connection};

/// A connection to a SQLite database.
///
/// Available if the feature "sqlite" is enabled.
pub struct SqliteConnection(
    pub(in crate::state::merkle::sql) PooledConnection<ConnectionManager<sqlite::SqliteConnection>>,
);

impl Connection for SqliteConnection {
    type ConnectionType = sqlite::SqliteConnection;

    fn as_inner(&self) -> &Self::ConnectionType {
        &*self.0
    }
}

/// The SQLite Backend
///
/// This struct provides the backend implementation details for SQLite databases.
///
/// Available if the feature "sqlite" is enabled.
#[derive(Clone)]
pub struct SqliteBackend {
    connection_pool: Pool<ConnectionManager<sqlite::SqliteConnection>>,
}

impl Backend for SqliteBackend {
    type Connection = SqliteConnection;

    fn connection(&self) -> Result<Self::Connection, InternalError> {
        self.connection_pool
            .get()
            .map(SqliteConnection)
            .map_err(|err| InternalError::from_source(Box::new(err)))
    }
}

impl From<Pool<ConnectionManager<sqlite::SqliteConnection>>> for SqliteBackend {
    fn from(pool: Pool<ConnectionManager<sqlite::SqliteConnection>>) -> Self {
        Self {
            connection_pool: pool,
        }
    }
}

/// The default size
pub const DEFAULT_MMAP_SIZE: i64 = 100 * 1024 * 1024;

/// Synchronous setting values.
///
/// See the [PRAGMA "synchronous"](https://sqlite.org/pragma.html#pragma_journal_mode)
/// documentation for more details.
///
/// Available if the feature "sqlite" is enabled.
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

impl fmt::Display for Synchronous {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Synchronous::Off => f.write_str("OFF"),
            Synchronous::Normal => f.write_str("NORMAL"),
            Synchronous::Full => f.write_str("FULL"),
        }
    }
}

/// Journal Mode setting values.
///
/// See the [PRAGMA "journal_mode"](https://sqlite.org/pragma.html#pragma_journal_mode)
/// documentation for more details.
///
/// Available if the feature "sqlite" is enabled.
#[derive(Debug)]
pub enum JournalMode {
    /// The DELETE journaling mode is the normal behavior. In the DELETE mode, the rollback journal
    /// is deleted at the conclusion of each transaction. Indeed, the delete operation is the
    /// action that causes the transaction to commit.
    Delete,
    /// The TRUNCATE journaling mode commits transactions by truncating the rollback journal to
    /// zero-length instead of deleting it.
    Truncate,
    /// The PERSIST journaling mode prevents the rollback journal from being deleted at the end of
    /// each transaction. Instead, the header of the journal is overwritten with zeros. This will
    /// prevent other database connections from rolling the journal back.
    Persist,
    /// The MEMORY journaling mode stores the rollback journal in volatile RAM.
    Memory,
    /// Enable "Write-Ahead Log" (WAL) journal mode.
    ///
    /// In SQLite, WAL journal mode provides considerable performance improvements in most
    /// scenarios.  See [https://sqlite.org/wal.html](https://sqlite.org/wal.html) for more
    /// details on this mode.
    ///
    /// While many of the disadvantages of this mode are unlikely to effect transact and its
    /// use-cases, WAL mode cannot be used with a database file on a networked file system.
    Wal,
    /// Disables the rollback journal completely.
    Off,
}

impl fmt::Display for JournalMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            JournalMode::Delete => f.write_str("DELETE"),
            JournalMode::Truncate => f.write_str("TRUNCATE"),
            JournalMode::Persist => f.write_str("PERSIST"),
            JournalMode::Memory => f.write_str("MEMORY"),
            JournalMode::Wal => f.write_str("WAL"),
            JournalMode::Off => f.write_str("OFF"),
        }
    }
}

/// A builder for a [SqliteBackend].
///
/// Available if the feature "sqlite" is enabled.
pub struct SqliteBackendBuilder {
    connection_path: Option<String>,
    create: bool,
    pool_size: Option<u32>,
    memory_map_size: i64,
    journal_mode: Option<JournalMode>,
    synchronous: Option<Synchronous>,
}

impl SqliteBackendBuilder {
    /// Constructs a new SqliteBackendBuilder.
    pub fn new() -> Self {
        Self {
            connection_path: None,
            create: false,
            pool_size: None,
            memory_map_size: DEFAULT_MMAP_SIZE,
            journal_mode: None,
            synchronous: None,
        }
    }

    /// Configures the [SqliteBackend] instance as a memory database, with a connection pool size of
    /// 1.
    pub fn with_memory_database(mut self) -> Self {
        self.connection_path = Some(":memory:".into());
        self.pool_size = Some(1);
        self
    }

    /// Set the path for the SQLite database.
    ///
    /// This is a required field.
    pub fn with_connection_path<S: Into<String>>(mut self, connection_path: S) -> Self {
        self.connection_path = Some(connection_path.into());
        self
    }

    /// Create the database file if it does not exist.
    pub fn with_create(mut self) -> Self {
        self.create = true;
        self
    }

    /// Set the connection pool size.
    pub fn with_connection_pool_size(mut self, pool_size: NonZeroU32) -> Self {
        self.pool_size = Some(pool_size.get());
        self
    }

    /// Set the size used for Memory-Mapped I/O.
    ///
    /// This can be disabled by setting the value to `0`.
    pub fn with_memory_map_size(mut self, memory_map_size: u64) -> Self {
        // The try from would fail if the u64 is greater than max i64, so we can unwrap this as
        // such.
        self.memory_map_size = i64::try_from(memory_map_size).unwrap_or(std::i64::MAX);
        self
    }

    /// Modify the "journal mode" setting for the SQLite connection.
    ///
    /// See the [pragma journal_mode
    /// documentation](https://sqlite.org/pragma.html#pragma_journal_mode) for more information.
    ///
    /// If unchanged, the default value is left to underlying SQLite installation.
    pub fn with_journal_mode(mut self, journal_mode: JournalMode) -> Self {
        self.journal_mode = Some(journal_mode);
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

    /// Constructs the [SqliteBackend] instance.
    ///
    /// # Errors
    ///
    /// This may return a [InvalidStateError] for a variety of reasons:
    ///
    /// * No connection_path provided
    /// * Unable to connect to the database.
    /// * Unable to configure the provided memory map size
    /// * Unable to configure the journal mode, if requested
    pub fn build(self) -> Result<SqliteBackend, InvalidStateError> {
        let path = self.connection_path.ok_or_else(|| {
            InvalidStateError::with_message("must provide a sqlite connection URI".into())
        })?;

        let mmap_size = self.memory_map_size;
        let journal_mode_opt = self.journal_mode;
        let synchronous_opt = self.synchronous;

        if !self.create && (path != ":memory:") && !std::path::Path::new(&path).exists() {
            return Err(InvalidStateError::with_message(format!(
                "Database file '{}' does not exist",
                path
            )));
        }

        let connection_manager = ConnectionManager::<diesel::sqlite::SqliteConnection>::new(&path);

        let mut pool_builder = Pool::builder();
        if let Some(pool_size) = self.pool_size {
            pool_builder = pool_builder.max_size(pool_size);
        }
        let pool = pool_builder
            .connection_customizer(Box::new(SqliteCustomizer::new(
                mmap_size,
                journal_mode_opt,
                synchronous_opt,
            )))
            .build(connection_manager)
            .map_err(|err| InvalidStateError::with_message(err.to_string()))?;

        // Validate that connections can be made
        let _conn = pool
            .get()
            .map_err(|err| InvalidStateError::with_message(err.to_string()))?;

        Ok(SqliteBackend {
            connection_pool: pool,
        })
    }
}

impl Default for SqliteBackendBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct SqliteCustomizer {
    on_connect_sql: String,
}

impl SqliteCustomizer {
    fn new(
        mmap_size: i64,
        journal_mode: Option<JournalMode>,
        synchronous: Option<Synchronous>,
    ) -> Self {
        let mut on_connect_sql = format!("PRAGMA mmap_size={};", mmap_size);

        if let Some(journal_mode) = journal_mode {
            on_connect_sql += &format!("PRAGMA journal_mode={};", journal_mode);
        }

        if let Some(synchronous) = synchronous {
            on_connect_sql += &format!("PRAGMA synchronous={};", synchronous);
        }

        on_connect_sql += "PRAGMA foreign_keys = ON;";
        Self { on_connect_sql }
    }
}

impl CustomizeConnection<diesel::sqlite::SqliteConnection, diesel::r2d2::Error>
    for SqliteCustomizer
{
    fn on_acquire(
        &self,
        conn: &mut diesel::sqlite::SqliteConnection,
    ) -> Result<(), diesel::r2d2::Error> {
        conn.batch_execute(&self.on_connect_sql)
            .map_err(diesel::r2d2::Error::QueryError)
    }
}

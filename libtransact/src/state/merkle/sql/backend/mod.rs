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

//! Database Backends
//!
//! A Backend provides a light-weight abstraction over database connections.

#[cfg(feature = "postgres")]
pub(in crate::state::merkle::sql) mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

use crate::error::InternalError;

#[cfg(feature = "state-merkle-sql-postgres-tests")]
pub use postgres::test::run_postgres_test;
#[cfg(feature = "postgres")]
pub use postgres::{
    InTransactionPostgresBackend, PostgresBackend, PostgresBackendBuilder, PostgresConnection,
};
#[cfg(feature = "sqlite")]
pub use sqlite::{
    InTransactionSqliteBackend, JournalMode, SqliteBackend, SqliteBackendBuilder, SqliteConnection,
    Synchronous,
};

/// A database connection.
pub trait Connection {
    /// The underlying internal connection.
    type ConnectionType: diesel::Connection;

    /// Access the internal connection.
    fn as_inner(&self) -> &Self::ConnectionType;
}

/// A database backend.
///
/// A Backend provides a light-weight abstraction over database connections.
pub trait Backend {
    /// The database connection.
    type Connection: Connection;

    /// Acquire a database connection.
    ///
    /// This method is soft-deprecated, as it is no longer used internally, and has been superseded
    /// by use with the execute trait.  It may be strongly deprecated in a future release, to be
    /// removed in a follow-up release.
    fn connection(&self) -> Result<Self::Connection, InternalError>;
}

pub trait Execute: Backend {
    fn execute<F, T>(&self, f: F) -> Result<T, InternalError>
    where
        F: Fn(&Self::Connection) -> Result<T, InternalError>;
}

pub trait WriteExclusiveExecute: Backend {
    /// Execute write operations against the database.
    ///
    /// This function will execute the provided closure with an exclusive write connection.  Via
    /// this function, only a single writer is allowed at a time.
    ///
    /// # Errors
    ///
    /// Returns a [`InternalError`] if the lock is poisoned or the connection cannot be required.
    /// Any [`InternalError`] results from the provided closure will be returned as well.
    fn execute_write<F, T>(&self, f: F) -> Result<T, InternalError>
    where
        F: Fn(&Self::Connection) -> Result<T, InternalError>;

    /// Execute read operation against the database.
    ///
    /// This function will execute the provided closure with an read connection.  Via
    /// this function, multiple readers are allowed, unless there is a write in progress.
    ///
    /// # Errors
    ///
    /// Returns a [`InternalError`] if the lock is poisoned or the connection cannot be required.
    /// Any [`InternalError`] results from the provided closure will be returned as well.
    fn execute_read<F, T>(&self, f: F) -> Result<T, InternalError>
    where
        F: Fn(&Self::Connection) -> Result<T, InternalError>;
}

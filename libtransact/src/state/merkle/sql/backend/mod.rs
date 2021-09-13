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
pub use postgres::{PostgresBackend, PostgresBackendBuilder, PostgresConnection};
#[cfg(feature = "sqlite")]
pub use sqlite::{JournalMode, SqliteBackend, SqliteBackendBuilder, SqliteConnection, Synchronous};

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
pub trait Backend: Sync + Send {
    /// The database connection.
    type Connection: Connection;

    /// Acquire a database connection.
    fn connection(&self) -> Result<Self::Connection, InternalError>;
}

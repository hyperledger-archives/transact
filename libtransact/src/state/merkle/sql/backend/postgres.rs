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

use diesel::r2d2::{ConnectionManager, Pool, PooledConnection};

use crate::error::{InternalError, InvalidStateError};

use super::{Backend, Connection};

pub struct PostgresConnection(
    pub(in crate::state::merkle::sql) PooledConnection<ConnectionManager<diesel::pg::PgConnection>>,
);

impl Connection for PostgresConnection {
    type ConnectionType = diesel::pg::PgConnection;

    fn as_inner(&self) -> &Self::ConnectionType {
        &*self.0
    }
}

/// The Postgres Backend
///
/// This struct provides the backend implementation details for postgres databases.
#[derive(Clone)]
pub struct PostgresBackend {
    connection_pool: Pool<ConnectionManager<diesel::pg::PgConnection>>,
}

impl Backend for PostgresBackend {
    type Connection = PostgresConnection;

    fn connection(&self) -> Result<Self::Connection, InternalError> {
        self.connection_pool
            .get()
            .map(PostgresConnection)
            .map_err(|err| InternalError::from_source(Box::new(err)))
    }
}

/// A Builder for the PostgresBackend.
#[derive(Default)]
pub struct PostgresBackendBuilder {
    url: Option<String>,
}

impl PostgresBackendBuilder {
    /// Constructs a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the URL for the postgres database instance.
    ///
    /// This URL should follow the format as documented at
    /// [https://www.postgresql.org/docs/9.4/libpq-connect.html#LIBPQ-CONNSTRING](https://www.postgresql.org/docs/9.4/libpq-connect.html#LIBPQ-CONNSTRING).
    ///
    /// This is a required field.
    pub fn with_url<S: Into<String>>(mut self, url: S) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Constructs the [PostgresBackend] instance.
    ///
    /// # Errors
    ///
    /// This may return a [InvalidStateError] for a variety of reasons:
    ///
    /// * No URL provided
    /// * Unable to connect to the database.
    pub fn build(self) -> Result<PostgresBackend, InvalidStateError> {
        let url = self.url.ok_or_else(|| {
            InvalidStateError::with_message("must provide a postgres connection URL".into())
        })?;

        let connection_manager = ConnectionManager::<diesel::pg::PgConnection>::new(url);
        let pool = Pool::builder()
            .build(connection_manager)
            .map_err(|err| InvalidStateError::with_message(err.to_string()))?;

        // Validate that connections can be made
        let _conn = pool
            .get()
            .map_err(|err| InvalidStateError::with_message(err.to_string()))?;

        Ok(PostgresBackend {
            connection_pool: pool,
        })
    }
}

#[cfg(feature = "state-merkle-sql-postgres-tests")]
pub mod test {
    use std::env;
    use std::error::Error;
    use std::panic;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use diesel::prelude::*;
    use diesel_migrations::{
        revert_latest_migration_in_directory, MigrationError, RunMigrationsError,
    };
    use lazy_static::lazy_static;

    use crate::state::merkle::sql::migration;

    lazy_static! {
        static ref SERIAL_TEST_LOCK: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
    }

    /// Execute a test against a postgres database.
    ///
    /// This function will run the migrations against the database provided by the environment
    /// variable STATE_MERKLE_SQL_POSTGRES_TEST_URL.  After the test completes, it will revert the
    /// migrations to a clean state. In other words, each test can assume a clean state, after the
    /// migrations have been applied.
    ///
    /// Additionally, tests will be run serially. This does not guarantee any order to the
    /// tests, but does guarantee that they will not overlap and interfere with the test data.
    pub fn run_postgres_test<T>(test: T) -> Result<(), Box<dyn Error>>
    where
        T: FnOnce(&str) -> Result<(), Box<dyn Error>> + panic::UnwindSafe,
    {
        let (migration_result, test_result) = {
            let _guard = SERIAL_TEST_LOCK.lock()?;

            let url = env::var("STATE_MERKLE_SQL_POSTGRES_TEST_URL")
                .ok()
                .unwrap_or_else(|| "postgres://postgres:test@localhost:5432/transact".into());
            {
                let conn = PgConnection::establish(&url)?;
                migration::postgres::run_migrations(&conn)?;
            }

            let test_url = url.clone();
            let result = panic::catch_unwind(move || test(&test_url));

            // rollback
            let url = env::var("STATE_MERKLE_SQL_POSTGRES_TEST_URL")
                .ok()
                .unwrap_or_else(|| "".into());
            let conn = PgConnection::establish(&url)?;
            let migration_result: Result<(), Box<dyn Error>> = loop {
                match revert_latest_migration_in_directory(
                    &conn,
                    &PathBuf::from("./src/state/merkle/sql/migration/postgres/migrations"),
                ) {
                    Ok(_s) => (),
                    Err(RunMigrationsError::MigrationError(MigrationError::NoMigrationRun)) => {
                        break Ok(())
                    }
                    Err(err) => break Err(Box::new(err)),
                }
            };

            (migration_result, result)
        };

        match test_result {
            Ok(res) => migration_result.and(res),
            Err(err) => {
                panic::resume_unwind(err);
            }
        }
    }
}

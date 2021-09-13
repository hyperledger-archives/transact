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

//! Defines methods and utilities to interact with merkle radix tables in a PostgreSQL database.

embed_migrations!("./src/state/merkle/sql/migration/postgres/migrations");

use crate::error::InternalError;
use crate::state::merkle::sql::backend::{Backend, Connection, PostgresBackend};

use super::MigrationManager;

/// Run database migrations to create tables defined for the SqlMerkleState.
///
/// # Arguments
///
/// * `conn` - Connection to Postgres database
///
#[allow(dead_code)]
pub fn run_migrations(conn: &diesel::pg::PgConnection) -> Result<(), InternalError> {
    embedded_migrations::run(conn).map_err(|err| InternalError::from_source(Box::new(err)))?;

    info!("Successfully applied PostgreSQL migrations");

    Ok(())
}

impl MigrationManager for PostgresBackend {
    fn run_migrations(&self) -> Result<(), InternalError> {
        run_migrations(self.connection()?.as_inner())
    }
}

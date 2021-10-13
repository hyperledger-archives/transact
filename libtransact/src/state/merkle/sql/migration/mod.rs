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

//! Provides the MigrationManager trait.

#[cfg(feature = "postgres")]
pub(crate) mod postgres;
#[cfg(feature = "sqlite")]
pub(crate) mod sqlite;

use crate::error::InternalError;

use super::backend::Backend;

#[cfg(feature = "postgres")]
pub use postgres::run_migrations as run_postgres_migrations;
#[cfg(feature = "sqlite")]
pub use sqlite::run_migrations as run_sqlite_migrations;

/// Provides backend migration execution.
///
/// Backend's that implement this trait can expose a migration operation on their underlying
/// database system.
pub trait MigrationManager: Backend {
    fn run_migrations(&self) -> Result<(), InternalError>;
}

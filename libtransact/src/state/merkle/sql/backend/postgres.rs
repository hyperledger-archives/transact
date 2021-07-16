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

#[cfg(feature = "state-merkle-sql-postgres-tests")]
#[cfg(test)]
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

// Copyright 2019 Cargill Incorporated
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! An Redis-backed implementation of the database traits.
//!
//! This is an experimental feature, and is only available by enabling the "redis-db" feature.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use redis::{self, Commands, PipelineCommands};

use super::{Database, DatabaseCursor, DatabaseError, DatabaseReader, DatabaseWriter};

/// A Database implementation, backed by a Redis instance.
#[derive(Clone)]
pub struct RedisDatabase {
    client: redis::Client,
    conn: Arc<Mutex<redis::Connection>>,
    primary: String,
    indexes: HashSet<String>,
}

impl RedisDatabase {
    /// Constructs a new database instances backed by a Redis database at the specified url.
    ///
    /// The "primary" is the main storage for the main dataset stored in this database instance.
    /// The list of indexes provide data related to entries in the primary field.
    ///
    /// # Errors
    ///
    /// A DatabaseError may be returned if the Redis instance cannot be reached.
    pub fn new(url: &str, primary: String, indexes: &[&str]) -> Result<Self, DatabaseError> {
        let indexes = indexes
            .iter()
            .map(|s| format!("{}_{}", primary, s))
            .collect();
        let client = redis::Client::open(url)
            .map_err(|e| DatabaseError::InitError(format!("failed to open redis client: {}", e)))?;
        let conn = client
            .get_connection()
            .map_err(|e| DatabaseError::ReaderError(format!("failed to create reader: {}", e)))?;
        Ok(Self {
            client,
            conn: Arc::new(Mutex::new(conn)),
            primary,
            indexes,
        })
    }
}

impl Database for RedisDatabase {
    fn get_reader<'a>(&'a self) -> Result<Box<dyn DatabaseReader + 'a>, DatabaseError> {
        Ok(Box::new(RedisDatabaseReader { db: self }))
    }

    fn get_writer<'a>(&'a self) -> Result<Box<dyn DatabaseWriter + 'a>, DatabaseError> {
        Ok(Box::new(RedisDatabaseWriter::new(self)))
    }

    fn clone_box(&self) -> Box<dyn Database> {
        Box::new(self.clone())
    }
}

struct RedisDatabaseReader<'db> {
    db: &'db RedisDatabase,
}

macro_rules! unlock_conn {
    ($self:ident, $err:path) => {{
        $self
            .db
            .conn
            .lock()
            .map_err(|_| $err("RedisDatabase connection lock was poisoned.".into()))
    }};
}

impl<'db> RedisDatabaseReader<'db> {
    fn with_index<T, F>(&self, index: &str, apply: F) -> Result<T, DatabaseError>
    where
        F: Fn(&str, &mut redis::Connection) -> Result<T, DatabaseError>,
    {
        let index_name = format!("{}_{}", &self.db.primary, index);
        if !self.db.indexes.contains(&index_name) {
            return Err(DatabaseError::ReaderError(format!(
                "{} is not a valid index",
                index
            )));
        }

        apply(
            &index_name,
            &mut *unlock_conn!(self, DatabaseError::ReaderError)?,
        )
    }
}

impl<'db> DatabaseReader for RedisDatabaseReader<'db> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let mut conn = unlock_conn!(self, DatabaseError::ReaderError)?;
        conn.hget::<_, _, Option<Vec<u8>>>(&self.db.primary, key)
            .map_err(|err| DatabaseError::ReaderError(format!("unable to retrieve value: {}", err)))
    }

    fn index_get(&self, index: &str, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        self.with_index(index, |index_name, conn| {
            match conn.hget::<_, _, Option<Vec<u8>>>(index_name, key) {
                Ok(Some(bytes)) => Ok(Some(bytes)),
                Ok(None) => Ok(None),
                Err(err) => Err(DatabaseError::ReaderError(format!(
                    "unable to retrieve from index {}: {}",
                    index, err
                ))),
            }
        })
    }

    fn cursor(&self) -> Result<DatabaseCursor, DatabaseError> {
        Err(DatabaseError::ReaderError(
            "\"cursor\" is not a supported operation for redis".into(),
        ))
    }

    fn index_cursor(&self, _: &str) -> Result<DatabaseCursor, DatabaseError> {
        Err(DatabaseError::ReaderError(
            "\"index_cursor\" is not a supported operation for redis".into(),
        ))
    }

    fn count(&self) -> Result<usize, DatabaseError> {
        unlock_conn!(self, DatabaseError::ReaderError)?
            .hlen::<_, usize>(&self.db.primary)
            .map_err(|e| DatabaseError::ReaderError(format!("unable to count: {}", e)))
    }

    fn index_count(&self, index: &str) -> Result<usize, DatabaseError> {
        self.with_index(index, |index_name, conn| {
            conn.hlen::<_, usize>(index_name).map_err(|e| {
                DatabaseError::ReaderError(format!("unable to count {}: {}", index, e))
            })
        })
    }
}

enum Update {
    Put(Vec<u8>),
    Overwrite(Vec<u8>),
    Delete,
}

struct RedisDatabaseWriter<'db> {
    db: &'db RedisDatabase,
    changes: HashMap<String, HashMap<Vec<u8>, Update>>,
}

impl<'db> RedisDatabaseWriter<'db> {
    fn new(db: &'db RedisDatabase) -> Self {
        let mut changes = HashMap::new();
        changes.insert(db.primary.clone(), HashMap::new());
        for index in db.indexes.iter() {
            changes.insert(index.clone(), HashMap::new());
        }
        Self { db, changes }
    }

    fn with_index_mut<T, F>(&mut self, index: &str, apply: F) -> Result<T, DatabaseError>
    where
        F: Fn(&mut HashMap<Vec<u8>, Update>) -> Result<T, DatabaseError>,
    {
        let index_name = format!("{}_{}", &self.db.primary, index);
        if !self.db.indexes.contains(&index_name) {
            return Err(DatabaseError::ReaderError(format!(
                "{} is not a valid index",
                index
            )));
        }

        let mut change_set = self
            .changes
            .get_mut(&index_name)
            .expect("No change map for primary, but should have been set in constructor");
        apply(&mut change_set)
    }

    fn with_index<T, F>(&self, index: &str, apply: F) -> Result<T, DatabaseError>
    where
        F: Fn(&str, &HashMap<Vec<u8>, Update>, &mut redis::Connection) -> Result<T, DatabaseError>,
    {
        let index_name = format!("{}_{}", &self.db.primary, index);
        if !self.db.indexes.contains(&index_name) {
            return Err(DatabaseError::ReaderError(format!(
                "{} is not a valid index",
                index
            )));
        }

        let change_set = self
            .changes
            .get(&index_name)
            .expect("No change map for primary, but should have been set in constructor");
        apply(
            &index_name,
            change_set,
            &mut *unlock_conn!(self, DatabaseError::WriterError)?,
        )
    }
}

impl<'db> DatabaseWriter for RedisDatabaseWriter<'db> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        let change_map = self
            .changes
            .get_mut(&self.db.primary)
            .expect("No change map for primary, but should have been set in constructor");

        match change_map.get(key) {
            Some(Update::Delete) => {
                change_map.insert(key.to_vec(), Update::Put(value.to_vec()));
            }
            Some(_) => return Err(DatabaseError::DuplicateEntry),
            None => {
                let entry_exists = unlock_conn!(self, DatabaseError::WriterError)?
                    .hexists::<_, _, bool>(&self.db.primary, key)
                    .map_err(|e| {
                        DatabaseError::WriterError(format!(
                            "unable to check for existing entry: {}",
                            e
                        ))
                    })?;
                if entry_exists {
                    return Err(DatabaseError::DuplicateEntry);
                }

                change_map.insert(key.to_vec(), Update::Put(value.to_vec()));
            }
        }

        Ok(())
    }

    fn overwrite(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        let change_map = self
            .changes
            .get_mut(&self.db.primary)
            .expect("No change map for primary, but should have been set in constructor");
        change_map.insert(key.to_vec(), Update::Overwrite(value.to_vec()));

        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        let change_map = self
            .changes
            .get_mut(&self.db.primary)
            .expect("No change map for primary, but should have been set in constructor");

        change_map.insert(key.to_vec(), Update::Delete);

        Ok(())
    }

    fn index_put(&mut self, index: &str, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        self.with_index_mut(index, |change_map| {
            change_map.insert(key.to_vec(), Update::Overwrite(value.to_vec()));
            Ok(())
        })
    }

    fn index_delete(&mut self, index: &str, key: &[u8]) -> Result<(), DatabaseError> {
        self.with_index_mut(index, |change_map| {
            change_map.insert(key.to_vec(), Update::Delete);

            Ok(())
        })
    }

    fn commit(self: Box<Self>) -> Result<(), DatabaseError> {
        let mut pipe = redis::pipe();
        let mut pipeline = pipe.atomic();

        for (table_name, change_set) in self.changes.into_iter() {
            for (k, v) in change_set.into_iter() {
                match v {
                    Update::Put(data) => {
                        pipeline = pipeline.hset_nx(&table_name, k, data);
                    }
                    Update::Overwrite(data) => {
                        pipeline = pipeline.hset(&table_name, k, data);
                    }
                    Update::Delete => {
                        pipeline = pipeline.hdel(&table_name, k);
                    }
                }
            }
        }

        pipeline
            .query(&mut *unlock_conn!(self, DatabaseError::WriterError)?)
            .map_err(|e| DatabaseError::WriterError(format!("unable to commit: {}", e)))?;

        Ok(())
    }

    fn as_reader(&self) -> &dyn DatabaseReader {
        self
    }
}

impl<'db> DatabaseReader for RedisDatabaseWriter<'db> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let change_map = self
            .changes
            .get(&self.db.primary)
            .expect("No change map for primary, but should have been set in constructor");
        match change_map.get(key) {
            Some(Update::Put(data)) => return Ok(Some(data.clone())),
            Some(Update::Overwrite(data)) => return Ok(Some(data.clone())),
            Some(Update::Delete) => return Ok(None),
            None => (),
        };

        let mut conn = unlock_conn!(self, DatabaseError::ReaderError)?;
        conn.hget::<_, _, Option<Vec<u8>>>(&self.db.primary, key)
            .map_err(|err| DatabaseError::ReaderError(format!("unable to retrieve value: {}", err)))
    }

    fn index_get(&self, index: &str, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        self.with_index(index, |index_name, change_map, conn| {
            match change_map.get(key) {
                Some(Update::Put(data)) => return Ok(Some(data.clone())),
                Some(Update::Overwrite(data)) => return Ok(Some(data.clone())),
                Some(Update::Delete) => return Ok(None),
                None => (),
            };

            match conn.hget::<_, _, Option<Vec<u8>>>(index_name, key) {
                Ok(Some(bytes)) => Ok(Some(bytes)),
                Ok(None) => Ok(None),
                Err(err) => Err(DatabaseError::ReaderError(format!(
                    "unable to retrieve from index {}: {}",
                    index, err
                ))),
            }
        })
    }

    fn cursor(&self) -> Result<DatabaseCursor, DatabaseError> {
        Err(DatabaseError::ReaderError(
            "\"cursor\" is not a supported operation for redis".into(),
        ))
    }

    fn index_cursor(&self, _: &str) -> Result<DatabaseCursor, DatabaseError> {
        Err(DatabaseError::ReaderError(
            "\"index_cursor\" is not a supported operation for redis".into(),
        ))
    }

    fn count(&self) -> Result<usize, DatabaseError> {
        unlock_conn!(self, DatabaseError::ReaderError)?
            .hlen::<_, usize>(&self.db.primary)
            .map_err(|e| DatabaseError::ReaderError(format!("unable to count: {}", e)))
    }

    fn index_count(&self, index: &str) -> Result<usize, DatabaseError> {
        self.with_index(index, |index_name, _change_map, conn| {
            conn.hlen::<_, usize>(index_name).map_err(|e| {
                DatabaseError::ReaderError(format!("unable to count {}: {}", index, e))
            })
        })
    }
}

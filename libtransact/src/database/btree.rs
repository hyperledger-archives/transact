/*
 * Copyright 2019 Bitwise IO, Inc.
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

use crate::database::error::DatabaseError;
use crate::database::{DatabaseCursor, DatabaseReader, DatabaseReaderCursor};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ops::Bound::{Excluded, Included};
use std::sync::{Arc, RwLock, RwLockReadGuard};

#[derive(Clone)]
pub struct BTreeDatabase {
    btree: Arc<RwLock<BTreeDbInternal>>,
}

impl BTreeDatabase {
    pub fn new(indexes: &[&str]) -> BTreeDatabase {
        BTreeDatabase {
            btree: Arc::new(RwLock::new(BTreeDbInternal::new(indexes))),
        }
    }
}

#[derive(Clone)]
pub struct BTreeDbInternal {
    main: BTreeMap<Vec<u8>, Vec<u8>>,
    indexes: HashMap<String, BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl BTreeDbInternal {
    fn new(indexes: &[&str]) -> BTreeDbInternal {
        let mut index_dbs = HashMap::with_capacity(indexes.len());
        for name in indexes {
            index_dbs.insert(name.to_string(), BTreeMap::new());
        }
        BTreeDbInternal {
            main: BTreeMap::new(),
            indexes: index_dbs,
        }
    }
}

pub struct BTreeReader<'a> {
    db: RwLockReadGuard<'a, BTreeDbInternal>,
}

impl<'a> DatabaseReader for BTreeReader<'a> {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.db.main.get(key) {
            Some(value) => Some(value.to_vec()),
            None => None,
        }
    }

    fn index_get(&self, index: &str, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        match self
            .db
            .indexes
            .get(index)
            .ok_or_else(|| DatabaseError::ReaderError(format!("Not an index: {}", index)))?
            .get(key)
        {
            Some(value) => Ok(Some(value.to_vec())),
            None => Ok(None),
        }
    }

    /// Returns a cursor against the main database. The cursor iterates over
    /// the entries in the natural key order.
    fn cursor(&self) -> Result<DatabaseCursor, DatabaseError> {
        Ok(Box::new(BTreeDatabaseCursor::new(self.db.main.clone())))
    }

    /// Returns a cursor against the given index. The cursor iterates over
    /// the entries in the index's natural key order.
    fn index_cursor(&self, index: &str) -> Result<DatabaseCursor, DatabaseError> {
        let index = self
            .db
            .indexes
            .get(index)
            .ok_or_else(|| DatabaseError::ReaderError(format!("Not an index: {}", index)))?;

        Ok(Box::new(BTreeDatabaseCursor::new(index.clone())))
    }

    /// Returns the number of entries in the main database.
    fn count(&self) -> Result<usize, DatabaseError> {
        Ok(self.db.main.len())
    }

    /// Returns the number of entries in the given index.
    fn index_count(&self, index: &str) -> Result<usize, DatabaseError> {
        Ok(self
            .db
            .indexes
            .get(index)
            .ok_or_else(|| DatabaseError::ReaderError(format!("Not an index: {}", index)))?
            .len())
    }
}

pub struct BTreeDatabaseCursor {
    db: BTreeMap<Vec<u8>, Vec<u8>>,
    current_key: Option<Vec<u8>>,
}

impl BTreeDatabaseCursor {
    pub fn new(db: BTreeMap<Vec<u8>, Vec<u8>>) -> BTreeDatabaseCursor {
        BTreeDatabaseCursor {
            db,
            current_key: None,
        }
    }
}

impl DatabaseReaderCursor for BTreeDatabaseCursor {
    fn first(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        match self.db.iter().next() {
            Some((key, value)) => Some((key.to_vec(), value.to_vec())),
            None => None,
        }
    }

    fn last(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        match self.db.iter().last() {
            Some((key, value)) => Some((key.to_vec(), value.to_vec())),
            None => None,
        }
    }
}

impl Iterator for BTreeDatabaseCursor {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.current_key.is_none() {
            match self.db.iter().next() {
                Some((key, value)) => {
                    self.current_key = Some(key.to_vec());
                    return Some((key.to_vec(), value.to_vec()));
                }
                None => return None,
            }
        }
        let last_key = match DatabaseReaderCursor::last(self) {
            Some((key, _)) => key.to_vec(),
            None => return None,
        };
        match self
            .db
            .range((
                Excluded(self.current_key.clone().unwrap()),
                Included(last_key),
            ))
            .next()
        {
            Some((key, value)) => {
                self.current_key = Some(key.to_vec());
                Some((key.to_vec(), value.to_vec()))
            }
            None => None,
        }
    }
}

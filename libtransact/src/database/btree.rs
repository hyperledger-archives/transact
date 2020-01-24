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

//! An in-memory implementation of the database traits.
//!
//! This in-memory implementation of the Database uses a BTree map.  All keys are sorted by their
//! natural order.
//!
//! Atomicity is provided via a RwLock.

use crate::database::error::DatabaseError;
use crate::database::{
    Database, DatabaseCursor, DatabaseReader, DatabaseReaderCursor, DatabaseWriter,
};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ops::Bound::{Excluded, Included};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

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
            index_dbs.insert((*name).to_string(), BTreeMap::new());
        }
        BTreeDbInternal {
            main: BTreeMap::new(),
            indexes: index_dbs,
        }
    }
}

impl Database for BTreeDatabase {
    fn get_reader<'a>(&'a self) -> Result<Box<dyn DatabaseReader + 'a>, DatabaseError> {
        Ok(Box::new(BTreeReader {
            db: self.btree.read().expect("Failed to get reader"),
        }))
    }

    fn get_writer<'a>(&'a self) -> Result<Box<dyn DatabaseWriter + 'a>, DatabaseError> {
        Ok(Box::new(BTreeWriter::new(
            self.btree.write().expect("Failed to get writer"),
        )))
    }

    fn clone_box(&self) -> Box<dyn Database> {
        Box::new(Clone::clone(self))
    }
}

pub struct BTreeReader<'a> {
    db: RwLockReadGuard<'a, BTreeDbInternal>,
}

impl<'a> DatabaseReader for BTreeReader<'a> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        Ok(self.db.main.get(key).map(|v| v.to_vec()))
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

pub struct BTreeWriter<'a> {
    db: RwLockWriteGuard<'a, BTreeDbInternal>,
    transactions: Vec<WriterTransaction>,
}

impl<'a> BTreeWriter<'a> {
    pub fn new(db: RwLockWriteGuard<'a, BTreeDbInternal>) -> BTreeWriter {
        BTreeWriter {
            db,
            transactions: vec![],
        }
    }
}

enum WriterTransaction {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    IndexPut {
        index: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    IndexDelete {
        index: String,
        key: Vec<u8>,
    },
    Overwrite {
        key: Vec<u8>,
        value: Vec<u8>,
    },
}

impl<'a> DatabaseWriter for BTreeWriter<'a> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        if self.db.main.contains_key(key) {
            return Err(DatabaseError::DuplicateEntry);
        }
        self.transactions.push(WriterTransaction::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        });
        Ok(())
    }

    fn overwrite(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        self.transactions.push(WriterTransaction::Overwrite {
            key: key.to_vec(),
            value: value.to_vec(),
        });
        Ok(())
    }

    fn index_put(&mut self, index: &str, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        if !self.db.indexes.contains_key(index) {
            return Err(DatabaseError::WriterError(format!(
                "Not an index: {}",
                index
            )));
        }

        self.transactions.push(WriterTransaction::IndexPut {
            index: index.to_string(),
            key: key.to_vec(),
            value: value.to_vec(),
        });
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        if !self.db.main.contains_key(key) {
            return Err(DatabaseError::WriterError("Key not found".to_string()));
        }
        self.transactions
            .push(WriterTransaction::Delete { key: key.to_vec() });
        Ok(())
    }

    fn index_delete(&mut self, index: &str, key: &[u8]) -> Result<(), DatabaseError> {
        if !self
            .db
            .indexes
            .get_mut(index)
            .ok_or_else(|| DatabaseError::WriterError(format!("Not an index: {}", index)))?
            .contains_key(key)
        {
            return Err(DatabaseError::WriterError("Key not found".to_string()));
        }
        self.transactions.push(WriterTransaction::IndexDelete {
            index: index.to_string(),
            key: key.to_vec(),
        });
        Ok(())
    }

    fn commit(self: Box<Self>) -> Result<(), DatabaseError> {
        BTreeWriter::commit(self.db, self.transactions)
    }

    fn as_reader(&self) -> &dyn DatabaseReader {
        self
    }
}

impl<'a> BTreeWriter<'a> {
    fn commit(
        mut db: RwLockWriteGuard<'a, BTreeDbInternal>,
        transactions: Vec<WriterTransaction>,
    ) -> Result<(), DatabaseError> {
        for transaction in transactions {
            match transaction {
                WriterTransaction::Put { key, value } => {
                    db.main.insert(key, value);
                }
                WriterTransaction::IndexPut { index, key, value } => {
                    db.indexes.get_mut(&index).unwrap().insert(key, value);
                }
                WriterTransaction::Delete { key } => {
                    db.main.remove(&key);
                }
                WriterTransaction::IndexDelete { index, key } => {
                    db.indexes.get_mut(&index).unwrap().remove(&key);
                }
                WriterTransaction::Overwrite { key, value } => {
                    db.main.insert(key, value);
                }
            }
        }
        Ok(())
    }
}

impl<'a> DatabaseReader for BTreeWriter<'a> {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let key_to_find = key.to_vec();
        for transaction in self.transactions.iter().rev() {
            match transaction {
                WriterTransaction::Put { key, value } => {
                    if &key_to_find == key {
                        return Ok(Some(value.clone()));
                    }
                }
                WriterTransaction::Delete { key } => {
                    if &key_to_find == key {
                        return Ok(None);
                    }
                }
                WriterTransaction::Overwrite { key, value } => {
                    if &key_to_find == key {
                        return Ok(Some(value.clone()));
                    }
                }
                _ => (),
            };
        }

        Ok(self.db.main.get(key).cloned())
    }

    fn index_get(&self, index: &str, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let key_to_find = key.to_vec();
        let index_to_find = index.to_string();

        for transaction in self.transactions.iter().rev() {
            match transaction {
                WriterTransaction::IndexPut { index, key, value } => {
                    if &key_to_find == key && &index_to_find == index {
                        return Ok(Some(value.clone()));
                    }
                }
                WriterTransaction::IndexDelete { index, key } => {
                    if &key_to_find == key && &index_to_find == index {
                        return Ok(None);
                    }
                }
                _ => (),
            };
        }
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
        let mut db = self.db.main.clone();
        for transaction in self.transactions.iter() {
            match transaction {
                WriterTransaction::Put { key, value } => {
                    db.insert(key.to_vec(), value.to_vec());
                }
                WriterTransaction::Delete { key } => {
                    db.remove(key);
                }
                WriterTransaction::Overwrite { key, value } => {
                    db.insert(key.to_vec(), value.to_vec());
                }
                _ => (),
            }
        }

        Ok(Box::new(BTreeDatabaseCursor::new(db)))
    }

    /// Returns a cursor against the given index. The cursor iterates over
    /// the entries in the index's natural key order.
    fn index_cursor(&self, index: &str) -> Result<DatabaseCursor, DatabaseError> {
        let mut index_db = self
            .db
            .indexes
            .get(index)
            .ok_or_else(|| DatabaseError::ReaderError(format!("Not an index: {}", index)))?
            .clone();

        for transaction in self.transactions.iter() {
            match transaction {
                WriterTransaction::IndexPut {
                    index: transaction_index,
                    key,
                    value,
                } => {
                    if index == transaction_index {
                        index_db.insert(key.to_vec(), value.to_vec());
                    }
                }
                WriterTransaction::IndexDelete {
                    index: transaction_index,
                    key,
                } => {
                    if index == transaction_index {
                        index_db.remove(key);
                    }
                }
                _ => (),
            }
        }

        Ok(Box::new(BTreeDatabaseCursor::new(index_db)))
    }

    /// Returns the number of entries in the main database.
    fn count(&self) -> Result<usize, DatabaseError> {
        let count = self
            .transactions
            .iter()
            .fold(0_i32, |acc, transaction| match transaction {
                WriterTransaction::Put { .. } => acc + 1,
                WriterTransaction::Delete { .. } => acc - 1,
                _ => acc,
            });
        let total = self.db.main.len() as i32 + count;

        Ok(total as usize)
    }

    /// Returns the number of entries in the given index
    fn index_count(&self, index: &str) -> Result<usize, DatabaseError> {
        let count = self
            .transactions
            .iter()
            .fold(0_i32, |acc, transaction| match transaction {
                WriterTransaction::IndexPut {
                    index: transaction_index,
                    ..
                } => {
                    if index == transaction_index {
                        acc + 1
                    } else {
                        acc
                    }
                }
                WriterTransaction::IndexDelete {
                    index: transaction_index,
                    ..
                } => {
                    if index == transaction_index {
                        acc - 1
                    } else {
                        acc
                    }
                }
                _ => acc,
            });
        let total = self
            .db
            .indexes
            .get(index)
            .ok_or_else(|| DatabaseError::ReaderError(format!("Not an index: {}", index)))?
            .len() as i32
            + count;

        Ok(total as usize)
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
    fn seek_first(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        match self.db.iter().next() {
            Some((key, value)) => Some((key.to_vec(), value.to_vec())),
            None => None,
        }
    }

    fn seek_last(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
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
        let last_key = match self.seek_last() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;

    /// Asserts that there are COUNT many objects in DB.
    fn assert_database_count<'a>(count: usize, reader: &'a dyn DatabaseReader) {
        assert_eq!(reader.count().unwrap(), count,);
    }

    /// Asserts that there are are COUNT many objects in DB's INDEX.
    fn assert_index_count<'a>(index: &str, count: usize, reader: &'a dyn DatabaseReader) {
        assert_eq!(reader.index_count(index).unwrap(), count,);
    }

    /// Asserts that KEY is associated with VAL in DB.
    fn assert_key_value<'a>(key: u8, val: u8, reader: &'a dyn DatabaseReader) {
        assert_eq!(reader.get(&[key]).unwrap(), Some(vec![val]));
    }

    /// Asserts that KEY is associated with VAL in DB's INDEX.
    fn assert_index_key_value<'a>(index: &str, key: u8, val: u8, reader: &'a dyn DatabaseReader) {
        assert_eq!(reader.index_get(index, &[key]).unwrap().unwrap(), [val],);
    }

    /// Asserts that KEY is not in DB.
    fn assert_not_in_database<'a>(key: u8, reader: &'a dyn DatabaseReader) {
        assert!(reader.get(&[key]).unwrap().is_none());
    }

    /// Asserts that KEY is not in DB's INDEX.
    fn assert_not_in_index<'a>(index: &str, key: u8, reader: &'a dyn DatabaseReader) {
        assert!(reader.index_get(index, &[key]).unwrap().is_none());
    }

    #[test]
    fn test_btree_database() {
        let database = BTreeDatabase::new(&["a", "b"]);
        assert_database_count(0, database.get_reader().unwrap().as_ref());
        assert_not_in_database(3, database.get_reader().unwrap().as_ref());
        assert_not_in_database(5, database.get_reader().unwrap().as_ref());

        // Add {3: 4}
        let mut writer = database.get_writer().unwrap();
        writer.put(&[3], &[4]).unwrap();

        // Check db before commit using writer as reader
        assert_database_count(1, writer.as_reader());
        assert_key_value(3, 4, writer.as_reader());

        writer.commit().unwrap();

        // Check db after commit using new reader
        assert_database_count(1, database.get_reader().unwrap().as_ref());
        assert_key_value(3, 4, database.get_reader().unwrap().as_ref());

        // //Add {5: 6}
        let mut writer = database.get_writer().unwrap();
        writer.put(&[5], &[6]).unwrap();
        writer.commit().unwrap();

        assert_database_count(2, database.get_reader().unwrap().as_ref());
        assert_key_value(5, 6, database.get_reader().unwrap().as_ref());
        assert_key_value(3, 4, database.get_reader().unwrap().as_ref());

        // Delete {3: 4}
        let mut writer = database.get_writer().unwrap();
        writer.delete(&[3]).unwrap();

        // Check db before commit using writer as reader
        assert_database_count(1, writer.as_reader());
        assert_key_value(5, 6, writer.as_reader());
        assert_not_in_database(3, writer.as_reader());

        writer.commit().unwrap();

        // Check db after commit using new reader
        assert_database_count(1, database.get_reader().unwrap().as_ref());
        assert_key_value(5, 6, database.get_reader().unwrap().as_ref());
        assert_not_in_database(3, database.get_reader().unwrap().as_ref());

        // Add {55: 5} in "a"
        assert_index_count("a", 0, database.get_reader().unwrap().as_ref());
        assert_index_count("b", 0, database.get_reader().unwrap().as_ref());
        assert_not_in_index("a", 5, database.get_reader().unwrap().as_ref());
        assert_not_in_index("b", 5, database.get_reader().unwrap().as_ref());

        let mut writer = database.get_writer().unwrap();
        writer.index_put("a", &[55], &[5]).unwrap();

        // Check db before commit using writer as reader
        assert_index_count("a", 1, writer.as_reader());
        assert_index_count("b", 0, writer.as_reader());
        assert_index_key_value("a", 55, 5, writer.as_reader());
        assert_not_in_index("b", 5, writer.as_reader());
        assert_database_count(1, writer.as_reader());
        assert_key_value(5, 6, writer.as_reader());
        assert_not_in_database(3, writer.as_reader());

        writer.commit().unwrap();

        // Check db after commit using new reader
        assert_index_count("a", 1, database.get_reader().unwrap().as_ref());
        assert_index_count("b", 0, database.get_reader().unwrap().as_ref());
        assert_index_key_value("a", 55, 5, database.get_reader().unwrap().as_ref());
        assert_not_in_index("b", 5, database.get_reader().unwrap().as_ref());
        assert_database_count(1, database.get_reader().unwrap().as_ref());
        assert_key_value(5, 6, database.get_reader().unwrap().as_ref());
        assert_not_in_database(3, database.get_reader().unwrap().as_ref());

        // Delete {55: 5} in "a"
        let mut writer = database.get_writer().unwrap();
        writer.index_delete("a", &[55]).unwrap();

        assert_index_count("a", 0, writer.as_reader());
        assert_index_count("b", 0, writer.as_reader());
        assert_not_in_index("a", 5, writer.as_reader());
        assert_not_in_index("b", 5, writer.as_reader());
        assert_database_count(1, writer.as_reader());
        assert_key_value(5, 6, writer.as_reader());
        assert_not_in_database(3, writer.as_reader());

        writer.commit().unwrap();

        assert_index_count("a", 0, database.get_reader().unwrap().as_ref());
        assert_index_count("b", 0, database.get_reader().unwrap().as_ref());
        assert_not_in_index("a", 5, database.get_reader().unwrap().as_ref());
        assert_not_in_index("b", 5, database.get_reader().unwrap().as_ref());
        assert_database_count(1, database.get_reader().unwrap().as_ref());
        assert_key_value(5, 6, database.get_reader().unwrap().as_ref());
        assert_not_in_database(3, database.get_reader().unwrap().as_ref());
    }

    #[test]
    /// Tests the implementation of btree database cursor from a database reader
    fn test_btree_reader_database_cursor() {
        let database = BTreeDatabase::new(&["a", "b"]);
        let mut writer = database.get_writer().unwrap();
        writer.put(&[3], &[4]).unwrap();
        writer.put(&[10], &[1]).unwrap();
        writer.put(&[4], &[12]).unwrap();

        writer.commit().unwrap();
        {
            let reader = database.get_reader().unwrap();

            let mut cursor = reader.cursor().unwrap();

            // assert cursor.next() returns the key/value pairs in the expected order
            assert_eq!(Some((vec!(3), vec!(4))), cursor.next());
            assert_eq!(Some((vec!(4), vec!(12))), cursor.next());
            assert_eq!(Some((vec!(10), vec!(1))), cursor.next());
            assert_eq!(None, cursor.next());
            assert_eq!(None, cursor.last());

            cursor = reader.cursor().unwrap();

            // assert cursor.seek_first() and cursor.seek_last() returns expected key/value pairs
            assert_eq!(Some((vec!(3), vec!(4))), cursor.seek_first());
            assert_eq!(Some((vec!(10), vec!(1))), cursor.seek_last());
        }

        let mut writer = database.get_writer().unwrap();
        writer.index_put("a", &[5], &[2]).unwrap();
        writer.index_put("a", &[11], &[12]).unwrap();
        writer.index_put("a", &[2], &[22]).unwrap();
        writer.commit().unwrap();

        let reader = database.get_reader().unwrap();
        let mut cursor = reader.index_cursor("a").unwrap();

        assert_eq!(Some((vec!(2), vec!(22))), cursor.next());
        assert_eq!(Some((vec!(5), vec!(2))), cursor.next());
        assert_eq!(Some((vec!(11), vec!(12))), cursor.next());
        assert_eq!(None, cursor.next());
        assert_eq!(Some((vec!(11), vec!(12))), cursor.seek_last());

        cursor = reader.index_cursor("a").unwrap();

        assert_eq!(Some((vec!(2), vec!(22))), cursor.seek_first());
        assert_eq!(Some((vec!(11), vec!(12))), cursor.seek_last());
    }

    #[test]
    /// Tests the implementation of btree database cursor from a database writer
    fn test_btree_writer_database_cursor() {
        let database = BTreeDatabase::new(&["a", "b"]);
        let mut writer = database.get_writer().unwrap();
        writer.put(&[3], &[4]).unwrap();
        writer.put(&[10], &[1]).unwrap();
        writer.put(&[4], &[12]).unwrap();

        {
            let mut cursor = writer.cursor().unwrap();

            // assert cursor.next() returns the key/value pairs in the expected order
            assert_eq!(Some((vec!(3), vec!(4))), cursor.next());
            assert_eq!(Some((vec!(4), vec!(12))), cursor.next());
            assert_eq!(Some((vec!(10), vec!(1))), cursor.next());
            assert_eq!(None, cursor.next());
            assert_eq!(Some((vec!(10), vec!(1))), cursor.seek_last());
        }

        {
            let mut cursor = writer.cursor().unwrap();

            // assert cursor.seek_first() and cursor.seek_last() returns expected key/value pairs
            assert_eq!(Some((vec!(3), vec!(4))), cursor.seek_first());
            assert_eq!(Some((vec!(10), vec!(1))), cursor.seek_last());
        }

        writer.commit().unwrap();
        let mut writer = database.get_writer().unwrap();
        writer.index_put("a", &[5], &[2]).unwrap();
        writer.index_put("a", &[11], &[12]).unwrap();
        writer.index_put("a", &[2], &[22]).unwrap();

        {
            let mut cursor = writer.index_cursor("a").unwrap();

            assert_eq!(Some((vec!(2), vec!(22))), cursor.next());
            assert_eq!(Some((vec!(5), vec!(2))), cursor.next());
            assert_eq!(Some((vec!(11), vec!(12))), cursor.next());
            assert_eq!(None, cursor.next());
            assert_eq!(Some((vec!(11), vec!(12))), cursor.seek_last());
        }

        {
            let mut cursor = writer.index_cursor("a").unwrap();

            assert_eq!(Some((vec!(2), vec!(22))), cursor.seek_first());
            assert_eq!(Some((vec!(11), vec!(12))), cursor.seek_last());
        }
    }
}

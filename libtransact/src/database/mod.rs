/*
 * Copyright 2018 Intel Corporation
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
 * ------------------------------------------------------------------------------
 */

//! Traits for reading and writing from databases.
//!
//! Transact operates on key-value entries at the database level, where both keys and values are
//! opaque bytes.
//!
//! # Readers and Writers
//!
//! Both the DatabsaeReader and DatabaseWriter traits imply that the underlying database
//! implementation maintains transactional consistency throughout their lifetimes.  For example,
//! while a cursor on a DatabaseReader is in use, any changes to the underlying data should not
//! alter the iteration of the reader.
//!
//! Changes to the underlying database are rendered via the DatabaseWriter's commit method.

pub mod btree;
pub mod error;
pub mod lmdb;
#[cfg(feature = "redis-db")]
pub mod redis;
#[cfg(feature = "sqlite-db")]
pub mod sqlite;

pub use crate::database::error::DatabaseError;

pub type DatabaseCursor<'a> = Box<dyn DatabaseReaderCursor<Item = (Vec<u8>, Vec<u8>)> + 'a>;

pub trait Database: Sync + Send {
    fn get_reader<'a>(&'a self) -> Result<Box<dyn DatabaseReader + 'a>, DatabaseError>;
    fn get_writer<'a>(&'a self) -> Result<Box<dyn DatabaseWriter + 'a>, DatabaseError>;
    fn clone_box(&self) -> Box<dyn Database>;
}

impl Clone for Box<dyn Database> {
    fn clone(&self) -> Box<dyn Database> {
        self.clone_box()
    }
}

/// A DatabaseReader provides read access to a database instance.
pub trait DatabaseReader {
    /// Returns the bytes stored at the given key, if found.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError>;

    /// Returns the bytes stored at the given key on a specified index, if found.
    fn index_get(&self, index: &str, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError>;

    /// Returns a cursor against the main database. The cursor iterates over
    /// the entries in the natural key order.
    fn cursor(&self) -> Result<DatabaseCursor, DatabaseError>;

    /// Returns a cursor against the given index. The cursor iterates over
    /// the entries in the index's natural key order.
    fn index_cursor(&self, index: &str) -> Result<DatabaseCursor, DatabaseError>;

    /// Returns the number of entries in the main database.
    fn count(&self) -> Result<usize, DatabaseError>;

    /// Returns the number of entries in the given index.
    fn index_count(&self, index: &str) -> Result<usize, DatabaseError>;
}

/// A DatabaseReader provides read access to a database instance.
pub trait DatabaseWriter: DatabaseReader {
    /// Writes the given key/value pair. If the key/value pair already exists,
    /// it will return a DatabaseError::DuplicateEntry.
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError>;

    /// Writes the given key/value pair. If the key/value pair already exists,
    /// it overwrites the old value
    fn overwrite(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError>;

    /// Deletes the given key/value pair. If the key does exist, it returns an error,
    fn delete(&mut self, key: &[u8]) -> Result<(), DatabaseError>;

    /// Writes the given key/value pair at index.
    fn index_put(&mut self, index: &str, key: &[u8], value: &[u8]) -> Result<(), DatabaseError>;

    /// Deletes the given key/value pair at index.
    fn index_delete(&mut self, index: &str, key: &[u8]) -> Result<(), DatabaseError>;

    // Commit changes to database
    fn commit(self: Box<Self>) -> Result<(), DatabaseError>;

    // Use writer as reader
    fn as_reader(&self) -> &dyn DatabaseReader;
}

pub trait DatabaseReaderCursor: Iterator {
    fn seek_first(&mut self) -> Option<(Vec<u8>, Vec<u8>)>;
    fn seek_last(&mut self) -> Option<(Vec<u8>, Vec<u8>)>;
}

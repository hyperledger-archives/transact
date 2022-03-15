/*
 * Copyright 2022 Cargill Incorporated
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

use std::sync::{Arc, RwLock};

use lru::LruCache;

use crate::error::InternalError;

/// A cache for data values of at least a minimum size.
///
/// This cache provides LRU behavior for the values.
#[derive(Clone)]
pub struct DataCache {
    min_data_size: usize,
    inner: Arc<RwLock<Inner>>,
}

impl DataCache {
    /// Construct a new `DataCache` with a minimum cacheable data size and a max cache size.
    pub fn new(min_data_size: usize, cache_size: u16) -> Self {
        Self {
            min_data_size,
            inner: Arc::new(RwLock::new(Inner {
                cache: LruCache::new(cache_size as usize),
            })),
        }
    }

    /// Return whether or not the given data is worth caching
    pub fn cacheable(&self, data: &[u8]) -> bool {
        data.len() >= self.min_data_size
    }

    /// Insert a new (address, data_hash, data) tuple into the cache.
    ///
    /// Given this is an LRU cache, this may result in the oldest entry being dropped from the
    /// cache.
    pub fn insert(
        &self,
        address: String,
        data_hash: String,
        data: Vec<u8>,
    ) -> Result<(), InternalError> {
        if !self.cacheable(&data) {
            return Ok(());
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| InternalError::with_message("DataCache lock was poisoned".into()))?;

        inner.cache.put(address, (data_hash, data));

        Ok(())
    }

    /// Peek at the last known data for a given address.
    ///
    /// This does not update the LRU status for an entry, if it exists.
    pub fn peek_last_known_data_for_address(
        &self,
        address: &str,
    ) -> Result<Option<(String, Vec<u8>)>, InternalError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| InternalError::with_message("DataCache lock was poisoned".into()))?;

        Ok(inner
            .cache
            .peek(address)
            .map(|(hash, data)| (hash.clone(), data.clone())))
    }

    /// Touch an entry for the given address
    ///
    /// This method updates the given entries LRU status. It has no effect if there is no entry at
    /// the given address.
    pub fn touch_entry(&self, address: &str) -> Result<(), InternalError> {
        self.inner
            .write()
            .map_err(|_| InternalError::with_message("DataCache lock was poisoned".into()))?
            .cache
            .get(address);

        Ok(())
    }
}

struct Inner {
    cache: LruCache<String, (String, Vec<u8>)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use sha2::{Digest, Sha512};

    /// Test that a given set of values are cacheable
    #[test]
    fn test_cacheable() {
        let cache = DataCache::new(10, 16);

        assert!(!cache.cacheable(b"hello"));
        assert!(cache.cacheable(b"hello world"));
    }

    #[test]
    fn test_peek_last_known_data_hash_for_address() -> Result<(), InternalError> {
        let cache = DataCache::new(10, 16);

        let data = b"hello world".to_vec();
        let hash = hash(&data);
        cache.insert("abc01234".into(), hash.clone(), data.clone())?;

        assert_eq!(
            Some((hash, data)),
            cache.peek_last_known_data_for_address("abc01234")?
        );

        Ok(())
    }

    /// Test that a set of values of cacheable size are stored, but only the most recent subset of
    /// the values are retained after the cache's size limit is exceeded.
    #[test]
    fn test_cache_limits() -> Result<(), InternalError> {
        let values = (0..20)
            .map(|i| {
                (
                    format!("0000{:02x}", i),
                    hash(format!("hello-world-{}", i).as_bytes()),
                    format!("hello-world-{}", i).as_bytes().to_vec(),
                )
            })
            .collect::<Vec<_>>();

        let cache = DataCache::new(10, 16);
        for (addr, hash, data) in values.iter().cloned() {
            cache.insert(addr, hash, data)?;
        }

        // These items should have been evicted from the cache
        for i in 0..4 {
            assert_eq!(
                None,
                cache.peek_last_known_data_for_address(&format!("0000{:02x}", i))?
            );
        }

        // the remainder should still be in the cache
        for (addr, hash, data) in &values[4..] {
            assert_eq!(
                Some((hash.clone(), data.clone())),
                cache.peek_last_known_data_for_address(&addr)?,
            );
        }

        Ok(())
    }

    /// Test that a value that is "touched" will be retained in the cache.
    #[test]
    fn test_cache_limits_with_touch() -> Result<(), InternalError> {
        let values = (0..20)
            .map(|i| {
                (
                    format!("0000{:02x}", i),
                    hash(format!("hello-world-{}", i).as_bytes()),
                    format!("hello-world-{}", i).as_bytes().to_vec(),
                )
            })
            .collect::<Vec<_>>();

        let cache = DataCache::new(10, 16);
        for (addr, hash, data) in values.iter().cloned() {
            cache.insert(addr, hash, data)?;
            cache.touch_entry("000000")?;
        }

        // entry 0 should still be in the map
        assert_eq!(
            Some((
                hash("hello-world-0".as_bytes()),
                "hello-world-0".as_bytes().to_vec()
            )),
            cache.peek_last_known_data_for_address("000000")?,
        );

        for i in 1..5 {
            assert_eq!(
                None,
                cache.peek_last_known_data_for_address(&format!("0000{:02x}", i))?
            );
        }

        // the remainder should still be in the cache
        for (addr, hash, data) in &values[5..] {
            assert_eq!(
                Some((hash.clone(), data.clone())),
                cache.peek_last_known_data_for_address(&addr)?,
            );
        }

        Ok(())
    }

    fn hash(bytes: &[u8]) -> String {
        Sha512::digest(bytes)
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect()
    }
}

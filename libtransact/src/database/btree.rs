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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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

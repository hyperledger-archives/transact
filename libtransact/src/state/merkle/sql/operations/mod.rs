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

pub(super) mod update_index;

#[cfg(feature = "sqlite")]
no_arg_sql_function!(
    last_insert_rowid,
    diesel::sql_types::BigInt,
    "Represents the SQLite last_insert_rowid() function"
);

pub struct MerkleRadixOperations<'a, C>
where
    C: diesel::Connection,
{
    conn: &'a C,
}

impl<'a, C> MerkleRadixOperations<'a, C>
where
    C: diesel::Connection,
{
    #[allow(dead_code)]
    pub fn new(conn: &'a C) -> Self {
        MerkleRadixOperations { conn }
    }
}

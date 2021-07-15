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

use std::io::Write;

use diesel::{
    backend::Backend,
    deserialize::{self, FromSql},
    serialize::{self, Output, ToSql},
    sql_types::Text,
    sqlite::Sqlite,
};

use crate::state::merkle::sql::schema::sqlite_merkle_radix_tree_node;

#[derive(Insertable, Queryable, QueryableByName, Identifiable)]
#[cfg_attr(test, derive(Debug, PartialEq))]
#[table_name = "sqlite_merkle_radix_tree_node"]
#[primary_key(hash, tree_id)]
pub struct MerkleRadixTreeNode {
    pub hash: String,
    pub tree_id: i64,
    pub leaf_id: Option<i64>,
    pub children: Children,
}

#[derive(AsExpression, Debug, FromSqlRow, Deserialize, Serialize)]
#[cfg_attr(test, derive(PartialEq))]
#[sql_type = "diesel::sql_types::Text"]
pub struct Children(pub Vec<Option<String>>);

impl FromSql<Text, Sqlite> for Children {
    fn from_sql(bytes: Option<&<Sqlite as Backend>::RawValue>) -> deserialize::Result<Self> {
        let t = <String as FromSql<Text, Sqlite>>::from_sql(bytes)?;
        Ok(serde_json::from_str(&t)?)
    }
}

impl ToSql<Text, Sqlite> for Children {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Sqlite>) -> serialize::Result {
        let s = serde_json::to_string(&self.0)?;
        <String as ToSql<Text, Sqlite>>::to_sql(&s, out)
    }
}

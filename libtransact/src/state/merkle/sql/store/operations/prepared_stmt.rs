/*
 * Derived from diesel-rs, which is licensed under Apache 2.0 and MIT:
 *
 *   https://github.com/diesel-rs/diesel/blob/b95c9d2b0d78dbe597ade51f35dcee4130235399/diesel/src/query_builder/sql_query.rs
 *
 * Modifications Copyright 2022 Cargill Incorporated
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

use std::marker::PhantomData;

use diesel::backend::Backend;
use diesel::connection::Connection;
use diesel::deserialize::QueryableByName;
use diesel::query_builder::{AstPass, QueryFragment, QueryId};
use diesel::query_dsl::{LoadQuery, RunQueryDsl};
use diesel::result::QueryResult;
use diesel::serialize::ToSql;
use diesel::sql_types::HasSqlType;

pub fn prepare_stmt(sql: &'static str) -> PreparedStmt {
    PreparedStmt { sql }
}

pub struct PreparedStmt {
    sql: &'static str,
}

impl PreparedStmt {
    pub fn bind<ST, Value>(self, value: Value) -> UncheckedBind<Self, Value, ST> {
        UncheckedBind::new(self, value)
    }
}

impl<DB> QueryFragment<DB> for PreparedStmt
where
    DB: Backend,
{
    fn walk_ast(&self, mut out: AstPass<DB>) -> QueryResult<()> {
        out.push_sql(self.sql);
        Ok(())
    }
}

impl QueryId for PreparedStmt {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<Conn, T> LoadQuery<Conn, T> for PreparedStmt
where
    Conn: Connection,
    T: QueryableByName<Conn::Backend>,
{
    fn internal_load(self, conn: &Conn) -> QueryResult<Vec<T>> {
        conn.query_by_name(&self)
    }
}

impl<Conn> RunQueryDsl<Conn> for PreparedStmt {}

#[derive(Debug, Clone, Copy)]
#[must_use = "Queries are only executed when calling `load`, `get_result` or similar."]
pub struct UncheckedBind<Query, Value, ST> {
    query: Query,
    value: Value,
    _marker: PhantomData<ST>,
}

impl<Query, Value, ST> UncheckedBind<Query, Value, ST> {
    pub fn new(query: Query, value: Value) -> Self {
        UncheckedBind {
            query,
            value,
            _marker: PhantomData,
        }
    }

    pub fn bind<ST2, Value2>(self, value: Value2) -> UncheckedBind<Self, Value2, ST2> {
        UncheckedBind::new(self, value)
    }
}

impl<Query, Value, ST> QueryId for UncheckedBind<Query, Value, ST>
where
    Query: QueryId,
    ST: QueryId,
{
    type QueryId = UncheckedBind<Query::QueryId, (), ST::QueryId>;

    const HAS_STATIC_QUERY_ID: bool = Query::HAS_STATIC_QUERY_ID && ST::HAS_STATIC_QUERY_ID;
}

impl<Query, Value, ST, DB> QueryFragment<DB> for UncheckedBind<Query, Value, ST>
where
    DB: Backend + HasSqlType<ST>,
    Query: QueryFragment<DB>,
    Value: ToSql<ST, DB>,
{
    fn walk_ast(&self, mut out: AstPass<DB>) -> QueryResult<()> {
        self.query.walk_ast(out.reborrow())?;
        out.push_bind_param_value_only(&self.value)?;
        Ok(())
    }
}

impl<Conn, Query, Value, ST, T> LoadQuery<Conn, T> for UncheckedBind<Query, Value, ST>
where
    Conn: Connection,
    T: QueryableByName<Conn::Backend>,
    Self: QueryFragment<Conn::Backend> + QueryId,
{
    fn internal_load(self, conn: &Conn) -> QueryResult<Vec<T>> {
        conn.query_by_name(&self)
    }
}

impl<Conn, Query, Value, ST> RunQueryDsl<Conn> for UncheckedBind<Query, Value, ST> {}

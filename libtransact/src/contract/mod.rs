/*
 * Copyright 2019 Cargill Incorporated
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

#[cfg(feature = "contract-address")]
pub mod address;
#[cfg(feature = "contract-context")]
pub mod context;
#[cfg(feature = "contract-handler")]
pub mod handler;

use std::hash::Hash;

use crate::contract::address::Addresser;
use crate::handler::{ApplyError, TransactionContext};
use crate::protocol::transaction::TransactionPair;

pub trait SmartContract: Send {
    type Key: Eq + Hash;
    type Addr: Addresser<Self::Key>;
    type Context;

    fn get_family_name(&self) -> &str;

    fn get_family_versions(&self) -> &[String];

    fn get_addresser(&self) -> Self::Addr;

    fn make_context<'a>(
        &self,
        addresser: Self::Addr,
        context: &'a mut dyn TransactionContext,
    ) -> &mut Self::Context;

    fn apply(
        &self,
        transaction: &TransactionPair,
        context: &mut Self::Context,
    ) -> Result<(), ApplyError>;
}

/*
 * Copyright 2018 Intel Corporation
 * Copyright 2019 Bitwise IO, Inc.
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

mod error;
pub mod kv;
mod node;
#[cfg(feature = "state-merkle-sql")]
pub mod sql;

use crate::state::Read;

pub use error::MerkleRadixLeafReadError;
pub use kv::{
    MerkleRadixTree, MerkleState, StateDatabaseError, CHANGE_LOG_INDEX, DUPLICATE_LOG_INDEX,
    INDEXES,
};

// These types make the clippy happy
type IterResult<T> = Result<T, MerkleRadixLeafReadError>;
type LeafIter<T> = Box<dyn Iterator<Item = IterResult<T>>>;

/// A Merkle-Radix tree leaf reader.
///
/// This trait provides an interface to a Merkle-Radix state implementation in order to return an
/// iterator over the leaves of the tree at a given state root hash.
pub trait MerkleRadixLeafReader: Read<StateId = String, Key = String, Value = Vec<u8>> {
    /// Returns an iterator over the leaves of a merkle radix tree.
    ///
    /// The leaves returned by this iterator are tied to a specific state ID - in the merkle tree,
    /// this is defined as a specific state root hash. The leaves are returned in natural address
    /// order.
    ///
    /// By providing an optional address prefix, the caller can limit the iteration
    /// over the leaves in a specific sub-tree.
    fn leaves(
        &self,
        state_id: &Self::StateId,
        subtree: Option<&str>,
    ) -> IterResult<LeafIter<(Self::Key, Self::Value)>>;
}

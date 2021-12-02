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

//! Merkle-Radix Tree state.
//!
//! ## Merkle Hashes
//!
//! Transact provides an addressable Merkle-Radix tree to store data for state. Let’s break that
//! down: The tree is a Merkle tree because it is a copy-on-write data structure which stores
//! successive node hashes from leaf-to-root upon any changes to the tree. For a given set of state
//! transitions, we can generate a single root hash which points to that version of the tree.
//!
//! The root hash can be used to verify that the same set of state transitions applied against the
//! same state ID in another database will produce the same results.
//!
//! ## Radix Addresses
//!
//! The tree is an addressable Radix tree because addresses uniquely identify the paths to leaf
//! nodes in the tree where information is stored. An address is a hex-encoded byte array.  In the
//! tree implementation, each byte is a Radix path segment which identifies the next node in the
//! path to the leaf containing the data associated with the address. This gives the tree a branch
//! factor of 256.
//!
//! ## State Pruning
//!
//! Transact's implementation of the merkle-radix tree for state includes the
//! [`Prune`](crate::state::Prune) trait.
//!
//! A given State root can be viewed as a successor to a previous state root on which it was built.
//! Using this knowledge, pruning operates in one of two ways, depending on where in the chain of
//! successors a given root is.
//!
//! * If state root has successors, then all of the nodes that were removed during the creation of
//!   the root are removed. In other words, any data that would not be accessible from the given
//!   state root from prior state roots would be removed.
//! * If a state root has no successors (it's the tip of a successor chain), then all of the nodes
//!   that were added by that state root are removed. In other words, any data that would not be
//!   accessible by any other state root that still exists in the tree.
//!
//! The later method exists in service of removing forks in a successor chain of a tree.

mod error;
pub mod kv;
mod node;
#[cfg(feature = "state-merkle-sql")]
pub mod sql;

use crate::state::Read;

pub use error::MerkleRadixLeafReadError;
pub use kv::{
    MerkleLeafIterator, MerkleRadixTree, MerkleState, StateDatabaseError, CHANGE_LOG_INDEX,
    DUPLICATE_LOG_INDEX, INDEXES,
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

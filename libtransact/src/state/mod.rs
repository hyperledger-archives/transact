/*
 * Copyright 2018 Bitwise IO, Inc.
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

//! Methods for interacting with State.
//!
//! Transact State is managed via the implementation of three traits: the
//! `Write`, `Read`, and `Prune`.  These provide commit, read access,
//! and a way to purge old state, respectively, to an underlying storage mechanism.

pub mod error;
pub mod hashmap;
#[cfg(feature = "state-merkle")]
pub mod merkle;

pub use crate::state::error::{StatePruneError, StateReadError, StateWriteError};
use std::collections::HashMap;

/// A change to be applied to state, in terms of keys and values.
///
/// A `StateChange` represents the basic level of changes that can be applied to
/// values in state.  This covers the setting of a key/value pair, or the
/// deletion of a key.
#[derive(Debug)]
pub enum StateChange {
    Set { key: String, value: Vec<u8> },
    Delete { key: String },
}

impl Clone for StateChange {
    fn clone(&self) -> Self {
        match self {
            StateChange::Set { key, value } => StateChange::Set {
                key: key.clone(),
                value: value.clone(),
            },
            StateChange::Delete { key } => StateChange::Delete { key: key.clone() },
        }
    }
}

/// `state::Write` provides a way to write to a particular state storage system.
///
/// It provides the ability for the caller to either compute the next `StateId` -
/// useful for validating expected results - or committing the results to an
/// underlying store.
///
/// A `StateId`, in the context of Write, is used to indicate the
/// starting state on which the changes will be applied.  It can be thought of
/// as the identifier of a checkpoint or snapshot.
///
/// All operations are made using `StateChange` instances. These are the
/// ordered set of changes to be applied onto the given `StateId`.
///
/// Implementations are expected to be thread-safe.
pub trait Write: Sync + Send + Clone {
    /// A reference to a checkpoint in state. It could be a merkle hash for
    /// a merkle database.
    type StateId;
    /// The Key that is being stored in state.
    type Key;
    /// The Value that is being stored in state.
    type Value;

    /// Given a `StateId` and a slice of `StateChange` values, persist the
    /// state changes and return the resulting next `StateId` value.
    ///
    /// This function will persist the state values to the
    /// underlying storage mechanism.
    ///
    /// # Errors
    ///
    /// Any issues with committing the processing results will return a
    /// `StateWriteError`.
    fn commit(
        &self,
        state_id: &Self::StateId,
        state_changes: &[StateChange],
    ) -> Result<Self::StateId, StateWriteError>;

    /// Given a `StateId` and a slice of `StateChange` values, compute the
    /// next `StateId` value.
    ///
    /// This function will compute the value of the next `StateId` without
    /// actually persisting the state changes. Effectively, it is a dry-run.
    ///
    /// Returns the next `StateId` value;
    ///
    /// # Errors
    ///
    /// `StateWriteError` is returned if any issues occur while trying to
    /// generate this next id.
    fn compute_state_id(
        &self,
        state_id: &Self::StateId,
        state_changes: &[StateChange],
    ) -> Result<Self::StateId, StateWriteError>;
}

/// `state::Prune` provides a way to remove state ids from a particular state
/// storage system.
///
/// Removing `StateIds` and the associated state makes it so the state storage
/// system does not grow unbounded.
pub trait Prune: Sync + Send + Clone {
    /// A reference to a checkpoint in state. It could be a merkle hash for
    /// a merkle database.
    type StateId;
    /// The Key that is being stored in state.
    type Key;
    /// The Value that is being stored in state.
    type Value;

    /// Prune any State prior to the given StateId.
    ///
    /// In storage mechanisms that have a concept of `StateId` ordering, this
    /// function should provide the functionality to prune older state values.
    ///
    /// It can be considered a clean-up or space-saving mechanism.
    ///
    /// It returns the keys that have been removed from state, if any.
    ///
    /// # Errors
    ///
    /// StatePruneError is returned if any issues occur while trying to
    /// prune past results.
    fn prune(&self, state_ids: Vec<Self::StateId>) -> Result<Vec<Self::Key>, StatePruneError>;
}

/// `state::Read` provides a way to retrieve state from a particular storage
/// system.
///
/// It provides the ability to read values from an underlying storage
///
/// Implementations are expected to be thread-safe.
pub trait Read: Send + Send {
    /// A reference to a checkpoint in state. It could be a merkle hash for
    /// a merkle database.
    type StateId;
    /// The Key that is being stored in state.
    type Key;
    /// The Value that is being stored in state.
    type Value;

    /// At a given `StateId`, attempt to retrieve the given slice of keys.
    ///
    /// The results of the get will be returned in a `HashMap`.  Only keys that
    /// were found will be in this map. Keys missing from the map can be
    /// assumed to be missing from the underlying storage system as well.
    ///
    /// # Errors
    ///
    /// `StateReadError` is returned if any issues occur while trying to fetch
    /// the values.
    fn get(
        &self,
        state_id: &Self::StateId,
        keys: &[Self::Key],
    ) -> Result<HashMap<Self::Key, Self::Value>, StateReadError>;

    fn clone_box(
        &self,
    ) -> Box<dyn Read<StateId = Self::StateId, Key = Self::Key, Value = Self::Value>>;
}

impl<S, K, V> Clone for Box<dyn Read<StateId = S, Key = K, Value = V>> {
    fn clone(&self) -> Box<dyn Read<StateId = S, Key = K, Value = V>> {
        self.clone_box()
    }
}

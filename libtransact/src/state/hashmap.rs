/*
 * Copyright 2018 Bitwise IO, Inc.
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

//! Provides a simple, in-memory implementation of backed by `std::collections::HashMap`.

use super::error::{StateReadError, StateWriteError};
use super::{Read, StateChange, Write};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// An collection of key-value pairs that represents state at a particular point.
pub type State = HashMap<String, String>;

/// A collection of states.
///
/// Contains immutable individual states and insert new states instead of
/// updating existing states.
pub type States = HashMap<String, State>;

/// An in-memory implementation of state.
///
/// Stores a series of individual `State`s in a collective `HashMap`, where each
/// individual state `HashMap` is stored by its ID. No individual `State` gets modified on
/// updates, a new `State` is simply inserted into `States`.
#[derive(Debug, Clone, Default)]
pub struct HashMapState {
    states: Arc<Mutex<States>>,
}

impl HashMapState {
    /// Create a new HashMapState.
    ///
    /// Adds the empty state as a starting state.
    pub fn new() -> Self {
        let states: Self = Default::default();
        let state = HashMap::new();

        states
            .states
            .lock()
            .expect("Couldn't lock states mutex!")
            .insert(Self::state_id(&state), state);

        states
    }

    /// Calculate the ID of the given state.
    pub fn state_id(state: &State) -> String {
        format!("{:?}", state)
    }

    fn next_state(
        current_state: &State,
        state_changes: &[StateChange<String, String>],
    ) -> (String, State) {
        let next_state = state_changes
            .iter()
            .fold(current_state.clone(), |mut memo, ch| {
                match ch {
                    StateChange::Set { key, value } => memo.insert(key.clone(), value.clone()),
                    StateChange::Delete { key } => memo.remove(key),
                };
                memo
            });

        (Self::state_id(&next_state), next_state)
    }
}

impl Write for HashMapState {
    type StateId = String;
    type Key = String;
    type Value = String;

    fn commit(
        &self,
        state_id: &Self::StateId,
        state_changes: &[StateChange<Self::Key, Self::Value>],
    ) -> Result<Self::StateId, StateWriteError> {
        let mut states = self.states.lock().expect("Couldn't lock states mutex!");
        let state = states.get(state_id).ok_or_else(|| {
            StateWriteError::InvalidStateId(format!("Unknown state id {}", state_id))
        })?;

        let (next_state_id, new_state_map) = HashMapState::next_state(&state, state_changes);

        states.insert(next_state_id.clone(), new_state_map);

        Ok(next_state_id)
    }

    fn compute_state_id(
        &self,
        state_id: &Self::StateId,
        state_changes: &[StateChange<Self::Key, Self::Value>],
    ) -> Result<Self::StateId, StateWriteError> {
        let states = self.states.lock().expect("Couldn't lock states mutex!");
        let state = states.get(state_id).ok_or_else(|| {
            StateWriteError::InvalidStateId(format!("Unknown state id {}", state_id))
        })?;

        let (next_state_id, _) = HashMapState::next_state(&state, state_changes);

        Ok(next_state_id)
    }
}

impl Read for HashMapState {
    type StateId = String;
    type Key = String;
    type Value = String;

    fn get(&self, state_id: &Self::StateId, keys: &[Self::Key]) -> Result<State, StateReadError> {
        let states = self.states.lock().expect("Couldn't lock states mutex!");
        let state = states.get(state_id).ok_or_else(|| {
            StateReadError::InvalidStateId(format!("Unknown state id {}", state_id))
        })?;

        Ok(keys
            .iter()
            .cloned()
            .filter_map(|k| state.get(&k).cloned().map(|v| (k, v)))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_state_changes(
        sets: Vec<(&str, &str)>,
        deletes: Vec<&str>,
    ) -> Vec<StateChange<String, String>> {
        sets.into_iter()
            .map(|(key, value)| StateChange::Set {
                key: key.into(),
                value: value.into(),
            })
            .chain(
                deletes
                    .into_iter()
                    .map(|key| StateChange::Delete { key: key.into() }),
            )
            .collect::<Vec<_>>()
    }

    #[test]
    fn test_commit() {
        let state = HashMapState::new();
        let state_id = HashMapState::state_id(&HashMap::new());

        assert_eq!(state.states.lock().unwrap().len(), 1);
        assert_eq!(
            0,
            state
                .get(&state_id, &["a".into(), "b".into()])
                .unwrap()
                .len()
        );

        let state_changes = make_state_changes(
            vec![("a", "value_a"), ("b", "value_b"), ("c", "value_c")],
            vec!["c"],
        );

        let next_state_id = state.compute_state_id(&state_id, &state_changes).unwrap();
        assert_ne!(next_state_id, state_id);
        assert_eq!(state.states.lock().unwrap().len(), 1);
        assert!(state
            .get(&next_state_id, &["a".into(), "b".into()])
            .is_err());

        let committed_state_id = state.commit(&state_id, &state_changes).unwrap();

        assert_eq!(next_state_id, committed_state_id);
        assert_eq!(state.states.lock().unwrap().len(), 2);
        let found_state = state
            .get(&committed_state_id, &["a".into(), "b".into()])
            .unwrap();

        let mut expected_state = HashMap::new();
        expected_state.insert("a".to_string(), "value_a".to_string());
        expected_state.insert("b".to_string(), "value_b".to_string());

        assert_eq!(expected_state, found_state);
        assert_eq!(2, found_state.len());
    }
}

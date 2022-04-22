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

use std::collections::HashMap;

use crate::error::{InternalError, InvalidStateError};
use crate::state::{Reader, StateError, ValueIter, ValueIterResult};

use crate::state::merkle::kv::{error::StateDatabaseError, MerkleRadixTree, MerkleState};

impl Reader for MerkleState {
    type Filter = str;

    fn get(
        &self,
        state_id: &Self::StateId,
        keys: &[Self::Key],
    ) -> Result<HashMap<Self::Key, Self::Value>, StateError> {
        let merkle_tree =
            MerkleRadixTree::new(self.db.clone(), Some(state_id)).map_err(|err| match err {
                StateDatabaseError::NotFound(msg) => {
                    StateError::from(InvalidStateError::with_message(msg))
                }
                _ => StateError::from(InternalError::from_source(Box::new(err))),
            })?;

        keys.iter().try_fold(HashMap::new(), |mut result, key| {
            let value = match merkle_tree.get_by_address(key) {
                Ok(value) => value.value,
                Err(err) => match err {
                    StateDatabaseError::NotFound(_) => None,
                    _ => return Err(InternalError::from_source(Box::new(err)).into()),
                },
            };
            if let Some(value) = value {
                result.insert(key.to_string(), value);
            }
            Ok(result)
        })
    }

    fn filter_iter(
        &self,
        state_id: &Self::StateId,
        filter: Option<&Self::Filter>,
    ) -> ValueIterResult<ValueIter<(Self::Key, Self::Value)>> {
        let merkle_tree =
            MerkleRadixTree::new(self.db.clone(), Some(state_id)).map_err(|err| match err {
                // the state ID doesn't exist
                StateDatabaseError::NotFound(msg) => {
                    StateError::from(InvalidStateError::with_message(msg))
                }
                _ => StateError::from(InternalError::from_source(Box::new(err))),
            })?;

        merkle_tree
            .leaves(filter)
            .map(|iter| {
                Box::new(iter.map(|item| {
                    item.map_err(|e| StateError::from(InternalError::from_source(Box::new(e))))
                })) as Box<dyn Iterator<Item = _>>
            })
            .map_err(|e| match e {
                // the subtree doesn't exist
                StateDatabaseError::NotFound(msg) => {
                    StateError::from(InvalidStateError::with_message(msg))
                }
                _ => StateError::from(InternalError::from_source(Box::new(e))),
            })
    }
}

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

use crate::error::{InternalError, InvalidStateError};
use crate::state::{Pruner, StateError};

use crate::state::merkle::kv::{error::StateDatabaseError, MerkleRadixTree, MerkleState};

impl Pruner for MerkleState {
    fn prune(&self, state_ids: Vec<Self::StateId>) -> Result<Vec<Self::Key>, StateError> {
        state_ids
            .iter()
            .try_fold(Vec::new(), |mut result, state_id| {
                result.extend(MerkleRadixTree::prune(&*self.db, state_id).map_err(
                    |err| match err {
                        StateDatabaseError::NotFound(msg) => {
                            StateError::from(InvalidStateError::with_message(msg))
                        }
                        _ => StateError::from(InternalError::from_source(Box::new(err))),
                    },
                )?);
                Ok(result)
            })
    }
}

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

use super::{State, StateError};

/// Predicts future state checkpoints.
pub trait DryRunCommitter: State {
    /// Defines the type of change to use for the prediction.
    type StateChange;

    /// Given a `StateId` and a slice of `StateChange` values, compute the next `StateId` value.
    ///
    /// This function will compute the value of the next `StateId` without actually persisting the
    /// state changes. Effectively, it is a dry-run.
    ///
    /// Returns the next `StateId` value;
    ///
    /// # Errors
    ///
    /// [`StateError`] is returned if any issues occur while trying to generate this next id.
    fn dry_run_commit(
        &self,
        state_id: &Self::StateId,
        state_changes: &[Self::StateChange],
    ) -> Result<Self::StateId, StateError>;
}

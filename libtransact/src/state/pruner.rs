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

/// Provides a way to remove no-longer needed state data from a particular state storage system.
///
/// Removing `StateIds` and the associated state makes it so the state storage system does not grow
/// unbounded.
pub trait Pruner: State {
    /// Prune keys from state for a given set of state IDs.
    ///
    /// In storage mechanisms that have a concept of `StateId` ordering, this function should
    /// provide the functionality to prune older state values.
    ///
    /// It can be considered a clean-up or space-saving mechanism.
    ///
    /// It returns the keys that have been removed from state, if any.
    ///
    /// # Errors
    ///
    /// [`StateError`] is returned if any issues occur while trying to prune past results.
    fn prune(&self, state_ids: Vec<Self::StateId>) -> Result<Vec<Self::Key>, StateError>;
}

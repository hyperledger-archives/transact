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

/// Provides a way to commit changes to a particular state storage system.
///
/// A `StateId`, in the context of `Committer`, is used to indicate the starting state on which the
/// changes will be applied.  It can be thought of as the identifier of a checkpoint or snapshot.
///
/// All operations are made using `StateChange` instances. These are the ordered set of changes to
/// be applied onto the given `StateId`.
pub trait Committer: State {
    /// Defines the type of change to apply
    type StateChange;

    /// Given a `StateId` and a slice of `StateChange` values, persist the state changes and return
    /// the resulting next `StateId` value.
    ///
    /// This function will persist the state values to the underlying storage mechanism.
    ///
    /// # Errors
    ///
    /// [`StateError`] is returned if any issues occur while trying to commit the changes.
    fn commit(
        &self,
        state_id: &Self::StateId,
        state_changes: &[Self::StateChange],
    ) -> Result<Self::StateId, StateError>;
}

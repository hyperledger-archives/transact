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

/// Defines the characteristics of a given state system.
///
/// A State system stores keys and values.  The keys and values may be made available via state
/// IDs, which define a checkpoint in the state. Keys that exist under one state ID are not
/// required to be available under another.  How this is handled is left up to the implementation.
///
/// For example, a `State` defined over a merkle database would prove the root merkle hash as its
/// state ID.
pub trait State {
    /// A reference to a checkpoint in state. It could be a merkle hash for a merkle database.
    type StateId;

    /// The Key that is being stored in state.
    type Key;

    /// The Value that is being stored in state.
    type Value;
}

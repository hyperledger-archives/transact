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

use super::{State, StateError};

pub type ValueIterResult<T> = Result<T, StateError>;
pub type ValueIter<T> = Box<dyn Iterator<Item = ValueIterResult<T>>>;

/// Provides a way to retrieve state values from a particular storage system.
///
/// This trait provides similar behaviour to the [`Read`](super::Read) trait, without the
/// explicit requirements about thread safety.
pub trait Reader: State {
    /// The filter used for the iterating over state values.
    type Filter: ?Sized;

    /// At a given `StateId`, attempt to retrieve the given slice of keys.
    ///
    /// The results of the get will be returned in a `HashMap`.  Only keys that were found will be
    /// in this map. Keys missing from the map can be assumed to be missing from the underlying
    /// storage system as well.
    ///
    /// # Errors
    ///
    /// [`StateError`] is returned if any issues occur while trying to fetch the values.
    fn get(
        &self,
        state_id: &Self::StateId,
        keys: &[Self::Key],
    ) -> Result<HashMap<Self::Key, Self::Value>, StateError>;

    /// Returns an iterator over the values of state.
    ///
    /// By providing an optional filter, the caller can limit the iteration over a subset of the
    /// values.
    ///
    /// The values are returned in their natural order.
    ///
    /// # Errors
    ///
    /// [`StateError`] is returned if any issues occur while trying to query the values.
    /// Additionally, a [`StateError`] may be returned on each call to `next` on the resulting
    /// iteration.
    fn filter_iter(
        &self,
        state_id: &Self::StateId,
        filter: Option<&Self::Filter>,
    ) -> ValueIterResult<ValueIter<(Self::Key, Self::Value)>>;
}

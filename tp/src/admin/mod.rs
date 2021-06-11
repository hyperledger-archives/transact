// Copyright 2021 Cargill Incorporated
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A trait used by the Sabre transaction handle for verifying if a signer is an admin
//!
//! Includes two implementations, `AllowAllAdminPermission` and `SettingsAdminPermission`.
//! `SettingsAdminPermission` checks Sawtooth Settings for admin keys. The
//! `AllowAllAdminPermission` will always return true.

mod allow_all;
mod settings;

pub use self::allow_all::AllowAllAdminPermission;
pub use self::settings::SettingsAdminPermission;
use sawtooth_sdk::processor::handler::ApplyError;

use crate::state::SabreState;

/// Used to verify if a signer is an admin
pub trait AdminPermission: Send {
    /// Returns if the signer is an admin
    ///
    /// # Arguments
    ///
    /// * `signer` - The public key of the transaction signer
    /// * `state` - `SabreState` for fetching information about admins
    fn is_admin(&self, signer: &str, state: &mut SabreState) -> Result<bool, ApplyError>;
}

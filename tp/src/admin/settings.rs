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

//! An implementations of `AdminPermission` that checks Sawtooth Settings and returns true if the
//! signer is listed as an admin.

use sabre_sdk::protocol::ADMINISTRATORS_SETTING_KEY;
use sawtooth_sdk::processor::handler::ApplyError;

use super::AdminPermission;

use crate::state::SabreState;

#[derive(Default)]
pub struct SettingsAdminPermission;

impl AdminPermission for SettingsAdminPermission {
    fn is_admin(&self, signer: &str, state: &mut SabreState) -> Result<bool, ApplyError> {
        let setting = match state.get_admin_setting() {
            Ok(Some(setting)) => setting,
            Ok(None) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Admins not set, cannot check signer permissions: {}",
                    signer,
                )));
            }
            Err(err) => {
                return Err(ApplyError::InvalidTransaction(format!(
                    "Unable to check state: {}",
                    err,
                )));
            }
        };

        for entry in setting.get_entries() {
            if entry.key == ADMINISTRATORS_SETTING_KEY {
                let values = entry.value.split(',');
                let value_vec: Vec<&str> = values.collect();
                return Ok(value_vec.contains(&signer));
            }
        }

        Ok(false)
    }
}

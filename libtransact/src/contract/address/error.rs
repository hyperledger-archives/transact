// Copyright 2019 Cargill Incorporated
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

#[derive(Debug)]
pub enum AddresserError {
    KeyHashAddresserError(String),
    DoubleKeyHashAddresserError(String),
    TripleKeyHashAddresserError(String),
}

impl std::fmt::Display for AddresserError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            AddresserError::KeyHashAddresserError(ref s) => {
                write!(f, "Unable to compute hash: {}", s.to_string())
            }
            AddresserError::DoubleKeyHashAddresserError(ref s) => {
                write!(f, "Unable to compute hash: {}", s.to_string())
            }
            AddresserError::TripleKeyHashAddresserError(ref s) => {
                write!(f, "Unable to compute hash: {}", s.to_string())
            }
        }
    }
}

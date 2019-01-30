/*
 * Copyright 2018 Bitwise IO, Inc.
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

use crate::protos::merkle;
use crate::protos::{FromNative, FromProto, IntoNative, IntoProto, ProtoConversionError};
#[derive(Debug, Clone, PartialEq)]
pub struct Successor {
    pub successor: Vec<u8>,
    pub deletions: Vec<Vec<u8>>,
}

impl FromProto<merkle::ChangeLogEntry_Successor> for Successor {
    fn from_proto(
        successor: merkle::ChangeLogEntry_Successor,
    ) -> Result<Self, ProtoConversionError> {
        Ok(Successor {
            successor: successor.get_successor().to_vec(),
            deletions: successor.get_deletions().to_vec(),
        })
    }
}

impl FromNative<Successor> for merkle::ChangeLogEntry_Successor {
    fn from_native(successor: Successor) -> Result<Self, ProtoConversionError> {
        let mut proto_successor = merkle::ChangeLogEntry_Successor::new();
        proto_successor.set_successor(successor.successor);
        proto_successor.set_deletions(protobuf::RepeatedField::from_vec(successor.deletions));
        Ok(proto_successor)
    }
}

impl IntoProto<merkle::ChangeLogEntry_Successor> for Successor {}
impl IntoNative<Successor> for merkle::ChangeLogEntry_Successor {}
#[cfg(test)]
mod tests {
    use super::*;
    use sawtooth_sdk;

    static BYTES1: [u8; 4] = [0x01, 0x02, 0x03, 0x04];
    static BYTES2: [u8; 4] = [0x05, 0x06, 0x07, 0x08];
    static BYTES3: [u8; 4] = [0x09, 0x0a, 0x0b, 0x0c];

    #[test]
    fn successor_fields() {
        let successor = Successor {
            successor: BYTES1.to_vec(),
            deletions: vec![BYTES2.to_vec(), BYTES3.to_vec()],
        };
        assert_eq!(BYTES1.to_vec(), successor.successor);
        assert_eq!(vec!(BYTES2.to_vec(), BYTES3.to_vec()), successor.deletions);
    }
}

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
use protobuf::Message;

use super::error::StateDatabaseError;

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

#[derive(Debug, Clone)]
pub struct ChangeLogEntry {
    pub parent: Vec<u8>,
    pub additions: Vec<Vec<u8>>,
    pub successors: Vec<Successor>,
}

impl FromProto<merkle::ChangeLogEntry> for ChangeLogEntry {
    fn from_proto(change_log_entry: merkle::ChangeLogEntry) -> Result<Self, ProtoConversionError> {
        Ok(ChangeLogEntry {
            parent: change_log_entry.get_parent().to_vec(),
            additions: change_log_entry.get_additions().to_vec(),
            successors: change_log_entry
                .get_successors()
                .to_vec()
                .into_iter()
                .map(Successor::from_proto)
                .collect::<Result<Vec<Successor>, ProtoConversionError>>()?,
        })
    }
}

impl FromNative<ChangeLogEntry> for merkle::ChangeLogEntry {
    fn from_native(change_log_entry: ChangeLogEntry) -> Result<Self, ProtoConversionError> {
        let mut proto_change_log_entry = merkle::ChangeLogEntry::new();
        proto_change_log_entry.set_parent(change_log_entry.parent);
        proto_change_log_entry.set_additions(protobuf::RepeatedField::from_vec(
            change_log_entry.additions,
        ));
        proto_change_log_entry.set_successors(protobuf::RepeatedField::from_vec(
            change_log_entry
                .successors
                .into_iter()
                .map(merkle::ChangeLogEntry_Successor::from_native)
                .collect::<Result<Vec<merkle::ChangeLogEntry_Successor>, ProtoConversionError>>()?,
        ));
        Ok(proto_change_log_entry)
    }
}

impl IntoProto<merkle::ChangeLogEntry> for ChangeLogEntry {}
impl IntoNative<ChangeLogEntry> for merkle::ChangeLogEntry {}

impl ChangeLogEntry {
    pub fn to_bytes(&self) -> Result<Vec<u8>, StateDatabaseError> {
        Ok(self.clone().into_proto()?.write_to_bytes()?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<ChangeLogEntry, StateDatabaseError> {
        Ok(ChangeLogEntry::from_proto(protobuf::parse_from_bytes(
            bytes,
        )?)?)
    }

    pub fn take_successors(&mut self) -> Vec<Successor> {
        ::std::mem::replace(&mut self.successors, Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "sawtooth-compat")]
    use crate::protos;

    #[cfg(feature = "sawtooth-compat")]
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

    #[test]
    fn change_log_entry_fields() {
        let entry = ChangeLogEntry {
            parent: BYTES1.to_vec(),
            additions: vec![BYTES2.to_vec(), BYTES3.to_vec()],
            successors: vec![Successor {
                successor: BYTES1.to_vec(),
                deletions: vec![BYTES2.to_vec(), BYTES3.to_vec()],
            }],
        };
        assert_eq!(BYTES1.to_vec(), entry.parent);
        assert_eq!(vec!(BYTES2.to_vec(), BYTES3.to_vec()), entry.additions);
        assert_eq!(
            vec!(Successor {
                successor: BYTES1.to_vec(),
                deletions: vec!(BYTES2.to_vec(), BYTES3.to_vec()),
            }),
            entry.successors
        )
    }

    #[cfg(feature = "sawtooth-compat")]
    #[test]
    fn change_log_entry_receipt_sawtooth10_compatibility() {
        let mut proto_entry = sawtooth_sdk::messages::merkle::ChangeLogEntry::new();
        let mut proto_successor = sawtooth_sdk::messages::merkle::ChangeLogEntry_Successor::new();
        proto_successor.set_successor(BYTES1.to_vec());
        proto_successor.set_deletions(protobuf::RepeatedField::from_vec(vec![
            BYTES2.to_vec(),
            BYTES3.to_vec(),
        ]));
        proto_entry.set_parent(BYTES1.to_vec());
        proto_entry.set_additions(protobuf::RepeatedField::from_vec(vec![
            BYTES2.to_vec(),
            BYTES3.to_vec(),
        ]));
        proto_entry.set_successors(protobuf::RepeatedField::from_vec(vec![proto_successor]));

        let entry_bytes = protobuf::Message::write_to_bytes(&proto_entry).unwrap();

        let proto: protos::merkle::ChangeLogEntry =
            protobuf::parse_from_bytes(&entry_bytes).unwrap();

        let change_log_entry: ChangeLogEntry = proto.into_native().unwrap();
        assert_eq!(BYTES1.to_vec(), change_log_entry.parent);
        assert_eq!(
            vec!(BYTES2.to_vec(), BYTES3.to_vec()),
            change_log_entry.additions
        );
        assert_eq!(
            vec!(Successor {
                successor: BYTES1.to_vec(),
                deletions: vec!(BYTES2.to_vec(), BYTES3.to_vec()),
            }),
            change_log_entry.successors
        )
    }
    #[test]
    fn change_log_entry_roundtrip() {
        let entry_before = ChangeLogEntry {
            parent: BYTES1.to_vec(),
            additions: vec![BYTES2.to_vec(), BYTES3.to_vec()],
            successors: vec![Successor {
                successor: BYTES1.to_vec(),
                deletions: vec![BYTES2.to_vec(), BYTES3.to_vec()],
            }],
        };

        let bytes = entry_before
            .to_bytes()
            .expect("Failed to serialize entry into bytes");
        let entry_afer =
            ChangeLogEntry::from_bytes(&bytes).expect("Failed to desearialize entry from bytes");

        assert_eq!(BYTES1.to_vec(), entry_afer.parent);
        assert_eq!(vec!(BYTES2.to_vec(), BYTES3.to_vec()), entry_afer.additions);
        assert_eq!(
            vec!(Successor {
                successor: BYTES1.to_vec(),
                deletions: vec!(BYTES2.to_vec(), BYTES3.to_vec()),
            }),
            entry_afer.successors
        )
    }
}

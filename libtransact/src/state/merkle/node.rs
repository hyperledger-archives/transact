/*
 * Copyright 2019-2021 Cargill Incorporated
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

use std::collections::BTreeMap;
use std::io::Cursor;

use cbor::decoder::GenericDecoder;
use cbor::encoder::GenericEncoder;
use cbor::value::{Bytes, Key, Text, Value};

use crate::error::InternalError;

impl From<cbor::EncodeError> for InternalError {
    fn from(e: cbor::EncodeError) -> Self {
        InternalError::from_source(Box::new(e))
    }
}

impl From<cbor::DecodeError> for InternalError {
    fn from(e: cbor::DecodeError) -> Self {
        InternalError::from_source(Box::new(e))
    }
}

/// Internal Node structure of the Radix tree
#[derive(Default, Clone)]
#[cfg_attr(test, derive(Debug, PartialEq))]
pub struct Node {
    pub value: Option<Vec<u8>>,
    pub children: BTreeMap<String, String>,
}

impl Node {
    /// Consumes this node and serializes it to bytes
    pub fn into_bytes(self) -> Result<Vec<u8>, InternalError> {
        let mut e = GenericEncoder::new(Cursor::new(Vec::new()));

        let mut map = BTreeMap::new();
        map.insert(
            Key::Text(Text::Text("v".to_string())),
            match self.value {
                Some(bytes) => Value::Bytes(Bytes::Bytes(bytes)),
                None => Value::Null,
            },
        );

        let children = self
            .children
            .into_iter()
            .map(|(k, v)| (Key::Text(Text::Text(k)), Value::Text(Text::Text(v))))
            .collect();

        map.insert(Key::Text(Text::Text("c".to_string())), Value::Map(children));

        e.value(&Value::Map(map))?;

        Ok(e.into_inner().into_writer().into_inner())
    }

    /// Deserializes the given bytes to a Node
    pub fn from_bytes(bytes: &[u8]) -> Result<Node, InternalError> {
        let input = Cursor::new(bytes);
        let mut decoder = GenericDecoder::new(cbor::Config::default(), input);
        let decoder_value = decoder.value()?;
        let (val, children_raw) = match decoder_value {
            Value::Map(mut root_map) => (
                root_map.remove(&Key::Text(Text::Text("v".to_string()))),
                root_map.remove(&Key::Text(Text::Text("c".to_string()))),
            ),
            _ => {
                return Err(InternalError::with_message(
                    "Invalid node record: is not a map".into(),
                ))
            }
        };

        let value = match val {
            Some(Value::Bytes(Bytes::Bytes(bytes))) => Some(bytes),
            Some(Value::Null) => None,
            _ => {
                return Err(InternalError::with_message(
                    "Invalid node record: incorrect value type".into(),
                ))
            }
        };

        let children = match children_raw {
            Some(Value::Map(child_map)) => {
                let mut result = BTreeMap::new();
                for (k, v) in child_map {
                    result.insert(key_to_string(k)?, text_to_string(v)?);
                }
                result
            }
            None => BTreeMap::new(),
            _ => {
                return Err(InternalError::with_message(
                    "Invalid node record: incorrect child map type".into(),
                ))
            }
        };

        Ok(Node { value, children })
    }
}

/// Converts a CBOR Key to its String content
fn key_to_string(key_val: Key) -> Result<String, InternalError> {
    match key_val {
        Key::Text(Text::Text(s)) => Ok(s),
        _ => Err(InternalError::with_message(
            "Invalid node record: invalid key type".into(),
        )),
    }
}

/// Converts a CBOR Text Value to its String content
fn text_to_string(text_val: Value) -> Result<String, InternalError> {
    match text_val {
        Value::Text(Text::Text(s)) => Ok(s),
        _ => Err(InternalError::with_message(
            "Invalid node record: invalid value type".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_serialize() {
        let n = Node {
            value: Some(b"hello".to_vec()),
            children: vec![("ab".to_string(), "123".to_string())]
                .into_iter()
                .collect(),
        };

        let packed = n
            .into_bytes()
            .unwrap()
            .iter()
            .map(|b| format!("{:x}", b))
            .collect::<Vec<_>>()
            .join("");
        // This expected output was generated using the python structures
        let output = "a26163a16261626331323361764568656c6c6f";

        assert_eq!(output, packed);
    }

    #[test]
    fn node_deserialize() {
        let packed =
            ::hex::decode("a26163a162303063616263617647676f6f64627965").expect("improper hex");

        let unpacked = Node::from_bytes(&packed).unwrap();
        assert_eq!(
            Node {
                value: Some(b"goodbye".to_vec()),
                children: vec![("00".to_string(), "abc".to_string())]
                    .into_iter()
                    .collect(),
            },
            unpacked
        );
    }

    #[test]
    fn node_roundtrip() {
        let n = Node {
            value: Some(b"hello".to_vec()),
            children: vec![("ab".to_string(), "123".to_string())]
                .into_iter()
                .collect(),
        };

        let packed = n.into_bytes().unwrap();
        let unpacked = Node::from_bytes(&packed).unwrap();

        assert_eq!(
            Node {
                value: Some(b"hello".to_vec()),
                children: vec![("ab".to_string(), "123".to_string())]
                    .into_iter()
                    .collect(),
            },
            unpacked
        )
    }
}

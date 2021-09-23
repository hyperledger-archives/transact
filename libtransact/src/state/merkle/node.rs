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
}

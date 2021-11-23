/*
 * Copyright 2021 Cargill Incorporated
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
 * ------------------------------------------------------------------------------
 */

//! Tools for reading length-delimited protobuf batches from a file

use std::io::Read;

use protobuf::Message;

use crate::error::InternalError;
use crate::protocol::batch::BatchPair;
use crate::protos::FromProto;

use super::BatchReader;

/// Decodes Protocol Buffer `Batches` from a length-delimited input reader.
pub struct ProtobufBatchReader<'a> {
    source: protobuf::CodedInputStream<'a>,
}

impl<'a> ProtobufBatchReader<'a> {
    pub fn new(source: &'a mut dyn Read) -> Self {
        let source = protobuf::CodedInputStream::new(source);
        ProtobufBatchReader { source }
    }
}

impl BatchReader for ProtobufBatchReader<'_> {
    /// Returns the next set of `Batches`.
    /// The vector of `Batches` will contain up to `max_batches` number of
    /// `Batches`. An empty vector indicates that the source has been consumed.
    fn next(&mut self, max_batches: usize) -> Result<Vec<BatchPair>, InternalError> {
        let mut results = Vec::with_capacity(max_batches);
        for _ in 0..max_batches {
            let eof = self
                .source
                .eof()
                .map_err(|err| InternalError::from_source(Box::new(err)))?;
            if eof {
                break;
            }

            // read the delimited length
            let next_len = self
                .source
                .read_raw_varint32()
                .map_err(|err| InternalError::from_source(Box::new(err)))?;
            let buf = self
                .source
                .read_raw_bytes(next_len)
                .map_err(|err| InternalError::from_source(Box::new(err)))?;

            let msg = Message::parse_from_bytes(&buf)
                .map_err(|err| InternalError::from_source(Box::new(err)))?;
            let batch = BatchPair::from_proto(msg)
                .map_err(|err| InternalError::from_source(Box::new(err)))?;
            results.push(batch);
        }
        Ok(results)
    }
}

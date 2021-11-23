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

//! Tools for reading length-delimited protobuf transactions from a file

use std::io::Read;

use protobuf::Message;

use crate::error::InternalError;
use crate::protocol::transaction::Transaction;
use crate::protos::FromProto;

use super::TransactionReader;

/// Decodes Protocol Buffer `Transactions` from a length-delimited input reader.
pub struct ProtobufTransactionReader<'a> {
    source: protobuf::CodedInputStream<'a>,
}

impl<'a> ProtobufTransactionReader<'a> {
    pub fn new(source: &'a mut dyn Read) -> Self {
        let source = protobuf::CodedInputStream::new(source);
        ProtobufTransactionReader { source }
    }
}

impl TransactionReader for ProtobufTransactionReader<'_> {
    /// Returns the next set of `Transactions`.
    /// The vector of `Transactions` will contain up to `max_txns` number of
    /// `Transactions`. An empty vector indicates that the source has been consumed.
    fn next(&mut self, max_txns: usize) -> Result<Vec<Transaction>, InternalError> {
        let mut results = Vec::with_capacity(max_txns);
        for _ in 0..max_txns {
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
            let txn = Transaction::from_proto(msg)
                .map_err(|err| InternalError::from_source(Box::new(err)))?;
            results.push(txn);
        }
        Ok(results)
    }
}

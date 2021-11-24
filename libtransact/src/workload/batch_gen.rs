/*
 * Copyright 2021 Cargill Incorporated
 * Copyright 2017 Intel Corporation
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

//! Tools for generating signed batches from a stream of transactions

use std::io::Read;
use std::io::Write;

use cylinder::Signer;
use protobuf::Message;

use crate::error::{InternalError, InvalidStateError};
use crate::protocol::batch::{Batch, BatchBuilder, BatchPair};
use crate::protocol::transaction::Transaction;
use crate::protos::batch::Batch as ProtobufBatch;
use crate::protos::FromNative;
use crate::workload::{
    batch_reader::{protobuf::ProtobufBatchReader, BatchReader},
    transaction_reader::{protobuf::ProtobufTransactionReader, TransactionReader},
};

use super::error::BatchingError;

type TransactionSource<'a> = ProtobufTransactionReader<'a>;

/// Produces signed batches from a length-delimited source of Transactions.
pub struct SignedBatchProducer<'a> {
    transaction_source: TransactionSource<'a>,
    max_batch_size: usize,
    signer: &'a dyn Signer,
}

/// Resulting batch or error.
type BatchResult = Result<Batch, BatchingError>;

impl<'a> SignedBatchProducer<'a> {
    /// Creates a new `SignedBatchProducer` with a given Transaction source and
    /// a max number of transactions per batch.
    pub fn new(source: &'a mut dyn Read, max_batch_size: usize, signer: &'a dyn Signer) -> Self {
        let transaction_source = ProtobufTransactionReader::new(source);
        SignedBatchProducer {
            transaction_source,
            max_batch_size,
            signer,
        }
    }

    /// Writes signed batches in a length-delimited fashion to the given writer.
    pub fn write_to(&mut self, writer: &'a mut dyn Write) -> Result<(), BatchingError> {
        loop {
            match self.next() {
                Some(Ok(batch)) => {
                    let proto_batch = ProtobufBatch::from_native(batch).map_err(|err| {
                        BatchingError::InternalError(InternalError::from_source(Box::new(err)))
                    })?;
                    if let Err(err) = proto_batch.write_length_delimited_to_writer(writer) {
                        return Err(BatchingError::InternalError(
                            InternalError::from_source_with_message(
                                Box::new(err),
                                "Error occurred reading messages".into(),
                            ),
                        ));
                    }
                }
                None => break,
                Some(Err(err)) => return Err(err),
            }
        }
        Ok(())
    }
}

impl<'a> Iterator for SignedBatchProducer<'a> {
    type Item = BatchResult;

    /// Gets the next BatchResult.
    /// `Ok(None)` indicates that the underlying source has been consumed.
    fn next(&mut self) -> Option<BatchResult> {
        let txns: Vec<Transaction> = match self.transaction_source.next(self.max_batch_size) {
            Ok(txns) => txns,
            Err(err) => {
                return Some(Err(BatchingError::InternalError(
                    InternalError::from_source_with_message(
                        Box::new(err),
                        "Error occurred reading messages".into(),
                    ),
                )))
            }
        };
        if txns.is_empty() {
            None
        } else {
            Some(batch_transactions(txns, self.signer))
        }
    }
}

fn batch_transactions(txns: Vec<Transaction>, signer: &dyn Signer) -> BatchResult {
    BatchBuilder::new()
        .with_transactions(txns)
        .build(signer)
        .map_err(|_| {
            BatchingError::InvalidStateError(InvalidStateError::with_message(
                "Failed to build batch".into(),
            ))
        })
}

type BatchSource<'a> = ProtobufBatchReader<'a>;

/// Produces batches from length-delimited source of Batches.
pub struct BatchListFeeder<'a> {
    batch_source: BatchSource<'a>,
}

/// Resulting BatchList or error.
pub(super) type BatchListResult = Result<BatchPair, BatchingError>;

impl<'a> BatchListFeeder<'a> {
    /// Creates a new `BatchListFeeder` with a given Batch source
    pub fn new(source: &'a mut dyn Read) -> Self {
        let batch_source = ProtobufBatchReader::new(source);
        BatchListFeeder { batch_source }
    }
}

impl<'a> Iterator for BatchListFeeder<'a> {
    type Item = BatchListResult;

    /// Gets the next Batch.
    /// `Ok(None)` indicates that the underlying source has been consumed.
    fn next(&mut self) -> Option<Self::Item> {
        let batches = match self.batch_source.next(1) {
            Ok(batches) => batches,
            Err(err) => return Some(Err(BatchingError::InternalError(err))),
        };

        batches.get(0).map(|b| Ok(b.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Cursor, Write};

    use cylinder::{secp256k1::Secp256k1Context, Context, Signature, Signer};

    use crate::protocol::batch::BatchHeader;
    use crate::protocol::transaction::{HashMethod, TransactionBuilder};
    use crate::protos::transaction::Transaction as ProtobufTransaction;
    use crate::protos::FromBytes;

    type BatchSource<'a> = ProtobufBatchReader<'a>;

    #[test]
    fn empty_transaction_source() {
        let encoded_bytes: Vec<u8> = Vec::new();
        let mut source = Cursor::new(encoded_bytes);

        let mut txn_stream: TransactionSource = ProtobufTransactionReader::new(&mut source);
        let txns = txn_stream.next(2).unwrap();
        assert_eq!(txns.len(), 0);
    }

    #[test]
    fn next_transactions() {
        let mut encoded_bytes: Vec<u8> = Vec::new();

        let signer = new_signer();

        write_txn_with_signer(&*signer, &mut encoded_bytes);
        write_txn_with_signer(&*signer, &mut encoded_bytes);
        write_txn_with_signer(&*signer, &mut encoded_bytes);

        let mut source = Cursor::new(encoded_bytes);

        let mut txn_stream: TransactionSource = ProtobufTransactionReader::new(&mut source);

        let mut txns = txn_stream.next(2).unwrap();
        assert_eq!(txns.len(), 2);

        // ensure that it is exhausted, even when more are requested
        txns = txn_stream.next(2).unwrap();
        assert_eq!(txns.len(), 1);
    }

    #[test]
    fn signed_batches_empty_transactions() {
        let encoded_bytes: Vec<u8> = Vec::new();
        let mut source = Cursor::new(encoded_bytes);

        let signer = new_signer();

        let mut producer = SignedBatchProducer::new(&mut source, 2, &*signer);
        let batch_result = producer.next();

        assert!(batch_result.is_none());
    }

    #[test]
    fn signed_batches_single_transaction() {
        let signer1 = new_signer();
        let mut encoded_bytes: Vec<u8> = Vec::new();
        let sig1 = write_txn_with_signer(&*signer1, &mut encoded_bytes);

        let mut source = Cursor::new(encoded_bytes);

        let signer = new_signer();

        let mut producer = SignedBatchProducer::new(&mut source, 2, &*signer);
        let mut batch_result = producer.next();
        assert!(batch_result.is_some());

        let batch = batch_result.unwrap().unwrap();

        let batch_header = BatchHeader::from_bytes(batch.header()).unwrap();
        assert_eq!(batch_header.transaction_ids().len(), 1);
        assert_eq!(batch_header.transaction_ids()[0], sig1.as_hex());

        // test exhaustion
        batch_result = producer.next();
        assert!(batch_result.is_none());
    }

    #[test]
    fn signed_batches_multiple_batches() {
        let mut encoded_bytes: Vec<u8> = Vec::new();

        let signer1 = new_signer();
        let sig1 = write_txn_with_signer(&*signer1, &mut encoded_bytes);

        let signer2 = new_signer();
        let sig2 = write_txn_with_signer(&*signer2, &mut encoded_bytes);

        let signer3 = new_signer();
        let sig3 = write_txn_with_signer(&*signer3, &mut encoded_bytes);

        let mut source = Cursor::new(encoded_bytes);

        let signer = new_signer();

        let mut producer = SignedBatchProducer::new(&mut source, 2, &*signer);
        let mut batch_result = producer.next();
        assert!(batch_result.is_some());

        let batch = batch_result.unwrap().unwrap();

        let signature = signer
            .sign(&batch.header())
            .expect("Unable to sign batch header");

        let batch_header = BatchHeader::from_bytes(batch.header()).unwrap();
        assert_eq!(batch_header.transaction_ids().len(), 2);
        assert_eq!(batch_header.transaction_ids()[0], sig1.as_hex());
        assert_eq!(batch_header.transaction_ids()[1], sig2.as_hex());
        assert_eq!(batch.header_signature(), signature.as_hex());

        // pull the next batch
        batch_result = producer.next();
        assert!(batch_result.is_some());

        let batch = batch_result.unwrap().unwrap();

        let batch_header = BatchHeader::from_bytes(batch.header()).unwrap();
        assert_eq!(batch_header.transaction_ids().len(), 1);
        assert_eq!(batch_header.transaction_ids()[0], sig3.as_hex());

        // test exhaustion
        batch_result = producer.next();
        assert!(batch_result.is_none());
    }

    #[test]
    fn generate_signed_batches() {
        let mut encoded_bytes: Vec<u8> = Vec::new();

        let signer1 = new_signer();
        let sig1 = write_txn_with_signer(&*signer1, &mut encoded_bytes);

        let signer2 = new_signer();
        let sig2 = write_txn_with_signer(&*signer2, &mut encoded_bytes);

        let signer3 = new_signer();
        let sig3 = write_txn_with_signer(&*signer3, &mut encoded_bytes);

        let mut source = Cursor::new(encoded_bytes);
        let output_bytes: Vec<u8> = Vec::new();
        let mut output = Cursor::new(output_bytes);

        let signer = new_signer();

        super::SignedBatchProducer::new(&mut source, 2, &*signer)
            .write_to(&mut output)
            .expect("Should have generated batches!");

        // reset for reading
        output.set_position(0);
        let mut batch_source: BatchSource = ProtobufBatchReader::new(&mut output);

        let batch = &(batch_source.next(1).unwrap())[0];
        let batch_header: BatchHeader = batch.header().clone();
        assert_eq!(batch_header.transaction_ids().len(), 2);
        assert_eq!(batch_header.transaction_ids()[0], sig1.as_hex());
        assert_eq!(batch_header.transaction_ids()[1], sig2.as_hex());

        let batch = &(batch_source.next(1).unwrap())[0];
        let batch_header: BatchHeader = batch.header().clone();
        assert_eq!(batch_header.transaction_ids().len(), 1);
        assert_eq!(batch_header.transaction_ids()[0], sig3.as_hex());
    }

    fn make_txn(signer: &dyn Signer) -> (Transaction, Signature) {
        let public_key = signer
            .public_key()
            .expect("failed to get pub key")
            .into_bytes();

        let txn = TransactionBuilder::new()
            .with_batcher_public_key(public_key)
            .with_family_name(String::from("test_family"))
            .with_family_version(String::from("1.0"))
            .with_inputs(vec!["inputs".as_bytes().to_vec()])
            .with_outputs(vec!["outputs".as_bytes().to_vec()])
            .with_payload_hash_method(HashMethod::Sha512)
            .with_payload("sig".as_bytes().to_vec())
            .build(signer)
            .expect("Failed to build transaction");

        let signature =
            Signature::from_hex(txn.header_signature()).expect("Failed to get signature");

        (txn, signature)
    }

    fn write_txn_with_signer(signer: &dyn Signer, out: &mut dyn Write) -> Signature {
        let (txn, signature) = make_txn(signer);
        let txn_proto = ProtobufTransaction::from_native(txn).unwrap();
        txn_proto
            .write_length_delimited_to_writer(out)
            .expect("Unable to write delimiter");
        signature
    }

    fn new_signer() -> Box<dyn Signer> {
        let context = Secp256k1Context::new();
        let key = context.new_random_private_key();
        context.new_signer(key.clone())
    }
}

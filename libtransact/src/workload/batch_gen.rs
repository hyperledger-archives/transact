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

use crate::protocol::batch::BatchPair;
use crate::protos::FromProto;
use crate::protos::{
    batch::{Batch, BatchHeader},
    transaction::Transaction,
};

use super::error::{BatchReadingError, BatchingError};
use super::source::LengthDelimitedMessageSource;

/// Generates signed batches from a stream of length-delimited transactions.
/// Constrains the batches to `max_batch_size` number of transactions per
/// batch.  The resulting batches are written in a length-delimited fashion to
/// the given writer.
pub fn generate_signed_batches<'a>(
    reader: &'a mut dyn Read,
    writer: &'a mut dyn Write,
    max_batch_size: usize,
    signer: &dyn Signer,
) -> Result<(), BatchingError> {
    let mut producer = SignedBatchProducer::new(reader, max_batch_size, signer);
    loop {
        match producer.next() {
            Some(Ok(batch)) => {
                if let Err(err) = batch.write_length_delimited_to_writer(writer) {
                    return Err(BatchingError::MessageError(err));
                }
            }
            None => break,
            Some(Err(err)) => return Err(err),
        }
    }

    Ok(())
}

type TransactionSource<'a> = LengthDelimitedMessageSource<'a, Transaction>;

/// Produces signed batches from a length-delimited source of Transactions.
pub struct SignedBatchProducer<'a> {
    transaction_source: TransactionSource<'a>,
    max_batch_size: usize,
    signer: &'a dyn Signer,
}

/// Resulting batch or error.
pub type BatchResult = Result<Batch, BatchingError>;

impl<'a> SignedBatchProducer<'a> {
    /// Creates a new `SignedBatchProducer` with a given Transaction source and
    /// a max number of transactions per batch.
    pub fn new(source: &'a mut dyn Read, max_batch_size: usize, signer: &'a dyn Signer) -> Self {
        let transaction_source = LengthDelimitedMessageSource::new(source);
        SignedBatchProducer {
            transaction_source,
            max_batch_size,
            signer,
        }
    }
}

impl<'a> Iterator for SignedBatchProducer<'a> {
    type Item = BatchResult;

    /// Gets the next BatchResult.
    /// `Ok(None)` indicates that the underlying source has been consumed.
    fn next(&mut self) -> Option<BatchResult> {
        let txns = match self.transaction_source.next(self.max_batch_size) {
            Ok(txns) => txns,
            Err(err) => return Some(Err(BatchingError::MessageError(err))),
        };
        if txns.is_empty() {
            None
        } else {
            Some(batch_transactions(txns, self.signer))
        }
    }
}

fn batch_transactions(txns: Vec<Transaction>, signer: &dyn Signer) -> BatchResult {
    let mut batch_header = BatchHeader::new();

    // set signer_public_key
    let pk = match signer.public_key() {
        Ok(pk) => pk,
        Err(err) => return Err(BatchingError::SigningError(err)),
    };
    let public_key = pk.as_hex();

    let txn_ids = txns
        .iter()
        .map(|txn| String::from(txn.get_header_signature()))
        .collect();
    batch_header.set_transaction_ids(protobuf::RepeatedField::from_vec(txn_ids));
    batch_header.set_signer_public_key(public_key);

    let header_bytes = batch_header.write_to_bytes()?;
    let signature = signer
        .sign(&header_bytes)
        .map_err(BatchingError::SigningError);
    match signature {
        Ok(signature) => {
            let mut batch = Batch::new();
            batch.set_header_signature(signature.as_hex());
            batch.set_header(header_bytes);
            batch.set_transactions(protobuf::RepeatedField::from_vec(txns));

            Ok(batch)
        }
        Err(err) => Err(err),
    }
}

pub struct SignedBatchIterator<'a> {
    transaction_iterator: &'a mut dyn Iterator<Item = Transaction>,
    max_batch_size: usize,
    signer: &'a dyn Signer,
}

impl<'a> SignedBatchIterator<'a> {
    pub fn new(
        iterator: &'a mut dyn Iterator<Item = Transaction>,
        max_batch_size: usize,
        signer: &'a dyn Signer,
    ) -> Self {
        SignedBatchIterator {
            transaction_iterator: iterator,
            max_batch_size,
            signer,
        }
    }
}

impl<'a> Iterator for SignedBatchIterator<'a> {
    type Item = BatchResult;

    fn next(&mut self) -> Option<Self::Item> {
        let txns = self
            .transaction_iterator
            .take(self.max_batch_size)
            .collect();

        Some(batch_transactions(txns, self.signer))
    }
}

type BatchSource<'a> = LengthDelimitedMessageSource<'a, Batch>;

/// Produces batches from length-delimited source of Batches.
pub struct BatchListFeeder<'a> {
    batch_source: BatchSource<'a>,
}

/// Resulting BatchList or error.
pub type BatchListResult = Result<BatchPair, BatchReadingError>;

impl<'a> BatchListFeeder<'a> {
    /// Creates a new `BatchListFeeder` with a given Batch source
    pub fn new(source: &'a mut dyn Read) -> Self {
        let batch_source = LengthDelimitedMessageSource::new(source);
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
            Err(err) => return Some(Err(BatchReadingError::MessageError(err))),
        };

        let batch_proto = match batches.get(0) {
            Some(batch_proto) => batch_proto,
            None => return None,
        };

        match BatchPair::from_proto(batch_proto.clone()) {
            Ok(batch) => Some(Ok(batch)),
            Err(err) => Some(Err(BatchReadingError::ProtoConversionError(err))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Cursor, Write};

    use cylinder::{secp256k1::Secp256k1Context, Context, Signer};

    use crate::protos::transaction::TransactionHeader;

    type BatchSource<'a> = LengthDelimitedMessageSource<'a, Batch>;

    #[test]
    fn empty_transaction_source() {
        let encoded_bytes: Vec<u8> = Vec::new();
        let mut source = Cursor::new(encoded_bytes);

        let mut txn_stream: TransactionSource = LengthDelimitedMessageSource::new(&mut source);
        let txns = txn_stream.next(2).unwrap();
        assert_eq!(txns.len(), 0);
    }

    #[test]
    fn next_transactions() {
        let mut encoded_bytes: Vec<u8> = Vec::new();

        write_txn_with_sig("sig1", &mut encoded_bytes);
        write_txn_with_sig("sig2", &mut encoded_bytes);
        write_txn_with_sig("sig3", &mut encoded_bytes);

        let mut source = Cursor::new(encoded_bytes);

        let mut txn_stream: TransactionSource = LengthDelimitedMessageSource::new(&mut source);

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
        let mut encoded_bytes: Vec<u8> = Vec::new();
        write_txn_with_sig("sig1", &mut encoded_bytes);

        let mut source = Cursor::new(encoded_bytes);

        let signer = new_signer();

        let mut producer = SignedBatchProducer::new(&mut source, 2, &*signer);
        let mut batch_result = producer.next();
        assert!(batch_result.is_some());

        let batch = batch_result.unwrap().unwrap();

        let batch_header: BatchHeader = Message::parse_from_bytes(&batch.header).unwrap();
        assert_eq!(batch_header.transaction_ids.len(), 1);
        assert_eq!(batch_header.transaction_ids[0], String::from("sig1"));

        // test exhaustion
        batch_result = producer.next();
        assert!(batch_result.is_none());
    }

    #[test]
    fn signed_batches_multiple_batches() {
        let mut encoded_bytes: Vec<u8> = Vec::new();

        write_txn_with_sig("sig1", &mut encoded_bytes);
        write_txn_with_sig("sig2", &mut encoded_bytes);
        write_txn_with_sig("sig3", &mut encoded_bytes);

        let mut source = Cursor::new(encoded_bytes);

        let signer = new_signer();

        let mut producer = SignedBatchProducer::new(&mut source, 2, &*signer);
        let mut batch_result = producer.next();
        assert!(batch_result.is_some());

        let batch = batch_result.unwrap().unwrap();

        let signature = signer
            .sign(&batch.header)
            .expect("Unable to sign batch header");

        let batch_header: BatchHeader = Message::parse_from_bytes(&batch.header).unwrap();
        assert_eq!(batch_header.transaction_ids.len(), 2);
        assert_eq!(batch_header.transaction_ids[0], String::from("sig1"));
        assert_eq!(batch_header.transaction_ids[1], String::from("sig2"));
        assert_eq!(batch.header_signature, signature.as_hex());

        // pull the next batch
        batch_result = producer.next();
        assert!(batch_result.is_some());

        let batch = batch_result.unwrap().unwrap();

        let batch_header: BatchHeader = Message::parse_from_bytes(&batch.header).unwrap();
        assert_eq!(batch_header.transaction_ids.len(), 1);
        assert_eq!(batch_header.transaction_ids[0], String::from("sig3"));

        // test exhaustion
        batch_result = producer.next();
        assert!(batch_result.is_none());
    }

    #[test]
    fn generate_signed_batches() {
        let mut encoded_bytes: Vec<u8> = Vec::new();

        write_txn_with_sig("sig1", &mut encoded_bytes);
        write_txn_with_sig("sig2", &mut encoded_bytes);
        write_txn_with_sig("sig3", &mut encoded_bytes);

        let mut source = Cursor::new(encoded_bytes);
        let output_bytes: Vec<u8> = Vec::new();
        let mut output = Cursor::new(output_bytes);

        let signer = new_signer();

        super::generate_signed_batches(&mut source, &mut output, 2, &*signer)
            .expect("Should have generated batches!");

        // reset for reading
        output.set_position(0);
        let mut batch_source: BatchSource = LengthDelimitedMessageSource::new(&mut output);

        let batch = &(batch_source.next(1).unwrap())[0];
        let batch_header: BatchHeader = Message::parse_from_bytes(&batch.header).unwrap();
        assert_eq!(batch_header.transaction_ids.len(), 2);
        assert_eq!(batch_header.transaction_ids[0], String::from("sig1"));
        assert_eq!(batch_header.transaction_ids[1], String::from("sig2"));

        let batch = &(batch_source.next(1).unwrap())[0];
        let batch_header: BatchHeader = Message::parse_from_bytes(&batch.header).unwrap();
        assert_eq!(batch_header.transaction_ids.len(), 1);
        assert_eq!(batch_header.transaction_ids[0], String::from("sig3"));
    }

    fn make_txn(sig: &str) -> Transaction {
        let mut txn_header = TransactionHeader::new();

        txn_header.set_batcher_public_key(String::from("some_public_key"));
        txn_header.set_family_name(String::from("test_family"));
        txn_header.set_family_version(String::from("1.0"));
        txn_header.set_signer_public_key(String::from("some_public_key"));
        txn_header.set_payload_sha512(String::from("some_sha512_hash"));

        let mut txn = Transaction::new();
        txn.set_header(txn_header.write_to_bytes().unwrap());
        txn.set_header_signature(String::from(sig));
        txn.set_payload(sig.as_bytes().to_vec());

        txn
    }

    fn write_txn_with_sig(sig: &str, out: &mut dyn Write) {
        let txn = make_txn(sig);
        txn.write_length_delimited_to_writer(out)
            .expect("Unable to write delimiter");
    }

    fn new_signer() -> Box<dyn Signer> {
        let context = Secp256k1Context::new();
        let key = context.new_random_private_key();
        context.new_signer(key)
    }
}

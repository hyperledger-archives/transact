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

//! Batches of transactions.
//!
//! A Batch is a signed collection of transactions. These transactions must all succeed during
//! execution, in order for the batch to be considered valid.  Only valid batches produce state
//! changes.

use std::error::Error as StdError;
use std::fmt;

use cylinder::{Signer, SigningError};
use protobuf::Message;

use crate::protos::{
    self, FromBytes, FromNative, FromProto, IntoBytes, IntoNative, IntoProto, ProtoConversionError,
};

use super::transaction::Transaction;

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct BatchHeader {
    signer_public_key: Vec<u8>,
    transaction_ids: Vec<Vec<u8>>,
}

impl BatchHeader {
    pub fn signer_public_key(&self) -> &[u8] {
        &self.signer_public_key
    }

    pub fn transaction_ids(&self) -> &[Vec<u8>] {
        &self.transaction_ids
    }
}

impl fmt::Debug for BatchHeader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("BatchHeader{ ")?;
        write!(
            f,
            "signer_public_key: {:?}, ",
            hex::encode(&self.signer_public_key)
        )?;
        f.write_str("transaction_ids: [")?;
        f.write_str(
            &self
                .transaction_ids
                .iter()
                .map(|id| format!("{:?}", hex::encode(id)))
                .collect::<Vec<_>>()
                .join(", "),
        )?;
        f.write_str("]")?;
        f.write_str(" }")
    }
}

impl FromProto<protos::batch::BatchHeader> for BatchHeader {
    fn from_proto(header: protos::batch::BatchHeader) -> Result<Self, ProtoConversionError> {
        Ok(BatchHeader {
            signer_public_key: hex::decode(header.get_signer_public_key())?,
            transaction_ids: header
                .get_transaction_ids()
                .to_vec()
                .into_iter()
                .map(|t| hex::decode(t).map_err(ProtoConversionError::from))
                .collect::<Result<_, _>>()?,
        })
    }
}

impl FromNative<BatchHeader> for protos::batch::BatchHeader {
    fn from_native(header: BatchHeader) -> Result<Self, ProtoConversionError> {
        let mut proto_header = protos::batch::BatchHeader::new();
        proto_header.set_signer_public_key(hex::encode(header.signer_public_key));
        proto_header.set_transaction_ids(
            header
                .transaction_ids
                .iter()
                .map(hex::encode)
                .collect::<protobuf::RepeatedField<String>>(),
        );
        Ok(proto_header)
    }
}

impl FromBytes<BatchHeader> for BatchHeader {
    fn from_bytes(bytes: &[u8]) -> Result<BatchHeader, ProtoConversionError> {
        let proto: protos::batch::BatchHeader =
            protobuf::parse_from_bytes(bytes).map_err(|err| {
                ProtoConversionError::SerializationError(format!(
                    "unable to get BatchHeader from bytes: {}",
                    err
                ))
            })?;
        proto.into_native()
    }
}

impl IntoBytes for BatchHeader {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|err| {
            ProtoConversionError::SerializationError(format!(
                "unable to get bytes from BatchHeader: {}",
                err
            ))
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::batch::BatchHeader> for BatchHeader {}
impl IntoNative<BatchHeader> for protos::batch::BatchHeader {}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct Batch {
    header: Vec<u8>,
    header_signature: String,
    transactions: Vec<Transaction>,
    trace: bool,
}

impl Batch {
    pub fn header(&self) -> &[u8] {
        &self.header
    }

    pub fn header_signature(&self) -> &str {
        &self.header_signature
    }

    pub fn transactions(&self) -> &[Transaction] {
        &self.transactions
    }

    pub fn trace(&self) -> bool {
        self.trace
    }

    pub fn into_pair(self) -> Result<BatchPair, BatchBuildError> {
        let header = BatchHeader::from_bytes(&self.header)?;

        Ok(BatchPair {
            batch: self,
            header,
        })
    }
}

impl fmt::Debug for Batch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("Batch{ ")?;

        write!(f, "header_signature: {:?}, ", self.header_signature)?;
        let header_len = self.header.len();
        write!(
            f,
            "header: <{} byte{}>,  ",
            header_len,
            if header_len == 1 { "" } else { "s" }
        )?;
        write!(f, "transactions: {:?}, ", &self.transactions)?;
        write!(f, "trace: {}", self.trace)?;

        f.write_str(" }")
    }
}

impl FromProto<protos::batch::Batch> for Batch {
    fn from_proto(batch: protos::batch::Batch) -> Result<Self, ProtoConversionError> {
        Ok(Batch {
            header: batch.header,
            header_signature: batch.header_signature,
            transactions: batch
                .transactions
                .into_iter()
                .map(|txn| txn.into_native())
                .collect::<Result<Vec<_>, _>>()?,
            trace: batch.trace,
        })
    }
}

impl FromNative<Batch> for protos::batch::Batch {
    fn from_native(batch: Batch) -> Result<Self, ProtoConversionError> {
        let mut proto_batch = protos::batch::Batch::new();
        proto_batch.set_header(batch.header);
        proto_batch.set_header_signature(batch.header_signature);
        proto_batch.set_transactions(
            batch
                .transactions
                .into_iter()
                .map(|txn| txn.into_proto())
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        );
        proto_batch.set_trace(batch.trace);
        Ok(proto_batch)
    }
}

impl FromBytes<Batch> for Batch {
    fn from_bytes(bytes: &[u8]) -> Result<Batch, ProtoConversionError> {
        let proto: protos::batch::Batch = protobuf::parse_from_bytes(bytes).map_err(|err| {
            ProtoConversionError::SerializationError(format!(
                "unable to get Batch from bytes: {}",
                err
            ))
        })?;
        proto.into_native()
    }
}

impl IntoBytes for Batch {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|err| {
            ProtoConversionError::SerializationError(format!(
                "unable to get bytes from Batch: {}",
                err
            ))
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::batch::Batch> for Batch {}
impl IntoNative<Batch> for protos::batch::Batch {}

impl FromProto<protos::batch::BatchList> for Vec<Batch> {
    fn from_proto(batch_list: protos::batch::BatchList) -> Result<Self, ProtoConversionError> {
        batch_list
            .batches
            .into_iter()
            .map(|batch| batch.into_native())
            .collect()
    }
}

impl FromNative<Vec<Batch>> for protos::batch::BatchList {
    fn from_native(batches: Vec<Batch>) -> Result<Self, ProtoConversionError> {
        let mut proto_batch_list = protos::batch::BatchList::new();
        let proto_batches = batches
            .into_iter()
            .map(Batch::into_proto)
            .collect::<Result<_, _>>()?;
        proto_batch_list.set_batches(proto_batches);
        Ok(proto_batch_list)
    }
}

impl FromBytes<Vec<Batch>> for Vec<Batch> {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        let proto: protos::batch::BatchList = protobuf::parse_from_bytes(bytes).map_err(|err| {
            ProtoConversionError::SerializationError(format!(
                "unable to get BatchList from bytes: {}",
                err
            ))
        })?;
        proto.into_native()
    }
}

impl IntoBytes for Vec<Batch> {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|err| {
            ProtoConversionError::SerializationError(format!(
                "unable to get bytes from BatchList: {}",
                err
            ))
        })?;
        Ok(bytes)
    }
}

impl IntoProto<protos::batch::BatchList> for Vec<Batch> {}
impl IntoNative<Vec<Batch>> for protos::batch::BatchList {}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BatchPair {
    batch: Batch,
    header: BatchHeader,
}

impl BatchPair {
    pub fn batch(&self) -> &Batch {
        &self.batch
    }

    pub fn header(&self) -> &BatchHeader {
        &self.header
    }

    pub fn take(self) -> (Batch, BatchHeader) {
        (self.batch, self.header)
    }
}

impl FromProto<protos::batch::Batch> for BatchPair {
    fn from_proto(batch: protos::batch::Batch) -> Result<Self, ProtoConversionError> {
        Batch::from_proto(batch)?
            .into_pair()
            .map_err(|err| ProtoConversionError::DeserializationError(err.to_string()))
    }
}

impl FromNative<BatchPair> for protos::batch::Batch {
    fn from_native(batch_pair: BatchPair) -> Result<Self, ProtoConversionError> {
        batch_pair.take().0.into_proto()
    }
}

impl FromBytes<BatchPair> for BatchPair {
    fn from_bytes(bytes: &[u8]) -> Result<BatchPair, ProtoConversionError> {
        Batch::from_bytes(bytes)?
            .into_pair()
            .map_err(|err| ProtoConversionError::DeserializationError(err.to_string()))
    }
}

impl IntoBytes for BatchPair {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.take().0.into_bytes()
    }
}

impl IntoProto<protos::batch::Batch> for BatchPair {}
impl IntoNative<BatchPair> for protos::batch::Batch {}

impl FromBytes<Vec<BatchPair>> for Vec<BatchPair> {
    fn from_bytes(bytes: &[u8]) -> Result<Vec<BatchPair>, ProtoConversionError> {
        let batches: Vec<Batch> = Vec::from_bytes(bytes)?;
        batches
            .into_iter()
            .map(|batch| {
                batch.into_pair().map_err(|err| {
                    ProtoConversionError::DeserializationError(format!(
                        "failed to get BatchPair from Batch: {}",
                        err
                    ))
                })
            })
            .collect()
    }
}

#[derive(Debug)]
pub enum BatchBuildError {
    MissingField(String),
    SerializationError(String),
    DeserializationError(String),
    SigningError(String),
}

impl StdError for BatchBuildError {
    fn description(&self) -> &str {
        match *self {
            BatchBuildError::MissingField(ref msg) => msg,
            BatchBuildError::SerializationError(ref msg) => msg,
            BatchBuildError::DeserializationError(ref msg) => msg,
            BatchBuildError::SigningError(ref msg) => msg,
        }
    }
}

impl std::fmt::Display for BatchBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            BatchBuildError::MissingField(ref s) => write!(f, "MissingField: {}", s),
            BatchBuildError::SerializationError(ref s) => write!(f, "SerializationError: {}", s),
            BatchBuildError::DeserializationError(ref s) => {
                write!(f, "DeserializationError: {}", s)
            }
            BatchBuildError::SigningError(ref s) => write!(f, "SigningError: {}", s),
        }
    }
}

impl From<ProtoConversionError> for BatchBuildError {
    fn from(e: ProtoConversionError) -> Self {
        BatchBuildError::DeserializationError(format!("{}", e))
    }
}

impl From<SigningError> for BatchBuildError {
    fn from(err: SigningError) -> Self {
        Self::SigningError(err.to_string())
    }
}

#[derive(Default, Clone)]
pub struct BatchBuilder {
    transactions: Option<Vec<Transaction>>,
    trace: Option<bool>,
}

impl BatchBuilder {
    pub fn new() -> Self {
        BatchBuilder::default()
    }

    pub fn with_transactions(mut self, transactions: Vec<Transaction>) -> BatchBuilder {
        self.transactions = Some(transactions);
        self
    }

    pub fn with_trace(mut self, trace: bool) -> BatchBuilder {
        self.trace = Some(trace);
        self
    }

    pub fn build_pair(self, signer: &dyn Signer) -> Result<BatchPair, BatchBuildError> {
        let transactions = self.transactions.ok_or_else(|| {
            BatchBuildError::MissingField("'transactions' field is required".to_string())
        })?;
        let trace = self.trace.unwrap_or(false);
        let transaction_ids = transactions
            .iter()
            .flat_map(|t| {
                vec![hex::decode(t.header_signature())
                    .map_err(|e| BatchBuildError::SerializationError(format!("{}", e)))]
            })
            .collect::<Result<_, _>>()?;

        let signer_public_key = signer.public_key()?.as_slice().to_vec();

        let header = BatchHeader {
            signer_public_key,
            transaction_ids,
        };

        let header_proto: protos::batch::BatchHeader = header
            .clone()
            .into_proto()
            .map_err(|e| BatchBuildError::SerializationError(format!("{}", e)))?;
        let header_bytes = header_proto
            .write_to_bytes()
            .map_err(|e| BatchBuildError::SerializationError(format!("{}", e)))?;

        let header_signature = signer
            .sign(&header_bytes)
            .map_err(|e| BatchBuildError::SigningError(format!("{}", e)))?
            .as_hex();

        let batch = Batch {
            header: header_bytes,
            header_signature,
            transactions,
            trace,
        };

        Ok(BatchPair { batch, header })
    }

    pub fn build(self, signer: &dyn Signer) -> Result<Batch, BatchBuildError> {
        Ok(self.build_pair(signer)?.batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use cylinder::{secp256k1::Secp256k1Context, Context, Signer};
    #[cfg(feature = "sawtooth-compat")]
    use protobuf::Message;
    #[cfg(feature = "sawtooth-compat")]
    use sawtooth_sdk;

    static KEY1: &str = "111111111111111111111111111111111111111111111111111111111111111111";
    static KEY2: &str = "222222222222222222222222222222222222222222222222222222222222222222";
    static KEY3: &str = "333333333333333333333333333333333333333333333333333333333333333333";
    static BYTES1: [u8; 4] = [0x01, 0x02, 0x03, 0x04];
    static BYTES2: [u8; 4] = [0x05, 0x06, 0x07, 0x08];
    static BYTES3: [u8; 4] = [0x09, 0x0a, 0x0b, 0x0c];
    static BYTES4: [u8; 4] = [0x0d, 0x0e, 0x0f, 0x10];
    static BYTES5: [u8; 4] = [0x11, 0x12, 0x13, 0x14];
    static SIGNATURE1: &str =
        "sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1";
    static SIGNATURE2: &str =
        "sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2";
    static SIGNATURE3: &str =
        "sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3";

    fn check_builder_batch(signer: &dyn Signer, pair: &BatchPair) {
        let signer_pub_key = signer
            .public_key()
            .expect("Failed to get signer public key");
        assert_eq!(
            vec![
                SIGNATURE2.as_bytes().to_vec(),
                SIGNATURE3.as_bytes().to_vec()
            ],
            pair.header().transaction_ids()
        );
        assert_eq!(signer_pub_key.as_slice(), pair.header().signer_public_key());
        assert_eq!(
            vec![
                Transaction::new(
                    BYTES2.to_vec(),
                    hex::encode(SIGNATURE2.to_string()),
                    BYTES3.to_vec()
                ),
                Transaction::new(
                    BYTES4.to_vec(),
                    hex::encode(SIGNATURE3.to_string()),
                    BYTES5.to_vec()
                ),
            ],
            pair.batch().transactions()
        );
        assert_eq!(true, pair.batch().trace());
    }

    #[test]
    fn batch_builder_chain() {
        let signer = new_signer();

        let pair = BatchBuilder::new()
            .with_transactions(vec![
                Transaction::new(
                    BYTES2.to_vec(),
                    hex::encode(SIGNATURE2.to_string()),
                    BYTES3.to_vec(),
                ),
                Transaction::new(
                    BYTES4.to_vec(),
                    hex::encode(SIGNATURE3.to_string()),
                    BYTES5.to_vec(),
                ),
            ])
            .with_trace(true)
            .build_pair(&*signer)
            .unwrap();

        check_builder_batch(&*signer, &pair);
    }

    #[test]
    fn batch_builder_separate() {
        let signer = new_signer();

        let mut builder = BatchBuilder::new();
        builder = builder.with_transactions(vec![
            Transaction::new(
                BYTES2.to_vec(),
                hex::encode(SIGNATURE2.to_string()),
                BYTES3.to_vec(),
            ),
            Transaction::new(
                BYTES4.to_vec(),
                hex::encode(SIGNATURE3.to_string()),
                BYTES5.to_vec(),
            ),
        ]);
        builder = builder.with_trace(true);
        let pair = builder.build_pair(&*signer).unwrap();

        check_builder_batch(&*signer, &pair);
    }

    #[test]
    fn batch_header_fields() {
        let header = BatchHeader {
            signer_public_key: hex::decode(KEY1).unwrap(),
            transaction_ids: vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap()],
        };

        assert_eq!(KEY1, hex::encode(header.signer_public_key()));
        assert_eq!(
            vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap(),],
            header.transaction_ids()
        );
    }

    #[test]
    // test that the batch header can be converted into bytes and back correctly
    fn batch_header_bytes() {
        let original = BatchHeader {
            signer_public_key: hex::decode(KEY1).unwrap(),
            transaction_ids: vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap()],
        };

        let header_bytes = original.clone().into_bytes().unwrap();
        let header = BatchHeader::from_bytes(&header_bytes).unwrap();

        assert_eq!(
            hex::encode(original.signer_public_key()),
            hex::encode(header.signer_public_key())
        );
        assert_eq!(original.transaction_ids(), header.transaction_ids());
    }

    #[cfg(feature = "sawtooth-compat")]
    #[test]
    fn batch_header_sawtooth10_compatibility() {
        // Create protobuf bytes using the Sawtooth SDK
        let mut proto = sawtooth_sdk::messages::batch::BatchHeader::new();
        proto.set_signer_public_key(KEY1.to_string());
        proto.set_transaction_ids(protobuf::RepeatedField::from_vec(vec![
            KEY2.to_string(),
            KEY3.to_string(),
        ]));
        let header_bytes = proto.write_to_bytes().unwrap();

        // Deserialize the header bytes into our protobuf
        let header_proto: protos::batch::BatchHeader =
            protobuf::parse_from_bytes(&header_bytes).unwrap();

        // Convert to a BatchHeader
        let header: BatchHeader = header_proto.into_native().unwrap();

        assert_eq!(KEY1, hex::encode(header.signer_public_key()));
        assert_eq!(
            vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap(),],
            header.transaction_ids(),
        );
    }

    #[test]
    fn batch_fields() {
        let batch = Batch {
            header: BYTES1.to_vec(),
            header_signature: SIGNATURE1.to_string(),
            transactions: vec![
                Transaction::new(BYTES2.to_vec(), SIGNATURE2.to_string(), BYTES3.to_vec()),
                Transaction::new(BYTES4.to_vec(), SIGNATURE3.to_string(), BYTES5.to_vec()),
            ],
            trace: true,
        };

        assert_eq!(BYTES1.to_vec(), batch.header());
        assert_eq!(SIGNATURE1, batch.header_signature());
        assert_eq!(
            vec![
                Transaction::new(BYTES2.to_vec(), SIGNATURE2.to_string(), BYTES3.to_vec()),
                Transaction::new(BYTES4.to_vec(), SIGNATURE3.to_string(), BYTES5.to_vec()),
            ],
            batch.transactions()
        );
        assert_eq!(true, batch.trace());
    }

    #[cfg(feature = "sawtooth-compat")]
    #[test]
    fn batch_sawtooth10_compatibility() {}

    fn new_signer() -> Box<dyn Signer> {
        let context = Secp256k1Context::new();
        let key = context.new_random_private_key();
        context.new_signer(key)
    }
}

#[cfg(all(feature = "nightly", test))]
mod benchmarks {
    extern crate test;
    use super::*;
    use test::Bencher;

    static KEY1: &str = "111111111111111111111111111111111111111111111111111111111111111111";
    static KEY2: &str = "222222222222222222222222222222222222222222222222222222222222222222";
    static KEY3: &str = "333333333333333333333333333333333333333333333333333333333333333333";
    static BYTES1: [u8; 4] = [0x01, 0x02, 0x03, 0x04];
    static BYTES2: [u8; 4] = [0x05, 0x06, 0x07, 0x08];
    static BYTES3: [u8; 4] = [0x09, 0x0a, 0x0b, 0x0c];
    static BYTES4: [u8; 4] = [0x0d, 0x0e, 0x0f, 0x10];
    static BYTES5: [u8; 4] = [0x11, 0x12, 0x13, 0x14];
    static SIGNATURE1: &str =
        "sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1";
    static SIGNATURE2: &str =
        "sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2";
    static SIGNATURE3: &str =
        "sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3";

    #[bench]
    fn bench_batch_creation(b: &mut Bencher) {
        b.iter(|| Batch {
            header: BYTES1.to_vec(),
            header_signature: SIGNATURE1.to_string(),
            transactions: vec![
                Transaction::new(BYTES2.to_vec(), SIGNATURE2.to_string(), BYTES3.to_vec()),
                Transaction::new(BYTES4.to_vec(), SIGNATURE3.to_string(), BYTES5.to_vec()),
            ],
            trace: true,
        });
    }

    #[bench]
    fn bench_batch_builder(b: &mut Bencher) {
        let signer = new_signer();
        let batch = BatchBuilder::new()
            .with_transactions(vec![
                Transaction::new(
                    BYTES2.to_vec(),
                    hex::encode(SIGNATURE2.to_string()),
                    BYTES3.to_vec(),
                ),
                Transaction::new(
                    BYTES4.to_vec(),
                    hex::encode(SIGNATURE3.to_string()),
                    BYTES5.to_vec(),
                ),
            ])
            .with_trace(true);
        b.iter(|| batch.clone().build_pair(&*signer));
    }

    #[bench]
    fn bench_batch_header_into_native(b: &mut Bencher) {
        let mut proto_header = protos::batch::BatchHeader::new();
        proto_header.set_signer_public_key(KEY1.to_string());
        proto_header.set_transaction_ids(protobuf::RepeatedField::from_vec(vec![
            KEY2.to_string(),
            KEY3.to_string(),
        ]));
        b.iter(|| proto_header.clone().into_native());
    }

    #[bench]
    fn bench_batch_header_into_proto(b: &mut Bencher) {
        let native_header = BatchHeader {
            signer_public_key: hex::decode(KEY1).unwrap(),
            transaction_ids: vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap()],
        };
        b.iter(|| native_header.clone().into_proto());
    }
}

// Copyright 2018 Cargill Incorporated
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

//! Contains functions which assist with the creation of Batches and
//! Transactions

use std::time::Instant;

use crypto::digest::Digest;
use crypto::sha2::Sha512;
use protobuf;
use protobuf::Message;
use sawtooth_sdk::messages::batch::Batch;
use sawtooth_sdk::messages::batch::BatchHeader;
use sawtooth_sdk::messages::batch::BatchList;
use sawtooth_sdk::messages::transaction::Transaction;
use sawtooth_sdk::messages::transaction::TransactionHeader;
use sawtooth_sdk::signing::Signer;

use crate::error::CliError;

/// The Intkey Multiply transaction family name (intkey_multiply)
const INTKEY_MULITPLY_FAMILY_NAME: &str = "intkey_multiply";

/// The Intkey Multiply transaction family version (1.0)
const INTKEY_MULITPLY_VERSION: &str = "1.0";

/// The namespace registry prefix for intkey (1cf126)
const INTKEY_PREFIX: &str = "1cf126";

/// Creates a nonce appropriate for a TransactionHeader
fn create_nonce() -> String {
    let elapsed = Instant::now().elapsed();
    format!("{}{}", elapsed.as_secs(), elapsed.subsec_nanos())
}

/// Returns a hex string representation of the supplied bytes
///
/// # Arguments
///
/// * `b` - input bytes
fn bytes_to_hex_str(b: &[u8]) -> String {
    b.iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join("")
}

/// Returns a state address for a given namespace registry
///
/// # Arguments
///
/// * `namespace` - the address prefix for this namespace
fn compute_intkey_address(name: &str) -> String {
    let mut sha = Sha512::new();
    sha.input(name.as_bytes());
    String::from(INTKEY_PREFIX) + &sha.result_str()[64..].to_string()
}

/// Returns a Transaction for the given Payload and Signer
///
/// # Arguments
///
/// * `payload` - a fully populated intkey multiply payload
/// * `signer` - the signer to be used to sign the transaction
/// * `public_key` - the public key associated with the signer
///
/// # Errors
///
/// If an error occurs during serialization of the provided payload or
/// internally created `TransactionHeader`, a `CliError::ProtobufError` is
/// returned.
///
/// If a signing error occurs, a `CliError::SigningError` is returned.
pub fn create_transaction(
    name_a: &str,
    name_b: &str,
    name_c: &str,
    payload: &[u8],
    signer: &Signer,
    public_key: &str,
) -> Result<Transaction, CliError> {
    let mut txn = Transaction::new();
    let mut txn_header = TransactionHeader::new();

    txn_header.set_family_name(String::from(INTKEY_MULITPLY_FAMILY_NAME));
    txn_header.set_family_version(String::from(INTKEY_MULITPLY_VERSION));
    txn_header.set_nonce(create_nonce());
    txn_header.set_signer_public_key(public_key.to_string());
    txn_header.set_batcher_public_key(public_key.to_string());

    let addresses = vec![
        compute_intkey_address(name_a),
        compute_intkey_address(name_b),
        compute_intkey_address(name_c),
    ];

    let input_addresses = addresses.clone();
    let output_addresses = addresses;

    txn_header.set_inputs(protobuf::RepeatedField::from_vec(input_addresses));
    txn_header.set_outputs(protobuf::RepeatedField::from_vec(output_addresses));

    let mut sha = Sha512::new();
    sha.input(&payload);
    let hash: &mut [u8] = &mut [0; 64];
    sha.result(hash);
    txn_header.set_payload_sha512(bytes_to_hex_str(hash));
    txn.set_payload(payload.to_vec());

    let txn_header_bytes = txn_header.write_to_bytes()?;
    txn.set_header(txn_header_bytes.clone());

    let b: &[u8] = &txn_header_bytes;
    txn.set_header_signature(signer.sign(b)?);

    Ok(txn)
}

/// Returns a Batch for the given Transaction and Signer
///
/// # Arguments
///
/// * `txn` - a Transaction
/// * `signer` - the signer to be used to sign the transaction
/// * `public_key` - the public key associated with the signer
///
/// # Errors
///
/// If an error occurs during serialization of the provided Transaction or
/// internally created `BatchHeader`, a `CliError::ProtobufError` is
/// returned.
///
/// If a signing error occurs, a `CliError::SigningError` is returned.
pub fn create_batch(
    txn: Transaction,
    signer: &Signer,
    public_key: &str,
) -> Result<Batch, CliError> {
    let mut batch = Batch::new();
    let mut batch_header = BatchHeader::new();

    batch_header.set_transaction_ids(protobuf::RepeatedField::from_vec(vec![txn
        .header_signature
        .clone()]));
    batch_header.set_signer_public_key(public_key.to_string());
    batch.set_transactions(protobuf::RepeatedField::from_vec(vec![txn]));

    let batch_header_bytes = batch_header.write_to_bytes()?;
    batch.set_header(batch_header_bytes.clone());

    let b: &[u8] = &batch_header_bytes;
    batch.set_header_signature(signer.sign(b)?);

    Ok(batch)
}

/// Returns a BatchList containing the provided Batch
///
/// # Arguments
///
/// * `batch` - a Batch
pub fn create_batch_list_from_one(batch: Batch) -> BatchList {
    let mut batch_list = BatchList::new();
    batch_list.set_batches(protobuf::RepeatedField::from_vec(vec![batch]));
    batch_list
}

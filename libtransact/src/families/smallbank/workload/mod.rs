/*
 * Copyright 2018 Intel Corporation
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

pub mod error;
pub mod playlist;

use std::cmp::Eq;
use std::collections::HashMap;
use std::hash::Hash;

use cylinder::Signer;
use protobuf::Message;

use crate::error::InvalidStateError;
use crate::protocol::{
    batch::{BatchBuilder, BatchPair},
    sabre::payload::ExecuteContractActionBuilder,
    transaction::TransactionPair,
};
use crate::protos::smallbank::{
    SmallbankTransactionPayload, SmallbankTransactionPayload_PayloadType,
};
use crate::workload::{BatchWorkload, ExpectedBatchResult, TransactionWorkload};

use self::playlist::make_addresses;
use self::playlist::SmallbankGeneratingIter;

pub struct SmallbankTransactionWorkload {
    generator: SmallbankGeneratingIter,
    signer: Box<dyn Signer>,
    dependencies: SignatureTracker<u32>,
}

impl SmallbankTransactionWorkload {
    pub fn new(generator: SmallbankGeneratingIter, signer: Box<dyn Signer>) -> Self {
        Self {
            generator,
            signer,
            dependencies: SignatureTracker::new(),
        }
    }

    fn add_signature_if_create_account(
        &mut self,
        payload: &SmallbankTransactionPayload,
        signature: String,
    ) {
        if payload.get_payload_type() == SmallbankTransactionPayload_PayloadType::CREATE_ACCOUNT {
            self.dependencies
                .add_signature(payload.get_create_account().get_customer_id(), signature);
        }
    }

    fn get_dependencies_for_customer_ids(&self, customer_ids: &[u32]) -> Vec<String> {
        customer_ids
            .iter()
            .filter_map(|id| self.dependencies.get_signature(id))
            .map(|sig| sig.to_owned())
            .collect()
    }

    fn get_dependencies(&self, payload: &SmallbankTransactionPayload) -> Vec<String> {
        match payload.get_payload_type() {
            SmallbankTransactionPayload_PayloadType::DEPOSIT_CHECKING => self
                .get_dependencies_for_customer_ids(&[payload
                    .get_deposit_checking()
                    .get_customer_id()]),
            SmallbankTransactionPayload_PayloadType::WRITE_CHECK => self
                .get_dependencies_for_customer_ids(&[payload.get_write_check().get_customer_id()]),
            SmallbankTransactionPayload_PayloadType::TRANSACT_SAVINGS => self
                .get_dependencies_for_customer_ids(&[payload
                    .get_transact_savings()
                    .get_customer_id()]),
            SmallbankTransactionPayload_PayloadType::SEND_PAYMENT => self
                .get_dependencies_for_customer_ids(&[
                    payload.get_send_payment().get_source_customer_id(),
                    payload.get_send_payment().get_dest_customer_id(),
                ]),
            SmallbankTransactionPayload_PayloadType::AMALGAMATE => self
                .get_dependencies_for_customer_ids(&[
                    payload.get_amalgamate().get_source_customer_id(),
                    payload.get_amalgamate().get_dest_customer_id(),
                ]),
            _ => vec![],
        }
    }
}

impl TransactionWorkload for SmallbankTransactionWorkload {
    fn next_transaction(
        &mut self,
    ) -> Result<(TransactionPair, Option<ExpectedBatchResult>), InvalidStateError> {
        let payload = self
            .generator
            .next()
            .ok_or_else(|| InvalidStateError::with_message("No payload available".to_string()))?;
        let addresses = make_addresses(&payload);
        let dependencies = self.get_dependencies(&payload);

        let payload_bytes = payload.write_to_bytes().map_err(|_| {
            InvalidStateError::with_message("Unable to convert payload to bytes".to_string())
        })?;

        let txn_pair = ExecuteContractActionBuilder::new()
            .with_name(String::from("smallbank"))
            .with_version(String::from("1.0"))
            .with_inputs(addresses.clone())
            .with_outputs(addresses)
            .with_payload(payload_bytes)
            .into_payload_builder()
            .map_err(|err| {
                InvalidStateError::with_message(format!(
                    "Unable to convert execute action into sabre payload: {}",
                    err
                ))
            })?
            .into_transaction_builder()
            .map_err(|err| {
                InvalidStateError::with_message(format!(
                    "Unable to convert execute payload into transaction: {}",
                    err
                ))
            })?
            .with_dependencies(dependencies)
            .build_pair(&*self.signer)
            .map_err(|err| {
                InvalidStateError::with_message(format!(
                    "Failed to build transaction pair: {}",
                    err
                ))
            })?;

        self.add_signature_if_create_account(
            &payload,
            txn_pair.transaction().header_signature().to_owned(),
        );

        Ok((txn_pair, None))
    }
}

pub struct SmallbankBatchWorkload {
    transaction_workload: SmallbankTransactionWorkload,
    signer: Box<dyn Signer>,
}

impl SmallbankBatchWorkload {
    pub fn new(
        transaction_workload: SmallbankTransactionWorkload,
        signer: Box<dyn Signer>,
    ) -> Self {
        Self {
            transaction_workload,
            signer,
        }
    }
}

impl BatchWorkload for SmallbankBatchWorkload {
    fn next_batch(
        &mut self,
    ) -> Result<(BatchPair, Option<ExpectedBatchResult>), InvalidStateError> {
        let (txn, result) = self.transaction_workload.next_transaction()?;
        Ok((
            BatchBuilder::new()
                .with_transactions(vec![txn.take().0])
                .build_pair(&*self.signer)
                .map_err(|err| {
                    InvalidStateError::with_message(format!("Failed to build batch pair: {}", err))
                })?,
            result,
        ))
    }
}

struct SignatureTracker<T>
where
    T: Eq + Hash,
{
    signature_by_id: HashMap<T, String>,
}

impl<T> SignatureTracker<T>
where
    T: Eq + Hash,
{
    pub fn new() -> SignatureTracker<T> {
        SignatureTracker {
            signature_by_id: HashMap::new(),
        }
    }

    pub fn get_signature(&self, id: &T) -> Option<&String> {
        self.signature_by_id.get(id)
    }

    pub fn add_signature(&mut self, id: T, signature: String) {
        self.signature_by_id.insert(id, signature);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use cylinder::{secp256k1::Secp256k1Context, Context, Signer};

    const NUM_CREATE_ACCOUNTS: usize = 100;
    const NUM_TO_CONSIDER: usize = 1_000;

    /// Verify that the SmallbankTransactionWorkload properly adds dependencies
    ///
    /// 1. Create a SmallbankGeneratingIter that will create 100 num_accounts
    /// 2. Create the transaction workload
    /// 3. Call next_transaction 100 times to verify that no transaction include any dependencies
    ///    These transactions should be creating new accounts. Collect the txn signatures.
    /// 4. Call next_transaction 1000 more times verifies that all these transactions should have
    ///    dependencies and all of those dependencies should have been captured in the first
    ///    iteration.
    #[test]
    fn test_dependencies() {
        let seed = 8411989621121823827u64;

        let payload_generator = SmallbankGeneratingIter::new(NUM_CREATE_ACCOUNTS, seed);

        let signer = new_signer();

        let mut transaction_workload =
            SmallbankTransactionWorkload::new(payload_generator, signer.clone());

        // store generate transaction signature that contain create account txn
        let mut create_account_txn_ids = Vec::new();

        let mut acc = 0;
        for _ in 0..100 {
            let (txn_pair, _) = transaction_workload
                .next_transaction()
                .expect("Unable to get txn pair");
            let (txn, header) = txn_pair.take();
            create_account_txn_ids.push(txn.header_signature().to_string());

            // If there was a dep, increment count
            if header.dependencies().len() > 0 {
                acc = acc + 1;
            }
        }

        // no transaction should have had any dep
        assert_eq!(acc, 0);

        for _ in 0..NUM_TO_CONSIDER {
            let (txn_pair, _) = transaction_workload
                .next_transaction()
                .expect("Unable to get txn pair");
            let header = txn_pair.header();
            // verify txn has dependencies
            assert!(header.dependencies().len() > 0);
            // if a txn has a dep not from the first 100 txn, increment count
            if header
                .dependencies()
                .iter()
                .any(|dep| !create_account_txn_ids.contains(&dep))
            {
                acc = acc + 1;
            }
        }

        // no transaction should have a dep that was not generate in the first 100 txn
        assert_eq!(acc, 0);
    }

    fn new_signer() -> Box<dyn Signer> {
        let context = Secp256k1Context::new();
        let key = context.new_random_private_key();
        context.new_signer(key)
    }
}

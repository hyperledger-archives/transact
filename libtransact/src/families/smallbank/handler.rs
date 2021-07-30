/*
 * Copyright 2021 Cargill Incorporated
 * Copyright 2018 Intel Corporation
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

use protobuf::Message;
use sha2::{Digest, Sha512};

use crate::handler::{ApplyError, TransactionContext, TransactionHandler};
use crate::protocol::transaction::TransactionPair;

use crate::protos::smallbank::{
    Account, SmallbankTransactionPayload, SmallbankTransactionPayload_AmalgamateTransactionData,
    SmallbankTransactionPayload_CreateAccountTransactionData,
    SmallbankTransactionPayload_DepositCheckingTransactionData,
    SmallbankTransactionPayload_PayloadType,
    SmallbankTransactionPayload_SendPaymentTransactionData,
    SmallbankTransactionPayload_TransactSavingsTransactionData,
    SmallbankTransactionPayload_WriteCheckTransactionData,
};

pub struct SmallbankTransactionHandler {
    family_name: String,
    family_versions: Vec<String>,
    namespaces: Vec<String>,
}

impl SmallbankTransactionHandler {
    #[allow(clippy::new_without_default)]
    pub fn new() -> SmallbankTransactionHandler {
        SmallbankTransactionHandler {
            family_name: "smallbank".to_string(),
            family_versions: vec!["1.0".to_string()],
            namespaces: vec![get_smallbank_prefix()],
        }
    }

    pub fn namespaces(&self) -> Vec<String> {
        self.namespaces.clone()
    }
}

impl TransactionHandler for SmallbankTransactionHandler {
    fn family_name(&self) -> &str {
        &self.family_name
    }

    fn family_versions(&self) -> &[String] {
        &self.family_versions
    }

    fn apply(
        &self,
        transaction_pair: &TransactionPair,
        context: &mut dyn TransactionContext,
    ) -> Result<(), ApplyError> {
        let mut payload = unpack_payload(transaction_pair.transaction().payload())?;
        debug!(
            "Smallbank txn {}: type {:?}",
            transaction_pair.transaction().header_signature(),
            payload.get_payload_type()
        );

        match payload.get_payload_type() {
            SmallbankTransactionPayload_PayloadType::CREATE_ACCOUNT => {
                apply_create_account(payload.take_create_account(), context)
            }
            SmallbankTransactionPayload_PayloadType::DEPOSIT_CHECKING => {
                apply_deposit_checking(&payload.take_deposit_checking(), context)
            }
            SmallbankTransactionPayload_PayloadType::WRITE_CHECK => {
                apply_write_check(&payload.take_write_check(), context)
            }
            SmallbankTransactionPayload_PayloadType::TRANSACT_SAVINGS => {
                apply_transact_savings(&payload.take_transact_savings(), context)
            }
            SmallbankTransactionPayload_PayloadType::SEND_PAYMENT => {
                apply_send_payment(&payload.take_send_payment(), context)
            }
            SmallbankTransactionPayload_PayloadType::AMALGAMATE => {
                apply_amalgamate(&payload.take_amalgamate(), context)
            }
            SmallbankTransactionPayload_PayloadType::PAYLOAD_TYPE_UNSET => Err(
                ApplyError::InvalidTransaction("Transaction type unset".into()),
            ),
        }
    }
}

fn unpack_payload(payload: &[u8]) -> Result<SmallbankTransactionPayload, ApplyError> {
    Message::parse_from_bytes(payload).map_err(|err| {
        warn!(
            "Invalid transaction: Failed to unmarshal SmallbankTransaction: {:?}",
            err
        );
        ApplyError::InvalidTransaction(format!(
            "Failed to unmarshal SmallbankTransaction: {:?}",
            err
        ))
    })
}

fn apply_create_account(
    mut create_account_data: SmallbankTransactionPayload_CreateAccountTransactionData,
    context: &mut dyn TransactionContext,
) -> Result<(), ApplyError> {
    match load_account(create_account_data.get_customer_id(), context)? {
        Some(_) => {
            warn!(
                "Invalid transaction: during CREATE_ACCOUNT, Customer {} already exists",
                create_account_data.get_customer_id()
            );
            Err(ApplyError::InvalidTransaction(format!(
                "Customer {} already exists",
                create_account_data.get_customer_id()
            )))
        }
        None => {
            if create_account_data.get_customer_name().is_empty() {
                warn!("Invalid transaction: during CREATE_ACCOUNT, Customer Name must be set");
                Err(ApplyError::InvalidTransaction(
                    "Customer Name must be set".into(),
                ))
            } else {
                let mut new_account = Account::new();
                new_account.set_customer_id(create_account_data.get_customer_id());
                new_account.set_customer_name(create_account_data.take_customer_name());
                new_account.set_savings_balance(create_account_data.get_initial_savings_balance());
                new_account
                    .set_checking_balance(create_account_data.get_initial_checking_balance());
                save_account(&new_account, context)
            }
        }
    }
}

fn apply_deposit_checking(
    deposit_checking_data: &SmallbankTransactionPayload_DepositCheckingTransactionData,
    context: &mut dyn TransactionContext,
) -> Result<(), ApplyError> {
    match load_account(deposit_checking_data.get_customer_id(), context)? {
        None => {
            warn!("Invalid transaction: during DEPOSIT_CHECKING, Account must exist");
            Err(ApplyError::InvalidTransaction("Account must exist".into()))
        }
        Some(mut account) => {
            let balance = account.get_checking_balance() + deposit_checking_data.get_amount();
            account.set_checking_balance(balance);
            save_account(&account, context)
        }
    }
}

fn apply_write_check(
    write_check_data: &SmallbankTransactionPayload_WriteCheckTransactionData,
    context: &mut dyn TransactionContext,
) -> Result<(), ApplyError> {
    match load_account(write_check_data.get_customer_id(), context)? {
        None => {
            warn!("Invalid transaction: during WRITE_CHECK, Account must exist");
            Err(ApplyError::InvalidTransaction("Account must exist".into()))
        }
        Some(mut account) => {
            let balance = account.get_checking_balance() - write_check_data.get_amount();
            account.set_checking_balance(balance);
            save_account(&account, context)
        }
    }
}

fn apply_transact_savings(
    transact_savings_data: &SmallbankTransactionPayload_TransactSavingsTransactionData,
    context: &mut dyn TransactionContext,
) -> Result<(), ApplyError> {
    match load_account(transact_savings_data.get_customer_id(), context)? {
        None => {
            warn!("Invalid transaction: during TRANSACT_SAVINGS, Account must exist");
            Err(ApplyError::InvalidTransaction("Account must exist".into()))
        }
        Some(mut account) => {
            if transact_savings_data.get_amount() < 0
                && (-transact_savings_data.get_amount() as u32) > account.get_savings_balance()
            {
                warn!(
                    "Invalid transaction: during TRANSACT_SAVINGS, Insufficient funds in source \
                    savings account"
                );
                return Err(ApplyError::InvalidTransaction(
                    "Insufficient funds in source savings account".into(),
                ));
            }

            let balance = {
                if transact_savings_data.get_amount() < 0 {
                    account.get_savings_balance() - (-transact_savings_data.get_amount() as u32)
                } else {
                    account.get_savings_balance() + (transact_savings_data.get_amount() as u32)
                }
            };

            account.set_savings_balance(balance);
            save_account(&account, context)
        }
    }
}

fn apply_send_payment(
    send_payment_data: &SmallbankTransactionPayload_SendPaymentTransactionData,
    context: &mut dyn TransactionContext,
) -> Result<(), ApplyError> {
    fn err() -> ApplyError {
        warn!("Invalid transaction: during SEND_PAYMENT, both source and dest accounts must exist");
        ApplyError::InvalidTransaction("Both source and dest accounts must exist".into())
    }

    let mut source_account =
        load_account(send_payment_data.get_source_customer_id(), context)?.ok_or_else(err)?;
    let mut dest_account =
        load_account(send_payment_data.get_dest_customer_id(), context)?.ok_or_else(err)?;

    if source_account.get_checking_balance() < send_payment_data.get_amount() {
        warn!(
            "Invalid transaction: during SEND_PAYMENT, Insufficient funds in source checking \
            account"
        );
        Err(ApplyError::InvalidTransaction(
            "Insufficient funds in source checking account".into(),
        ))
    } else {
        let source_balance = source_account.get_checking_balance() - send_payment_data.get_amount();
        source_account.set_checking_balance(source_balance);
        let dest_balance = dest_account.get_checking_balance() + send_payment_data.get_amount();
        dest_account.set_checking_balance(dest_balance);
        save_account(&source_account, context).and(save_account(&dest_account, context))
    }
}

fn apply_amalgamate(
    amalgamate_data: &SmallbankTransactionPayload_AmalgamateTransactionData,
    context: &mut dyn TransactionContext,
) -> Result<(), ApplyError> {
    fn err() -> ApplyError {
        warn!("Invalid transaction: during AMALGAMATE, both source and dest accounts must exist");
        ApplyError::InvalidTransaction("Both source and dest accounts must exist".into())
    }

    let mut source_account =
        load_account(amalgamate_data.get_source_customer_id(), context)?.ok_or_else(err)?;
    let mut dest_account =
        load_account(amalgamate_data.get_dest_customer_id(), context)?.ok_or_else(err)?;

    if source_account.get_savings_balance() == 0 {
        warn!("Invalid transaction: Source account savings balance cannot be 0 in amalgamate");
        return Err(ApplyError::InvalidTransaction(
            "Source account savings balance cannot be 0 in amalgamate".to_string(),
        ));
    }

    let balance = dest_account.get_checking_balance() + source_account.get_savings_balance();
    source_account.set_savings_balance(0);
    dest_account.set_checking_balance(balance);
    save_account(&source_account, context).and(save_account(&dest_account, context))
}

fn unpack_account(account_data: &[u8]) -> Result<Account, ApplyError> {
    Message::parse_from_bytes(account_data).map_err(|err| {
        warn!(
            "Invalid transaction: Failed to unmarshal Account: {:?}",
            err
        );
        ApplyError::InvalidTransaction(format!("Failed to unmarshal Account: {:?}", err))
    })
}

fn load_account(
    customer_id: u32,
    context: &mut dyn TransactionContext,
) -> Result<Option<Account>, ApplyError> {
    let response = context
        .get_state_entry(&create_smallbank_address(&format!("{}", customer_id)))
        .map_err(|err| {
            warn!("Invalid transaction: Failed to load Account: {:?}", err);
            ApplyError::InvalidTransaction(format!("Failed to load Account: {:?}", err))
        })?;
    match response {
        Some(packed) => unpack_account(&packed).map(Some),
        None => Ok(None),
    }
}

fn save_account(account: &Account, context: &mut dyn TransactionContext) -> Result<(), ApplyError> {
    let address = create_smallbank_address(&format!("{}", account.get_customer_id()));
    let data = protobuf::Message::write_to_bytes(account).map_err(|err| {
        warn!(
            "Invalid transaction: Failed to serialize Account: {:?}",
            err
        );
        ApplyError::InvalidTransaction(format!("Failed to serialize Account: {:?}", err))
    })?;

    context.set_state_entry(address, data).map_err(|err| {
        warn!("Invalid transaction: Failed to save Account: {:?}", err);
        ApplyError::InvalidTransaction(format!("Failed to save Account: {:?}", err))
    })
}

fn get_smallbank_prefix() -> String {
    let mut sha = Sha512::new();
    sha.update("smallbank");
    hex::encode(&sha.finalize())[..6].to_string()
}

fn create_smallbank_address(payload: &str) -> String {
    let mut sha = Sha512::new();
    sha.update(payload.as_bytes());
    get_smallbank_prefix() + &hex::encode(&sha.finalize())[..64]
}

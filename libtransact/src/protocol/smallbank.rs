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
 * -----------------------------------------------------------------------------
 */

use protobuf::Message;

use crate::protos::smallbank::SmallbankTransactionPayload_PayloadType;
use crate::protos::{
    self, FromBytes, FromNative, FromProto, IntoBytes, IntoNative, IntoProto, ProtoConversionError,
};

pub struct Account {
    customer_id: u32,
    customer_name: String,
    savings_balance: u32,
    checking_balance: u32,
}

impl Account {
    pub fn new(
        customer_id: u32,
        customer_name: String,
        savings_balance: u32,
        checking_balance: u32,
    ) -> Self {
        Account {
            customer_id,
            customer_name,
            savings_balance,
            checking_balance,
        }
    }

    pub fn customer_id(&self) -> &u32 {
        &self.customer_id
    }

    pub fn customer_name(&self) -> &str {
        &self.customer_name
    }

    pub fn savings_balance(&self) -> &u32 {
        &self.savings_balance
    }

    pub fn checking_balance(&self) -> &u32 {
        &self.checking_balance
    }
}

impl FromProto<protos::smallbank::Account> for Account {
    fn from_proto(account: protos::smallbank::Account) -> Result<Self, ProtoConversionError> {
        Ok(Account {
            customer_id: account.get_customer_id(),
            customer_name: account.get_customer_name().to_string(),
            savings_balance: account.get_savings_balance(),
            checking_balance: account.get_checking_balance(),
        })
    }
}

impl IntoBytes for Account {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError("Unable to get bytes from Account".to_string())
        })?;
        Ok(bytes)
    }
}

impl FromNative<Account> for protos::smallbank::Account {
    fn from_native(account: Account) -> Result<Self, ProtoConversionError> {
        let mut proto_account = protos::smallbank::Account::new();

        proto_account.set_customer_id(*account.customer_id());
        proto_account.set_customer_name(account.customer_name().to_string());
        proto_account.set_savings_balance(*account.savings_balance());
        proto_account.set_checking_balance(*account.checking_balance());

        Ok(proto_account)
    }
}

impl FromBytes<Account> for Account {
    fn from_bytes(bytes: &[u8]) -> Result<Account, ProtoConversionError> {
        let proto: protos::smallbank::Account = Message::parse_from_bytes(bytes).map_err(|_| {
            ProtoConversionError::SerializationError("Unable to get Account from bytes".to_string())
        })?;
        proto.into_native()
    }
}

impl IntoProto<protos::smallbank::Account> for Account {}
impl IntoNative<Account> for protos::smallbank::Account {}

#[derive(Debug, Clone)]
pub enum SmallbankTransactionPayload {
    CreateAccountTransactionData(CreateAccount),
    DepositCheckingTransactionData(DepositChecking),
    WriteCheckTransactionData(WriteCheck),
    TransactSavingsTransactionData(TransactSavings),
    SendPaymentTransactionData(SendPayment),
    AmalgamateTransactionData(Amalgamate),
}

impl FromProto<protos::smallbank::SmallbankTransactionPayload> for SmallbankTransactionPayload {
    fn from_proto(
        smallbank_payload: protos::smallbank::SmallbankTransactionPayload,
    ) -> Result<Self, ProtoConversionError> {
        match smallbank_payload.get_payload_type() {
            SmallbankTransactionPayload_PayloadType::CREATE_ACCOUNT => {
                Ok(SmallbankTransactionPayload::CreateAccountTransactionData(
                    CreateAccount::from_proto(smallbank_payload.get_create_account().clone())?,
                ))
            }
            SmallbankTransactionPayload_PayloadType::DEPOSIT_CHECKING => {
                Ok(SmallbankTransactionPayload::DepositCheckingTransactionData(
                    DepositChecking::from_proto(smallbank_payload.get_deposit_checking().clone())?,
                ))
            }
            SmallbankTransactionPayload_PayloadType::WRITE_CHECK => {
                Ok(SmallbankTransactionPayload::WriteCheckTransactionData(
                    WriteCheck::from_proto(smallbank_payload.get_write_check().clone())?,
                ))
            }
            SmallbankTransactionPayload_PayloadType::TRANSACT_SAVINGS => {
                Ok(SmallbankTransactionPayload::TransactSavingsTransactionData(
                    TransactSavings::from_proto(smallbank_payload.get_transact_savings().clone())?,
                ))
            }
            SmallbankTransactionPayload_PayloadType::SEND_PAYMENT => {
                Ok(SmallbankTransactionPayload::SendPaymentTransactionData(
                    SendPayment::from_proto(smallbank_payload.get_send_payment().clone())?,
                ))
            }
            SmallbankTransactionPayload_PayloadType::AMALGAMATE => {
                Ok(SmallbankTransactionPayload::AmalgamateTransactionData(
                    Amalgamate::from_proto(smallbank_payload.get_amalgamate().clone())?,
                ))
            }
            _ => Err(ProtoConversionError::InvalidTypeError(
                "Cannot convert SmallbankTransactionPayload_PayloadType with type unset."
                    .to_string(),
            )),
        }
    }
}

impl IntoBytes for SmallbankTransactionPayload {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from SmallbankTransactionPayload".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl FromNative<SmallbankTransactionPayload> for protos::smallbank::SmallbankTransactionPayload {
    fn from_native(
        smallbank_payload: SmallbankTransactionPayload,
    ) -> Result<Self, ProtoConversionError> {
        let mut proto_smallbank_payload = protos::smallbank::SmallbankTransactionPayload::new();

        match smallbank_payload {
            SmallbankTransactionPayload::CreateAccountTransactionData(payload) => {
                proto_smallbank_payload.set_payload_type(
                    protos::smallbank::SmallbankTransactionPayload_PayloadType::CREATE_ACCOUNT,
                );
                proto_smallbank_payload.set_create_account(payload.into_proto()?);
                Ok(proto_smallbank_payload)
            }
            SmallbankTransactionPayload::DepositCheckingTransactionData(payload) => {
                proto_smallbank_payload.set_payload_type(
                    protos::smallbank::SmallbankTransactionPayload_PayloadType::DEPOSIT_CHECKING,
                );
                proto_smallbank_payload.set_deposit_checking(payload.into_proto()?);
                Ok(proto_smallbank_payload)
            }
            SmallbankTransactionPayload::WriteCheckTransactionData(payload) => {
                proto_smallbank_payload.set_payload_type(
                    protos::smallbank::SmallbankTransactionPayload_PayloadType::WRITE_CHECK,
                );
                proto_smallbank_payload.set_write_check(payload.into_proto()?);
                Ok(proto_smallbank_payload)
            }
            SmallbankTransactionPayload::TransactSavingsTransactionData(payload) => {
                proto_smallbank_payload.set_payload_type(
                    protos::smallbank::SmallbankTransactionPayload_PayloadType::TRANSACT_SAVINGS,
                );
                proto_smallbank_payload.set_transact_savings(payload.into_proto()?);
                Ok(proto_smallbank_payload)
            }
            SmallbankTransactionPayload::SendPaymentTransactionData(payload) => {
                proto_smallbank_payload.set_payload_type(
                    protos::smallbank::SmallbankTransactionPayload_PayloadType::SEND_PAYMENT,
                );
                proto_smallbank_payload.set_send_payment(payload.into_proto()?);
                Ok(proto_smallbank_payload)
            }
            SmallbankTransactionPayload::AmalgamateTransactionData(payload) => {
                proto_smallbank_payload.set_payload_type(
                    protos::smallbank::SmallbankTransactionPayload_PayloadType::AMALGAMATE,
                );
                proto_smallbank_payload.set_amalgamate(payload.into_proto()?);
                Ok(proto_smallbank_payload)
            }
        }
    }
}

impl FromBytes<SmallbankTransactionPayload> for SmallbankTransactionPayload {
    fn from_bytes(bytes: &[u8]) -> Result<SmallbankTransactionPayload, ProtoConversionError> {
        let proto: protos::smallbank::SmallbankTransactionPayload =
            Message::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get SmallbankTransactionPayload from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoProto<protos::smallbank::SmallbankTransactionPayload> for SmallbankTransactionPayload {}
impl IntoNative<SmallbankTransactionPayload> for protos::smallbank::SmallbankTransactionPayload {}

#[derive(Debug, Clone)]
pub struct CreateAccount {
    customer_id: u32,
    customer_name: String,
    initial_savings_balance: u32,
    initial_checking_balance: u32,
}

impl CreateAccount {
    pub fn new(
        customer_id: u32,
        customer_name: String,
        initial_savings_balance: u32,
        initial_checking_balance: u32,
    ) -> Self {
        CreateAccount {
            customer_id,
            customer_name,
            initial_savings_balance,
            initial_checking_balance,
        }
    }

    pub fn customer_id(&self) -> &u32 {
        &self.customer_id
    }

    pub fn customer_name(&self) -> &str {
        &self.customer_name
    }

    pub fn initial_savings_balance(&self) -> &u32 {
        &self.initial_savings_balance
    }

    pub fn initial_checking_balance(&self) -> &u32 {
        &self.initial_checking_balance
    }
}

impl FromProto<protos::smallbank::SmallbankTransactionPayload_CreateAccountTransactionData>
    for CreateAccount
{
    fn from_proto(
        create_account: protos::smallbank::SmallbankTransactionPayload_CreateAccountTransactionData,
    ) -> Result<Self, ProtoConversionError> {
        Ok(CreateAccount {
            customer_id: create_account.get_customer_id(),
            customer_name: create_account.get_customer_name().to_string(),
            initial_savings_balance: create_account.get_initial_savings_balance(),
            initial_checking_balance: create_account.get_initial_checking_balance(),
        })
    }
}

impl IntoBytes for CreateAccount {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from CreateAccount".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl FromNative<CreateAccount>
    for protos::smallbank::SmallbankTransactionPayload_CreateAccountTransactionData
{
    fn from_native(create_account: CreateAccount) -> Result<Self, ProtoConversionError> {
        let mut proto_create_account =
            protos::smallbank::SmallbankTransactionPayload_CreateAccountTransactionData::new();

        proto_create_account.set_customer_id(*create_account.customer_id());
        proto_create_account.set_customer_name(create_account.customer_name().to_string());
        proto_create_account.set_initial_savings_balance(*create_account.initial_savings_balance());
        proto_create_account
            .set_initial_checking_balance(*create_account.initial_checking_balance());

        Ok(proto_create_account)
    }
}

impl FromBytes<CreateAccount> for CreateAccount {
    fn from_bytes(bytes: &[u8]) -> Result<CreateAccount, ProtoConversionError> {
        let proto: protos::smallbank::SmallbankTransactionPayload_CreateAccountTransactionData =
            Message::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get CreateAccount from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoProto<protos::smallbank::SmallbankTransactionPayload_CreateAccountTransactionData>
    for CreateAccount
{
}
impl IntoNative<CreateAccount>
    for protos::smallbank::SmallbankTransactionPayload_CreateAccountTransactionData
{
}

#[derive(Debug, Clone)]
pub struct DepositChecking {
    customer_id: u32,
    amount: u32,
}

impl DepositChecking {
    pub fn new(customer_id: u32, amount: u32) -> Self {
        DepositChecking {
            customer_id,
            amount,
        }
    }

    pub fn customer_id(&self) -> &u32 {
        &self.customer_id
    }

    pub fn amount(&self) -> &u32 {
        &self.amount
    }
}

impl FromProto<protos::smallbank::SmallbankTransactionPayload_DepositCheckingTransactionData>
    for DepositChecking
{
    fn from_proto(
        bytes_entry: protos::smallbank::SmallbankTransactionPayload_DepositCheckingTransactionData,
    ) -> Result<Self, ProtoConversionError> {
        Ok(DepositChecking {
            customer_id: bytes_entry.get_customer_id(),
            amount: bytes_entry.get_amount(),
        })
    }
}

impl IntoBytes for DepositChecking {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from DepositChecking".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl FromNative<DepositChecking>
    for protos::smallbank::SmallbankTransactionPayload_DepositCheckingTransactionData
{
    fn from_native(deposit_checking: DepositChecking) -> Result<Self, ProtoConversionError> {
        let mut proto_deposit_checking =
            protos::smallbank::SmallbankTransactionPayload_DepositCheckingTransactionData::new();

        proto_deposit_checking.set_customer_id(*deposit_checking.customer_id());
        proto_deposit_checking.set_amount(*deposit_checking.amount());

        Ok(proto_deposit_checking)
    }
}

impl FromBytes<DepositChecking> for DepositChecking {
    fn from_bytes(bytes: &[u8]) -> Result<DepositChecking, ProtoConversionError> {
        let proto: protos::smallbank::SmallbankTransactionPayload_DepositCheckingTransactionData =
            Message::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get DepositChecking from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoProto<protos::smallbank::SmallbankTransactionPayload_DepositCheckingTransactionData>
    for DepositChecking
{
}
impl IntoNative<DepositChecking>
    for protos::smallbank::SmallbankTransactionPayload_DepositCheckingTransactionData
{
}

#[derive(Debug, Clone)]
pub struct WriteCheck {
    customer_id: u32,
    amount: u32,
}

impl WriteCheck {
    pub fn new(customer_id: u32, amount: u32) -> Self {
        WriteCheck {
            customer_id,
            amount,
        }
    }

    pub fn customer_id(&self) -> &u32 {
        &self.customer_id
    }

    pub fn amount(&self) -> &u32 {
        &self.amount
    }
}

impl FromProto<protos::smallbank::SmallbankTransactionPayload_WriteCheckTransactionData>
    for WriteCheck
{
    fn from_proto(
        bytes_entry: protos::smallbank::SmallbankTransactionPayload_WriteCheckTransactionData,
    ) -> Result<Self, ProtoConversionError> {
        Ok(WriteCheck {
            customer_id: bytes_entry.get_customer_id(),
            amount: bytes_entry.get_amount(),
        })
    }
}

impl IntoBytes for WriteCheck {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from WriteCheck".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl FromNative<WriteCheck>
    for protos::smallbank::SmallbankTransactionPayload_WriteCheckTransactionData
{
    fn from_native(write_check: WriteCheck) -> Result<Self, ProtoConversionError> {
        let mut proto_write_check =
            protos::smallbank::SmallbankTransactionPayload_WriteCheckTransactionData::new();

        proto_write_check.set_customer_id(*write_check.customer_id());
        proto_write_check.set_amount(*write_check.amount());

        Ok(proto_write_check)
    }
}

impl FromBytes<WriteCheck> for WriteCheck {
    fn from_bytes(bytes: &[u8]) -> Result<WriteCheck, ProtoConversionError> {
        let proto: protos::smallbank::SmallbankTransactionPayload_WriteCheckTransactionData =
            Message::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get WriteCheck from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoProto<protos::smallbank::SmallbankTransactionPayload_WriteCheckTransactionData>
    for WriteCheck
{
}
impl IntoNative<WriteCheck>
    for protos::smallbank::SmallbankTransactionPayload_WriteCheckTransactionData
{
}

#[derive(Debug, Clone)]
pub struct TransactSavings {
    customer_id: u32,
    amount: i32,
}

impl TransactSavings {
    pub fn new(customer_id: u32, amount: i32) -> Self {
        TransactSavings {
            customer_id,
            amount,
        }
    }

    pub fn customer_id(&self) -> &u32 {
        &self.customer_id
    }

    pub fn amount(&self) -> &i32 {
        &self.amount
    }
}

impl FromProto<protos::smallbank::SmallbankTransactionPayload_TransactSavingsTransactionData>
    for TransactSavings
{
    fn from_proto(
        bytes_entry: protos::smallbank::SmallbankTransactionPayload_TransactSavingsTransactionData,
    ) -> Result<Self, ProtoConversionError> {
        Ok(TransactSavings {
            customer_id: bytes_entry.get_customer_id(),
            amount: bytes_entry.get_amount(),
        })
    }
}

impl IntoBytes for TransactSavings {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from TransactSavings".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl FromNative<TransactSavings>
    for protos::smallbank::SmallbankTransactionPayload_TransactSavingsTransactionData
{
    fn from_native(transact_savings: TransactSavings) -> Result<Self, ProtoConversionError> {
        let mut proto_transact_savings =
            protos::smallbank::SmallbankTransactionPayload_TransactSavingsTransactionData::new();

        proto_transact_savings.set_customer_id(*transact_savings.customer_id());
        proto_transact_savings.set_amount(*transact_savings.amount());

        Ok(proto_transact_savings)
    }
}

impl FromBytes<TransactSavings> for TransactSavings {
    fn from_bytes(bytes: &[u8]) -> Result<TransactSavings, ProtoConversionError> {
        let proto: protos::smallbank::SmallbankTransactionPayload_TransactSavingsTransactionData =
            Message::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get TransactSavings from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoProto<protos::smallbank::SmallbankTransactionPayload_TransactSavingsTransactionData>
    for TransactSavings
{
}
impl IntoNative<TransactSavings>
    for protos::smallbank::SmallbankTransactionPayload_TransactSavingsTransactionData
{
}

#[derive(Debug, Clone)]
pub struct SendPayment {
    source_customer_id: u32,
    dest_customer_id: u32,
    amount: u32,
}

impl SendPayment {
    pub fn new(source_customer_id: u32, dest_customer_id: u32, amount: u32) -> Self {
        SendPayment {
            source_customer_id,
            dest_customer_id,
            amount,
        }
    }

    pub fn source_customer_id(&self) -> &u32 {
        &self.source_customer_id
    }

    pub fn dest_customer_id(&self) -> &u32 {
        &self.dest_customer_id
    }

    pub fn amount(&self) -> &u32 {
        &self.amount
    }
}

impl FromProto<protos::smallbank::SmallbankTransactionPayload_SendPaymentTransactionData>
    for SendPayment
{
    fn from_proto(
        bytes_entry: protos::smallbank::SmallbankTransactionPayload_SendPaymentTransactionData,
    ) -> Result<Self, ProtoConversionError> {
        Ok(SendPayment {
            source_customer_id: bytes_entry.get_source_customer_id(),
            dest_customer_id: bytes_entry.get_dest_customer_id(),
            amount: bytes_entry.get_amount(),
        })
    }
}

impl IntoBytes for SendPayment {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from SendPayment".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl FromNative<SendPayment>
    for protos::smallbank::SmallbankTransactionPayload_SendPaymentTransactionData
{
    fn from_native(send_payment: SendPayment) -> Result<Self, ProtoConversionError> {
        let mut proto_send_payment =
            protos::smallbank::SmallbankTransactionPayload_SendPaymentTransactionData::new();

        proto_send_payment.set_source_customer_id(*send_payment.source_customer_id());
        proto_send_payment.set_dest_customer_id(*send_payment.dest_customer_id());
        proto_send_payment.set_amount(*send_payment.amount());

        Ok(proto_send_payment)
    }
}

impl FromBytes<SendPayment> for SendPayment {
    fn from_bytes(bytes: &[u8]) -> Result<SendPayment, ProtoConversionError> {
        let proto: protos::smallbank::SmallbankTransactionPayload_SendPaymentTransactionData =
            Message::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get SendPayment from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoProto<protos::smallbank::SmallbankTransactionPayload_SendPaymentTransactionData>
    for SendPayment
{
}
impl IntoNative<SendPayment>
    for protos::smallbank::SmallbankTransactionPayload_SendPaymentTransactionData
{
}

#[derive(Debug, Clone)]
pub struct Amalgamate {
    source_customer_id: u32,
    dest_customer_id: u32,
}

impl Amalgamate {
    pub fn new(source_customer_id: u32, dest_customer_id: u32) -> Self {
        Amalgamate {
            source_customer_id,
            dest_customer_id,
        }
    }

    pub fn source_customer_id(&self) -> &u32 {
        &self.source_customer_id
    }

    pub fn dest_customer_id(&self) -> &u32 {
        &self.dest_customer_id
    }
}

impl FromProto<protos::smallbank::SmallbankTransactionPayload_AmalgamateTransactionData>
    for Amalgamate
{
    fn from_proto(
        bytes_entry: protos::smallbank::SmallbankTransactionPayload_AmalgamateTransactionData,
    ) -> Result<Self, ProtoConversionError> {
        Ok(Amalgamate {
            source_customer_id: bytes_entry.get_source_customer_id(),
            dest_customer_id: bytes_entry.get_dest_customer_id(),
        })
    }
}

impl IntoBytes for Amalgamate {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        let proto = self.into_proto()?;
        let bytes = proto.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from Amalgamate".to_string(),
            )
        })?;
        Ok(bytes)
    }
}

impl FromNative<Amalgamate>
    for protos::smallbank::SmallbankTransactionPayload_AmalgamateTransactionData
{
    fn from_native(amalgamate: Amalgamate) -> Result<Self, ProtoConversionError> {
        let mut proto_amalgamate =
            protos::smallbank::SmallbankTransactionPayload_AmalgamateTransactionData::new();

        proto_amalgamate.set_source_customer_id(*amalgamate.source_customer_id());
        proto_amalgamate.set_dest_customer_id(*amalgamate.dest_customer_id());

        Ok(proto_amalgamate)
    }
}

impl FromBytes<Amalgamate> for Amalgamate {
    fn from_bytes(bytes: &[u8]) -> Result<Amalgamate, ProtoConversionError> {
        let proto: protos::smallbank::SmallbankTransactionPayload_AmalgamateTransactionData =
            Message::parse_from_bytes(bytes).map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get Amalgamate from bytes".to_string(),
                )
            })?;
        proto.into_native()
    }
}

impl IntoProto<protos::smallbank::SmallbankTransactionPayload_AmalgamateTransactionData>
    for Amalgamate
{
}
impl IntoNative<Amalgamate>
    for protos::smallbank::SmallbankTransactionPayload_AmalgamateTransactionData
{
}

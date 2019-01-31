/*
 * Copyright 2019 Bitwise IO, Inc.
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

//! Contains the internal types and functionality of the Executor.
//!                                                                                                 -------- ExecutionAdapter
//! ---- Iterator<Item = ExecutionTask> ----\        | Single thread that                         /
//!                                           \      | listens for RegistrationChanges and       /
//! ---- Iterator<Item = ExecutionTask> ------------ | ExecutionEvents on the same channel -----/
//!                                           /                                                 \
//! ---- Iterator<Item = ExecutionTask> ----/                                                    \
//!                                                                                                --------- ExecutionAdapter
//!

use crate::execution::adapter::ExecutionResult;
use crate::execution::adapter::TransactionFamily;
use crate::scheduler::ExecutionTask;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::mpsc::{Receiver, Sender};

/// The `TransactionPair` and `ContextId` along with where to send
/// results.
pub type ExecutionEvent = (Sender<ExecutionResult>, ExecutionTask);

/// The type that gets sent to the `ExecutionAdapter`.
pub enum ExecutionCommand {
    /// There is an `ExecutionEvent`, the `ExecutionAdapter` will process it if it can
    Event(Box<ExecutionEvent>),
    /// Shut down the execution adapter.
    Sentinel,
}

/// A registration or unregistration request from the `ExecutionAdapter`.
pub enum RegistrationChange {
    UnregisterRequest((TransactionFamily, NamedExecutionEventSender)),
    RegisterRequest((TransactionFamily, NamedExecutionEventSender)),
}

/// One of either a `RegistrationChange` or an `ExecutionEvent`.
/// The single internal thread in the Executor is listening for these.
pub enum RegistrationExecutionEvent {
    RegistrationChange(RegistrationChange),
    Execution(Box<ExecutionEvent>),
}

///`RegistrationChange` and `ExecutionEvent` multiplex sender
pub type RegistrationExecutionEventSender = Sender<RegistrationExecutionEvent>;

///`RegistrationChange` and `ExecutionEvent` multiplex sender
pub type RegistrationExecutionEventReceiver = Receiver<RegistrationExecutionEvent>;

/// Sender part of a channel to send from the internal looping thread to the `ExecutionAdapter`
pub type ExecutionEventSender = Sender<ExecutionCommand>;

/// Receiver part of a channel for the `ExecutionAdapter` to receive from the internal looping thread.
pub type ExecutionEventReceiver = Receiver<ExecutionCommand>;

/// ExecutionEvents that don't currently have a `ExecutionAdapter` to send to.
pub type ParkedExecutionEvents = Vec<ExecutionEvent>;

/// A Map to do lookups of `ExecutionEvent`s by `TransactionFamily` for finding `ExecutionEvent`s that were
/// waiting for a just registered `TransactionFamily`
pub type ParkedExecutionEventsMap = HashMap<TransactionFamily, ParkedExecutionEvents>;

/// An ExecutionEventSender along with a hashable name or id.
#[derive(Clone)]
pub struct NamedExecutionEventSender {
    pub sender: ExecutionEventSender,
    name: usize,
}

impl NamedExecutionEventSender {
    pub fn new(sender: ExecutionEventSender, name: usize) -> Self {
        NamedExecutionEventSender { sender, name }
    }
}

impl Hash for NamedExecutionEventSender {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.name.hash(hasher)
    }
}

impl PartialEq for NamedExecutionEventSender {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(&other.name)
    }
}

impl Eq for NamedExecutionEventSender {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::adapter::TransactionStatus;
    use crate::signing::{hash::HashSigner, Signer};
    use crate::transaction::{HashMethod, TransactionBuilder, TransactionPair};
    use std::{self, collections::HashSet, sync::mpsc::channel};

    static FAMILY_NAME: &str = "test_family";
    static FAMILY_VERSION: &str = "0.1";
    static KEY1: &str = "111111111111111111111111111111111111111111111111111111111111111111";
    static KEY2: &str = "222222222222222222222222222222222222222222222222222222222222222222";
    static KEY3: &str = "333333333333333333333333333333333333333333333333333333333333333333";
    static KEY4: &str = "444444444444444444444444444444444444444444444444444444444444444444";
    static KEY5: &str = "555555555555555555555555555555555555555555555555555555555555555555";
    static KEY6: &str = "666666666666666666666666666666666666666666666666666666666666666666";
    static KEY7: &str = "777777777777777777777777777777777777777777777777777777777777777777";
    static NONCE: &str = "f9kdzz";
    static BYTES2: [u8; 4] = [0x05, 0x06, 0x07, 0x08];

    static NUMBER_OF_TRANSACTIONS: usize = 20;

    /// Walks sequentially through the multiplexing of ExecutionEvents and RegistrationChanges on the
    /// same channel, processing the ExecutionTasks in the ExecutionEvents and returning the result.
    #[test]
    fn test_executor_internal() {
        // Create the three channels with their associated Senders and Receivers.

        let (sender, result_receiver) = channel::<ExecutionResult>();

        let (registration_execution_event_sender, internal_receiver): (
            RegistrationExecutionEventSender,
            RegistrationExecutionEventReceiver,
        ) = channel::<RegistrationExecutionEvent>();

        let (execution_adapter_sender, receiver) = channel::<ExecutionCommand>();

        // Register the transaction family

        let tf = TransactionFamily::new(FAMILY_NAME.to_string(), FAMILY_VERSION.to_string());
        let named_sender = NamedExecutionEventSender::new(execution_adapter_sender, 0);
        let registration_event = RegistrationExecutionEvent::RegistrationChange(
            RegistrationChange::RegisterRequest((tf, named_sender)),
        );

        registration_execution_event_sender
            .send(registration_event)
            .expect("The receiver is dropped");

        let execution_tasks = create_iterator();

        // Send the ExecutionEvents on the multiplexing channel.

        for reg_ex_event in execution_tasks
            .map(|execution_task| (sender.clone(), execution_task))
            .map(|execution_event| RegistrationExecutionEvent::Execution(Box::new(execution_event)))
        {
            registration_execution_event_sender
                .send(reg_ex_event)
                .expect("Receiver has been dropped");
        }

        let mut parked_transaction_map: ParkedExecutionEventsMap = HashMap::new();
        let mut unparked_transactions: Vec<ExecutionEvent> = vec![];
        let mut named_senders: HashMap<TransactionFamily, HashSet<NamedExecutionEventSender>> =
            HashMap::new();

        // Main Executor loop

        while let Ok(event) = internal_receiver.try_recv() {
            match event {
                RegistrationExecutionEvent::Execution(execution_event) => {
                    let (_, execution_state) = execution_event.as_ref();

                    let tf = TransactionFamily::from_pair(execution_state.pair());
                    match named_senders.get(&tf) {
                        Some(senders) => match senders.iter().nth(0) {
                            Some(sender) => {
                                sender
                                    .sender
                                    .send(ExecutionCommand::Event(execution_event))
                                    .expect("The receiver has been dropped");
                            }
                            None => {
                                let parked = match parked_transaction_map.get_mut(&tf) {
                                    Some(parked) => {
                                        parked.push(*execution_event);
                                        None
                                    }
                                    None => Some(vec![*execution_event]),
                                };
                                if let Some(p) = parked {
                                    parked_transaction_map.insert(tf, p);
                                }
                            }
                        },
                        None => {
                            let parked = match parked_transaction_map.get_mut(&tf) {
                                Some(parked) => {
                                    parked.push(*execution_event);
                                    None
                                }
                                None => Some(vec![*execution_event]),
                            };
                            if let Some(p) = parked {
                                parked_transaction_map.insert(tf, p);
                            }
                        }
                    }
                }
                RegistrationExecutionEvent::RegistrationChange(registration_event) => {
                    match registration_event {
                        RegistrationChange::RegisterRequest((tf, sender)) => {
                            parked_transaction_map
                                .entry(tf.clone())
                                .and_modify(|parked| {
                                    for p in parked.drain(0..) {
                                        unparked_transactions.push(p);
                                    }
                                });
                            let senders_option = match named_senders.get_mut(&tf) {
                                Some(senders) => {
                                    senders.insert(sender);
                                    None
                                }
                                None => {
                                    let mut s = HashSet::new();
                                    s.insert(sender);
                                    Some(s)
                                }
                            };
                            if let Some(senders) = senders_option {
                                named_senders.insert(tf, senders);
                            }
                        }
                        RegistrationChange::UnregisterRequest((tf, sender)) => {
                            named_senders.entry(tf).and_modify(|senders| {
                                senders.remove(&sender);
                            });
                        }
                    }
                }
            }
        }

        // Process the ExecutionTask and return an ExecutionResult.

        while let Ok(event) = receiver.try_recv() {
            if let ExecutionCommand::Event(execution_event) = event {
                let (result_sender, task) = *execution_event;
                let transaction_status = TransactionStatus::Valid;
                let execution_result = ExecutionResult {
                    transaction_id: task.pair().transaction().header_signature().to_string(),
                    status: transaction_status,
                };
                result_sender
                    .send(execution_result)
                    .expect("The receiver has been dropped");
            }
        }

        // Accumulate the ExecutionResults and assert there are 10

        let mut results = vec![];

        while let Ok(result) = result_receiver.try_recv() {
            results.push(result);
        }

        assert_eq!(
            results.len(),
            NUMBER_OF_TRANSACTIONS,
            "Incorrect number of results received",
        );
    }

    fn create_txn(signer: &Signer) -> TransactionPair {
        TransactionBuilder::new()
            .with_batcher_public_key(hex::decode(KEY1).unwrap())
            .with_dependencies(vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap()])
            .with_family_name(FAMILY_NAME.to_string())
            .with_family_version(FAMILY_VERSION.to_string())
            .with_inputs(vec![
                hex::decode(KEY4).unwrap(),
                hex::decode(&KEY5[0..4]).unwrap(),
            ])
            .with_nonce(NONCE.to_string().into_bytes())
            .with_outputs(vec![
                hex::decode(KEY6).unwrap(),
                hex::decode(&KEY7[0..4]).unwrap(),
            ])
            .with_payload_hash_method(HashMethod::SHA512)
            .with_payload(BYTES2.to_vec())
            .build_pair(signer)
            .expect("The TransactionBuilder was not given the correct items")
    }

    fn create_iterator() -> impl Iterator<Item = ExecutionTask> {
        let signer = HashSigner::new();
        let context_id = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

        (0..NUMBER_OF_TRANSACTIONS)
            .map(move |_| create_txn(&signer))
            .map(move |txn_pair| ExecutionTask::new(txn_pair, context_id.clone()))
    }
}

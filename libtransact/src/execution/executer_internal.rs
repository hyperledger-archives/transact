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

//! Contains the internal types and functionality of the Executer.
//                                                                                                 -------- ExecutionAdapter
// ---- Iterator<Item = ExecutionTask> ----\        | Single thread that                         /
//                                           \      | listens for RegistrationChanges and       /
// ---- Iterator<Item = ExecutionTask> ------------ | ExecutionEvents on the same channel -----/
//                                           /                                                 \
// ---- Iterator<Item = ExecutionTask> ----/                                                    \
//                                                                                                --------- ExecutionAdapter
//

use crate::execution::adapter::{ExecutionAdapter, ExecutionAdapterError};
use crate::execution::{ExecutionRegistry, TransactionFamily};
use crate::scheduler::ExecutionTask;
use crate::scheduler::ExecutionTaskCompletionNotification;
use log::warn;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{channel, Receiver, Sender},
    Arc,
};
use std::thread::JoinHandle;
use std::time::Duration;

/// The `TransactionPair` and `ContextId` along with where to send
/// results.
pub type ExecutionEvent = (Sender<ExecutionTaskCompletionNotification>, ExecutionTask);

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
/// The single internal thread in the Executer is listening for these.
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

#[derive(Debug)]
pub enum ExecuterThreadError {
    InvalidState,
    ResourcesUnavailable,
}

impl std::error::Error for ExecuterThreadError {
    fn cause(&self) -> Option<&std::error::Error> {
        None
    }

    fn description(&self) -> &str {
        match *self {
            ExecuterThreadError::InvalidState => {
                "ExecuterThread in an invalid state when 'start' was called"
            }
            ExecuterThreadError::ResourcesUnavailable => {
                "ExecuterThread unable to access a resource needed for operation"
            }
        }
    }
}

impl std::fmt::Display for ExecuterThreadError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ExecuterThreadError::InvalidState => write!(f, "Invalid State: {}", self.description()),
            ExecuterThreadError::ResourcesUnavailable => {
                write!(f, "ResourcesUnavailable: {}", self.description())
            }
        }
    }
}

pub struct ExecuterThread {
    execution_adapters: Vec<Box<ExecutionAdapter>>,
    join_handles: Vec<JoinHandle<()>>,
    internal_thread: Option<JoinHandle<()>>,
    sender: Option<RegistrationExecutionEventSender>,
    stop: Arc<AtomicBool>,
}

impl ExecuterThread {
    pub fn new(execution_adapters: Vec<Box<ExecutionAdapter>>) -> Self {
        ExecuterThread {
            execution_adapters,
            join_handles: vec![],
            internal_thread: None,
            sender: None,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn sender(&self) -> Option<RegistrationExecutionEventSender> {
        self.sender.as_ref().cloned()
    }

    pub fn start(&mut self) -> Result<(), ExecuterThreadError> {
        if self.sender.is_none() {
            let (registry_sender, receiver) = channel();

            for (index, mut execution_adapter) in self.execution_adapters.drain(0..).enumerate() {
                let (sender, adapter_receiver) = channel();
                let ee_sender = NamedExecutionEventSender::new(sender, index);

                execution_adapter.start(Box::new(InternalRegistry {
                    event_sender: ee_sender,
                    registry_sender: registry_sender.clone(),
                }));

                match Self::start_execution_adapter_thread(
                    Arc::clone(&self.stop),
                    execution_adapter,
                    adapter_receiver,
                    &registry_sender,
                    index,
                ) {
                    Ok(join_handle) => {
                        self.join_handles.push(join_handle);
                    }
                    Err(err) => {
                        warn!("Unable to start thread for execution adapter: {}", err);
                        return Err(ExecuterThreadError::ResourcesUnavailable);
                    }
                }
            }

            self.sender = Some(registry_sender);
            match self.start_thread(receiver) {
                Ok(join_handle) => {
                    self.internal_thread = Some(join_handle);
                }
                Err(err) => {
                    warn!("unable to start internal executer thread: {}", err);
                    return Err(ExecuterThreadError::ResourcesUnavailable);
                }
            }

            Ok(())
        } else {
            Err(ExecuterThreadError::InvalidState)
        }
    }

    pub fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(internal) = self.internal_thread {
            if let Err(err) = internal.join() {
                warn!("During stop of executer thread: {:?}", err);
            }
        }
    }

    fn start_execution_adapter_thread(
        stop: Arc<AtomicBool>,
        execution_adapter: Box<ExecutionAdapter>,
        receiver: ExecutionEventReceiver,
        sender: &RegistrationExecutionEventSender,
        index: usize,
    ) -> Result<JoinHandle<()>, std::io::Error> {
        let sender = sender.clone();

        std::thread::Builder::new()
            .name(format!("execution_adapter_thread_{}", index))
            .spawn(move || loop {
                let sender = sender.clone();
                if let Ok(execution_command) = receiver.recv_timeout(Duration::from_millis(200)) {
                    match execution_command {
                        ExecutionCommand::Event(execution_event) => {
                            let (results_sender, task) = *execution_event;
                            let (pair, context_id) = task.take();

                            let callback = Box::new(move |result| {
                                // Without this line, the function is considered a FnOnce, instead
                                // of an Fn.  This seems to be a strange quirk of the compiler
                                let res_sender = results_sender.clone();
                                match result {
                                    Ok(tp_processing_result) => {
                                        if let Err(err) = res_sender.send(tp_processing_result) {
                                            warn!(
                                            "Sending TransactionProcessingResult on channel: {}",
                                            err
                                        );
                                        }
                                    }
                                    Err(ExecutionAdapterError::TimeoutError(transaction_pair)) => {
                                        let sender = sender.clone();

                                        let execution_task =
                                            ExecutionTask::new(transaction_pair, context_id);
                                        let execution_event = (res_sender, execution_task);
                                        if let Err(err) =
                                            sender.send(RegistrationExecutionEvent::Execution(
                                                Box::new(execution_event),
                                            ))
                                        {
                                            warn!("During retry of TimeOutError: {}", err);
                                        }
                                    }
                                    Err(ExecutionAdapterError::RoutingError(transaction_pair)) => {
                                        let sender = sender.clone();

                                        let execution_task =
                                            ExecutionTask::new(transaction_pair, context_id);
                                        let execution_event = (res_sender, execution_task);
                                        if let Err(err) =
                                            sender.send(RegistrationExecutionEvent::Execution(
                                                Box::new(execution_event),
                                            ))
                                        {
                                            warn!("During retry of RoutingError: {}", err);
                                        }
                                    }
                                }
                            });
                            execution_adapter.execute(pair, context_id, callback);
                        }
                        ExecutionCommand::Sentinel => {
                            execution_adapter.stop();
                            break;
                        }
                    }
                } else if stop.load(Ordering::Relaxed) {
                    execution_adapter.stop();
                    break;
                }
            })
    }

    fn start_thread(
        &self,
        receiver: RegistrationExecutionEventReceiver,
    ) -> Result<JoinHandle<()>, std::io::Error> {
        let stop = Arc::clone(&self.stop);
        std::thread::Builder::new()
            .name("internal_executer_thread".to_string())
            .spawn(move || {
                let mut fanout_threads: HashMap<
                    TransactionFamily,
                    HashSet<NamedExecutionEventSender>,
                > = HashMap::new();
                let mut parked: ParkedExecutionEventsMap = HashMap::new();
                let mut unparked = vec![];
                loop {
                    for execution_event in unparked.drain(0..) {
                        Self::try_send_execution_event(
                            Box::new(execution_event),
                            &fanout_threads,
                            &mut parked,
                        );
                    }

                    if let Ok(reg_execution_event) =
                        receiver.recv_timeout(Duration::from_millis(200))
                    {
                        match reg_execution_event {
                            RegistrationExecutionEvent::Execution(execution_event) => {
                                Self::try_send_execution_event(
                                    execution_event,
                                    &fanout_threads,
                                    &mut parked,
                                )
                            }
                            RegistrationExecutionEvent::RegistrationChange(
                                RegistrationChange::RegisterRequest((transaction_family, sender)),
                            ) => {
                                if let Some(p) = parked.get_mut(&transaction_family) {
                                    unparked.append(p);
                                }
                                let found = if let Some(ea_senders) =
                                    fanout_threads.get_mut(&transaction_family)
                                {
                                    ea_senders.insert(sender);
                                    None
                                } else {
                                    let mut s = HashSet::new();
                                    s.insert(sender);
                                    Some(s)
                                };

                                if let Some(f) = found {
                                    fanout_threads.insert(transaction_family, f);
                                }
                            }
                            RegistrationExecutionEvent::RegistrationChange(
                                RegistrationChange::UnregisterRequest((transaction_family, sender)),
                            ) => {
                                fanout_threads
                                    .entry(transaction_family)
                                    .and_modify(|ea_senders| {
                                        ea_senders.remove(&sender);
                                    });
                            }
                        }
                    } else if stop.load(Ordering::Relaxed) {
                        for sender in
                            fanout_threads
                                .values()
                                .fold(HashSet::new(), |mut set, item| {
                                    for s in item {
                                        set.insert(s);
                                    }
                                    set
                                })
                        {
                            if let Err(err) = sender.sender.send(ExecutionCommand::Sentinel) {
                                warn!("During stop of ExecuterThread internal thread: {}", err);
                            }
                        }

                        break;
                    }
                }
            })
    }

    fn try_send_execution_event(
        execution_event: Box<ExecutionEvent>,
        fanout_threads: &HashMap<TransactionFamily, HashSet<NamedExecutionEventSender>>,
        parked: &mut ParkedExecutionEventsMap,
    ) {
        let tf = TransactionFamily::from_pair(&execution_event.1.pair());
        if let Some(ea_senders) = fanout_threads.get(&tf) {
            if let Some(sender) = ea_senders.iter().nth(0) {
                if let Err(err) = sender.sender.send(ExecutionCommand::Event(execution_event)) {
                    warn!("During send of ExecutionCommand: {}", err);
                }
            } else {
                Self::park_execution_event(parked, *execution_event, tf);
            }
        } else {
            Self::park_execution_event(parked, *execution_event, tf);
        }
    }

    fn park_execution_event(
        parked: &mut ParkedExecutionEventsMap,
        execution_event: ExecutionEvent,
        transaction_family: TransactionFamily,
    ) {
        let p: Option<ParkedExecutionEvents> = match parked.get_mut(&transaction_family) {
            Some(p) => {
                p.push(execution_event);
                None
            }
            None => {
                let mut p = vec![];
                p.push(execution_event);
                Some(p)
            }
        };
        if let Some(p) = p {
            parked.insert(transaction_family, p);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::adapter::test_adapter::TestExecutionAdapter;
    use crate::scheduler::ExecutionTaskCompletionNotification;
    use crate::signing::{hash::HashSigner, Signer};
    use crate::transaction::{HashMethod, TransactionBuilder, TransactionPair};
    use std::{self, collections::HashSet, sync::mpsc::channel};

    static FAMILY_NAME: &str = "test";
    static FAMILY_VERSION: &str = "1.0";
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
    fn test_executer_internal() {
        // Create the three channels with their associated Senders and Receivers.

        let (sender, notification_receiver) = channel::<ExecutionTaskCompletionNotification>();

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

        // Main Executer loop

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

        // Process the ExecutionTask and return an ExecutionTaskCompletionNotification.

        while let Ok(event) = receiver.try_recv() {
            if let ExecutionCommand::Event(execution_event) = event {
                let (result_sender, task) = *execution_event;

                let notification = ExecutionTaskCompletionNotification::Valid(*task.context_id());
                result_sender
                    .send(notification)
                    .expect("The receiver has been dropped");
            }
        }

        // Accumulate the ExecutionTaskCompletionNotification and assert there are 10

        let mut results = vec![];

        while let Ok(result) = notification_receiver.try_recv() {
            results.push(result);
        }

        assert_eq!(
            results.len(),
            NUMBER_OF_TRANSACTIONS,
            "Incorrect number of results received",
        );
    }

    #[test]
    fn test_executer_thread() {
        let noop_adapter = TestExecutionAdapter::new();

        let adapter = noop_adapter.clone();

        let mut executer_thread: ExecuterThread = ExecuterThread::new(vec![Box::new(noop_adapter)]);

        executer_thread
            .start()
            .expect("Start can only be called once");

        let sender = executer_thread
            .sender()
            .expect("Sender is some after start is called");

        let execution_tasks = create_iterator();

        let (s, receiver) = channel();

        for reg_ex_event in execution_tasks
            .map(|execution_task| (s.clone(), execution_task))
            .map(|execution_event| RegistrationExecutionEvent::Execution(Box::new(execution_event)))
        {
            sender
                .send(reg_ex_event)
                .expect("Receiver has been dropped");
        }

        assert!(
            receiver.try_recv().is_err(),
            "The result is not available yet"
        );

        adapter.register("test", "1.0");

        let mut results = vec![];

        while let Ok(result) = receiver.recv_timeout(Duration::from_millis(200)) {
            results.push(result);
        }

        assert_eq!(
            results.len(),
            NUMBER_OF_TRANSACTIONS,
            "Incorrect number of results received",
        );

        executer_thread.stop();
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

struct InternalRegistry {
    registry_sender: RegistrationExecutionEventSender,
    event_sender: NamedExecutionEventSender,
}

impl ExecutionRegistry for InternalRegistry {
    fn register_transaction_family(&mut self, family: TransactionFamily) {
        if let Err(err) = self
            .registry_sender
            .send(RegistrationExecutionEvent::RegistrationChange(
                RegistrationChange::RegisterRequest((family, self.event_sender.clone())),
            ))
        {
            warn!(
                "During sending registration of transaction family on channel: {}",
                err
            );
        }
    }

    fn unregister_transaction_family(&mut self, family: &TransactionFamily) {
        if let Err(err) = self
            .registry_sender
            .send(RegistrationExecutionEvent::RegistrationChange(
                RegistrationChange::UnregisterRequest((family.clone(), self.event_sender.clone())),
            ))
        {
            warn!(
                "During sending unregistration of transaction family on channel: {}",
                err
            );
        }
    }
}

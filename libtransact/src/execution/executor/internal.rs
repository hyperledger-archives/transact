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
//                                                                                                 -------- ExecutionAdapter
// ---- Iterator<Item = ExecutionTask> ----\        | Single thread that                         /
//                                           \      | listens for RegistrationChanges and       /
// ---- Iterator<Item = ExecutionTask> ------------ | ExecutionEvents on the same channel -----/
//                                           /                                                 \
// ---- Iterator<Item = ExecutionTask> ----/                                                    \
//                                                                                                --------- ExecutionAdapter
//

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::JoinHandle;

use log::warn;

use crate::execution::adapter::{ExecutionAdapter, ExecutionAdapterError};
use crate::execution::{ExecutionRegistry, TransactionFamily};
use crate::scheduler::{ExecutionTask, ExecutionTaskCompletionNotifier};

use super::reader::ExecutionTaskReader;

/// The `TransactionPair` and `ContextId` along with where to send
/// results.
pub type ExecutionEvent = (Box<dyn ExecutionTaskCompletionNotifier>, ExecutionTask);

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
pub enum ExecutorCommand {
    RegistrationChange(RegistrationChange),
    Execution(Box<ExecutionEvent>),
    CreateReader(
        Box<dyn Iterator<Item = ExecutionTask> + Send>,
        Box<dyn ExecutionTaskCompletionNotifier>,
    ),
    ReaderDone(usize),
    Shutdown,
}

///`RegistrationChange` and `ExecutionEvent` multiplex sender
pub type ExecutorCommandSender = Sender<ExecutorCommand>;

///`RegistrationChange` and `ExecutionEvent` multiplex sender
pub type ExecutorCommandReceiver = Receiver<ExecutorCommand>;

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
pub enum ExecutorThreadError {
    InvalidState,
    ResourcesUnavailable,
}

impl std::error::Error for ExecutorThreadError {}

impl std::fmt::Display for ExecutorThreadError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ExecutorThreadError::InvalidState => write!(f, "Invalid State: ExecutorThread in an invalid state when 'start' was called"),
            ExecutorThreadError::ResourcesUnavailable => {
                write!(f, "ResourcesUnavailable: ExecutorThread unable to access a resource needed for operation")
            }
        }
    }
}

pub struct ExecutorThread {
    execution_adapters: Vec<Box<dyn ExecutionAdapter>>,
    join_handles: Vec<JoinHandle<()>>,
    internal_thread: Option<JoinHandle<()>>,
    sender: Option<ExecutorCommandSender>,
}

impl ExecutorThread {
    pub fn new(execution_adapters: Vec<Box<dyn ExecutionAdapter>>) -> Self {
        ExecutorThread {
            execution_adapters,
            join_handles: vec![],
            internal_thread: None,
            sender: None,
        }
    }

    pub fn sender(&self) -> Option<ExecutorCommandSender> {
        self.sender.as_ref().cloned()
    }

    pub fn start(&mut self) -> Result<(), ExecutorThreadError> {
        if self.sender.is_none() {
            let (registry_sender, receiver) = channel();

            for (index, mut execution_adapter) in self.execution_adapters.drain(0..).enumerate() {
                let (sender, adapter_receiver) = channel();
                let ee_sender = NamedExecutionEventSender::new(sender, index);

                if let Err(err) = execution_adapter.start(Box::new(InternalRegistry {
                    event_sender: ee_sender,
                    registry_sender: registry_sender.clone(),
                })) {
                    warn!("Unable to start execution adapter: {}", err);
                    return Err(ExecutorThreadError::ResourcesUnavailable);
                }

                match Self::start_execution_adapter_thread(
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
                        return Err(ExecutorThreadError::ResourcesUnavailable);
                    }
                }
            }

            self.sender = Some(registry_sender.clone());
            match self.start_thread(registry_sender, receiver) {
                Ok(join_handle) => {
                    self.internal_thread = Some(join_handle);
                }
                Err(err) => {
                    warn!("unable to start internal executor thread: {}", err);
                    return Err(ExecutorThreadError::ResourcesUnavailable);
                }
            }

            Ok(())
        } else {
            Err(ExecutorThreadError::InvalidState)
        }
    }

    pub fn stop(mut self) {
        if let Some(sender) = self.sender.take() {
            if let Err(err) = sender.send(ExecutorCommand::Shutdown) {
                warn!("Unable to send shutdown signal to executor thread: {}", err);
            }

            if let Some(internal) = self.internal_thread.take() {
                if let Err(err) = internal.join() {
                    warn!("During stop of executor thread: {:?}", err);
                }
            }
        }
    }

    fn start_execution_adapter_thread(
        execution_adapter: Box<dyn ExecutionAdapter>,
        receiver: ExecutionEventReceiver,
        sender: &ExecutorCommandSender,
        index: usize,
    ) -> Result<JoinHandle<()>, std::io::Error> {
        let sender = sender.clone();

        std::thread::Builder::new()
            .name(format!("execution_adapter_thread_{}", index))
            .spawn(move || {
                while let Ok(execution_command) = receiver.recv() {
                    match execution_command {
                        ExecutionCommand::Event(execution_event) => {
                            let sender = sender.clone();
                            let (completion_notifier, task) = *execution_event;
                            let (pair, context_id) = task.take();

                            let callback = Box::new(move |result| {
                                // Without this line, the function is considered a FnOnce, instead
                                // of an Fn.  This seems to be a strange quirk of the compiler
                                let completion_notifier = completion_notifier.clone();
                                match result {
                                    Ok(tp_processing_result) => {
                                        completion_notifier.notify(tp_processing_result);
                                    }
                                    Err(ExecutionAdapterError::TimeoutError(transaction_pair)) => {
                                        let execution_task =
                                            ExecutionTask::new(*transaction_pair, context_id);
                                        let execution_event = (completion_notifier, execution_task);
                                        if let Err(err) = sender.send(ExecutorCommand::Execution(
                                            Box::new(execution_event),
                                        )) {
                                            warn!("During retry of TimeOutError: {}", err);
                                        }
                                    }
                                    Err(ExecutionAdapterError::RoutingError(transaction_pair)) => {
                                        let execution_task =
                                            ExecutionTask::new(*transaction_pair, context_id);
                                        let execution_event = (completion_notifier, execution_task);
                                        if let Err(err) = sender.send(ExecutorCommand::Execution(
                                            Box::new(execution_event),
                                        )) {
                                            warn!("During retry of RoutingError: {}", err);
                                        }
                                    }
                                    Err(ExecutionAdapterError::GeneralExecutionError(err)) => {
                                        error!("General Execution Error: {}", err);
                                    }
                                }
                            });
                            if let Err(err) = execution_adapter.execute(pair, context_id, callback)
                            {
                                error!("Unable to execute on adapter {}: {}", index, err);
                                break;
                            }
                        }
                        ExecutionCommand::Sentinel => {
                            break;
                        }
                    }
                }

                if let Err(err) = execution_adapter.stop() {
                    error!("Unable to cleanly stop adapter {}: {}", index, err);
                }
            })
    }

    fn start_thread(
        &self,
        sender: ExecutorCommandSender,
        receiver: ExecutorCommandReceiver,
    ) -> Result<JoinHandle<()>, std::io::Error> {
        std::thread::Builder::new()
            .name("internal_executor_thread".to_string())
            .spawn(move || {
                let mut fanout_threads: HashMap<
                    TransactionFamily,
                    HashSet<NamedExecutionEventSender>,
                > = HashMap::new();
                let mut parked: ParkedExecutionEventsMap = HashMap::new();
                let mut unparked = vec![];
                let mut reader_index = 0usize;
                let mut readers: BTreeMap<usize, ExecutionTaskReader> = BTreeMap::new();
                loop {
                    for execution_event in unparked.drain(0..) {
                        Self::try_send_execution_event(
                            Box::new(execution_event),
                            &fanout_threads,
                            &mut parked,
                        );
                    }

                    match receiver.recv() {
                        Ok(ExecutorCommand::Execution(execution_event)) => {
                            Self::try_send_execution_event(
                                execution_event,
                                &fanout_threads,
                                &mut parked,
                            )
                        }
                        Ok(ExecutorCommand::RegistrationChange(
                            RegistrationChange::RegisterRequest((transaction_family, sender)),
                        )) => {
                            if let Some(p) = parked.get_mut(&transaction_family) {
                                unparked.append(p);
                            }

                            let found = if let Some(ea_senders) =
                                fanout_threads.get_mut(&transaction_family)
                            {
                                ea_senders.insert(sender);
                                None
                            } else {
                                // ignore clippy error, must match type of ea_senders
                                #[allow(clippy::mutable_key_type)]
                                let mut s = HashSet::new();
                                s.insert(sender);
                                Some(s)
                            };

                            if let Some(f) = found {
                                fanout_threads.insert(transaction_family, f);
                            }
                        }
                        Ok(ExecutorCommand::RegistrationChange(
                            RegistrationChange::UnregisterRequest((transaction_family, sender)),
                        )) => {
                            fanout_threads
                                .entry(transaction_family)
                                .and_modify(|ea_senders| {
                                    ea_senders.remove(&sender);
                                });
                        }

                        Ok(ExecutorCommand::CreateReader(task_iterator, notifier)) => {
                            let (index, _) = reader_index.overflowing_add(1);
                            let mut reader = ExecutionTaskReader::new(index);

                            if let Err(err) = reader.start(task_iterator, notifier, sender.clone())
                            {
                                error!("Unable to start task reader: {}", err);
                                continue;
                            }

                            readers.insert(index, reader);
                            reader_index = index;
                        }

                        Ok(ExecutorCommand::ReaderDone(reader_id)) => {
                            if let Some(reader) = readers.remove(&reader_id) {
                                reader.stop();
                            }
                        }

                        Ok(ExecutorCommand::Shutdown) => {
                            break;
                        }
                        Err(err) => {
                            error!("Received error while processing executor commands: {}", err);
                            break;
                        }
                    }
                }

                Self::shutdown_fanout_threads(&fanout_threads);
                for (_, reader) in readers.into_iter() {
                    reader.stop();
                }
            })
    }

    fn shutdown_fanout_threads(
        fanout_threads: &HashMap<TransactionFamily, HashSet<NamedExecutionEventSender>>,
    ) {
        for sender in fanout_threads
            .values()
            .fold(HashSet::new(), |mut set, item| {
                for s in item {
                    set.insert(s);
                }
                set
            })
        {
            if let Err(err) = sender.sender.send(ExecutionCommand::Sentinel) {
                warn!("During stop of ExecutorThread internal thread: {}", err);
            }
        }
    }

    fn try_send_execution_event(
        execution_event: Box<ExecutionEvent>,
        fanout_threads: &HashMap<TransactionFamily, HashSet<NamedExecutionEventSender>>,
        parked: &mut ParkedExecutionEventsMap,
    ) {
        let tf = TransactionFamily::from_pair(&execution_event.1.pair());
        if let Some(ea_senders) = fanout_threads.get(&tf) {
            if let Some(sender) = ea_senders.iter().next() {
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

    use std::{self, collections::HashSet, sync::mpsc::channel};

    use cylinder::{secp256k1::Secp256k1Context, Context, Signer};

    use crate::execution::adapter::test_adapter::TestExecutionAdapter;
    use crate::protocol::transaction::{HashMethod, TransactionBuilder, TransactionPair};
    use crate::scheduler::ExecutionTaskCompletionNotification;

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
    fn test_executor_internal() {
        // Create the three channels with their associated Senders and Receivers.

        let (sender, notification_receiver) = channel::<ExecutionTaskCompletionNotification>();

        let notifier: Box<dyn ExecutionTaskCompletionNotifier> =
            Box::new(ChannelExecutionTaskCompletionNotifier { tx: sender });

        let (registration_execution_event_sender, internal_receiver): (
            ExecutorCommandSender,
            ExecutorCommandReceiver,
        ) = channel::<ExecutorCommand>();

        let (execution_adapter_sender, receiver) = channel::<ExecutionCommand>();

        // Register the transaction family

        let tf = TransactionFamily::new(FAMILY_NAME.to_string(), FAMILY_VERSION.to_string());
        let named_sender = NamedExecutionEventSender::new(execution_adapter_sender, 0);
        let registration_event = ExecutorCommand::RegistrationChange(
            RegistrationChange::RegisterRequest((tf, named_sender)),
        );

        registration_execution_event_sender
            .send(registration_event)
            .expect("The receiver is dropped");

        let execution_tasks = create_iterator();

        // Send the ExecutionEvents on the multiplexing channel.

        for reg_ex_event in execution_tasks
            .map(|execution_task| (notifier.clone(), execution_task))
            .map(|execution_event| ExecutorCommand::Execution(Box::new(execution_event)))
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
                ExecutorCommand::Execution(execution_event) => {
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
                ExecutorCommand::RegistrationChange(registration_event) => match registration_event
                {
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
                },
                _ => panic!("Should not have sent command during test"),
            }
        }

        // Process the ExecutionTask and return an ExecutionTaskCompletionNotification.

        while let Ok(event) = receiver.try_recv() {
            if let ExecutionCommand::Event(execution_event) = event {
                let (notifier, task) = *execution_event;

                let notification = ExecutionTaskCompletionNotification::Valid(
                    *task.context_id(),
                    task.pair().transaction().header_signature().into(),
                );
                notifier.notify(notification);
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
    fn test_executor_thread() {
        let noop_adapter = TestExecutionAdapter::new();

        let adapter = noop_adapter.clone();

        let mut executor_thread: ExecutorThread = ExecutorThread::new(vec![Box::new(noop_adapter)]);

        executor_thread
            .start()
            .expect("Start can only be called once");

        let sender = executor_thread
            .sender()
            .expect("Sender is some after start is called");

        let execution_tasks = create_iterator();

        let (tx, receiver) = channel();
        let notifier: Box<dyn ExecutionTaskCompletionNotifier> =
            Box::new(ChannelExecutionTaskCompletionNotifier { tx });

        for reg_ex_event in execution_tasks
            .map(|execution_task| (notifier.clone(), execution_task))
            .map(|execution_event| ExecutorCommand::Execution(Box::new(execution_event)))
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

        for _ in 0..NUMBER_OF_TRANSACTIONS {
            if let Ok(result) = receiver.recv() {
                results.push(result);
            }
        }

        assert_eq!(
            results.len(),
            NUMBER_OF_TRANSACTIONS,
            "Incorrect number of results received",
        );

        executor_thread.stop();
    }

    fn create_txn(signer: &dyn Signer) -> TransactionPair {
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
        let context_id = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let signer = new_signer();

        (0..NUMBER_OF_TRANSACTIONS)
            .map(move |_| create_txn(&*signer))
            .map(move |txn_pair| ExecutionTask::new(txn_pair, context_id.clone()))
    }

    fn new_signer() -> Box<dyn Signer> {
        let context = Secp256k1Context::new();
        let key = context.new_random_private_key();
        context.new_signer(key)
    }

    #[derive(Clone)]
    struct ChannelExecutionTaskCompletionNotifier {
        tx: Sender<ExecutionTaskCompletionNotification>,
    }

    impl ExecutionTaskCompletionNotifier for ChannelExecutionTaskCompletionNotifier {
        fn notify(&self, notification: ExecutionTaskCompletionNotification) {
            self.tx
                .send(notification)
                .expect("Unable to send the notification");
        }

        fn clone_box(&self) -> Box<dyn ExecutionTaskCompletionNotifier> {
            Box::new(self.clone())
        }
    }
}

struct InternalRegistry {
    registry_sender: ExecutorCommandSender,
    event_sender: NamedExecutionEventSender,
}

impl ExecutionRegistry for InternalRegistry {
    fn register_transaction_family(&mut self, family: TransactionFamily) {
        if let Err(err) = self
            .registry_sender
            .send(ExecutorCommand::RegistrationChange(
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
            .send(ExecutorCommand::RegistrationChange(
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

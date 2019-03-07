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

use crate::context::ContextId;
use crate::execution::adapter::{ExecutionAdapter, ExecutionAdapterError};
use crate::execution::{ExecutionRegistry, TransactionFamily};
use crate::protocol::transaction::TransactionPair;
use crate::scheduler::ExecutionTaskCompletionNotification;
use std::sync::{Arc, Mutex};

struct TestExecutionAdapterState {
    registry: Option<Box<dyn ExecutionRegistry>>,
    available: bool,
}

#[derive(Clone)]
pub struct TestExecutionAdapter {
    state: Arc<Mutex<TestExecutionAdapterState>>,
}

impl TestExecutionAdapter {
    pub fn new() -> Self {
        TestExecutionAdapter {
            state: Arc::new(Mutex::new(TestExecutionAdapterState {
                registry: None,
                available: false,
            })),
        }
    }

    pub fn register(&self, name: &str, version: &str) {
        self.state
            .lock()
            .expect("Noop mutex is poisoned")
            .register(name, version);
    }

    pub fn unregister(&self, name: &str, version: &str) {
        self.state
            .lock()
            .expect("Noop mutex is poisoned")
            .unregister(name, version);
    }
}

impl ExecutionAdapter for TestExecutionAdapter {
    fn start(&mut self, execution_registry: Box<dyn ExecutionRegistry>) {
        self.state
            .lock()
            .expect("mutex is not poisoned")
            .on_start(execution_registry);
    }

    fn execute(
        &self,
        transaction_pair: TransactionPair,
        _context_id: ContextId,
        on_done: Box<
            dyn Fn(Result<ExecutionTaskCompletionNotification, ExecutionAdapterError>) + Send,
        >,
    ) {
        self.state.lock().expect("mutex is not poisoned").execute(
            transaction_pair,
            _context_id,
            on_done,
        );
    }

    fn stop(self: Box<Self>) -> bool {
        true
    }
}

impl TestExecutionAdapterState {
    fn on_start(&mut self, callback: Box<dyn ExecutionRegistry>) {
        self.registry = Some(callback);
    }

    fn execute(
        &self,
        transaction_pair: TransactionPair,
        context_id: ContextId,
        on_done: Box<
            dyn Fn(Result<ExecutionTaskCompletionNotification, ExecutionAdapterError>) + Send,
        >,
    ) {
        on_done(if self.available {
            Ok(ExecutionTaskCompletionNotification::Valid(context_id))
        } else {
            Err(ExecutionAdapterError::RoutingError(Box::new(
                transaction_pair,
            )))
        });
    }

    fn register(&mut self, name: &str, version: &str) {
        if let Some(registry) = &mut self.registry {
            self.available = true;
            let tf = TransactionFamily::new(name.to_string(), version.to_string());
            registry.register_transaction_family(tf);
        }
    }

    fn unregister(&mut self, name: &str, version: &str) {
        if let Some(registry) = &mut self.registry {
            self.available = false;
            let tf = TransactionFamily::new(name.to_string(), version.to_string());
            registry.unregister_transaction_family(&tf)
        }
    }
}

impl Default for TestExecutionAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::protocol::transaction::{HashMethod, TransactionBuilder};
    use crate::signing::hash::HashSigner;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    static FAMILY_VERSION: &str = "1.0";

    #[test]
    fn test_noop_adapter() {
        let mut noop_adapter = TestExecutionAdapter::new();
        let registry = MockRegistry::default();

        let transaction_pair1 = make_transaction();
        let transaction_pair2 = make_transaction();

        noop_adapter.start(Box::new(registry.clone()));

        noop_adapter.register("test", "1.0");

        assert!(
            registry.registered.load(Ordering::Relaxed),
            "The noop adapter is registered",
        );

        let context_id = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

        let on_done = Box::new(
            move |notification: Result<
                ExecutionTaskCompletionNotification,
                ExecutionAdapterError,
            >| {
                assert!(
                    notification.is_ok(),
                    "There was no error handling the transaction"
                );
                assert!(
                    match notification.unwrap() {
                        ExecutionTaskCompletionNotification::Valid(_) => true,
                        _ => false,
                    },
                    "The transaction was not valid"
                );
            },
        );

        noop_adapter.execute(transaction_pair1, context_id.clone(), on_done);

        let on_done_error = Box::new(
            move |notification: Result<
                ExecutionTaskCompletionNotification,
                ExecutionAdapterError,
            >| {
                assert!(
                    notification.is_err(),
                    "There was an error due to the TransactionFamily not being registered"
                );
            },
        );

        noop_adapter.unregister("test", "1.0");

        assert!(
            !registry.registered.load(Ordering::Relaxed),
            "The noop adapter is unregistered",
        );

        noop_adapter.execute(transaction_pair2, context_id, on_done_error);
    }

    fn make_transaction() -> TransactionPair {
        let signer = HashSigner::new();

        TransactionBuilder::new()
            .with_batcher_public_key(vec![])
            .with_dependencies(vec![vec![]])
            .with_family_name("test".to_string())
            .with_family_version(FAMILY_VERSION.to_string())
            .with_inputs(vec![vec![]])
            .with_outputs(vec![vec![]])
            .with_nonce(vec![])
            .with_payload(vec![])
            .with_payload_hash_method(HashMethod::SHA512)
            .build_pair(&signer)
            .expect("The TransactionBuilder was supplied all the options")
    }

    #[derive(Clone, Default)]
    struct MockRegistry {
        registered: Arc<AtomicBool>,
    }

    impl ExecutionRegistry for MockRegistry {
        fn register_transaction_family(&mut self, _family: TransactionFamily) {
            self.registered.store(true, Ordering::Relaxed);
        }

        fn unregister_transaction_family(&mut self, _family: &TransactionFamily) {
            self.registered.store(false, Ordering::Relaxed);
        }
    }
}

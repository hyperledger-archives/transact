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
use crate::execution::adapter::{
    ExecutionAdapter, ExecutionAdapterError, ExecutionResult, OnDoneCallback, OnRegisterCallback,
    OnUnregisterCallback, TransactionFamily, TransactionStatus,
};
use crate::transaction::TransactionPair;
use std::sync::{Arc, Mutex};

struct TestExecutionAdapterState {
    registration_callback: Option<Box<OnRegisterCallback>>,
    unregistration_callback: Option<Box<OnUnregisterCallback>>,
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
                registration_callback: None,
                unregistration_callback: None,
                available: false,
            })),
        }
    }

    pub fn register(&self) {
        self.state
            .lock()
            .expect("Noop mutex is poisoned")
            .register();
    }

    pub fn unregister(&self) {
        self.state
            .lock()
            .expect("Noop mutex is poisoned")
            .unregister();
    }
}

impl ExecutionAdapter for TestExecutionAdapter {
    fn on_register(&self, callback: Box<OnRegisterCallback>) {
        self.state
            .lock()
            .expect("mutex is not poisoned")
            .on_register(callback);
    }

    fn on_unregister(&self, callback: Box<OnUnregisterCallback>) {
        self.state
            .lock()
            .expect("mutex is not poisoned")
            .on_unregister(callback);
    }

    fn execute(
        &self,
        transaction_pair: TransactionPair,
        _context_id: ContextId,
        on_done: Box<OnDoneCallback>,
    ) {
        self.state.lock().expect("mutex is not poisoned").execute(
            transaction_pair,
            _context_id,
            on_done,
        );
    }
}

impl TestExecutionAdapterState {
    fn on_register(&mut self, callback: Box<OnRegisterCallback>) {
        self.registration_callback = Some(callback);
    }

    fn on_unregister(&mut self, callback: Box<OnUnregisterCallback>) {
        self.unregistration_callback = Some(callback);
    }

    fn execute(
        &self,
        transaction_pair: TransactionPair,
        _context_id: ContextId,
        on_done: Box<OnDoneCallback>,
    ) {
        let mut on_done = on_done;
        if self.available {
            let transaction_status = TransactionStatus::Valid;

            let transaction_result = ExecutionResult {
                transaction_id: transaction_pair
                    .transaction()
                    .header_signature()
                    .to_string(),
                status: transaction_status,
            };

            on_done(Ok(transaction_result));
        } else {
            on_done(Err(ExecutionAdapterError::RoutingError(transaction_pair)));
        }
    }

    fn register(&mut self) {
        if let Some(register_callback) = &mut self.registration_callback {
            self.available = true;
            let tf = TransactionFamily::new("test".to_string(), "1.0".to_string());
            register_callback(tf);
        }
    }

    fn unregister(&mut self) {
        if let Some(unregister_callback) = &mut self.unregistration_callback {
            self.available = false;
            let tf = TransactionFamily::new("test".to_string(), "1.0".to_string());
            unregister_callback(tf)
        }
    }
}

impl Default for TestExecutionAdapter {
    fn default() -> Self {
        Self::new()
    }
}

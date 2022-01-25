// Copyright 2021 Cargill Incorporated
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
// limitations under the License

//! An example application that uses the command family workload to generate
//! transactions and run them with Sabre

#[macro_use]
extern crate log;

mod error;

use std::io::Read;

use clap::{App, Arg};
use cylinder::{secp256k1::Secp256k1Context, Context, Signer};
use flexi_logger::{DeferredNow, LogSpecBuilder, Logger};
use log::Record;
use protobuf::Message;
use sawtooth_sabre::{
    handler::SabreTransactionHandler, ADMINISTRATORS_SETTING_ADDRESS, ADMINISTRATORS_SETTING_KEY,
};
use transact::context::{manager::sync::ContextManager, ContextLifecycle};
use transact::database::btree::BTreeDatabase;
use transact::execution::{
    adapter::static_adapter::StaticExecutionAdapter,
    executor::{ExecutionTaskSubmitter, Executor},
};
use transact::families::command::workload::{
    CommandBatchWorkload, CommandGeneratingIter, CommandTransactionWorkload,
};
use transact::protocol::{
    batch::{BatchBuilder, BatchPair},
    receipt::{StateChange, TransactionResult},
    sabre::payload::{
        CreateContractActionBuilder, CreateContractRegistryActionBuilder,
        CreateNamespaceRegistryActionBuilder, CreateNamespaceRegistryPermissionActionBuilder,
    },
    transaction::Transaction,
};
use transact::protos::sabre_payload::{Setting, Setting_Entry};
use transact::sawtooth::SawtoothToTransactHandlerAdapter;
use transact::scheduler::{serial::SerialScheduler, BatchExecutionResult, Scheduler};
use transact::state::{
    merkle::{self, MerkleRadixTree, MerkleState},
    StateChange as ChangeSet, Write,
};
use transact::workload::BatchWorkload;

use crate::error::SabreCommandExecutorError;

const APP_NAME: &str = env!("CARGO_PKG_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");

const COMMAND_NAME: &str = "command";
const COMMAND_VERSION: &str = "1.0";
const COMMAND_PREFIX: &str = "06abbc";

fn main() {
    if let Err(e) = run() {
        error!("ERROR: {}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<(), SabreCommandExecutorError> {
    let mut app = App::new(APP_NAME);

    app = app
        .version(VERSION)
        .author("Cargill")
        .about("Command line for command executor app")
        .arg(
            Arg::with_name("verbose")
                .help("Log verbosely")
                .short("v")
                .global(true)
                .multiple(true),
        )
        .arg(
            Arg::with_name("transactions")
                .long("transactions")
                .short("n")
                .takes_value(true)
                .required(true)
                .help("Number of transactions to run"),
        )
        .arg(
            Arg::with_name("contract")
                .long("contract")
                .short("c")
                .takes_value(true)
                .required(true)
                .help("The file path of the command smart contract .wasm file"),
        );

    let matches = app.get_matches();

    let log_level = match matches.occurrences_of("verbose") {
        0 => log::LevelFilter::Warn,
        1 => log::LevelFilter::Info,
        2 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    };
    setup_logging(log_level);

    let contract = matches.value_of("contract").ok_or_else(|| {
        SabreCommandExecutorError::InvalidState("missing argument 'contract'".into())
    })?;

    let transactions = matches.value_of("transactions").ok_or_else(|| {
        SabreCommandExecutorError::InvalidState("missing argument 'transactions'".into())
    })?;
    let num_transactions = transactions.parse::<u32>().map_err(|_| {
        SabreCommandExecutorError::InvalidArgument("'transactions' must be a valid integer".into())
    })?;

    let mut submitted_transactions = 0;

    let generator = CommandGeneratingIter::new(10);
    let signer = create_signer();

    let transaction_workload = CommandTransactionWorkload::new(generator, signer.clone());

    let mut command_workload = CommandBatchWorkload::new(transaction_workload, signer.clone());

    let db = Box::new(BTreeDatabase::new(&merkle::INDEXES));
    let merkle_db = MerkleRadixTree::new(db.clone(), None).map_err(|err| {
        SabreCommandExecutorError::Internal(format!("Unable to create MerkleRadixTree: {}", err))
    })?;
    let merkle_state = MerkleState::new(db.clone());

    let context_manager = ContextManager::new(Box::new(MerkleState::new(db)));
    let mut executor = create_executor(&context_manager)?;
    executor.start().map_err(|err| {
        SabreCommandExecutorError::Internal(format!("Failed to start executor: {}", err))
    })?;
    let task_executor = executor.execution_task_submitter().map_err(|err| {
        SabreCommandExecutorError::Internal(format!(
            "Unable to get task executor after starting executor: {}",
            err
        ))
    })?;

    // Get the signer's public key to be used as the admin key
    let admin_key = signer.public_key().map_err(|err| {
        SabreCommandExecutorError::Internal(format!("Unable to get signer public key: {}", err))
    })?;

    // Set the admin key
    let admin_keys_state_change = set_admin_key_state(admin_key.as_hex())?;

    let initial_state_root = merkle_db.get_merkle_root();
    let mut current_state_root = merkle_state
        .commit(
            &initial_state_root,
            vec![admin_keys_state_change].as_slice(),
        )
        .map_err(|_| SabreCommandExecutorError::Internal("Unable to commit".into()))?;

    // Create the initial transactions to use the command smart contract
    // in sabre
    let txns = vec![
        create_contract_registry_txn(&*signer)?,
        upload_contract_txn(&*signer, contract)?,
        create_command_namespace_registry_txn(&*signer)?,
        command_namespace_permissions_txn(&*signer)?,
    ];
    let setup_batch = BatchBuilder::new()
        .with_transactions(txns)
        .build_pair(&*signer)
        .map_err(|err| {
            SabreCommandExecutorError::InvalidState(format!(
                "Unable to build batch pair for sabre setup: {}",
                err
            ))
        })?;

    // Submit the batch containing the transactions for uploading the command
    // smart contract
    let current_result = execute_transaction(
        &task_executor,
        Box::new(context_manager.clone()),
        &current_state_root,
        setup_batch,
    )?;

    // let state_change_pairs = get_state_change(current_result);
    let state_changes = get_state_change(current_result);

    for change in state_changes {
        current_state_root = merkle_state
            .commit(&current_state_root, vec![change].as_slice())
            .map_err(|e| SabreCommandExecutorError::Internal(e.to_string()))?;
    }

    // Submit one command transaction each loop until the number of requested
    // transactions have been submitted
    while submitted_transactions < num_transactions {
        // Get a batch from the command workload
        let (next_batch, _) = command_workload.next_batch().map_err(|err| {
            SabreCommandExecutorError::Internal(format!(
                "Unable to get command transaction batch: {}",
                err
            ))
        })?;
        // Submit the batch to sabre
        let current_result = execute_transaction(
            &task_executor,
            Box::new(context_manager.clone()),
            &current_state_root,
            next_batch,
        )?;

        let state_changes = get_state_change(current_result);

        for change in state_changes {
            current_state_root = merkle_state
                .commit(&current_state_root, vec![change].as_slice())
                .map_err(|e| SabreCommandExecutorError::Internal(e.to_string()))?;
        }

        submitted_transactions += 1;
        info!("Total transactions submitted: {}", submitted_transactions);
    }

    Ok(())
}

fn set_admin_key_state(admin_key: String) -> Result<ChangeSet, SabreCommandExecutorError> {
    let mut admin_keys_entry = Setting_Entry::new();
    admin_keys_entry.set_key(ADMINISTRATORS_SETTING_KEY.into());
    admin_keys_entry.set_value(admin_key);

    let mut admin_keys_setting = Setting::new();
    admin_keys_setting.set_entries(vec![admin_keys_entry].into());

    let admin_keys_setting_bytes = admin_keys_setting.write_to_bytes().map_err(|err| {
        SabreCommandExecutorError::InvalidState(format!(
            "Unable to write admin keys setting to bytes: {}",
            err
        ))
    })?;

    Ok(ChangeSet::Set {
        key: ADMINISTRATORS_SETTING_ADDRESS.into(),
        value: admin_keys_setting_bytes,
    })
}

fn get_state_change(result: BatchExecutionResult) -> Vec<ChangeSet> {
    let results = get_result(result);

    let mut change_sets = Vec::new();

    for state_changes in results {
        for state_change in state_changes {
            match state_change {
                StateChange::Set { key, value } => change_sets.push(ChangeSet::Set {
                    key: key.clone(),
                    value,
                }),
                StateChange::Delete { key } => {
                    change_sets.push(ChangeSet::Delete { key: key.clone() })
                }
            }
        }
    }

    change_sets
}

fn get_result(batch_result: BatchExecutionResult) -> Vec<Vec<StateChange>> {
    let mut results = Vec::new();

    for receipt in batch_result.receipts {
        match receipt.transaction_result {
            TransactionResult::Valid { state_changes, .. } => results.push(state_changes),
            TransactionResult::Invalid { error_message, .. } => {
                // Check to see if the invalid transaction result is expected because
                // one of the possible transactions created by the workload is RETURN_INVALID
                if error_message
                    .contains("Wasm contract returned invalid transaction: command, 1.0")
                {
                    continue;
                } else {
                    panic!("Transaction failed: {:?}", error_message)
                }
            }
        }
    }
    results
}

fn create_executor(
    context_manager: &ContextManager,
) -> Result<Executor, SabreCommandExecutorError> {
    Ok(Executor::new(vec![Box::new(
        StaticExecutionAdapter::new_adapter(
            vec![Box::new(SawtoothToTransactHandlerAdapter::new(
                SabreTransactionHandler::new(),
            ))],
            context_manager.clone(),
        )
        .map_err(|err| {
            SabreCommandExecutorError::Internal(format!(
                "Unable to create StaticExecutionAdapter: {}",
                err
            ))
        })?,
    )]))
}

fn create_signer() -> Box<dyn Signer> {
    let context = Secp256k1Context::new();
    let key = context.new_random_private_key();
    context.new_signer(key)
}

fn execute_transaction(
    task_executor: &ExecutionTaskSubmitter,
    context_lifecycle: Box<dyn ContextLifecycle>,
    state_root: &str,
    batch_pair: BatchPair,
) -> Result<BatchExecutionResult, SabreCommandExecutorError> {
    let mut scheduler =
        SerialScheduler::new(context_lifecycle, state_root.to_string()).map_err(|err| {
            SabreCommandExecutorError::Internal(format!("Unable to create scheduler: {}", err))
        })?;

    let (result_sender, result_receiver) = std::sync::mpsc::channel();

    scheduler
        .set_result_callback(Box::new(move |batch_result| {
            if let Err(err) = result_sender.send(batch_result) {
                error!("Error: unable to send batch result: {}", err)
            }
        }))
        .map_err(|err| {
            SabreCommandExecutorError::Internal(format!("Unable to set result callback: {}", err))
        })?;

    scheduler.add_batch(batch_pair).map_err(|err| {
        SabreCommandExecutorError::Internal(format!("Unable to add batch to scheduler: {}", err))
    })?;
    scheduler.finalize().map_err(|err| {
        SabreCommandExecutorError::Internal(format!("Failed to finalize scheduler: {}", err))
    })?;

    run_schedule(task_executor, &mut scheduler)?;

    let mut result: Option<BatchExecutionResult> = None;
    loop {
        match result_receiver.recv() {
            Ok(Some(res)) => result = Some(res),
            Ok(None) => {
                break result.ok_or_else(|| {
                    SabreCommandExecutorError::Internal(
                        "No batch result was returned from the execution".into(),
                    )
                })
            }
            Err(_) => {
                break Err(SabreCommandExecutorError::Internal(
                    "Unable to receive batch result".into(),
                ))
            }
        }
    }
}

fn run_schedule(
    executor: &ExecutionTaskSubmitter,
    scheduler: &mut dyn Scheduler,
) -> Result<(), SabreCommandExecutorError> {
    let task_iterator = scheduler.take_task_iterator().map_err(|err| {
        SabreCommandExecutorError::Internal(format!("Unable to take task iterator: {}", err))
    })?;
    executor
        .submit(
            task_iterator,
            scheduler.new_notifier().map_err(|err| {
                SabreCommandExecutorError::Internal(format!("Unable to get notifier: {}", err))
            })?,
        )
        .map_err(|err| {
            SabreCommandExecutorError::Internal(format!("Failed to execute schedule: {}", err))
        })?;
    Ok(())
}

fn setup_logging(log_level: log::LevelFilter) {
    let mut log_spec_builder = LogSpecBuilder::new();
    log_spec_builder.default(log_level);

    match Logger::with(log_spec_builder.build())
        .format(log_format)
        .log_to_stdout()
        .start()
    {
        Ok(_) => {}
        Err(err) => panic!("Failed to start logger: {}", err),
    }
}

// log format for cli that will only show the log message
pub fn log_format(
    w: &mut dyn std::io::Write,
    _now: &mut DeferredNow,
    record: &Record,
) -> Result<(), std::io::Error> {
    write!(w, "{}", record.args(),)
}

fn create_contract_registry_txn(
    signer: &dyn Signer,
) -> Result<Transaction, SabreCommandExecutorError> {
    let public_key = signer.public_key().map_err(|err| {
        SabreCommandExecutorError::Internal(format!("Unable to get signer public key: {}", err))
    })?;
    CreateContractRegistryActionBuilder::new()
        .with_name(COMMAND_NAME.into())
        .with_owners(vec![public_key.as_hex()])
        .into_payload_builder()
        .map_err(|err| {
            SabreCommandExecutorError::Internal(format!(
                "Unable to get sabre payload for contract registry creation: {}",
                err
            ))
        })?
        .into_transaction_builder()
        .map_err(|err| {
            SabreCommandExecutorError::Internal(format!(
                "Unable to get transaction builder for contract registry creation: {}",
                err
            ))
        })?
        .build(signer)
        .map_err(|err| {
            SabreCommandExecutorError::Internal(format!(
                "Unable to build create contract registry transaction: {}",
                err
            ))
        })
}

fn upload_contract_txn(
    signer: &dyn Signer,
    contract: &str,
) -> Result<Transaction, SabreCommandExecutorError> {
    let contract_path = std::path::Path::new(contract);
    let contract_file = std::fs::File::open(contract_path).map_err(|err| {
        SabreCommandExecutorError::InvalidState(format!(
            "Failed to open command contract file: {}",
            err
        ))
    })?;
    let mut buf_reader = std::io::BufReader::new(contract_file);
    let mut contract = Vec::new();
    buf_reader.read_to_end(&mut contract).map_err(|err| {
        SabreCommandExecutorError::Internal(format!("IoError while reading contract: {}", err))
    })?;

    let action_addresses = vec![COMMAND_PREFIX.into()];

    CreateContractActionBuilder::new()
        .with_name(COMMAND_NAME.into())
        .with_version(COMMAND_VERSION.into())
        .with_inputs(action_addresses.clone())
        .with_outputs(action_addresses)
        .with_contract(contract)
        .into_payload_builder()
        .map_err(|err| {
            SabreCommandExecutorError::Internal(format!(
                "Unable to get sabre payload for contract creation: {}",
                err
            ))
        })?
        .into_transaction_builder()
        .map_err(|err| {
            SabreCommandExecutorError::Internal(format!(
                "Unable to get transaction builder for contract creation: {}",
                err
            ))
        })?
        .build(signer)
        .map_err(|err| {
            SabreCommandExecutorError::Internal(format!(
                "Unable to build create contract transaction: {}",
                err
            ))
        })
}

fn create_command_namespace_registry_txn(
    signer: &dyn Signer,
) -> Result<Transaction, SabreCommandExecutorError> {
    let public_key = signer.public_key().map_err(|err| {
        SabreCommandExecutorError::Internal(format!("Unable to get signer public key: {}", err))
    })?;
    CreateNamespaceRegistryActionBuilder::new()
        .with_namespace(COMMAND_PREFIX.into())
        .with_owners(vec![public_key.as_hex()])
        .into_payload_builder()
        .map_err(|err| {
            SabreCommandExecutorError::Internal(format!(
                "Unable to get sabre payload for namespace registry creation: {}",
                err
            ))
        })?
        .into_transaction_builder()
        .map_err(|err| {
            SabreCommandExecutorError::Internal(format!(
                "Unable to get transaction builder for namespace registry creation: {}",
                err
            ))
        })?
        .build(signer)
        .map_err(|err| {
            SabreCommandExecutorError::Internal(format!(
                "Unable to build create namespace registry transaction: {}",
                err
            ))
        })
}

fn command_namespace_permissions_txn(
    signer: &dyn Signer,
) -> Result<Transaction, SabreCommandExecutorError> {
    CreateNamespaceRegistryPermissionActionBuilder::new()
        .with_namespace(COMMAND_PREFIX.into())
        .with_contract_name(COMMAND_NAME.into())
        .with_read(true)
        .with_write(true)
        .into_payload_builder()
        .map_err(|err| {
            SabreCommandExecutorError::Internal(format!(
                "Unable to get sabre payload for create namespace registry permission: {}",
                err
            ))
        })?
        .into_transaction_builder()
        .map_err(|err| {
            SabreCommandExecutorError::Internal(format!(
                "Unable to get transaction builder for create namespace registry permission: {}",
                err
            ))
        })?
        .build(signer)
        .map_err(|err| {
            SabreCommandExecutorError::Internal(format!(
                "Unable to build create namespace registry transaction: {}",
                err
            ))
        })
}

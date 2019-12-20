// Copyright 2019 IBM Corp.
// Copyright 2019 Cargill Incorporated
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

use sawtooth_xo::handler::XoTransactionHandler;
use transact::context::manager::sync::ContextManager;
use transact::contract::address::{key_hash::KeyHashAddresser, Addresser};
use transact::database::{btree::BTreeDatabase, Database};
use transact::execution::{adapter::static_adapter::StaticExecutionAdapter, executor::Executor};
use transact::protocol::receipt::TransactionReceipt;
use transact::protocol::{
    batch::{BatchBuilder, BatchPair},
    receipt::StateChange,
    transaction::{HashMethod, TransactionBuilder},
};
use transact::sawtooth::SawtoothToTransactHandlerAdapter;
use transact::scheduler::{
    serial::SerialScheduler, BatchExecutionResult, Scheduler, TransactionExecutionResult,
};
use transact::signing::{hash::HashSigner, Signer};
use transact::state::merkle::{self, MerkleRadixTree, MerkleState};
use transact::state::StateChange as ChangeSet;
use transact::state::Write;

use hex;
use std::io;
use std::str;
use std::sync::{Arc, Mutex};

fn main() {
    let db = Box::new(BTreeDatabase::new(&merkle::INDEXES));
    let merkle_db = MerkleRadixTree::new(db.clone(), None).unwrap();
    let merkle_state = MerkleState::new(db.clone());
    let orig_root = merkle_db.get_merkle_root();

    let signer = HashSigner::new(vec![00u8, 1, 2]);
    let addresser = KeyHashAddresser::new("5b7349".to_string());
    let game_address = addresser
        .compute(&"my_game".to_string())
        .expect("Unable to compute `game_address` using Addresser");
    let current_result = play_game(
        &orig_root,
        db.clone(),
        &signer,
        "my_game,create,",
        &game_address,
    );

    // The `game_address` has already been calculated using the KeyHashAddresser, don't need to
    // capture the key value which represents the game's radix address from this function.
    let (_, value) = get_state_change(current_result);

    let state_change = ChangeSet::Set {
        key: game_address.to_string(),
        value: value.clone(),
    };
    let mut state_root = merkle_state.commit(&orig_root, &[state_change]).unwrap();
    assert_ne!(orig_root, state_root);
    print_current_state(&value);

    loop {
        let next_tx = get_next_tx();
        let current_result = play_game(
            &state_root,
            db.clone(),
            &signer,
            &format!("my_game,take,{}", next_tx),
            &game_address,
        );
        let (key, value) = get_state_change(current_result);
        let state_change = ChangeSet::Set {
            key,
            value: value.clone(),
        };
        state_root = merkle_state.commit(&state_root, &[state_change]).unwrap();

        let value = print_current_state(&value);
        if value.contains("WIN") || value.contains("TIE") {
            break;
        }
    }
}

fn print_current_state(value: &[u8]) -> &str {
    let val = match str::from_utf8(&value) {
        Ok(v) => v,
        Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };

    let split: Vec<&str> = val.split(',').collect();
    println!("Board:");
    println!(
        "\t  {} | {} | {} ",
        &split[1][0..1],
        &split[1][1..2],
        &split[1][2..3]
    );
    println!("\t ---|---|---");
    println!(
        "\t  {} | {} | {} ",
        &split[1][3..4],
        &split[1][4..5],
        &split[1][5..6]
    );
    println!("\t ---|---|---");
    println!(
        "\t  {} | {} | {} ",
        &split[1][6..7],
        &split[1][7..8],
        &split[1][8..9]
    );
    println!();
    println!("Status: {}", split[2]);
    val
}

fn get_next_tx() -> String {
    println!("Please input an integer from 1 to 9 ");

    let mut action = String::new();
    io::stdin()
        .read_line(&mut action)
        .expect("Failed to read line");
    println!();
    action.trim().to_string()
}

fn get_state_change(result: BatchExecutionResult) -> (String, Vec<u8>) {
    let mut receipt = get_receipt(result);

    assert_eq!(receipt.state_changes.len(), 1);
    let state_change = receipt.state_changes.pop();

    match state_change {
        Some(c) => match c {
            StateChange::Set { key, value } => (key, value),
            _ => panic!("nothing should be deleted"),
        },
        _ => panic!("invalid"),
    }
}

// the db should be a clone of the merkle tree
fn play_game(
    state_root: &str,
    db: Box<dyn Database>,
    signer: &dyn Signer,
    tx: &str,
    game_address: &str,
) -> BatchExecutionResult {
    println!("Current state_root: {}", state_root);
    println!();

    // create context manager using the db
    let context_manager = ContextManager::new(Box::new(MerkleState::new(db)));
    let executor = create_executor(&context_manager);
    start_executor(&executor);

    let mut scheduler = SerialScheduler::new(Box::new(context_manager), state_root.to_string())
        .expect("Failed to create scheduler");

    // Create async channel to submit transactions and receive results
    let (result_sender, result_receiver) = std::sync::mpsc::channel();

    // set up the scheduler to use result_sender to send the result to us
    scheduler
        .set_result_callback(Box::new(move |batch_result| {
            result_sender
                .send(batch_result)
                .expect("Unable to send batch result")
        }))
        .expect("Failed to set result callback");

    let batch_pair = create_batch(signer, game_address, tx);

    scheduler
        .add_batch(batch_pair)
        .expect("Failed to add batch");
    scheduler.finalize().expect("Failed to finalize scheduler");

    run_schedule(&executor, &mut scheduler);

    result_receiver
        .recv()
        .expect("Unable to receive result from executor")
        .expect("Should not have received None from the executor")
}

fn create_executor(context_manager: &ContextManager) -> Arc<Mutex<Option<Executor>>> {
    Arc::new(Mutex::new(Some(Executor::new(vec![Box::new(
        StaticExecutionAdapter::new_adapter(
            vec![Box::new(SawtoothToTransactHandlerAdapter::new(
                XoTransactionHandler::new(),
            ))],
            context_manager.clone(),
        )
        .expect("Unable to create static execution adapter"),
    )]))))
}

fn start_executor(executor: &Arc<Mutex<Option<Executor>>>) {
    executor
        .lock()
        .expect("Should not have poisoned the lock")
        .as_mut()
        .expect("Should not be None")
        .start()
        .expect("Start should not have failed");
}

fn create_batch(signer: &dyn Signer, game_address: &str, payload: &str) -> BatchPair {
    let txn_pair = TransactionBuilder::new()
        .with_batcher_public_key(signer.public_key().to_vec())
        .with_family_name("xo".to_string())
        .with_family_version("1.0".to_string())
        .with_inputs(vec![hex::decode(&game_address).unwrap()])
        .with_nonce(b"test_nonce".to_vec())
        .with_outputs(vec![hex::decode(&game_address).unwrap()])
        .with_payload_hash_method(HashMethod::SHA512)
        .with_payload(payload.as_bytes().to_vec())
        .build_pair(signer)
        .expect("The TransactionBuilder was not given the correct items");

    BatchBuilder::new()
        .with_transactions(vec![txn_pair.take().0])
        .build_pair(signer)
        .expect("Unable to build batch a pair")
}

fn run_schedule(executor: &Arc<Mutex<Option<Executor>>>, scheduler: &mut dyn Scheduler) {
    let task_iterator = scheduler
        .take_task_iterator()
        .expect("Failed to take task iterator");
    executor
        .lock()
        .expect("Should not have poisoned the lock")
        .as_ref()
        .expect("Should not be None")
        .execute(
            task_iterator,
            scheduler.new_notifier().expect("Failed to get notifier"),
        )
        .expect("Failed to execute schedule");
}

fn get_receipt(batch_result: BatchExecutionResult) -> TransactionReceipt {
    assert_eq!(1, batch_result.results.len());

    let mut batch_result = batch_result;

    let txn_result = batch_result
        .results
        .pop()
        .expect("Length 1, but no first element");
    match txn_result {
        TransactionExecutionResult::Valid(receipt) => receipt,
        TransactionExecutionResult::Invalid(invalid_result) => {
            panic!("Transaction failed: {:?}", invalid_result)
        }
    }
}

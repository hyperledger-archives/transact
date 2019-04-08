// Copyright 2018 Cargill Incorporated
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

#[macro_use]
extern crate clap;
extern crate crypto;
#[macro_use]
extern crate log;
extern crate protobuf;
extern crate sawtooth_sdk;
extern crate simple_logger;

mod addressing;
mod handler;
mod payload;
mod protos;
mod state;
mod wasm_executor;

use handler::SabreTransactionHandler;
use log::LogLevel;
use sawtooth_sdk::processor::TransactionProcessor;

fn main() {
    let matches = clap_app!(wasm_store_tp =>
        (version: crate_version!())
        (about: "Implements the Sawtooth Sabre transaction family")
        (@arg connect: -C --connect +takes_value
         "connection endpoint for validator")
        (@arg verbose: -v --verbose +multiple
         "increase output verbosity"))
    .get_matches();

    let logger = match matches.occurrences_of("verbose") {
        1 => simple_logger::init_with_level(LogLevel::Info),
        2 => simple_logger::init_with_level(LogLevel::Debug),
        0 | _ => simple_logger::init_with_level(LogLevel::Warn),
    };

    logger.expect("Failed to create logger");

    let connect = matches
        .value_of("connect")
        .unwrap_or("tcp://localhost:4004");

    let handler = SabreTransactionHandler::new();
    let mut processor = TransactionProcessor::new(connect);

    processor.add_handler(&handler);
    processor.start();
}

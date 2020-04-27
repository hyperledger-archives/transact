/*
 * Copyright 2018 Bitwise IO, Inc.
 * Copyright 2020 Cargill Incorporated
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

extern crate protoc_rust;

use protoc_rust::Customize;

use std::env;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("protos");
    let proto_path = Path::new("./protos");
    fs::create_dir_all(&dest_path).unwrap();

    // Run protoc
    protoc_rust::Codegen::new()
        .out_dir(&dest_path.to_str().unwrap())
        .inputs(&[
            proto_path.join("batch.proto").to_str().unwrap(),
            proto_path.join("transaction.proto").to_str().unwrap(),
            proto_path.join("events.proto").to_str().unwrap(),
            proto_path
                .join("transaction_receipt.proto")
                .to_str()
                .unwrap(),
            proto_path.join("merkle.proto").to_str().unwrap(),
            proto_path.join("command.proto").to_str().unwrap(),
            #[cfg(feature = "key-value-state")]
            proto_path.join("key_value_state.proto").to_str().unwrap(),
        ])
        .includes(&[proto_path.to_str().unwrap()])
        .customize(Customize::default())
        .run()
        .expect("Protoc Error");

    // Create mod.rs accordingly
    let mut mod_file = File::create(dest_path.join("mod.rs")).unwrap();
    mod_file
        .write_all(b"pub mod batch;\npub mod events;\n#[cfg(feature = \"key-value-state\")]\npub mod key_value_state;\npub mod transaction;\npub mod transaction_receipt;\npub mod merkle;\npub mod command;\n")
        .unwrap();
}

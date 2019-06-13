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
use std::fs::File;
use std::io::prelude::*;

use sawtooth_sdk::signing;

use crate::error::CliError;
use crate::key;
use crate::submit::submit_batch_list;
use crate::transaction::{create_batch, create_batch_list_from_one, create_transaction};

pub fn do_multiply(
    name_a: &str,
    name_b: &str,
    name_c: &str,
    output: &str,
    key_name: Option<&str>,
    url: &str,
) -> Result<(), CliError> {
    let private_key = key::load_signing_key(key_name)?;
    let context = signing::create_context("secp256k1")?;
    let public_key = context.get_public_key(&private_key)?.as_hex();
    let factory = signing::CryptoFactory::new(&*context);
    let signer = factory.new_signer(&private_key);
    let payload = vec![name_a, name_b, name_c].join(",");
    let txn_payload = payload.as_bytes();

    if output.is_empty() {
        let txn = create_transaction(name_a, name_b, name_c, txn_payload, &signer, &public_key)?;
        let batch = create_batch(txn, &signer, &public_key)?;
        let batch_list = create_batch_list_from_one(batch);

        submit_batch_list(url, &batch_list)?;

        return Ok(());
    }

    let mut buffer = File::create(output)?;
    buffer.write_all(&txn_payload).map_err(CliError::IoError)?;
    Ok(())
}

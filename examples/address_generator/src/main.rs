//
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

use clap::{App, Arg};

use transact::contract::address::{
    double_key_hash::DoubleKeyHashAddresser, key_hash::KeyHashAddresser,
    triple_key_hash::TripleKeyHashAddresser, Addresser,
};

fn main() -> Result<(), String> {
    let matches = App::new(clap::crate_name!())
        .version(clap::crate_version!())
        .author(clap::crate_authors!())
        .about(clap::crate_description!())
        .arg(
            Arg::with_name("prefix")
                .required(true)
                .takes_value(true)
                .help("Prefix of the radix address")
                .index(1),
        )
        .arg(
            Arg::with_name("key")
                .required(true)
                .multiple(true)
                .takes_value(true)
                .max_values(3)
                .index(2)
                .help("Natural key used to compute a radix address"),
        )
        .arg(
            Arg::with_name("length")
                .long("length")
                .required(false)
                .max_values(2)
                .multiple(true)
                .use_delimiter(true)
                .help("Length of hash used to construct a radix address"),
        )
        .get_matches();

    // The prefix is used to construct the Addresser.
    let prefix = matches
        .value_of("prefix")
        .expect("Unable to get argument: prefix")
        .to_string();

    // Key(s) to be hashed and used to construct the radix address.
    let keys: Vec<_> = matches
        .values_of("key")
        .expect("Unable to get argument: key")
        .collect();

    // Length(s) of the hashes used to construct the radix address.
    let lengths = match matches.values_of("length") {
        Some(vals) => vals
            .map(|l| {
                l.parse()
                    .map_err(|_| "Argument must be an integer: length".to_string())
            })
            .collect::<Result<Vec<usize>, String>>()?,
        _ => vec![],
    };

    // The Addresser implementation to be constructed depends on the amount of `keys` supplied.
    // Once the appropriate Addresser implementation is constructed, the radix address will be
    // calculated and printed out.
    match keys.len() {
        1 => {
            let addresser = KeyHashAddresser::new(prefix);
            let addr = addresser
                .compute(&keys[0].to_string())
                .map_err(|e| format!("Unable to compute address: {}", e))?;
            println!("{}", addr);
        }
        2 => {
            let addresser = match lengths.len() {
                0 => DoubleKeyHashAddresser::new(prefix, None)
                    .map_err(|e| format!("Unable to construct Addresser: {}", e))?,
                _ => DoubleKeyHashAddresser::new(prefix, Some(lengths[0]))
                    .map_err(|e| format!("Unable to construct Addresser: {}", e))?,
            };
            let addr = addresser
                .compute(&(keys[0].to_string(), keys[1].to_string()))
                .map_err(|e| format!("Unable to compute address: {}", e))?;
            println!("{}", addr);
        }
        3 => {
            let addresser = match lengths.len() {
                0 => TripleKeyHashAddresser::new(prefix, None, None)
                    .map_err(|e| format!("Unable to construct Addresser: {}", e))?,
                1 => TripleKeyHashAddresser::new(prefix, Some(lengths[0]), None)
                    .map_err(|e| format!("Unable to construct Addresser: {}", e))?,
                _ => TripleKeyHashAddresser::new(prefix, Some(lengths[0]), Some(lengths[1]))
                    .map_err(|e| format!("Unable to construct Addresser: {}", e))?,
            };
            let addr = addresser
                .compute(&(
                    keys[0].to_string(),
                    keys[1].to_string(),
                    keys[2].to_string(),
                ))
                .map_err(|e| format!("Unable to compute address: {}", e))?;
            println!("{}", addr);
        }
        _ => {
            // This match case is unreachable as the number of arguments is already validated by
            // clap, which immediately returns an error if 1 - 3 `keys` are not provided.
            unreachable!();
        }
    }

    Ok(())
}

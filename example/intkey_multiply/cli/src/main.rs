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

mod error;
mod key;
mod multiply;
mod submit;
mod transaction;

const APP_NAME: &str = env!("CARGO_PKG_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");

fn run() -> Result<(), error::CliError> {
    // Below, unwrap() is used on required arguments, since they will always
    // contain a value (and lack of value is should cause a panic). unwrap()
    // is also used on get_matches() because SubcommandRequiredElseHelp will
    // ensure a value.

    let matches = clap_app!(myapp =>
        (name: APP_NAME)
        (version: VERSION)
        (about: "Sawtooth Intkey Multiply CLI")
        (@setting SubcommandRequiredElseHelp)
        (@subcommand multiply =>
            (about: "multiply two intkey values together")
            (@arg name_a: +required +takes_value "Intkey key to store multiplied value")
            (@arg name_b:  +takes_value +required "Intkey key for the first value to multiply")
            (@arg name_c: +takes_value +required "Intkey key for the second value to multiply")
            (@arg output: --output -o +takes_value "File name to write payload to.")
            (@arg key: -k --key +takes_value "Signing key name")
            (@arg url: --url +takes_value "URL to the Sawtooth REST API")
        )
    )
    .get_matches();

    if let Some(multiply_matches) = matches.subcommand_matches("multiply") {
        let name_a = multiply_matches.value_of("name_a").unwrap();
        let name_b = multiply_matches.value_of("name_b").unwrap();
        let name_c = multiply_matches.value_of("name_c").unwrap();
        let output = multiply_matches.value_of("output").unwrap_or("");
        let key_name = multiply_matches.value_of("key");
        let url = multiply_matches
            .value_of("url")
            .unwrap_or("http://localhost:8008/");

        multiply::do_multiply(name_a, name_b, name_c, output, key_name, &url)?;
    }

    Ok(())
}

fn main() {
    if let Err(e) = run() {
        println!("{}", e);
        std::process::exit(1);
    }
}

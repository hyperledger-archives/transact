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

/*
    This test needs to be run against the sawtooth_sabre docker-compose file and from within the
    test docker file. This is the only place that cargo test should be run, otherwise the test
    will fail.

    The test is order dependent so the tests are written as one test to ensure ordering.
*/

extern crate subprocess;
extern crate serde_json;

use serde_json::Value;
use subprocess::Exec;
use std::io::{BufReader, BufRead};
use std::error::Error as StdError;
use std::fs::File;
use std::io::Read;

const INTKEY_MULTIPLY_DEF: &str =
    "/project/example/intkey_multiply/intkey_multiply.yaml";

// Path to a payload to multiply intkey value B and C and store in A.
const GOOD_PAYLOAD: &str = "/project/integration/payloads/A_B_C_payload";
// Path to a payload to multiply intkey value C and nonexisties and store in A.
const BAD_PAYLOAD: &str = "/project/integration/payloads/A_Bad_C_payload";
const SIGNER: &str = "/root/.sawtooth/keys/root.pub";

#[derive(Debug)]
pub enum TestError {
    TestError(String),
}

impl StdError for TestError {
    fn description(&self) -> &str {
        match *self {
            TestError::TestError(ref s) => &s,
        }
    }

    fn cause(&self) -> Option<&StdError> {
        match *self {
            TestError::TestError(ref _s) => None,
        }
    }
}

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            TestError::TestError (ref s) => write!(f, "Error: {}", s),
        }
    }
}

// Execute the sabre cli command and parses the output returned to stdout.
fn sabre_cli(command: String) -> Result<Value, TestError> {
    let mut command_vec = command.split(" ").collect::<Vec<&str>>();
    command_vec.append(&mut vec!("--url", "http://rest-api:9708", "--wait", "300"));
    let x = Exec::cmd("sabre").args(&command_vec).stream_stdout()
        .map_err(|err| TestError::TestError(err.to_string()))?;
    let br = BufReader::new(x);
    for line in br.lines() {
        let response_string = line.map_err(|err| TestError::TestError(err.to_string()))?;
        if response_string.starts_with("StatusResponse ") {
            let json_string = match response_string.get(15..){
                Some(x) => x,
                None => return Err(TestError::TestError("Unable to get response".into()))
            };
            let json: Value = serde_json::from_str(&json_string)
                .map_err(|err| TestError::TestError(err.to_string()))?;;
            return Ok(json)
        }
    }
    return Err(TestError::TestError("No response received".into()))
}

/// The following test tests the Sabre Cli, Sabre Transaction Processor and the Intkey Multiply
/// example smart contract.
/// The tests executes sabre cli commands to upload and executes the smart contract and then
/// checks that they are correctly either committed or invalid.
#[test]
fn test_sabre() {
    let mut f = File::open(SIGNER).expect("file not found");

    let mut signer = String::new();
    f.read_to_string(&mut signer)
        .expect("something went wrong reading the file");

    // remove newline character
    signer.pop();

    // Test that Sabre will return an invalid transaction when the Contract does not
    // exist
    //
    // Send ExecuteContractAction with the following:
    //      Name: intkey_multiply
    //      Version: 1.0
    //      Inputs: 1cf126
    //      Outputs: 1cf126
    //      Payload: A payload that tries to multiply B and C together and set in A.
    //
    // Result: Invalid Transaction, Contract does not exist
    let response = match sabre_cli("exec --contract intkey_multiply:1.0 --payload ".to_string() +
        &GOOD_PAYLOAD + " --inputs 1cf126 --outputs 1cf126") {
            Ok(x) => x,
            Err(err) => panic!(format!("No Response {}", err))
        };
    assert!(response["data"][0]["status"]=="INVALID");
    let message: String = response["data"][0]["invalid_transactions"][0]["message"].to_string();
    println!("{}", message);
    assert!(message.contains("Contract does not exist"));

    // Test that Sabre will return an invalid transaction if the ContractRegistry does not exist
    //
    // Send CreateContractAction with the following:
    //      Name: intkey_multiply
    //      Version: 1.0
    //      Inputs: 1cf126
    //      Outputs: 1cf126
    //      contract: The compiled intkey_multiply wasm contract.
    //
    // Result: Invalid Transaction, The Contract Registry does not exist
    let response = match sabre_cli("upload -f".to_string() + &INTKEY_MULTIPLY_DEF){
        Ok(x) => x,
        Err(err) => panic!(format!("No Response {}", err))
    };
    assert!(response["data"][0]["status"]=="INVALID");
    let message: String = response["data"][0]["invalid_transactions"][0]["message"].to_string();
    println!("{}", message);
    assert!(message.contains("The Contract Registry does not exist"));

    // Test that Sabre will set a ContractRegistry.
    //
    // Send CreateContractRegistryAction with the following:
    //      Name: intkey_multiply
    //      Owners: signing key
    //
    // Result: Committed.
    let response = match sabre_cli("cr --create intkey_multiply --owner ".to_string() +
        &signer) {
            Ok(x) => x,
            Err(err) => panic!(format!("No Response {}", err))
    };
    assert!(response["data"][0]["status"]=="COMMITTED");

    // Test that Sabre will set a Contract.
    //
    // Send CreateContractAction with the following:
    //      Name: intkey_multiply
    //      Version: 1.0
    //      Inputs: 1cf126
    //      Outputs: 1cf126
    //      Contract: The compiled intkey_multiply wasm contract.
    //
    // Result: Committed.
    let response = match sabre_cli("upload -f".to_string() + &INTKEY_MULTIPLY_DEF){
        Ok(x) => x,
        Err(err) => panic!(format!("No Response {}", err))
    };
    assert!(response["data"][0]["status"]=="COMMITTED");

    // Test that Sabre will return an invalid transaction when the NamespaceRegistry does not
    // exist
    //
    // Send ExecuteContractAction with the following:
    //      Name: intkey_multiply
    //      Version: 1.0
    //      Inputs: 1cf126
    //      Outputs: 1cf126
    //      Payload: A payload that tries to multiply B and C together and set in A.
    //
    // Result: Invalid Transaction, Namespace Registry does not exist
    let response = match sabre_cli("exec --contract intkey_multiply:1.0 --payload ".to_string() +
        &GOOD_PAYLOAD + " --inputs 1cf126 --outputs 1cf126") {
            Ok(x) => x,
            Err(err) => panic!(format!("No Response {}", err))
        };
    assert!(response["data"][0]["status"]=="INVALID");
    let message: String = response["data"][0]["invalid_transactions"][0]["message"].to_string();
    println!("{}", message);
    assert!(message.contains("Namespace Registry does not exist"));

    // Test that Sabre will set a new Namespace Registry.
    //
    // Send CreateNamespaceRegistryAction with the following:
    //      Namespace: 1cf126
    //      Owner: signing key
    //
    // Result: Committed
    let response = match sabre_cli("ns --create 1cf126 --owner ".to_string() + &signer) {
        Ok(x) => x,
        Err(err) => panic!(format!("No Response {}", err))
    };
    assert!(response["data"][0]["status"]=="COMMITTED");

    // Test that Sabre will return an invalid transaction when the Contract does not
    // have permissions to access the namespace.
    //
    // Send ExecuteContractAction with the following:
    //      Name: intkey_multiply
    //      Version: 1.0
    //      Inputs: 1cf126
    //      Outputs: 1cf126
    //      Payload: A payload that tries to multiply B and C together and set in A.
    //
    // Result: Invalid Transaction, Contract does not have permission
    let response = match sabre_cli("exec --contract intkey_multiply:1.0 --payload ".to_string() +
        &GOOD_PAYLOAD + " --inputs 1cf126 --outputs 1cf126") {
            Ok(x) => x,
            Err(err) => panic!(format!("No Response {}", err))
        };
    assert!(response["data"][0]["status"]=="INVALID");
    let message: String = response["data"][0]["invalid_transactions"][0]["message"].to_string();
    println!("{}", message);
    assert!(message.contains("Contract does not have permission"));

    // Test that Sabre will add a permission to the inktey namespace registry to give Intkey
    // Multiply read and write permissions.
    //
    // Send CreateNamespaceRegistryPermissionAction with the following:
    //      Namespace: 1cf126
    //      Contract_name: intkey_multiply
    //      Read: true
    //      Write: true
    //
    // Result: Committed
    let response = match sabre_cli("perm 1cf126 intkey_multiply --read --write".to_string()) {
            Ok(x) => x,
            Err(err) => panic!(format!("No Response {}", err))
        };
    assert!(response["data"][0]["status"]=="COMMITTED");

    // Test that Sabre will successfully execute the contract.
    //
    // Send ExecuteContractAction with the following:
    //      Name: intkey_multiply
    //      Version: 1.0
    //      Inputs: 1cf126
    //      Outputs: 1cf126
    //      Payload: A payload that tries to multiply B and C together and set in A.
    //
    // Result: Committed. Set Inktey State.
    let response = match sabre_cli("exec --contract intkey_multiply:1.0 --payload ".to_string() +
        &GOOD_PAYLOAD + " --inputs 1cf126 --outputs 1cf126") {
            Ok(x) => x,
            Err(err) => panic!(format!("No Response {}", err))
        };
    assert!(response["data"][0]["status"]=="COMMITTED");

    // Test that Sabre will successfully try to execute the contract but the smart contract will
    // return an invalid transaction because A has already been state.
    //
    // Send ExecuteContractAction with the following:
    //      Name: intkey_multiply
    //      Version: 1.0
    //      Inputs: 1cf126
    //      Outputs: 1cf126
    //      Payload: A payload that tries to multiply B and C together and set in A.
    //
    // Result: Invalid Transaction, Wasm contract returned invalid transaction
    let response = match sabre_cli("exec --contract intkey_multiply:1.0 --payload ".to_string() +
        &GOOD_PAYLOAD + " --inputs 1cf126 --outputs 1cf126") {
            Ok(x) => x,
            Err(err) => panic!(format!("No Response {}", err))
        };
    assert!(response["data"][0]["status"]=="INVALID");
    let message: String = response["data"][0]["invalid_transactions"][0]["message"].to_string();
    println!("{}", message);
    assert!(message.contains("Wasm contract returned invalid transaction"));

    // Test that Sabre will successfully try to execute the contract but the smart contract will
    // return an invalid transaction because Bad does not exist in state.
    //
    // Send ExecuteContractAction with the following:
    //      Name: intkey_multiply
    //      Version: 1.0
    //      Inputs: 1cf126
    //      Outputs: 1cf126
    //      Payload: A payload that tries to multiply Bad and C together and set in A.
    //
    // Result: Invalid Transaction, Wasm contract returned invalid transaction
    let response = match sabre_cli("exec --contract intkey_multiply:1.0 --payload ".to_string() +
        &BAD_PAYLOAD + " --inputs 1cf126 --outputs 1cf126") {
            Ok(x) => x,
            Err(err) => panic!(format!("No Response {}", err))
        };
    assert!(response["data"][0]["status"]=="INVALID");
    let message: String = response["data"][0]["invalid_transactions"][0]["message"].to_string();
    println!("{}", message);
    assert!(message.contains("Wasm contract returned invalid transaction"));
}

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

//! The following are logging macros that will be used when running a smart contract in
//! Sabre. The log level is the same level set on the sabre transaction processor running the
//! smart contract.

#[macro_export]
macro_rules! log {
    ($lvl:path, $($arg:tt)+) => ({
        let lvl = $lvl;
        if ::sabre_sdk::log_enabled(lvl) {
            let x = format_args!($($arg)*).to_string();
            ::sabre_sdk::log_message(lvl, x);
        }
    })
}

#[macro_export]
macro_rules! trace {
    ($($arg:tt)*) =>(log!(::sabre_sdk::LogLevel::Trace, $($arg)*))
}

#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) =>(log!(::sabre_sdk::LogLevel::Debug, $($arg)*))
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)*) =>(log!(::sabre_sdk::LogLevel::Info, $($arg)*))
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) =>(log!(::sabre_sdk::LogLevel::Warn, $($arg)*))
}
#[macro_export]
macro_rules! error {
    ($($arg:tt)*) =>(log!(::sabre_sdk::LogLevel::Error, $($arg)*))
}

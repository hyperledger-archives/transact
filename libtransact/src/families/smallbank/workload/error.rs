/*
 * Copyright 2017 Intel Corporation
 * Copyright 2021 Cargill Incorporated
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
 * ------------------------------------------------------------------------------
 */
use std::error::Error;
use std::fmt;
use std::io::Error as StdIoError;

use cylinder::SigningError;
use yaml_rust::EmitError;

use crate::protos::ProtoConversionError;

#[derive(Debug)]
pub enum PlaylistError {
    IoError(StdIoError),
    YamlOutputError(EmitError),
    YamlInputError(yaml_rust::ScanError),
    MessageError(protobuf::ProtobufError),
    SigningError(SigningError),
    BuildError(String),
    ProtoConversionError(ProtoConversionError),
}

impl fmt::Display for PlaylistError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PlaylistError::IoError(ref err) => {
                write!(f, "Error occurred writing messages: {}", err)
            }
            PlaylistError::YamlOutputError(_) => write!(f, "Error occurred generating YAML output"),
            PlaylistError::YamlInputError(_) => write!(f, "Error occurred reading YAML input"),
            PlaylistError::MessageError(ref err) => {
                write!(f, "Error occurred creating protobuf: {}", err)
            }
            PlaylistError::SigningError(ref err) => {
                write!(f, "Error occurred signing transactions: {}", err)
            }
            PlaylistError::BuildError(ref err) => {
                write!(f, "Error occurred building transactions: {}", err)
            }
            PlaylistError::ProtoConversionError(ref err) => {
                write!(f, "Error occurred during protobuf conversion: {}", err)
            }
        }
    }
}

impl Error for PlaylistError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            PlaylistError::IoError(ref err) => Some(err),
            PlaylistError::YamlOutputError(_) => None,
            PlaylistError::YamlInputError(_) => None,
            PlaylistError::MessageError(ref err) => Some(err),
            PlaylistError::SigningError(ref err) => Some(err),
            PlaylistError::BuildError(_) => None,
            PlaylistError::ProtoConversionError(ref err) => Some(err),
        }
    }
}

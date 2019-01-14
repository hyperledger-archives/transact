use std::error::Error as StdError;

#[derive(Debug)]
pub enum Error {
    SigningError(String),
}

impl StdError for Error {
    fn description(&self) -> &str {
        match *self {
            Error::SigningError(ref msg) => msg,
        }
    }

    fn cause(&self) -> Option<&StdError> {
        match *self {
            Error::SigningError(_) => None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Error::SigningError(ref s) => write!(f, "SigningError: {}", s),
        }
    }
}

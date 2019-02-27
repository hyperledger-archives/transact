use std;
use std::error::Error as StdError;

use crate::batch::BatchBuildError;
use crate::transaction::TransactionBuildError;

#[derive(Debug)]
pub enum WorkloadError {
    // Returned when an error occurs while using BatchBuilder.
    BatchBuildError(BatchBuildError),

    // Returned when an error occurs while using TransactionBuilder.
    TransactionBuildError(TransactionBuildError),
}

impl std::error::Error for WorkloadError {
    fn description(&self) -> &str {
        match *self {
            WorkloadError::BatchBuildError(ref err) => err.description(),
            WorkloadError::TransactionBuildError(ref err) => err.description(),
        }
    }

    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            WorkloadError::BatchBuildError(ref err) => Some(err),
            WorkloadError::TransactionBuildError(ref err) => Some(err),
        }
    }
}

impl std::fmt::Display for WorkloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            WorkloadError::BatchBuildError(ref err) => {
                write!(f, "BatchBuildError: {}", err.description())
            }
            WorkloadError::TransactionBuildError(ref err) => {
                write!(f, "TransactionBuildError: {}", err.description())
            }
        }
    }
}

impl From<BatchBuildError> for WorkloadError {
    fn from(err: BatchBuildError) -> WorkloadError {
        WorkloadError::BatchBuildError(err)
    }
}

impl From<TransactionBuildError> for WorkloadError {
    fn from(err: TransactionBuildError) -> WorkloadError {
        WorkloadError::TransactionBuildError(err)
    }
}

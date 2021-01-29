use crate::protocol::batch::BatchBuildError;
use crate::protocol::transaction::TransactionBuildError;

#[derive(Debug)]
pub enum WorkloadError {
    // Returned when an error occurs while using BatchBuilder.
    BatchBuildError(BatchBuildError),

    // Returned when an error occurs while using TransactionBuilder.
    TransactionBuildError(TransactionBuildError),

    // Underlying workload raised an error
    InvalidState(String),
}

impl std::error::Error for WorkloadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            WorkloadError::BatchBuildError(ref err) => Some(err),
            WorkloadError::TransactionBuildError(ref err) => Some(err),
            WorkloadError::InvalidState(_) => None,
        }
    }
}

impl std::fmt::Display for WorkloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            WorkloadError::BatchBuildError(ref err) => write!(f, "BatchBuildError: {}", err),
            WorkloadError::TransactionBuildError(ref err) => {
                write!(f, "TransactionBuildError: {}", err)
            }
            WorkloadError::InvalidState(ref err) => {
                write!(f, "InvalidState: {}", err)
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

#[cfg(feature = "workload-runner")]
#[derive(Debug, PartialEq)]
pub enum WorkloadRunnerError {
    /// Error raised when failing to submit the batch
    SubmitError(String),
    TooManyRequests,
    /// Error raised when adding workload to the runner
    WorkloadAddError(String),
    /// Error raised when removing workload from the runner
    WorkloadRemoveError(String),
}

#[cfg(feature = "workload-runner")]
impl std::error::Error for WorkloadRunnerError {}

#[cfg(feature = "workload-runner")]
impl std::fmt::Display for WorkloadRunnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            WorkloadRunnerError::SubmitError(ref err) => {
                write!(f, "Unable to submit batch: {}", err)
            }
            WorkloadRunnerError::TooManyRequests => {
                write!(f, "Unable to submit batch because of TooManyRequests")
            }
            WorkloadRunnerError::WorkloadAddError(ref err) => {
                write!(f, "Unable to add workload: {}", err)
            }
            WorkloadRunnerError::WorkloadRemoveError(ref err) => {
                write!(f, "Unable to remove workload: {}", err)
            }
        }
    }
}

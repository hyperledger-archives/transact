use crate::protocol::batch::BatchBuildError;
use crate::protocol::transaction::TransactionBuildError;

#[derive(Debug)]
pub enum WorkloadError {
    // Returned when an error occurs while using BatchBuilder.
    BatchBuildError(BatchBuildError),

    // Returned when an error occurs while using TransactionBuilder.
    TransactionBuildError(TransactionBuildError),
}

impl std::error::Error for WorkloadError {
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
            WorkloadError::BatchBuildError(ref err) => write!(f, "BatchBuildError: {}", err),
            WorkloadError::TransactionBuildError(ref err) => {
                write!(f, "TransactionBuildError: {}", err)
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

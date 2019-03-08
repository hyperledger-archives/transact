pub mod error;
pub mod xo;

use crate::protocol::batch::BatchPair;
use crate::protocol::transaction::TransactionPair;
use crate::workload::error::WorkloadError;

pub trait TransactionWorkload {
    fn next_transaction(&mut self) -> Result<TransactionPair, WorkloadError>;
}

pub trait BatchWorkload {
    fn next_batch(&mut self) -> Result<BatchPair, WorkloadError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn test_transaction_workload(workload: &mut TransactionWorkload) {
        workload.next_transaction().unwrap();
        workload.next_transaction().unwrap();
    }

    pub fn test_batch_workload(workload: &mut BatchWorkload) {
        workload.next_batch().unwrap();
        workload.next_batch().unwrap();
    }
}

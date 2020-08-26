use crate::protocol::batch::BatchBuilder;
use crate::protocol::batch::BatchPair;
use crate::protocol::transaction::HashMethod;
use crate::protocol::transaction::TransactionBuilder;
use crate::protocol::transaction::TransactionPair;
use crate::workload::error::WorkloadError;
use crate::workload::BatchWorkload;
use crate::workload::TransactionWorkload;

use cylinder::{secp256k1::Secp256k1Context, Context, Signer};
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand_hc::Hc128Rng;
use sha2::{Digest, Sha512};

static FAMILY_NAME: &str = "xo";
static FAMILY_VERSION: &str = "0.1";
static NONCE_SIZE: usize = 32;

pub struct XoTransactionWorkload {
    rng: Hc128Rng,
    signer: Box<dyn Signer>,
}

impl Default for XoTransactionWorkload {
    fn default() -> Self {
        Self::new(None, None)
    }
}

impl XoTransactionWorkload {
    pub fn new(seed: Option<u64>, signer: Option<Box<dyn Signer>>) -> Self {
        let rng = match seed {
            Some(seed) => Hc128Rng::seed_from_u64(seed),
            None => Hc128Rng::from_entropy(),
        };
        let signer = signer.unwrap_or_else(new_signer);

        XoTransactionWorkload { rng, signer }
    }
}

impl TransactionWorkload for XoTransactionWorkload {
    fn next_transaction(&mut self) -> Result<TransactionPair, WorkloadError> {
        let nonce = self
            .rng
            .sample_iter(&Alphanumeric)
            .take(NONCE_SIZE)
            .collect::<String>()
            .into_bytes();

        let payload = Payload::new_as_create_with_random_name(&mut self.rng);

        Ok(TransactionBuilder::new()
            .with_family_name(FAMILY_NAME.to_string())
            .with_family_version(FAMILY_VERSION.to_string())
            .with_inputs(payload.inputs())
            .with_outputs(payload.outputs())
            .with_nonce(nonce)
            .with_payload(payload.bytes())
            .with_payload_hash_method(HashMethod::SHA512)
            .build_pair(&*self.signer)?)
    }
}

pub struct XoBatchWorkload {
    transaction_workload: XoTransactionWorkload,
    signer: Box<dyn Signer>,
}

impl Default for XoBatchWorkload {
    fn default() -> Self {
        Self::new(None, None)
    }
}

impl XoBatchWorkload {
    pub fn new(seed: Option<u64>, signer: Option<Box<dyn Signer>>) -> Self {
        let signer = signer.unwrap_or_else(new_signer);
        XoBatchWorkload {
            transaction_workload: XoTransactionWorkload::new(seed, Some(signer.clone())),
            signer,
        }
    }
}

impl BatchWorkload for XoBatchWorkload {
    fn next_batch(&mut self) -> Result<BatchPair, WorkloadError> {
        Ok(BatchBuilder::new()
            .with_transactions(vec![self.transaction_workload.next_transaction()?.take().0])
            .build_pair(&*self.signer)?)
    }
}

enum Action {
    CREATE,
}

struct Payload {
    name: String,
    action: Action,
}

impl Payload {
    /// Creates a Payload initialized with Action::CREATE and a randomly
    /// generated name.
    pub fn new_as_create_with_random_name(rnd: &mut Hc128Rng) -> Self {
        let length = rnd.gen_range(5, 20);

        Payload::new_as_create(
            rnd.sample_iter(&Alphanumeric)
                .take(length)
                .collect::<String>()
                .as_str(),
        )
    }

    /// Creates a Payload initialized with Action::CREATE and the specified
    /// name.
    pub fn new_as_create(name: &str) -> Self {
        Payload {
            name: String::from(name),
            action: Action::CREATE,
        }
    }

    pub fn bytes(&self) -> Vec<u8> {
        match self.action {
            Action::CREATE => format!("create,{},", self.name),
        }
        .into_bytes()
    }

    fn address(&self) -> Vec<u8> {
        let mut address = Vec::new();

        let mut prefix_sha = Sha512::new();
        prefix_sha.input(FAMILY_NAME);
        address.append(&mut prefix_sha.result()[..6].to_vec());

        let mut name_sha = Sha512::new();
        name_sha.input(&self.name);
        address.append(&mut name_sha.result()[..64].to_vec());

        address
    }

    pub fn inputs(&self) -> Vec<Vec<u8>> {
        vec![self.address()]
    }

    pub fn outputs(&self) -> Vec<Vec<u8>> {
        vec![self.address()]
    }
}

fn new_signer() -> Box<dyn Signer> {
    let context = Secp256k1Context::new();
    let key = context.new_random_private_key();
    context.new_signer(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::tests::test_batch_workload;
    use crate::workload::tests::test_transaction_workload;

    #[test]
    fn test_xo_transaction_workload() {
        let mut workload = XoTransactionWorkload::default();
        test_transaction_workload(&mut workload)
    }

    #[test]
    fn test_xo_batch_workload() {
        let mut workload = XoBatchWorkload::default();
        test_batch_workload(&mut workload)
    }
}

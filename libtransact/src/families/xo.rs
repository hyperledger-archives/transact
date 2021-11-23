use crate::error::InvalidStateError;
use crate::protocol::batch::BatchBuilder;
use crate::protocol::batch::BatchPair;
use crate::protocol::transaction::HashMethod;
use crate::protocol::transaction::TransactionBuilder;
use crate::protocol::transaction::TransactionPair;
use crate::workload::BatchWorkload;
use crate::workload::ExpectedBatchResult;
use crate::workload::TransactionWorkload;

use cylinder::{secp256k1::Secp256k1Context, Context, Signer};
use rand::{distributions::Alphanumeric, prelude::*};
use sha2::{Digest, Sha512};

static FAMILY_NAME: &str = "xo";
static FAMILY_VERSION: &str = "0.1";
static NONCE_SIZE: usize = 32;

pub struct XoTransactionWorkload {
    rng: StdRng,
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
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_entropy(),
        };
        let signer = signer.unwrap_or_else(new_signer);

        XoTransactionWorkload { rng, signer }
    }
}

impl TransactionWorkload for XoTransactionWorkload {
    fn next_transaction(
        &mut self,
    ) -> Result<(TransactionPair, Option<ExpectedBatchResult>), InvalidStateError> {
        let nonce = std::iter::repeat(())
            .map(|()| self.rng.sample(Alphanumeric))
            .map(char::from)
            .take(NONCE_SIZE)
            .collect::<String>()
            .into_bytes();

        let payload = Payload::new_as_create_with_random_name(&mut self.rng);

        Ok((
            TransactionBuilder::new()
                .with_family_name(FAMILY_NAME.to_string())
                .with_family_version(FAMILY_VERSION.to_string())
                .with_inputs(payload.inputs())
                .with_outputs(payload.outputs())
                .with_nonce(nonce)
                .with_payload(payload.bytes())
                .with_payload_hash_method(HashMethod::Sha512)
                .build_pair(&*self.signer)
                .map_err(|err| {
                    InvalidStateError::with_message(format!(
                        "Failed to build transaction pair: {}",
                        err
                    ))
                })?,
            None,
        ))
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
    fn next_batch(
        &mut self,
    ) -> Result<(BatchPair, Option<ExpectedBatchResult>), InvalidStateError> {
        let (txn, result) = self.transaction_workload.next_transaction()?;
        Ok((
            BatchBuilder::new()
                .with_transactions(vec![txn.take().0])
                .build_pair(&*self.signer)
                .map_err(|err| {
                    InvalidStateError::with_message(format!("Failed to build batch pair: {}", err))
                })?,
            result,
        ))
    }
}

enum Action {
    Create,
}

struct Payload {
    name: String,
    action: Action,
}

impl Payload {
    /// Creates a Payload initialized with Action::Create and a randomly
    /// generated name.
    pub fn new_as_create_with_random_name(rnd: &mut StdRng) -> Self {
        let length = rnd.gen_range(5..20);

        Payload::new_as_create(
            rnd.sample_iter(&Alphanumeric)
                .map(char::from)
                .take(length)
                .collect::<String>()
                .as_str(),
        )
    }

    /// Creates a Payload initialized with Action::Create and the specified
    /// name.
    pub fn new_as_create(name: &str) -> Self {
        Payload {
            name: String::from(name),
            action: Action::Create,
        }
    }

    pub fn bytes(&self) -> Vec<u8> {
        match self.action {
            Action::Create => format!("create,{},", self.name),
        }
        .into_bytes()
    }

    fn address(&self) -> Vec<u8> {
        let mut address = Vec::new();

        let mut prefix_sha = Sha512::new();
        prefix_sha.update(FAMILY_NAME);
        address.append(&mut prefix_sha.finalize()[..6].to_vec());

        let mut name_sha = Sha512::new();
        name_sha.update(&self.name);
        address.append(&mut name_sha.finalize()[..64].to_vec());

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

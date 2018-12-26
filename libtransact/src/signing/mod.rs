pub mod error;
pub mod hash;

use std;

pub use crate::signing::error::Error;

pub trait Signer {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, Error>;
    fn public_key(&self) -> &[u8];
}

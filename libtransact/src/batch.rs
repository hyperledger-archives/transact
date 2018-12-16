use hex;

use crate::transaction::Transaction;

use super::protos;

pub struct BatchHeader {
    signer_public_key: Vec<u8>,
    transaction_ids: Vec<Vec<u8>>,
}

impl BatchHeader {
    pub fn signer_public_key(&self) -> &[u8] {
        &self.signer_public_key
    }

    pub fn transaction_ids(&self) -> &[Vec<u8>] {
        &self.transaction_ids
    }
}

impl From<protos::batch::BatchHeader> for BatchHeader {
    fn from(header: protos::batch::BatchHeader) -> Self {
        BatchHeader {
            signer_public_key: hex::decode(header.get_signer_public_key()).unwrap(),
            transaction_ids: header
                .get_transaction_ids()
                .to_vec()
                .into_iter()
                .map(|t| hex::decode(t).unwrap())
                .collect(),
        }
    }
}

pub struct Batch {
    header: Vec<u8>,
    header_signature: String,
    transactions: Vec<Transaction>,
    trace: bool,
}

impl Batch {
    pub fn header(&self) -> &[u8] {
        &self.header
    }

    pub fn header_signature(&self) -> &str {
        &self.header_signature
    }

    pub fn transactions(&self) -> &[Transaction] {
        &self.transactions
    }

    pub fn trace(&self) -> bool {
        self.trace
    }
}

impl From<protos::batch::Batch> for Batch {
    fn from(batch: protos::batch::Batch) -> Self {
        Batch {
            header: batch.get_header().to_vec(),
            header_signature: batch.get_header_signature().to_string(),
            transactions: batch
                .get_transactions()
                .to_vec()
                .into_iter()
                .map(Transaction::from)
                .collect(),
            trace: batch.get_trace(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protobuf::Message;
    use sawtooth_sdk;

    static KEY1: &str = "111111111111111111111111111111111111111111111111111111111111111111";
    static KEY2: &str = "222222222222222222222222222222222222222222222222222222222222222222";
    static KEY3: &str = "333333333333333333333333333333333333333333333333333333333333333333";
    static BYTES1: [u8; 4] = [0x01, 0x02, 0x03, 0x04];
    static BYTES2: [u8; 4] = [0x05, 0x06, 0x07, 0x08];
    static BYTES3: [u8; 4] = [0x09, 0x0a, 0x0b, 0x0c];
    static BYTES4: [u8; 4] = [0x0d, 0x0e, 0x0f, 0x10];
    static BYTES5: [u8; 4] = [0x11, 0x12, 0x13, 0x14];
    static SIGNATURE1: &str =
        "sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1";
    static SIGNATURE2: &str =
        "sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2sig2";
    static SIGNATURE3: &str =
        "sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3sig3";

    #[test]
    fn batch_header_fields() {
        let header = BatchHeader {
            signer_public_key: hex::decode(KEY1).unwrap(),
            transaction_ids: vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap()],
        };

        assert_eq!(KEY1, hex::encode(header.signer_public_key()));
        assert_eq!(
            vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap(),],
            header.transaction_ids()
        );
    }

    #[test]
    fn batch_header_sawtooth10_compatibility() {
        // Create protobuf bytes using the Sawtooth SDK
        let mut proto = sawtooth_sdk::messages::batch::BatchHeader::new();
        proto.set_signer_public_key(KEY1.to_string());
        proto.set_transaction_ids(protobuf::RepeatedField::from_vec(vec![
            KEY2.to_string(),
            KEY3.to_string(),
        ]));
        let header_bytes = proto.write_to_bytes().unwrap();

        // Deserialize the header bytes into our protobuf
        let header_proto: protos::batch::BatchHeader =
            protobuf::parse_from_bytes(&header_bytes).unwrap();

        // Convert to a BatchHeader
        let header: BatchHeader = header_proto.into();

        assert_eq!(KEY1, hex::encode(header.signer_public_key()));
        assert_eq!(
            vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap(),],
            header.transaction_ids(),
        );
    }

    #[test]
    fn batch_fields() {
        let batch = Batch {
            header: BYTES1.to_vec(),
            header_signature: SIGNATURE1.to_string(),
            transactions: vec![
                Transaction::new(BYTES2.to_vec(), SIGNATURE2.to_string(), BYTES3.to_vec()),
                Transaction::new(BYTES4.to_vec(), SIGNATURE3.to_string(), BYTES5.to_vec()),
            ],
            trace: true,
        };

        assert_eq!(BYTES1.to_vec(), batch.header());
        assert_eq!(SIGNATURE1, batch.header_signature());
        assert_eq!(
            vec![
                Transaction::new(BYTES2.to_vec(), SIGNATURE2.to_string(), BYTES3.to_vec()),
                Transaction::new(BYTES4.to_vec(), SIGNATURE3.to_string(), BYTES5.to_vec()),
            ],
            batch.transactions()
        );
        assert_eq!(true, batch.trace());
    }

    #[test]
    fn batch_sawtooth10_compatibility() {}
}

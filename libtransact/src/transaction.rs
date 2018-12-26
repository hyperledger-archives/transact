use hex;

use crate::protos;

#[derive(Debug, PartialEq, Clone)]
pub enum HashMethod {
    SHA512,
}

#[derive(Debug, PartialEq, Clone)]
pub struct TransactionHeader {
    batcher_public_key: Vec<u8>,
    dependencies: Vec<Vec<u8>>,
    family_name: String,
    inputs: Vec<Vec<u8>>,
    outputs: Vec<Vec<u8>>,
    nonce: Vec<u8>,
    payload_hash: Vec<u8>,
    payload_hash_method: HashMethod,
    signer_public_key: Vec<u8>,
}

impl TransactionHeader {
    pub fn batcher_public_key(&self) -> &[u8] {
        &self.batcher_public_key
    }

    pub fn dependencies(&self) -> &[Vec<u8>] {
        &self.dependencies
    }

    pub fn family_name(&self) -> &str {
        &self.family_name
    }

    pub fn inputs(&self) -> &[Vec<u8>] {
        &self.inputs
    }

    pub fn nonce(&self) -> &[u8] {
        &self.nonce
    }

    pub fn outputs(&self) -> &[Vec<u8>] {
        &self.outputs
    }

    pub fn payload_hash(&self) -> &[u8] {
        &self.payload_hash
    }

    pub fn payload_hash_method(&self) -> &HashMethod {
        &self.payload_hash_method
    }

    pub fn signer_public_key(&self) -> &[u8] {
        &self.signer_public_key
    }
}

impl From<protos::transaction::TransactionHeader> for TransactionHeader {
    fn from(header: protos::transaction::TransactionHeader) -> Self {
        TransactionHeader {
            family_name: header.get_family_name().to_string(),
            batcher_public_key: hex::decode(header.get_batcher_public_key()).unwrap(),
            dependencies: header
                .get_dependencies()
                .iter()
                .map(|d| hex::decode(d).unwrap())
                .collect(),
            inputs: header
                .get_inputs()
                .iter()
                .map(|d| hex::decode(d).unwrap())
                .collect(),
            nonce: header.get_nonce().to_string().into_bytes(),
            outputs: header
                .get_outputs()
                .iter()
                .map(|d| hex::decode(d).unwrap())
                .collect(),
            payload_hash: hex::decode(header.get_payload_sha512()).unwrap(),
            payload_hash_method: HashMethod::SHA512,
            signer_public_key: hex::decode(header.get_signer_public_key()).unwrap(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Transaction {
    header: Vec<u8>,
    header_signature: String,
    payload: Vec<u8>,
}

impl Transaction {
    pub fn new(header: Vec<u8>, header_signature: String, payload: Vec<u8>) -> Self {
        Transaction {
            header,
            header_signature,
            payload,
        }
    }

    pub fn header(&self) -> &[u8] {
        &self.header
    }

    pub fn header_signature(&self) -> &str {
        &self.header_signature
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl From<protos::transaction::Transaction> for Transaction {
    fn from(transaction: protos::transaction::Transaction) -> Self {
        Transaction {
            header: transaction.get_header().to_vec(),
            header_signature: transaction.get_header_signature().to_string(),
            payload: transaction.get_payload().to_vec(),
        }
    }
}

pub struct TransactionPair {
    transaction: Transaction,
    header: TransactionHeader,
}

impl TransactionPair {
    pub fn transaction(&self) -> &Transaction {
        &self.transaction
    }

    pub fn header(&self) -> &TransactionHeader {
        &self.header
    }

    pub fn take(self) -> (Transaction, TransactionHeader) {
        (self.transaction, self.header)
    }
}

#[cfg(test)]
mod tests {
    use super::super::protos;
    use super::*;
    use protobuf::Message;
    use sawtooth_sdk;

    static FAMILY_NAME: &str = "test_family";
    static KEY1: &str = "111111111111111111111111111111111111111111111111111111111111111111";
    static KEY2: &str = "222222222222222222222222222222222222222222222222222222222222222222";
    static KEY3: &str = "333333333333333333333333333333333333333333333333333333333333333333";
    static KEY4: &str = "444444444444444444444444444444444444444444444444444444444444444444";
    static KEY5: &str = "555555555555555555555555555555555555555555555555555555555555555555";
    static KEY6: &str = "666666666666666666666666666666666666666666666666666666666666666666";
    static KEY7: &str = "777777777777777777777777777777777777777777777777777777777777777777";
    static KEY8: &str = "888888888888888888888888888888888888888888888888888888888888888888";
    static NONCE: &str = "f9kdzz";
    static HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";
    static BYTES1: [u8; 4] = [0x01, 0x02, 0x03, 0x04];
    static BYTES2: [u8; 4] = [0x05, 0x06, 0x07, 0x08];
    static SIGNATURE1: &str =
        "sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1sig1";

    #[test]
    fn transaction_header_fields() {
        let header = TransactionHeader {
            batcher_public_key: hex::decode(KEY1).unwrap(),
            dependencies: vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap()],
            family_name: FAMILY_NAME.to_string(),
            inputs: vec![
                hex::decode(KEY4).unwrap(),
                hex::decode(&KEY5[0..4]).unwrap(),
            ],
            nonce: NONCE.to_string().into_bytes(),
            outputs: vec![
                hex::decode(KEY6).unwrap(),
                hex::decode(&KEY7[0..4]).unwrap(),
            ],
            payload_hash: hex::decode(HASH).unwrap(),
            payload_hash_method: HashMethod::SHA512,
            signer_public_key: hex::decode(KEY8).unwrap(),
        };
        assert_eq!(KEY1, hex::encode(header.batcher_public_key()));
        assert_eq!(
            vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap(),],
            header.dependencies()
        );
        assert_eq!(FAMILY_NAME, header.family_name());
        assert_eq!(
            vec![
                hex::decode(KEY4).unwrap(),
                hex::decode(&KEY5[0..4]).unwrap(),
            ],
            header.inputs()
        );
        assert_eq!(
            vec![
                hex::decode(KEY6).unwrap(),
                hex::decode(&KEY7[0..4]).unwrap(),
            ],
            header.outputs()
        );
        assert_eq!(HASH, hex::encode(header.payload_hash()));
        assert_eq!(HashMethod::SHA512, *header.payload_hash_method());
        assert_eq!(KEY8, hex::encode(header.signer_public_key()));
    }

    #[test]
    fn transaction_header_sawtooth10_compatibility() {
        // Create protobuf bytes using the Sawtooth SDK
        let mut proto = sawtooth_sdk::messages::transaction::TransactionHeader::new();
        proto.set_batcher_public_key(KEY1.to_string());
        proto.set_dependencies(protobuf::RepeatedField::from_vec(vec![
            KEY2.to_string(),
            KEY3.to_string(),
        ]));
        proto.set_family_name(FAMILY_NAME.to_string());
        proto.set_inputs(protobuf::RepeatedField::from_vec(vec![
            KEY4.to_string(),
            (&KEY5[0..4]).to_string(),
        ]));
        proto.set_nonce(NONCE.to_string());
        proto.set_outputs(protobuf::RepeatedField::from_vec(vec![
            KEY6.to_string(),
            (&KEY7[0..4]).to_string(),
        ]));
        proto.set_payload_sha512(HASH.to_string());
        proto.set_signer_public_key(KEY8.to_string());
        let header_bytes = proto.write_to_bytes().unwrap();

        // Deserialize the header bytes into our protobuf
        let header_proto: protos::transaction::TransactionHeader =
            protobuf::parse_from_bytes(&header_bytes).unwrap();

        // Convert to a TransactionHeader
        let header: TransactionHeader = header_proto.into();

        assert_eq!(KEY1, hex::encode(header.batcher_public_key()));
        assert_eq!(
            vec![hex::decode(KEY2).unwrap(), hex::decode(KEY3).unwrap()],
            header.dependencies()
        );
        assert_eq!(FAMILY_NAME, header.family_name());
        assert_eq!(
            vec![
                hex::decode(KEY4).unwrap(),
                hex::decode(&KEY5[0..4]).unwrap()
            ],
            header.inputs()
        );
        assert_eq!(NONCE, String::from_utf8(header.nonce().to_vec()).unwrap());
        assert_eq!(
            vec![
                hex::decode(KEY6).unwrap(),
                hex::decode(&KEY7[0..4]).unwrap()
            ],
            header.outputs()
        );
        assert_eq!(hex::decode(HASH).unwrap(), header.payload_hash());
        assert_eq!(HashMethod::SHA512, *header.payload_hash_method());
        assert_eq!(hex::decode(KEY8).unwrap(), header.signer_public_key());
    }

    #[test]
    fn transaction_fields() {
        let transaction = Transaction {
            header: BYTES1.to_vec(),
            header_signature: SIGNATURE1.to_string(),
            payload: BYTES2.to_vec(),
        };

        assert_eq!(BYTES1.to_vec(), transaction.header());
        assert_eq!(SIGNATURE1, transaction.header_signature());
        assert_eq!(BYTES2.to_vec(), transaction.payload());
    }

    #[test]
    fn transaction_sawtooth10_compatibility() {
        // Create protobuf bytes using the Sawtooth SDK
        let mut proto = sawtooth_sdk::messages::transaction::Transaction::new();
        proto.set_header(BYTES1.to_vec());
        proto.set_header_signature(SIGNATURE1.to_string());
        proto.set_payload(BYTES2.to_vec());
        let transaction_bytes = proto.write_to_bytes().unwrap();

        // Deserialize the header bytes into our protobuf
        let transaction_proto: protos::transaction::Transaction =
            protobuf::parse_from_bytes(&transaction_bytes).unwrap();

        // Convert to a Transaction
        let transaction: Transaction = transaction_proto.into();

        assert_eq!(BYTES1.to_vec(), transaction.header());
        assert_eq!(SIGNATURE1, transaction.header_signature());
        assert_eq!(BYTES2.to_vec(), transaction.payload());
    }
}

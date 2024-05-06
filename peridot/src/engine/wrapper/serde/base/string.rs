use std::string::FromUtf8Error;

use crate::engine::wrapper::serde::{PeridotDeserializer, PeridotSerializer};

impl PeridotSerializer for String {
    type Input = String;
    type Error = FromUtf8Error;

    fn serialize(input: &Self::Input) -> Result<Vec<u8>, Self::Error> {
        Ok(input.as_bytes().to_vec())
    }
}

impl PeridotDeserializer for String {
    type Output = String;
    type Error = FromUtf8Error;

    fn deserialize(bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        String::from_utf8(bytes.to_vec())
    }
}

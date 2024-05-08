use std::{num::ParseIntError, string::FromUtf8Error};

use crate::engine::wrapper::serde::{PeridotDeserializer, PeridotSerializer};

impl PeridotSerializer for i64 {
    type Input = i64;
    type Error = ParseIntError;

    fn serialize(input: &Self::Input) -> Result<Vec<u8>, Self::Error> {
        Ok(input.to_string().as_bytes().to_vec())
    }
}

impl PeridotDeserializer for i64 {
    type Output = i64;
    type Error = ParseIntError;

    fn deserialize(bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        let string = String::deserialize(bytes).unwrap();

        string.parse::<i64>()
    }
}

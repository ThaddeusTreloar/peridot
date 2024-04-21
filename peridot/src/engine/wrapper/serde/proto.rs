use std::marker::PhantomData;

use prost::Message;

use super::{PeridotDeserializer, PeridotSerializer};

#[derive(Debug)]
pub struct Proto<T> {
    _type: PhantomData<T>,
}

impl <T> Default for Proto<T> {
    fn default() -> Self {
        Self {
            _type: Default::default()
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProtoSerdeError {}

impl<T> PeridotSerializer for Proto<T> 
where
    T: Message
{
    type Error = prost::EncodeError;
    type Input = T;

    fn serialize(input: &Self::Input) -> Result<Vec<u8>, Self::Error> {
        let mut buffer = Vec::new();

        input.encode(&mut buffer)?;

        Ok(buffer)
    }
}

impl<T> PeridotDeserializer for Proto<T> 
where
    T: Message + Default
{
    type Error = prost::DecodeError;
    type Output = T;

    fn deserialize(bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        T::decode(bytes)
    }
}
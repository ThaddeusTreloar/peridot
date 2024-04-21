use std::marker::PhantomData;

use super::{PeridotDeserializer, PeridotSerializer};

#[derive(Debug)]
pub struct NativeBytes<T> {
    _type: PhantomData<T>,
}

impl <T> Default for NativeBytes<T> {
    fn default() -> Self {
        Self {
            _type: Default::default()
        }
    }
}

impl<T> PeridotSerializer for NativeBytes<T> 
where
    T: serde::ser::Serialize
{
    type Error = bincode::Error;
    type Input = T;

    fn serialize(input: &Self::Input) -> Result<Vec<u8>, Self::Error> {
        bincode::serialize(input)
    }
}

impl<T> PeridotDeserializer for NativeBytes<T> 
where
    T: serde::de::DeserializeOwned
{
    type Error = bincode::Error;
    type Output = T;

    fn deserialize(bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        bincode::deserialize(bytes)
    }
}
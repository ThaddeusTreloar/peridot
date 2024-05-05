use std::{error::Error, marker::PhantomData, string::FromUtf8Error};

pub mod arvo;
pub mod json;
pub mod native;
pub mod proto;
pub mod serializers;

pub trait PeridotSerializer {
    type Input;
    type Error: Error;

    fn serialize(input: &Self::Input) -> Result<Vec<u8>, Self::Error>;
}

pub trait PeridotStatefulSerializer {
    type Input;
    type Error: Error;

    fn serialize(&self, input: &Self::Input) -> Result<Vec<u8>, Self::Error>;
}

pub trait PeridotDeserializer {
    type Output;
    type Error: Error;

    fn deserialize(bytes: &[u8]) -> Result<Self::Output, Self::Error>;
}

pub trait PeridotStatefulDeserializer {
    type Output;
    type Error: Error;

    fn deserialize(&self, bytes: &[u8]) -> Result<Self::Output, Self::Error>;
}

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

/*
TODO: Later we will have a default implementation for all Serde types

impl <'a, T> PeridotSerializer for T
where T: TryFrom<&'a [u8]>
{
    type Error = <Self as TryFrom<&'a [u8]>>::Error;
    type Input = T;

    fn serialize(input: &Self::Input) -> Result<Vec<u8>, Self::Error> {
        unimplemented!("")
    }
}
*/

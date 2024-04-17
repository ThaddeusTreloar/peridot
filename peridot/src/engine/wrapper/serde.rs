use std::{error::Error, marker::PhantomData, string::FromUtf8Error};

pub trait PSerialize {
    type Input;
    type Error: Error;

    fn serialize(input: &Self::Input) -> Result<Vec<u8>, Self::Error>;
}

pub trait PDeserialize {
    type Output;
    type Error: Error;

    fn deserialize(bytes: &[u8]) -> Result<Self::Output, Self::Error>;
}

pub struct Json<T> {
    _type: PhantomData<T>,
}

impl<S> PSerialize for Json<S>
where
    S: serde::Serialize,
{
    type Input = S;
    type Error = serde_json::Error;

    fn serialize(input: &Self::Input) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(input)
    }
}

impl<D> PDeserialize for Json<D>
where
    D: serde::de::DeserializeOwned,
{
    type Output = D;
    type Error = serde_json::Error;

    fn deserialize(bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        serde_json::from_slice(bytes)
    }
}

pub struct NativeBytes<T> {
    _type: PhantomData<T>,
}

#[derive(Debug, thiserror::Error)]
pub enum NativeSerdeError {}

impl<T> PSerialize for NativeBytes<T> {
    type Error = NativeSerdeError;
    type Input = T;

    fn serialize(_input: &Self::Input) -> Result<Vec<u8>, Self::Error> {
        unimplemented!("NativeBytes::serialize not implemented")
    }
}

impl<T> PDeserialize for NativeBytes<T> {
    type Error = NativeSerdeError;
    type Output = T;

    fn deserialize(_bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        unimplemented!("NativeBytes::deserialize not implemented")
    }
}

pub struct Avro<T> {
    _type: PhantomData<T>,
}

#[derive(Debug, thiserror::Error)]
pub enum AvroSerdeError {}

impl<T> PSerialize for Avro<T> {
    type Error = AvroSerdeError;
    type Input = T;

    fn serialize(_input: &Self::Input) -> Result<Vec<u8>, Self::Error> {
        unimplemented!("Avro::serialize not implemented")
    }
}

impl<T> PDeserialize for Avro<T> {
    type Error = AvroSerdeError;
    type Output = T;

    fn deserialize(_bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        unimplemented!("Avro::deserialize not implemented")
    }
}

pub struct ProtoBuf<T> {
    _type: PhantomData<T>,
}

#[derive(Debug, thiserror::Error)]
pub enum ProtoBufSerdeError {}

impl<T> PSerialize for ProtoBuf<T> {
    type Error = ProtoBufSerdeError;
    type Input = T;

    fn serialize(_input: &Self::Input) -> Result<Vec<u8>, Self::Error> {
        unimplemented!("ProtoBuf::serialize not implemented")
    }
}

impl<T> PDeserialize for ProtoBuf<T> {
    type Error = ProtoBufSerdeError;
    type Output = T;

    fn deserialize(_bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        unimplemented!("ProtoBuf::deserialize not implemented")
    }
}

impl PSerialize for String {
    type Input = String;
    type Error = FromUtf8Error;

    fn serialize(input: &Self::Input) -> Result<Vec<u8>, Self::Error> {
        Ok(input.as_bytes().to_vec())
    }
}

impl PDeserialize for String {
    type Output = String;
    type Error = FromUtf8Error;

    fn deserialize(bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        String::from_utf8(bytes.to_vec())
    }
}

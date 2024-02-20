use std::{marker::PhantomData, string::FromUtf8Error, error::Error};

pub trait PSerialize<T>
{
    type Error: Error;

    fn serialize(input: &T) -> Result<Vec<u8>, Self::Error>;
}

pub trait PDeserialize 
{
    type Output;
    type Error: Error;

    fn deserialize(bytes: &[u8]) -> Result<Self::Output, Self::Error>;
}

pub struct Json<T> 
{
    _type: PhantomData<T>
}

impl <S> PSerialize<S> for Json<S>
where S: serde::Serialize
{
    type Error = serde_json::Error;

    fn serialize(input: &S) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(input)
    }
}

impl <D> PDeserialize for Json<D>
where D: serde::de::DeserializeOwned
{
    type Output = D;
    type Error = serde_json::Error;

    fn deserialize(bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        serde_json::from_slice(bytes)
    }
}

pub struct Native<T> 
{
    _type: PhantomData<T>
}

impl PSerialize<String> for String
{
    type Error = FromUtf8Error;

    fn serialize(input: &String) -> Result<Vec<u8>, Self::Error> {
        Ok(input.as_bytes().to_vec())
    }
}

impl PDeserialize for String
{
    type Output = String;
    type Error = FromUtf8Error;

    fn deserialize(bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        String::from_utf8(bytes.to_vec())
    }
}
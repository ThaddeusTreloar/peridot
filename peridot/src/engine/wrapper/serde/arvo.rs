use std::marker::PhantomData;

use apache_avro::{from_value, Codec, Reader, Schema, Writer};
use serde::{de::DeserializeOwned, Serialize};

use super::{PeridotDeserializer, PeridotSerializer, PeridotStatefulDeserializer, PeridotStatefulSerializer};

pub struct Avro<T> {
    schema: Schema,
    codec: Option<Codec>,
    _type: PhantomData<T>,
}

impl <T> Avro<T> {
    fn new_with_schema(schema: Schema) -> Avro<T> {
        Avro { 
            schema, 
            codec: None, 
            _type: Default::default() 
        }
    }

    fn with_codec(mut self, codec: Codec) -> Self {
        let _ = self.codec.replace(codec);

        self
    }
}

impl<T> PeridotStatefulSerializer for Avro<T> 
where
    T: Serialize
{
    type Error = apache_avro::Error;
    type Input = T;

    fn serialize(&self, input: &Self::Input) -> Result<Vec<u8>, Self::Error> {
        let mut writer = match self.codec {
            Some(codec) => Writer::with_codec(&self.schema, Vec::new(), codec),
            None => Writer::new(&self.schema, Vec::new())
        };

        writer.append_ser(input);

        writer.into_inner()
    }
}

impl<T> PeridotDeserializer for Avro<T> 
where
    T: DeserializeOwned
{
    type Error = apache_avro::Error;
    type Output = T;

    fn deserialize(bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        let mut reader = Reader::new(bytes)?;

        match reader.next() {
            Some(Ok(value)) => from_value(&value),
            Some(Err(e)) => Err(e),
            None => panic!("No value found when getting next serde value.")
        }
    }
}

impl<T> PeridotStatefulDeserializer for Avro<T> 
where
    T: DeserializeOwned
{
    type Error = apache_avro::Error;
    type Output = T;

    fn deserialize(&self, bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        let mut reader = Reader::with_schema(&self.schema, bytes)?;

        match reader.next() {
            Some(Ok(value)) => from_value(&value),
            Some(Err(e)) => Err(e),
            None => panic!("No value found when getting next serde value.")
        }
    }
}
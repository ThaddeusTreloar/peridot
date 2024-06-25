/*
 * Copyright 2024 Thaddeus Treloar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

use std::{error::Error, marker::PhantomData, string::FromUtf8Error};

pub mod arvo;
pub mod base;
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

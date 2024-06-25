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

use std::marker::PhantomData;

use super::{PeridotDeserializer, PeridotSerializer};

#[derive(Debug)]
pub struct Json<T> {
    _type: PhantomData<T>,
}

impl<T> Default for Json<T> {
    fn default() -> Self {
        Self {
            _type: Default::default(),
        }
    }
}

impl<S> PeridotSerializer for Json<S>
where
    S: serde::Serialize,
{
    type Input = S;
    type Error = serde_json::Error;

    fn serialize(input: &Self::Input) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(input)
    }
}

impl<D> PeridotDeserializer for Json<D>
where
    D: serde::de::DeserializeOwned,
{
    type Output = D;
    type Error = serde_json::Error;

    fn deserialize(bytes: &[u8]) -> Result<Self::Output, Self::Error> {
        serde_json::from_slice(bytes)
    }
}

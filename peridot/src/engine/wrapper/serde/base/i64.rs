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

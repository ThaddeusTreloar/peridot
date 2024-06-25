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

#[derive(Debug)]
pub struct Serializers<KS, VS> {
    key_serialiser: Option<KS>,
    value_serialiser: Option<VS>,
}

impl<KS, VS> Default for Serializers<KS, VS> {
    fn default() -> Self {
        Self {
            key_serialiser: None,
            value_serialiser: None,
        }
    }
}

impl<KS, VS> Serializers<KS, VS> {
    fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    fn new_with_key_ser(key_serialiser: KS) -> Self {
        Self {
            key_serialiser: Some(key_serialiser),
            ..Default::default()
        }
    }

    fn new_with_value_ser(value_serialiser: VS) -> Self {
        Self {
            value_serialiser: Some(value_serialiser),
            ..Default::default()
        }
    }

    fn new_with_serialisers(key_serialiser: KS, value_serialiser: VS) -> Self {
        Self {
            key_serialiser: Some(key_serialiser),
            value_serialiser: Some(value_serialiser),
        }
    }

    fn with_key_ser(mut self, key_serialiser: KS) -> Self {
        let _ = self.key_serialiser.replace(key_serialiser);

        self
    }

    fn with_value_ser(mut self, value_serialiser: VS) -> Self {
        let _ = self.value_serialiser.replace(value_serialiser);

        self
    }
}

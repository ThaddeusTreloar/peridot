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

#[derive(Debug, Default, Clone)]
pub struct Checkpoint {
    pub store_name: String,
    pub offset: i64,
}

impl Checkpoint {
    pub fn new(store_name: String, offset: i64) -> Self {
        Self { store_name, offset }
    }

    pub fn set_offset(&mut self, new_offset: i64) {
        self.offset = new_offset
    }

    pub fn set_offset_if_greater(&mut self, maybe_new_offset: i64) {
        if maybe_new_offset > self.offset {
            self.offset = maybe_new_offset
        }
    }
}

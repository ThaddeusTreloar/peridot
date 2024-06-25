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

use super::{MessageHeaders, PeridotTimestamp};

pub struct PartialMessage<K, V> {
    pub(crate) topic: Option<String>,
    pub(crate) timestamp: Option<PeridotTimestamp>,
    pub(crate) partition: Option<i32>,
    pub(crate) offset: Option<i64>,
    pub(crate) headers: Option<MessageHeaders>,
    pub(crate) key: Option<K>,
    pub(crate) value: Option<V>,
}

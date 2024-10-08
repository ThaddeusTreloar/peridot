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

mod extractors;
mod headers;
mod message;
mod offset;
mod partial_message;
mod timestamp;

pub use extractors::{headers::*, key_value::*, value::*};
pub use headers::*;
pub use message::*;
pub(crate) use partial_message::*;
pub use timestamp::*;

pub trait FromMessageOwned<K, V> {
    fn from_message_owned(msg: Message<K, V>) -> (Self, PartialMessageOwned<K, V>)
    where
        Self: Sized;
}

pub trait FromMessageMut<K, V> {
    fn from_message_mut<'a>(msg: &'a mut Message<K, V>) -> Self
    where
        Self: Sized;
}

pub trait FromMessage<K, V> {
    fn from_message<'a>(msg: &'a Message<K, V>) -> PartialMessage<'a, K, V>
    where
        Self: Sized;
}

pub trait PatchMessage<K, V> {
    type RK;
    type RV;

    fn patch_message(self, msg: PartialMessageOwned<K, V>) -> Message<Self::RK, Self::RV>
    where
        Self: Sized;
}

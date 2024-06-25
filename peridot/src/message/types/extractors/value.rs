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

use std::fmt::Debug;

use crate::message::types::{FromMessage, Message, PartialMessage, PatchMessage};

pub struct Value<K>(pub K);

impl<K> Value<K> {
    pub fn value(self) -> K {
        let Self(value) = self;

        value
    }
}

impl<K, V> FromMessage<K, V> for Value<V> {
    fn from_message(
        Message {
            topic,
            timestamp,
            partition,
            offset,
            headers,
            key,
            value,
        }: Message<K, V>,
    ) -> (Self, PartialMessage<K, V>)
    where
        Self: Sized,
    {
        let partial_message = PartialMessage {
            topic: Some(topic),
            timestamp: Some(timestamp),
            partition: Some(partition),
            offset: Some(offset),
            headers: Some(headers),
            key: Some(key),
            value: None,
        };

        (Self(value), partial_message)
    }
}

impl<K, V, VR> PatchMessage<K, V> for Value<VR> {
    type RK = K;
    type RV = VR;

    fn patch(self, partial_message: PartialMessage<K, V>) -> Message<Self::RK, Self::RV> {
        match partial_message {
            PartialMessage {
                topic: Some(topic),
                timestamp: Some(timestamp),
                partition: Some(partition),
                offset: Some(offset),
                headers: Some(headers),
                key: Some(key),
                ..
            } => Message {
                topic,
                timestamp,
                partition,
                offset,
                headers,
                key,
                value: self.value(),
            },
            _ => panic!("Missing value in partial message, this should not be possible."),
        }
    }
}

impl<K, V, VR> PatchMessage<K, V> for VR
where
    VR: Debug,
{
    type RK = K;
    type RV = VR;

    fn patch(self, partial_message: PartialMessage<K, V>) -> Message<Self::RK, Self::RV> {
        match partial_message {
            PartialMessage {
                topic: Some(topic),
                timestamp: Some(timestamp),
                partition: Some(partition),
                offset: Some(offset),
                headers: Some(headers),
                key: Some(key),
                ..
            } => Message {
                topic,
                timestamp,
                partition,
                offset,
                headers,
                key,
                value: self,
            },
            _ => panic!("Missing value in partial message, this should not be possible."),
        }
    }
}

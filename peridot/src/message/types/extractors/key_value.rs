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

use partial_message::PartialMessage;

use crate::message::types::{partial_message, FromMessage, Message, PatchMessage};

pub struct KeyValue<K, V>(pub K, pub V);

/*
impl<K, V> FromMessage<K, V> for (K, V)
{
    fn from_message(
        Message {
            topic,
            timestamp,
            partition,
            offset,
            headers,
            key,
            value
        }: Message<K, V>
    ) -> (Self, PartialMessage<K, V>)
    where Self: Sized
    {
        let partial_message = PartialMessage {
            topic: Some(topic),
            timestamp: Some(timestamp),
            partition: Some(partition),
            offset: Some(offset),
            headers: Some(headers),
            key: None,
            value: None,
        };

        ((key, value), partial_message)
    }
}

impl<K, V, KR, VR> PatchMessage<K, V> for (KR, VR) {
    type RK = KR;
    type RV = VR;

    fn patch(
        self,
        partial_message: PartialMessage<K, V>
    ) -> Message<Self::RK, Self::RV> {
        match partial_message {
            PartialMessage {
                topic: Some(topic),
                timestamp: Some(timestamp),
                partition: Some(partition),
                offset: Some(offset),
                headers: Some(headers),
                ..
            } => {
                Message {
                    topic,
                    timestamp,
                    partition,
                    offset,
                    headers,
                    key: self.0,
                    value: self.1,
                }
            },
            _ => panic!("Missing value in partial message, this should not be possible.")
        }
    }
}
 */

impl<K, V> FromMessage<K, V> for KeyValue<K, V> {
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
            key: None,
            value: None,
        };

        (KeyValue(key, value), partial_message)
    }
}

impl<K, V, KR, VR> PatchMessage<K, V> for KeyValue<KR, VR> {
    type RK = KR;
    type RV = VR;

    fn patch(self, partial_message: PartialMessage<K, V>) -> Message<Self::RK, Self::RV> {
        let Self(key, value) = self;

        match partial_message {
            PartialMessage {
                topic: Some(topic),
                timestamp: Some(timestamp),
                partition: Some(partition),
                offset: Some(offset),
                headers: Some(headers),
                ..
            } => Message {
                topic,
                timestamp,
                partition,
                offset,
                headers,
                key,
                value,
            },
            _ => panic!("Missing value in partial message, this should not be possible."),
        }
    }
}

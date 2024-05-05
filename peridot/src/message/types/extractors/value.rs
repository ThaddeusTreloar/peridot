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

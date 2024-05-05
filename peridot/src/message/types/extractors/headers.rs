use crate::message::types::{FromMessage, Message, MessageHeaders, PartialMessage, PatchMessage};

pub struct Headers(MessageHeaders);

impl<K, V> FromMessage<K, V> for Headers {
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
            headers: None,
            key: Some(key),
            value: Some(value),
        };

        (Headers(headers), partial_message)
    }
}

impl<K, V> PatchMessage<K, V> for Headers {
    type RK = K;
    type RV = V;

    fn patch(self, partial_message: PartialMessage<K, V>) -> Message<Self::RK, Self::RV> {
        match partial_message {
            PartialMessage {
                topic: Some(topic),
                timestamp: Some(timestamp),
                partition: Some(partition),
                offset: Some(offset),
                key: Some(key),
                value: Some(value),
                ..
            } => Message {
                topic,
                timestamp,
                partition,
                offset,
                headers: self.0,
                key,
                value,
            },
            _ => panic!("Missing value in partial message, this should not be possible."),
        }
    }
}

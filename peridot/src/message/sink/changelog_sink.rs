use std::{
    pin::Pin,
    task::{Context, Poll},
};

use rdkafka::producer::FutureRecord;

use crate::{
    engine::{
        wrapper::serde::{NativeBytes, PSerialize},
        QueueMetadata,
    },
    message::sink::{MessageSink, NonCommittingSink},
};

pub struct ChangelogSink<K, V> {
    changelog_topic: String,
    queue_metadata: QueueMetadata,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<K, V> ChangelogSink<K, V> {
    pub fn from_queue_metadata(queue_metadata: QueueMetadata, changelog_topic: String) -> Self {
        Self {
            changelog_topic,
            queue_metadata,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChangelogSinkError {}

impl<K, V> NonCommittingSink for ChangelogSink<K, V> {}

impl<K, V> MessageSink<K, V> for ChangelogSink<K, V>
where
    K: Clone,
    V: Clone,
{
    type Error = ChangelogSinkError;

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_commit(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        message: crate::message::types::Message<K, V>,
    ) -> Result<(), Self::Error> {
        let key =
            NativeBytes::serialize(message.key()).expect("Failed to serialise key in StateSink.");
        let value = NativeBytes::serialize(message.value())
            .expect("Failed to serialise value in StateSink.");

        let record = FutureRecord {
            topic: &self.changelog_topic,
            partition: None,
            payload: Some(&value),
            key: Some(&key),
            timestamp: message.timestamp().into(),
            headers: Some(message.headers().into_owned_headers()),
        };

        let _delivery_future = self
            .queue_metadata
            .producer()
            .send_result(record)
            .expect("Failed to queue record.");

        Ok(())
    }
}

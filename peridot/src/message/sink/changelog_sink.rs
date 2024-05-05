use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::FutureExt;
use pin_project_lite::pin_project;
use rdkafka::{
    error::KafkaError,
    producer::{DeliveryFuture, FutureRecord},
    Message,
};
use serde::Serialize;
use tracing::{info, warn};

use crate::{
    engine::{
        queue_manager::queue_metadata::QueueMetadata,
        wrapper::serde::{native::NativeBytes, PeridotSerializer},
    },
    message::sink::{MessageSink, NonCommittingSink},
};

pin_project! {
    pub struct ChangelogSink<K, V> {
        changelog_topic: String,
        queue_metadata: QueueMetadata,
        delivery_futures: Vec<DeliveryFuture>,
        _key_type: std::marker::PhantomData<K>,
        _value_type: std::marker::PhantomData<V>,
    }
}

impl<K, V> ChangelogSink<K, V> {
    pub fn from_queue_metadata(queue_metadata: QueueMetadata, changelog_topic: String) -> Self {
        Self {
            changelog_topic,
            queue_metadata,
            delivery_futures: Default::default(),
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChangelogSinkError {
    #[error(
        "Failed to send message for topic: {}, partition: {}, offset: {}, caused by: {}",
        topic,
        partition,
        offset,
        err
    )]
    FailedToSendMessageError {
        topic: String,
        partition: i32,
        offset: i64,
        err: KafkaError,
    },
}

impl<K, V> NonCommittingSink for ChangelogSink<K, V> {}

impl<K, V> MessageSink<K, V> for ChangelogSink<K, V>
where
    K: Serialize,
    V: Serialize,
{
    type Error = ChangelogSinkError;

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_commit(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        tracing::debug!("Checking changelog delivery futures.");

        let this = self.project();

        let mut futures: Vec<_> = this.delivery_futures.drain(0..).collect();

        for mut future in futures.into_iter() {
            match future.poll_unpin(cx) {
                Poll::Ready(Ok(result)) => match result {
                    Ok((partition, offset)) => {
                        tracing::debug!("Successfully sent changelog record with offset: {}, for topic: {}, partition: {}", offset, this.changelog_topic, partition);
                    }
                    Err((err, msg)) => {
                        let ret_err = ChangelogSinkError::FailedToSendMessageError {
                            topic: this.changelog_topic.to_owned(),
                            partition: msg.partition(),
                            offset: msg.offset(),
                            err,
                        };

                        tracing::error!("{}", ret_err);

                        Err(ret_err)?
                    }
                },
                Poll::Ready(Err(e)) => {
                    tracing::error!("Delivery future failed while awaiting, caused by: {}", e);
                }
                _ => {
                    tracing::debug!("Delivery future not yet completed.");
                    this.delivery_futures.push(future);
                }
            }
        }

        if this.delivery_futures.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        message: crate::message::types::Message<K, V>,
    ) -> Result<(), Self::Error> {
        let this = self.project();

        let key =
            NativeBytes::serialize(message.key()).expect("Failed to serialise key in StateSink.");
        let value = NativeBytes::serialize(message.value())
            .expect("Failed to serialise value in StateSink.");

        let record = FutureRecord {
            topic: this.changelog_topic,
            partition: Some(message.partition()),
            payload: Some(&value),
            key: Some(&key),
            timestamp: message.timestamp().into(),
            headers: Some(message.headers().into_owned_headers()),
        };

        let delivery_future = this
            .queue_metadata
            .producer()
            .send_result(record)
            .expect("Failed to queue record.");

        this.delivery_futures.push(delivery_future);

        tracing::debug!(
            "Queued record for changelog topic: {}, partition: {}",
            this.changelog_topic,
            message.partition()
        );

        Ok(())
    }
}

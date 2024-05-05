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
use tracing::{debug, info, warn};

use crate::{
    engine::{
        queue_manager::queue_metadata::QueueMetadata,
        wrapper::serde::{native::NativeBytes, PeridotSerializer},
    },
    message::sink::{MessageSink, NonCommittingSink},
};

pin_project! {
    pub struct TopicSink<KS, VS> {
        topic: String,
        queue_metadata: QueueMetadata,
        delivery_futures: Vec<DeliveryFuture>,
        _key_ser_type: std::marker::PhantomData<KS>,
        _value_ser_type: std::marker::PhantomData<VS>,
    }
}

impl<KS, VS> TopicSink<KS, VS> {
    pub fn from_queue_metadata(queue_metadata: QueueMetadata, topic: &str) -> Self {
        Self {
            topic: topic.to_owned(),
            queue_metadata,
            delivery_futures: Default::default(),
            _key_ser_type: Default::default(),
            _value_ser_type: Default::default(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TopicSinkError {
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

impl<KS, VS> MessageSink<KS::Input, VS::Input> for TopicSink<KS, VS>
where
    KS: PeridotSerializer,
    KS::Input: Serialize,
    VS: PeridotSerializer,
    VS::Input: Serialize,
{
    type Error = TopicSinkError;

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_commit(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // TODO: move this logic to poll_ready.
        tracing::debug!("Checking topic delivery futures.");

        let this = self.project();

        let mut futures: Vec<_> = this.delivery_futures.drain(0..).collect();

        for mut future in futures.into_iter() {
            match future.poll_unpin(cx) {
                Poll::Ready(Ok(result)) => match result {
                    Ok((partition, offset)) => {
                        tracing::debug!("Successfully sent topic record with offset: {}, for topic: {}, partition: {}", offset, this.topic, partition);
                    }
                    Err((err, msg)) => {
                        let ret_err = TopicSinkError::FailedToSendMessageError {
                            topic: this.topic.to_owned(),
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
        // TODO: the logic from poll_commit will go here.
        // we will check the buffered message requests, then if the buffer is full,
        // check all of the producer send requests.
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        message: crate::message::types::Message<KS::Input, VS::Input>,
    ) -> Result<(), Self::Error> {
        let this = self.project();

        let key_bytes =
            KS::serialize(message.key()).expect("Failed to serialise key in StateSink.");

        let value_bytes =
            VS::serialize(message.value()).expect("Failed to serialise value in StateSink.");

        tracing::trace!(
            "Raw output key bytes for topic: {}, partition: {}, offset: {}, key: {:?}",
            message.topic(),
            message.partition(),
            message.offset(),
            &key_bytes
        );

        tracing::trace!(
            "Raw output value bytes for topic: {}, partition: {}, offset: {}, value: {:?}",
            message.topic(),
            message.partition(),
            message.offset(),
            &value_bytes
        );

        let record = FutureRecord {
            topic: this.topic,
            partition: None,
            key: Some(&key_bytes),
            payload: Some(&value_bytes),
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
            "Queued record for topic: {}, partition: {}",
            this.topic,
            message.partition()
        );

        Ok(())
    }
}

use std::{
    collections::VecDeque,
    default,
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
use tracing::{debug, info, warn, Level};

use crate::{
    engine::{
        queue_manager::queue_metadata::QueueMetadata,
        wrapper::serde::{native::NativeBytes, PeridotSerializer},
    },
    message::sink::{MessageSink, NonCommittingSink},
    util::common_format::{topic_partition_offset, topic_partition_offset_err},
};

mod error;
pub use error::TopicSinkError;

pub(crate) const CHANGELOG_OFFSET_HEADER: &str = "peridot-internal-changelog-offset";

#[derive(Debug, Clone, Default, derive_more::Display)]
pub(crate) enum TopicType {
    #[default]
    Output,
    Changelog,
    #[display(fmt = "Bench")]
    Bench(tokio::sync::mpsc::Sender<(i32, i64)>),
}

impl TopicType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Output => "Output",
            Self::Changelog => "Changelog",
            Self::Bench(_) => "Bench",
        }
    }
}

pin_project! {
    pub struct TopicSink<KS, VS> {
        topic: String,
        queue_metadata: QueueMetadata,
        delivery_futures: VecDeque<DeliveryFuture>,
        topic_type: TopicType,
        highest_offset: i64,
        _key_ser_type: std::marker::PhantomData<KS>,
        _value_ser_type: std::marker::PhantomData<VS>,
    }
}

impl<KS, VS> TopicSink<KS, VS> {
    pub(crate) fn from_queue_metadata(queue_metadata: QueueMetadata, topic: &str) -> Self {
        Self {
            topic: topic.to_owned(),
            queue_metadata,
            delivery_futures: Default::default(),
            topic_type: Default::default(),
            highest_offset: 0,
            _key_ser_type: Default::default(),
            _value_ser_type: Default::default(),
        }
    }

    pub(crate) fn with_topic_type(mut self, topic_type: TopicType) -> Self {
        self.topic_type = topic_type;

        self
    }
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

    fn poll_commit(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<i64, Self::Error>> {
        let span = tracing::span!(
            Level::DEBUG,
            "->TopicSink::poll_commit",
            target_topic = self.topic,
            sink_type = self.topic_type.as_str(),
        )
        .entered();

        tracing::debug!("Checking topic delivery futures.");

        let this = self.project();

        while let Some(mut future) = this.delivery_futures.pop_front() {
            match future.poll_unpin(cx) {
                Poll::Ready(Ok(result)) => match result {
                    Ok((partition, offset)) => {
                        if let TopicType::Bench(_) = this.topic_type {
                            *this.highest_offset = offset;
                        }

                        tracing::trace!("Successfully sent topic record for offset: {}", offset);
                    }
                    Err((err, msg)) => match err {
                        KafkaError::MessageProduction(e) => {}
                        err => {
                            tracing::error!(
                                "Error in topic production for {}",
                                topic_partition_offset_err(
                                    this.topic,
                                    msg.partition(),
                                    msg.offset(),
                                    &err
                                )
                            );

                            let _ = this.delivery_futures.drain(..);

                            let ret_err = TopicSinkError::UnknownError {
                                topic: this.topic.to_owned(),
                                partition: msg.partition(),
                                offset: msg.offset(),
                                err,
                            };

                            Err(ret_err)?
                        }
                    },
                },
                Poll::Ready(Err(e)) => {
                    todo!("Delivery future cancelled");
                }
                _ => {
                    this.delivery_futures.push_front(future);

                    return Poll::Pending;
                }
            }
        }

        if let TopicType::Bench(sender) = this.topic_type {
            if *this.highest_offset > 0 {
                sender
                    .try_send((this.queue_metadata.partition(), *this.highest_offset))
                    .unwrap();
            }
        }

        Poll::Ready(Ok(*this.highest_offset))
    }

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        mut message: crate::message::types::Message<KS::Input, VS::Input>,
    ) -> Result<crate::message::types::Message<KS::Input, VS::Input>, Self::Error> {
        let span = tracing::span!(
            Level::DEBUG,
            "->TopicSink::start_send",
            target_topic = self.topic,
            sink_type = self.topic_type.as_str(),
        )
        .entered();

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

        let record = match this.topic_type {
            TopicType::Output | TopicType::Bench(_) => FutureRecord {
                topic: this.topic,
                partition: None,
                key: Some(&key_bytes),
                payload: Some(&value_bytes),
                timestamp: message.timestamp().into(),
                headers: Some(message.headers().into_owned_headers()),
            },
            TopicType::Changelog => FutureRecord {
                topic: this.topic,
                partition: Some(message.partition()),
                key: Some(&key_bytes),
                payload: Some(&value_bytes),
                timestamp: message.timestamp().into(),
                headers: Some(message.headers().into_owned_headers()),
            },
        };

        let delivery_future = this
            .queue_metadata
            .producer()
            .send_result(record)
            .expect("Failed to queue record.");

        this.delivery_futures.push_back(delivery_future);

        tracing::debug!(
            "Queued record for topic: {}, partition: {}",
            this.topic,
            message.partition()
        );

        if let TopicType::Changelog = this.topic_type {
            let bytes = <i64 as PeridotSerializer>::serialize(this.highest_offset).unwrap();

            message.headers_mut().set(CHANGELOG_OFFSET_HEADER, bytes);
        }

        Ok(message)
    }
}

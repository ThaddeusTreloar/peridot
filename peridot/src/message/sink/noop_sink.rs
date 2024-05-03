use std::{
    pin::Pin,
    task::{Context, Poll}, time::Duration,
};

use pin_project_lite::pin_project;
use rdkafka::{consumer::Consumer, producer::Producer, Offset, TopicPartitionList};

use crate::engine::queue_manager::queue_metadata::QueueMetadata;

use super::{CommitingSink, MessageSink};

pin_project! {
    #[project = NoopProjection]
    pub struct NoopSink<K, V> {
        queue_metadata: QueueMetadata,
        offsets: TopicPartitionList,
        _key_type: std::marker::PhantomData<K>,
        _value_type: std::marker::PhantomData<V>,
    }
}

impl<K, V> NoopSink<K, V> {
    pub fn from_queue_metadata(queue_metadata: QueueMetadata) -> Self {
        Self {
            queue_metadata,
            offsets: Default::default(),
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

impl <K, V> CommitingSink for NoopSink<K, V> {}

#[derive(Debug, thiserror::Error)]
pub enum NoopSinkError {}

impl<K, V> MessageSink<K, V> for NoopSink<K, V>
{
    type Error = NoopSinkError;

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_commit(cx)
    }

    fn poll_commit(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let NoopProjection {
            queue_metadata,
            offsets,
            ..
        } = self.project();

        queue_metadata.producer().send_offsets_to_transaction(
            offsets, 
            &queue_metadata
                .engine_context()
                .group_metadata(), 
            Duration::from_millis(1000)
        ).expect("Failed to send offsets to transaction.");

        queue_metadata.producer()
            .commit_transaction(Duration::from_millis(1000))
            .expect("Failed to commit transaction.");

        queue_metadata.producer()
            .begin_transaction()
            .expect("Failed to begin transaction.");

        Poll::Ready(Ok(()))
    }

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        message: crate::message::types::Message<K, V>,
    ) -> Result<(), Self::Error> {
        let NoopProjection {
            offsets,
            ..
        } = self.project();

        offsets
            .add_partition_offset(message.topic(), message.partition(), Offset::Offset(message.offset()))
            .expect("Failed to add partition offset.");

        Ok(())
    }
}

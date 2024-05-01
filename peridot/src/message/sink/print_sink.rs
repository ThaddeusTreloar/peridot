use std::{
    fmt::Display,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
use rdkafka::{consumer::Consumer, TopicPartitionList};
use tracing::info;

use crate::{
    engine::{queue_manager::queue_metadata::QueueMetadata, wrapper::serde::PeridotSerializer},
    message::types::Message,
};

use super::MessageSink;

pin_project! {
    pub struct PrintSink<KS, VS>
    where
        KS: PeridotSerializer,
        VS: PeridotSerializer,
    {
        queue_metadata: QueueMetadata,
        _key_serialiser_type: PhantomData<KS>,
        _value_serialiser_type: PhantomData<VS>,
    }
}

impl<KS, VS> PrintSink<KS, VS>
where
    KS: PeridotSerializer,
    VS: PeridotSerializer,
{
    pub fn new(queue_metadata: QueueMetadata) -> Self {
        Self {
            queue_metadata,
            _key_serialiser_type: PhantomData,
            _value_serialiser_type: PhantomData,
        }
    }

    pub fn from_queue_metadata(queue_metadata: QueueMetadata) -> Self {
        Self::new(queue_metadata)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PrintSinkError {}

impl<KS, VS> MessageSink<KS::Input, VS::Input> for PrintSink<KS, VS>
where
    KS: PeridotSerializer,
    VS: PeridotSerializer,
    KS::Input: Display,
    VS::Input: Display,
{
    type Error = PrintSinkError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        message: Message<KS::Input, VS::Input>,
    ) -> Result<(), Self::Error> {
        let ser_key = KS::serialize(message.key()).expect("Failed to serialise key.");
        let ser_value = VS::serialize(message.value()).expect("Failed to serialise value.");

        info!("Debug Sink: Sending message: {}", message);
        info!(
            "Debug Sink: Serialised message: {{ key: {:?}, value: {:?} }}",
            ser_key, ser_value
        );
        info!(
            "Debug Sink: Queue metadata: {{ partition: {}, source_topic: {} }}",
            self.queue_metadata.partition(),
            self.queue_metadata.source_topic()
        );

        let mut topic_partition_list = TopicPartitionList::default();
        topic_partition_list
            .add_partition_offset(
                self.queue_metadata.source_topic(),
                message.partition(),
                rdkafka::Offset::Offset(message.offset() + 1),
            )
            .expect("Failed to add partition offset");

        self.queue_metadata
            .engine_context()
            .client_manager()
            .consumer_ref()
            .commit(&topic_partition_list, rdkafka::consumer::CommitMode::Async)
            .expect("Failed to make async commit in state store");

        Ok(())
    }

    fn poll_commit(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

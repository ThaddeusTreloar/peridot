use std::pin::Pin;

use pin_project_lite::pin_project;
use serde::Serialize;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::{
    engine::QueueMetadata,
    message::{
        forward::Forward,
        stream::{transparent::TransparentQueue, MessageStream, PipelineStage},
    },
    pipeline::{
        forked_forward::PipelineForkedForward,
        sink::PipelineSink,
        stream::{transparent::TransparentPipeline, ChannelSinkPipeline},
    },
};

use super::partition_handler::TablePartitionHandler;

pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub (super) struct QueueReceiverHandler<K, V>
    {
        changelog_topic: String,
        downstream: ChannelSinkPipeline<K, V>,
    }
}

impl<M> QueueReceiverHandler<M> {
    pub(super) fn new(changelog_topic: String, downstream: TransparentPipeline<M>) -> Self {
        Self {
            changelog_topic,
            downstream,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QueueReceiverHandlerError {}

impl<M> PipelineSink<M> for QueueReceiverHandler<M>
where
    M: MessageStream + Send + 'static,
    M::KeyType: Serialize + Send + 'static,
    M::ValueType: Serialize + Send + 'static,
{
    type Error = QueueReceiverHandlerError;
    type SinkType = TablePartitionHandler<M::KeyType, M::ValueType>;

    fn start_send(
        self: Pin<&mut Self>,
        pipeline_stage: PipelineStage<M>,
    ) -> Result<(), Self::Error> {
        let PipelineStage(metadata, message_stream) = pipeline_stage;

        // TODO: Handle this forwarder
        let partition_handler = TablePartitionHandler::new(metadata, String::from("some_topic"));

        let passthrough_channel = tokio::sync::mpsc::unbounded_channel();

        let forwarder = PipelineForkedForward::new(message_stream, partition_handler);

        tokio::spawn(forwarder);

        Ok(())
    }
}

use std::pin::Pin;

use pin_project_lite::pin_project;
use serde::Serialize;

use crate::{
    message::stream::{MessageStream, PipelineStage},
    pipeline::sink::{Forward, PipelineSink},
};

use super::partition_handler::TablePartitionHandler;

pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub (super) struct QueueReceiverHandler
    {
        changelog_topic: String,
    }
}

impl QueueReceiverHandler {
    pub(super) fn new(changelog_topic: String) -> Self {
        Self { changelog_topic }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QueueReceiverHandlerError {}

impl<M> PipelineSink<M> for QueueReceiverHandler
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
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();

        let partition_handler =
            TablePartitionHandler::new(metadata, String::from("some_topic"), sender);

        let forwarder = Forward::new(message_stream, partition_handler);

        tokio::spawn(forwarder);

        Ok(())
    }
}

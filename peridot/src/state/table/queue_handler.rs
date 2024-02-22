use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{ready, Future};
use pin_project_lite::pin_project;

use crate::{
    pipeline::{
        message::stream::{MessageStream, PipelineStage},
        pipeline::{sink::PipelineSink, stream::PipelineStream},
    },
    state::backend::{ReadableStateBackend, WriteableStateBackend},
};

use super::partition_handler::TablePartitionHandler;

pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub (super) struct QueueReceiverHandler<B, P>
    {
        backend: Arc<B>,
        #[pin]
        queue_receiver: P,
    }
}

impl<B, P> QueueReceiverHandler<B, P> {
    pub(super) fn new(backend: Arc<B>, queue_receiver: P) -> Self {
        Self {
            backend,
            queue_receiver,
        }
    }
}

impl<B, P> Future for QueueReceiverHandler<B, P>
where
    B: ReadableStateBackend
        + WriteableStateBackend<B::KeyType, B::ValueType>
        + Send
        + Sync
        + 'static,
    B::KeyType: Clone + Send,
    B::ValueType: Clone + Send,
    P: PipelineStream<KeyType = B::KeyType, ValueType = B::ValueType> + Send,
    P::MStream: 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        while let Some(PipelineStage(metadata, queue)) =
            ready!(this.queue_receiver.as_mut().poll_next(cx))
        {
            //let partition_handler = TablePartitionHandler::new(this.backend.clone(), queue, metadata, String::from("some_topic"));

            //tokio::spawn(partition_handler);
        }

        Poll::Ready(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QueueReceiverHandlerError {}

impl<B, P> PipelineSink<P::MStream> for QueueReceiverHandler<B, P>
where
    P: PipelineStream,
{
    type SinkType = TablePartitionHandler<P::MStream>;
    type Error = QueueReceiverHandlerError;

    fn start_send(
        self: Pin<&mut Self>,
        message: PipelineStage<P::MStream>,
    ) -> Result<(), Self::Error> {
        let (metadata, queue) = message;
    }
}

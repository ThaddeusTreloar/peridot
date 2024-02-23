use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Future;
use pin_project_lite::pin_project;

use crate::{
    app::error::PeridotAppRuntimeError,
    engine::util::ExactlyOnce,
    message::{
        forward::Forward,
        sink::MessageSink,
        stream::{MessageStream, PipelineStage},
    },
    serde_ext::PSerialize,
};

use super::{
    sink::MessageSinkFactory,
    stream::PipelineStream,
};

pin_project! {
    #[project = SinkProjection]
    pub struct PipelineForward<S, SF, G = ExactlyOnce>
    where
        S: PipelineStream,
    {
        #[pin]
        queue_stream: S,
        sink_factory: SF,
        _delivery_guarantee: PhantomData<G>
    }
}

impl<S, SF, G> PipelineForward<S, SF, G>
where
    S: PipelineStream,
{
    pub fn new(queue_stream: S, sink_factory: SF) -> Self {
        Self {
            queue_stream,
            sink_factory,
            _delivery_guarantee: PhantomData,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("Queue receiver error: {0}")]
    QueueReceiverError(String),
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineForwardError {}

impl<S, SF, G> Future for PipelineForward<S, SF, G>
where
    S: PipelineStream + Send + 'static,
    S::MStream: MessageStream + Send + 'static,
    SF: MessageSinkFactory + Send + 'static,
    SF::SinkType: Send + 'static,
    <SF::SinkType as MessageSink>::KeySerType:
        PSerialize<Input = <S::MStream as MessageStream>::KeyType>,
    <SF::SinkType as MessageSink>::ValueSerType:
        PSerialize<Input = <S::MStream as MessageStream>::ValueType>,
{
    type Output = Result<(), PeridotAppRuntimeError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let SinkProjection {
            mut queue_stream,
            sink_factory,
            ..
        } = self.project();

        loop {
            let PipelineStage(metadata, message_stream) = match queue_stream.as_mut().poll_next(cx)
            {
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(q)) => q,
            };

            let message_sink = sink_factory.new_sink(metadata.clone());

            let forward = Forward::new(message_stream, message_sink);

            tokio::spawn(forward);
        }
    }
}

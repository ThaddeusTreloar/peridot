use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Future;
use pin_project_lite::pin_project;

use crate::{
    engine::util::ExactlyOnce,
    message::{
        forked_forward::ForkedForward,
        sink::MessageSink,
        stream::{MessageStream, PipelineStage},
    },
    serde_ext::PSerialize,
};

use super::{
    sink::MessageSinkFactory,
    stream::{ChannelSinkPipeline, PipelineStream},
};

pin_project! {
    #[project = SinkProjection]
    pub struct PipelineForkedForward<S, SF, G = ExactlyOnce>
    where
        S: PipelineStream,
    {
        #[pin]
        queue_stream: S,
        #[pin]
        fork_sink: ChannelSinkPipeline<<S::MStream as MessageStream>::KeyType, <S::MStream as MessageStream>::ValueType>,
        sink_factory: SF,
        _delivery_guarantee: PhantomData<G>
    }
}

impl<S, SF, G> PipelineForkedForward<S, SF, G>
where
    S: PipelineStream,
{
    pub fn new(
        queue_stream: S,
        sink_factory: SF,
        fork_sink: ChannelSinkPipeline<
            <S::MStream as MessageStream>::KeyType,
            <S::MStream as MessageStream>::ValueType,
        >,
    ) -> Self {
        Self {
            queue_stream,
            sink_factory,
            fork_sink,
            _delivery_guarantee: PhantomData,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineForkedForwardError {}

impl<S, SF, G> Future for PipelineForkedForward<S, SF, G>
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
    type Output = Result<(), PipelineForkedForwardError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let SinkProjection {
            mut queue_stream,
            mut sink_factory,
            mut fork_sink,
            ..
        } = self.project();

        loop {
            let PipelineStage(metadata, message_stream) = match queue_stream.as_mut().poll_next(cx)
            {
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(q)) => q,
            };

            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

            let message_sink = self.sink_factory.new_sink(metadata.clone());

            let forward = ForkedForward::new(message_stream, message_sink, sender);

            fork_sink
                .send((metadata, sender))
                .expect("Failed to send pipeline downstream");

            tokio::spawn(forward);
        }
    }
}

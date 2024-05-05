use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;

use crate::{
    engine::util::ExactlyOnce,
    message::{
        fork::Fork,
        sink::MessageSink,
        stream::{MessageStream, PipelineStage},
    },
};

use super::{sink::MessageSinkFactory, stream::PipelineStream};

pin_project! {
    #[project = PipelineForkProjection]
    pub struct PipelineFork<S, SF, G = ExactlyOnce>
    where
        S: PipelineStream,
    {
        #[pin]
        queue_stream: S,
        sink_factory: SF,
        _delivery_guarantee: PhantomData<G>
    }
}

impl<S, SF, G> PipelineFork<S, SF, G>
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

impl<S, SF, G> PipelineStream for PipelineFork<S, SF, G>
where
    S: PipelineStream + Send + 'static,
    S::MStream: MessageStream,
    S::KeyType: Clone + Send + 'static,
    S::ValueType: Clone + Send + 'static,
    SF: MessageSinkFactory<S::KeyType, S::ValueType> + Send + 'static,
    SF::SinkType: Send + 'static,
{
    type KeyType = <S::MStream as MessageStream>::KeyType;
    type ValueType = <S::MStream as MessageStream>::ValueType;
    type MStream = Fork<S::MStream, SF::SinkType>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<PipelineStage<Self::MStream>>> {
        let PipelineForkProjection {
            mut queue_stream,
            sink_factory,
            ..
        } = self.project();

        match queue_stream.as_mut().poll_next(cx) {
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(PipelineStage(metadata, message_stream))) => {
                let message_sink = sink_factory.new_sink(metadata.clone());

                let forwarder = Fork::new(message_stream, message_sink, metadata.clone());

                let pipeline_stage = PipelineStage::new(metadata, forwarder);

                Poll::Ready(Some(pipeline_stage))
            }
        }
    }
}

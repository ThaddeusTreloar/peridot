use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;

use crate::{
    engine::util::ExactlyOnce,
    message::{
        state_fork::StateSinkFork, fork::Fork, sink::MessageSink, stream::{MessageStream, PipelineStage}
    },
};

use super::{sink::MessageSinkFactory, stream::PipelineStream};

pin_project! {
    #[project = StateForkProjection]
    pub struct StateFork<S, SF, G = ExactlyOnce>
    where
        S: PipelineStream,
    {
        #[pin]
        queue_stream: S,
        sink_factory: SF,
        state_name: String,
        has_changelog: bool,
        _delivery_guarantee: PhantomData<G>
    }
}

impl<S, SF, G> StateFork<S, SF, G>
where
    S: PipelineStream,
{
    pub fn new(queue_stream: S, sink_factory: SF, state_name: String) -> Self {
        Self {
            queue_stream,
            sink_factory,
            state_name,
            has_changelog: false,
            _delivery_guarantee: PhantomData,
        }
    }

    pub fn new_with_changelog(queue_stream: S, sink_factory: SF, state_name: String) -> Self {
        let mut state_fork = Self::new(queue_stream, sink_factory, state_name);

        state_fork.has_changelog = true;

        state_fork
    }
}

impl<S, SF, G> PipelineStream for StateFork<S, SF, G>
where
    S: PipelineStream + Send + 'static,
    S::MStream: MessageStream,
    S::KeyType: Clone + Send + 'static,
    S::ValueType: Clone + Send + 'static,
    SF: MessageSinkFactory<
        S::KeyType,
        S::ValueType
    > + Send + 'static,
    SF::SinkType: Send + 'static,
{
    type KeyType = <S::MStream as MessageStream>::KeyType;
    type ValueType = <S::MStream as MessageStream>::ValueType;
    type MStream = Fork<S::MStream, SF::SinkType>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<PipelineStage<Self::MStream>>> {
        let StateForkProjection {
            mut queue_stream,
            sink_factory,
            state_name,
            has_changelog,
            ..
        } = self.project();

        match queue_stream.as_mut().poll_next(cx) {
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(PipelineStage(metadata, message_stream))) => {
                if *has_changelog {
                    let changelog_stream = metadata.take_changelog_queue(state_name)
                        .expect("Failed to get changelog queue for changelog backed state store!");

                } else {
                    unimplemented!("Create changelog Fork")
                }
                
                let message_sink = sink_factory.new_sink(metadata.clone());

                // request changelog from appengine

                //let forwarder = StateFork::new(message_stream, message_sink);

                //let pipeline_stage = PipelineStage::new(metadata, forwarder);

                //Poll::Ready(Some(pipeline_stage))

                unimplemented!("Create changelog Fork")
            }
        }
    }
}

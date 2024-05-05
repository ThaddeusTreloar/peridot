use std::{
    marker::PhantomData, pin::Pin, sync::Arc, task::{Context, Poll}
};

use pin_project_lite::pin_project;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    engine::{context::EngineContext, util::ExactlyOnce},
    message::{
        fork::Fork, sink::MessageSink, state_fork::StateSinkFork, stream::{MessageStream, PipelineStage}
    }, state::backend::StateBackend,
};

use super::{sink::{state_sink::StateSinkFactory, MessageSinkFactory}, stream::PipelineStream};

pin_project! {
    #[project = StateForkProjection]
    pub struct StateForkPipeline<S, B, G = ExactlyOnce>
    where
        S: PipelineStream,
    {
        #[pin]
        queue_stream: S,
        sink_factory: StateSinkFactory<B, <S::MStream as MessageStream>::KeyType,<S::MStream as MessageStream>::ValueType>,
        store_name: String,
        engine_context: Arc<EngineContext>,
        has_changelog: bool,
        _delivery_guarantee: PhantomData<G>
    }
}

impl<S, B, G> StateForkPipeline<S, B, G>
where
    S: PipelineStream,
{
    pub fn new(queue_stream: S, sink_factory: StateSinkFactory<B, <S::MStream as MessageStream>::KeyType,<S::MStream as MessageStream>::ValueType>, store_name: String, engine_context: Arc<EngineContext>) -> Self {
        Self {
            queue_stream,
            sink_factory,
            store_name,
            engine_context,
            has_changelog: false,
            _delivery_guarantee: PhantomData,
        }
    }

    pub fn new_with_changelog(queue_stream: S, sink_factory: StateSinkFactory<B, <S::MStream as MessageStream>::KeyType,<S::MStream as MessageStream>::ValueType>, store_name: String,engine_context: Arc<EngineContext>) -> Self {
        let mut state_fork = Self::new(queue_stream, sink_factory, store_name, engine_context);

        state_fork.has_changelog = true;

        state_fork
    }
}

impl<S, B, G> PipelineStream for StateForkPipeline<S, B, G>
where
    S: PipelineStream + Send + 'static,
    S::MStream: MessageStream,
    S::KeyType: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S::ValueType: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    B: StateBackend + Send + Sync + 'static,
{
    type KeyType = <S::MStream as MessageStream>::KeyType;
    type ValueType = <S::MStream as MessageStream>::ValueType;
    type MStream = StateSinkFork<B, S::MStream>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<PipelineStage<Self::MStream>>> {
        let StateForkProjection {
            mut queue_stream,
            sink_factory,
            store_name,
            has_changelog,
            engine_context,
            ..
        } = self.project();

        match queue_stream.as_mut().poll_next(cx) {
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(PipelineStage(metadata, message_stream))) => {
                if *has_changelog {
                    let changelog_stream = metadata.take_changelog_queue(store_name)
                        .expect("Failed to get changelog queue for changelog backed state store!");

                    let message_sink = sink_factory.new_sink(metadata.clone());

                    let forwarder = StateSinkFork::new_with_changelog(
                        changelog_stream, 
                        message_stream, 
                        message_sink, 
                        engine_context.clone(),
                        store_name.clone(),
                        metadata.partition(),
                    );

                    let pipeline_stage = PipelineStage::new(metadata, forwarder);

                    Poll::Ready(Some(pipeline_stage))
                } else {
                    todo!("No changelog state fork.")
                }
            }
        }
    }
}

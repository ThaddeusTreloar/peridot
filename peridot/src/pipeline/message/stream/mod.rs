
use std::{pin::Pin, task::{Context, Poll}, marker::PhantomData, sync::Arc};

use crate::engine::QueueMetadata;

use super::{types::{Message, FromMessage, PatchMessage}, map::MapMessage};

pub mod connector;

pub trait MessageStream {
    type KeyType;
    type ValueType;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Message<Self::KeyType, Self::ValueType>>>;
}

pub trait MessageStreamExt<K, V>: MessageStream {}

pub struct PipelineStage<M>(
    pub QueueMetadata,
    pub M,
);

impl<M> PipelineStage<M>
where M: MessageStream
{
    pub fn new(queue_metadata: QueueMetadata, message_stream: M) -> Self {
        PipelineStage (
            queue_metadata,
            message_stream,
        )
    }

    pub fn map<F, E, R>(self, f: Arc<F>) -> PipelineStage<MapMessage<M, F, E, R>>
    where
        F: Fn(E) -> R,
        E: FromMessage<M::KeyType, M::ValueType>,
        R: PatchMessage<M::KeyType, M::ValueType>,
        Self: Sized,
    {
        let PipelineStage(queue_metadata, message_stream) = self;

        let wrapped = MapMessage::new(message_stream, f);

        PipelineStage::new(queue_metadata, wrapped)
    }
}
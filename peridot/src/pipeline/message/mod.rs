use std::{pin::Pin, task::{Context, Poll}, marker::PhantomData};

use crate::engine::QueueMetadata;

use self::{types::{Message, FromMessage, PatchMessage}, map::MapMessage};

pub mod map;
pub mod types;

pub trait MessageStream<K, V> {
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Message<K, V>>>;
}

pub trait MessageStreamExt {}

pub struct PipelineStage<M, K, V> {
    pub queue_metadata: QueueMetadata,
    pub message_stream: M,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<M, K, V> PipelineStage<M, K, V>
where
    M: MessageStream<K, V>,
{
    pub fn new(queue_metadata: QueueMetadata, message_stream: M) -> Self {
        PipelineStage {
            queue_metadata,
            message_stream,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }

    /*
    pub fn map<E, R, F, RK, RV>(self, f: F) -> PipelineStage<MapMessage<M, F, E, R, K, V>, RK, RV>
    where
        F: FnMut(E) -> R,
        E: FromMessage<K, V>,
        R: PatchMessage<K, V, RK, RV>,
        Self: Sized,
    {
        let wrapped = MapMessage::new(self.message_stream, f);

        PipelineStage::new(self.queue_metadata, wrapped)
    } */
}
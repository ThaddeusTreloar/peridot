use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::engine::queue_manager::queue_metadata::QueueMetadata;

use super::{
    map::MapMessage,
    types::{FromMessage, Message, PatchMessage},
};

//pub mod import;
pub mod serialiser;
//pub mod transparent;

pub type ChannelStream<K, V> = UnboundedReceiver<Message<K, V>>;
pub type ChannelSink<K, V> = UnboundedSender<Message<K, V>>;

pub enum StreamCommit {
    Success,
    Fail,
}

pub trait MessageStream {
    type KeyType;
    type ValueType;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<MessageStreamPoll<Self::KeyType, Self::ValueType>>;
}

pub trait MessageStreamExt: MessageStream {}

pub struct PipelineStage<M>(pub QueueMetadata, pub M);

impl<M> PipelineStage<M>
where
    M: MessageStream,
{
    pub fn new(queue_metadata: QueueMetadata, message_stream: M) -> Self {
        PipelineStage(queue_metadata, message_stream)
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

#[derive(Debug, thiserror::Error)]
pub enum MessageCommitError {}

pub(crate) enum MessageStreamPoll<K, V> {
    Commit(Result<(), MessageCommitError>),
    Message(Message<K, V>),
    Closed,
}

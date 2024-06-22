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
pub mod head;
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
pub enum MessageCommitError {
    #[error("Sink commit error: {}", 0)]
    SinkCommitError(Box<dyn std::error::Error>),
}

pub enum MessageStreamPoll<K, V> {
    // Next offset to be read, or unrecoverable commit error
    Commit(Result<i64, MessageCommitError>),
    Message(Message<K, V>),
    Closed,
}

impl<K, V> MessageStreamPoll<K, V> {
    // Rename this at some point and also make it slightly safer
    pub(crate) fn translate<RK, RV>(self) -> Poll<MessageStreamPoll<RK, RV>> {
        match self {
            MessageStreamPoll::Message(_) => panic!("Cannot translate MessageStreamPoll::Message."),
            MessageStreamPoll::Closed => Poll::Ready(MessageStreamPoll::Closed),
            MessageStreamPoll::Commit(i) => Poll::Ready(MessageStreamPoll::Commit(i)),
        }
    }
}

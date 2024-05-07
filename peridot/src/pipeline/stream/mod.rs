use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{
    engine::queue_manager::queue_metadata::QueueMetadata,
    message::{
        join::Combiner,
        sink::MessageSink,
        stream::{ChannelStream, MessageStream, PipelineStage},
        types::{FromMessage, Message, PatchMessage},
    },
    pipeline::{
        fork::PipelineFork, forward::PipelineForward, join::JoinPipeline, join_by::JoinBy,
        map::MapPipeline, sink::MessageSinkFactory,
    },
};

//pub mod import;
pub mod serialiser;
pub mod transparent;

pub type ChannelStreamPipeline<K, V> = UnboundedReceiver<(QueueMetadata, ChannelStream<K, V>)>;
pub type ChannelSinkPipeline<K, V> = UnboundedSender<(QueueMetadata, ChannelStream<K, V>)>;

pub trait PipelineStream {
    type KeyType;
    type ValueType;
    type MStream: MessageStream<KeyType = Self::KeyType, ValueType = Self::ValueType> + Send;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<PipelineStage<Self::MStream>>>;
}

// TODO: consider removing this as all methods have the nearly identical logic.
// The methods have been remove for now.
pub trait PipelineStreamExt: PipelineStream {}

impl<P> PipelineStreamExt for P where P: PipelineStream {}

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{
    engine::QueueMetadata,
    message::{
        sink::MessageSink,
        stream::{ChannelStream, MessageStream, PipelineStage},
    },
    serde_ext::PSerialize,
};

use self::transparent::TransparentPipeline;

use super::{forked_forward::PipelineForkedForward, map::MapPipeline, sink::MessageSinkFactory};

use super::forward::PipelineForward;

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

pub trait PipelineStreamExt: PipelineStream {
    fn map<F, E, R>(self, f: F) -> MapPipeline<Self, F, E, R>
    where
        Self: Sized,
    {
        MapPipeline::new(self, f)
    }

    fn forward<SF>(self, sink: SF) -> PipelineForward<Self, SF>
    where
        SF: MessageSinkFactory + Send + 'static,
        <SF::SinkType as MessageSink>::KeySerType: PSerialize<Input = <<Self as PipelineStream>::MStream as MessageStream>::KeyType>
            + Send
            + 'static,
        <SF::SinkType as MessageSink>::ValueSerType: PSerialize<Input = <<Self as PipelineStream>::MStream as MessageStream>::ValueType>
            + Send
            + 'static,
        Self: Sized,
    {
        PipelineForward::new(self, sink)
    }

    fn forked_forward<SF, G>(
        self,
        sink_factory: SF,
    ) -> (
        PipelineForkedForward<Self, SF, G>,
        TransparentPipeline<Self::KeyType, Self::ValueType>,
    ) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

        let downstream = TransparentPipeline::new(receiver);

        (
            PipelineForkedForward::new(self, sink_factory, sender),
            downstream,
        )
    }

    fn count() {}
    fn filter() {}
    fn fold() {}
    fn join() {}
    fn reduce() {}
}

impl<P> PipelineStreamExt for P where P: PipelineStream {}

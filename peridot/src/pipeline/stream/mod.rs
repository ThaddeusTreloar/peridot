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
    pipeline::{
        fork::PipelineFork, forward::PipelineForward, join::Join, join_by::JoinBy,
        map::MapPipeline, sink::MessageSinkFactory,
    },
    state::backend::IntoView,
};

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
        <SF::SinkType as MessageSink>::KeyType: Send + 'static,
        <SF::SinkType as MessageSink>::ValueType: Send + 'static,
        Self: Sized,
    {
        PipelineForward::new(self, sink)
    }

    fn fork<SF, G>(self, sink_factory: SF) -> PipelineFork<Self, SF, G>
    where
        Self: Sized,
    {
        PipelineFork::new(self, sink_factory)
    }

    fn count() {}
    fn filter() {}
    fn fold() {}

    fn join<T, C, RV>(self, table: T, combiner: C) -> Join<Self, T, C>
    where
        T: IntoView + Send + 'static,
        C: Fn(&Self::ValueType, &T::ValueType) -> RV,
        Self::KeyType: PartialEq<T::KeyType>,
        Self: Sized,
    {
        Join::new(self, table, combiner)
    }

    fn join_by<T, J, C, RV>(self, table: T, joiner: J, combiner: C) -> JoinBy<Self, T, J, C>
    where
        T: IntoView + Send + 'static,
        J: Fn(&Self::KeyType, &T::KeyType) -> bool,
        C: Fn(&Self::ValueType, &T::ValueType) -> RV,
        Self: Sized,
    {
        JoinBy::new(self, table, joiner, combiner)
    }

    fn reduce() {}
}

impl<P> PipelineStreamExt for P where P: PipelineStream {}

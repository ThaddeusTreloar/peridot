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
    state::backend::{GetView, GetViewDistributor},
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

pub trait PipelineStreamExt: PipelineStream {
    fn map<F, E, R>(self, f: F) -> MapPipeline<Self, F, E, R>
    where
        F: Fn(E) -> R,
        E: FromMessage<Self::KeyType, Self::ValueType>,
        R: PatchMessage<Self::KeyType, Self::ValueType>,
        Self: Sized,
    {
        MapPipeline::new(self, f)
    }

    fn forward<SF>(self, sink: SF) -> PipelineForward<Self, SF>
    where
        SF: MessageSinkFactory<Self::KeyType, Self::ValueType> + Send + 'static,
        Self::KeyType: Send + 'static,
        Self::ValueType: Send + 'static,
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

    fn join<T, C>(self, table: T, combiner: C) -> JoinPipeline<Self, T, C>
    where
        T: GetView + Send + 'static,
        C: Combiner<Self::ValueType, T::ValueType>,
        Self::KeyType: PartialEq<T::KeyType>,
        Self: Sized,
    {
        JoinPipeline::new(self, table, combiner)
    }
    /*
    fn join_by<T, J, C>(self, table: T, joiner: J, combiner: C) -> JoinBy<Self, T, J, C>
    where
        T: GetViewDistributor + Send + 'static,
        J: Fn(&Self::KeyType, &T::KeyType) -> bool,
        C: Fn(&Self::ValueType, &T::ValueType) -> C::Output,
        Self: Sized,
    {
        JoinBy::new(self, table, joiner, combiner)
    } */

    fn reduce() {}
}

impl<P> PipelineStreamExt for P where P: PipelineStream {}

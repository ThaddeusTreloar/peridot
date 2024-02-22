use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    message::{
        sink::MessageSink,
        stream::{MessageStream, PipelineStage},
    },
    serde_ext::PSerialize,
};

use self::map::MapPipeline;

use super::sink::{PipelineForward, PipelineSink};

pub mod count;
pub mod filter;
pub mod fold;
pub mod join;
pub mod map;
pub mod reduce;
pub mod stream;

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

    fn count() {}
    fn filter() {}
    fn fold() {}
    fn join() {}
    fn reduce() {}
}

pub trait PipelineStreamSinkExt: PipelineStream {
    fn sink<Si>(self, sink: Si) -> PipelineForward<Self, Si>
    where
        Si: PipelineSink<Self::MStream> + Send + 'static,
        <Si::SinkType as MessageSink>::KeySerType:
            PSerialize<Input = <<Self as PipelineStream>::MStream as MessageStream>::KeyType>,
        <Si::SinkType as MessageSink>::ValueSerType:
            PSerialize<Input = <<Self as PipelineStream>::MStream as MessageStream>::ValueType>,
        Self: Sized,
    {
        PipelineForward::new(self, sink)
    }
}

impl<P> PipelineStreamExt for P where P: PipelineStream {}

impl<P> PipelineStreamSinkExt for P where P: PipelineStream {}

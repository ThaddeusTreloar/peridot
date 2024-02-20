use std::{pin::Pin, task::{Context, Poll}};

use crate::pipeline::{message::{stream::{PipelineStage, MessageStream}, types::{FromMessage, PatchMessage}, sink::MessageSink}, serde_ext::PSerialize};

use self::map::MapPipeline;

use super::sink::Sink;

pub mod map;
pub mod stream;

pub trait PipelineStream
{
    type KeyType;
    type ValueType;
    type MStream: MessageStream<KeyType = Self::KeyType, ValueType = Self::ValueType> + Send;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<PipelineStage<Self::MStream>>>;
}

pub trait PipelineStreamExt: PipelineStream
{
    fn map<F, E, R>(self, f: F) -> MapPipeline<Self, F, E, R>
    where
        Self: Sized,
    {
        MapPipeline::new(self, f)
    }
}

pub trait PipelineStreamSinkExt: PipelineStream
{
    fn sink<Si>(self, topic: &str) -> Sink<Self, Si>
    where
        Si: MessageSink + Send + 'static,
        Si::KeySerType: PSerialize<Input = <<Self as PipelineStream>::MStream as MessageStream>::KeyType>,
        Si::ValueSerType: PSerialize<Input = <<Self as PipelineStream>::MStream as MessageStream>::ValueType>,
        Self: Sized,
    {
        Sink::new(self, String::from(topic))
    }
}

impl <P> PipelineStreamExt for P
where P: PipelineStream{}

impl <P> PipelineStreamSinkExt for P
where 
    P: PipelineStream {}
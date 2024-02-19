use std::{pin::Pin, task::{Context, Poll}};

use crate::pipeline::{message::{stream::{PipelineStage, MessageStream}, types::{FromMessage, PatchMessage}, sink::MessageSink}, serde_ext::PSerialize};

use self::map::MapPipeline;

use super::sink::Sink;

pub mod map;
pub mod stream;

pub trait PipelineStream
{
    type MStream: MessageStream;

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
    fn sink<KS, VS, Si>(self, topic: &str) -> Sink<KS, VS, Self, <Self as PipelineStream>::MStream, Si>
    where
        KS: PSerialize,
        VS: PSerialize,
        Si: MessageSink + Send + 'static,
        <Self as PipelineStream>::MStream: MessageSink,
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
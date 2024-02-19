use std::{pin::Pin, task::{Context, Poll}};

use crate::pipeline::{message::{stream::{PipelineStage, MessageStream}, types::{FromMessage, PatchMessage}, sink::MessageSink}, serde_ext::PSerialize};

use self::map::MapPipeline;

use super::sink::Sink;

pub mod map;
pub mod stream;

pub trait PipelineStream<K, V> 
{
    type M;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<PipelineStage<K, V, Self::M>>>;
}

pub trait PipelineStreamExt<K, V>: PipelineStream<K, V> 
{
    fn map<RK, RV, E, R, F>(self, f: F) -> MapPipeline<Self, K, V, RK, RV, Self::M, F, E, R>
    where
        F: Fn(E) -> R,
        E: FromMessage<K, V>,
        R: PatchMessage<K, V, RK, RV>,
        Self: Sized,
        Self::M: MessageStream<K, V>
    {
        MapPipeline::new(self, f)
    }
}

pub trait PipelineStreamSinkExt<KS, VS>: PipelineStream<KS::Input, VS::Input> 
where 
    KS: PSerialize,
    VS: PSerialize,
{
    fn sink<Si>(self, topic: &str) -> Sink<KS, VS, Self, Self::M, Si>
    where
        Si: MessageSink<KS, VS> + Send + 'static,
        Self: Sized,
        Self::M: MessageStream<KS::Input, VS::Input>
    {
        Sink::new(self, String::from(topic))
    }
}

impl <P, K, V> PipelineStreamExt<K, V> for P
where P: PipelineStream<K, V>{}

impl <P, KS, VS> PipelineStreamSinkExt<KS, VS> for P
where 
    KS: PSerialize,
    VS: PSerialize,
    P: PipelineStream<KS::Input, VS::Input>{}
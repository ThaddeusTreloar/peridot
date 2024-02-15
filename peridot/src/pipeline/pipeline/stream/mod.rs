use std::{pin::Pin, task::{Context, Poll}};

use crate::pipeline::{message::{stream::{MessageStream, PipelineStage}, types::{FromMessage, PatchMessage}, sink::MessageSink}, serde_ext::PSerialize};

use self::map::MapPipeline;

use super::sink::Sink;

pub mod map;
pub mod stream;

pub trait PipelineStream<K, V, M> 
where M: MessageStream<K, V>,
{
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<PipelineStage<K, V, M>>>;
}

pub trait PipelineStreamExt<K, V, M>: PipelineStream<K, V, M> 
where M: MessageStream<K, V> {

    fn map<RK, RV, E, R, F>(self, f: F) -> MapPipeline<Self, K, V, RK, RV, M, F, E, R>
    where
        F: Fn(E) -> R,
        E: FromMessage<K, V>,
        R: PatchMessage<K, V, RK, RV>,
        Self: Sized,
    {
        MapPipeline::new(self, f)
    }
}

pub trait PipelineStreamSinkExt<KS, VS, M>: PipelineStream<KS::Input, VS::Input, M> 
where 
    KS: PSerialize,
    VS: PSerialize,
    M: MessageStream<KS::Input, VS::Input> 
{
    fn sink<Si>(self, topic: &str) -> Sink<KS, VS, Self, M, Si>
    where
        Si: MessageSink<KS, VS> + Send + 'static,
        Self: Sized,
    {
        Sink::new(self, String::from(topic))
    }
}

impl <P, M, K, V> PipelineStreamExt<K, V, M> for P
where P: PipelineStream<K, V, M>,
    M: MessageStream<K, V> {}

impl <P, KS, VS, M> PipelineStreamSinkExt<KS, VS, M> for P
where 
    KS: PSerialize,
    VS: PSerialize,
    P: PipelineStream<KS::Input, VS::Input, M>,
    M: MessageStream<KS::Input, VS::Input> {}
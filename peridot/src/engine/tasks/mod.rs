use std::{pin::Pin, task::{Context, Poll}, marker::PhantomData};

use pin_project_lite::pin_project;
use tracing::info;

use crate::{app::{AppBuilder, error::PeridotAppRuntimeError, Head}, pipeline::{pipeline::{stream::{stream::Pipeline, PipelineStream, PipelineStreamSinkExt}, sink::Sink}, serde_ext::{PDeserialize, PSerialize}, message::{sink::MessageSink, stream::{MessageStream, PipelineStage, connector::QueueConnector}}}};

use super::{util::DeliveryGuaranteeType, AppEngine};


#[derive(Debug, thiserror::Error)]
pub enum FromBuilderError {
    #[error(transparent)]
    GenericError(#[from] PeridotAppRuntimeError),
}

pub trait Builder<K, V> {
    type Output: PipelineStream<K, V>;

    fn generate_pipeline(&self) -> Self::Output;
}

pub struct IngressTask<KS, VS, G> {
    _delivery_guarantee: PhantomData<G>,
    _key_ser: PhantomData<KS>,
    _val_ser: PhantomData<VS>,
}

impl <KS, VS, G> Builder<KS::Output, VS::Output> for IngressTask<KS, VS, G>
where 
    G: DeliveryGuaranteeType,
    KS: PDeserialize,
    VS: PDeserialize,
{
    type Output = Pipeline<KS, VS, G>;

    fn generate_pipeline(&self) -> Self::Output {
        unimplemented!("")
    }
}

pub trait FromBuilder<K, V, B> 
where 
    B: Builder<K, V>,
{
    type Output;

    fn from_builder(builder: &B, target: &str) -> Result<Self::Output, FromBuilderError>;
}

pin_project! {
    pub struct Stream<K, V, P>
    {
        #[pin]
        inner: P,
        _key_type: PhantomData<K>,
        _value_type: PhantomData<V>
    }
}

impl <K, V, P> PipelineStream<K, V> for Stream<K, V, P>
where P: PipelineStream<K, V>
{
    type M = P::M;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<PipelineStage<K, V, P::M>>> {
        let this = self.project();

        match this.inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(queue)) => Poll::Ready(
                Some(
                    queue
                )
            ),
        }
    }
}

impl <K, V, B> FromBuilder<K, V, B> for Stream<K, V, B::Output> 
where 
    B: Builder<K, V>,
{
    type Output = Self;

    fn from_builder(builder: &B, topic: &str) -> Result<Self::Output, FromBuilderError> 
    {
        Ok(
            Self {
                inner: builder.generate_pipeline(),
                _key_type: PhantomData,
                _value_type: PhantomData,
            }
        )
    }
}

/*
pub trait IntoSink<KS, VS, M, S> 
where
    KS: PSerialize,
    VS: PSerialize,
    M: MessageStream<KS::Input, VS::Input>,
    S: MessageSink<KS, VS>
{
    type Output;

    fn into_sink(self, topic: &str) -> Self::Output;
}

impl <P, KS, VS, M, S> IntoSink<KS, VS, M, S> for P
where
    P: PipelineStream<KS::Input, VS::Input, M>,
    KS: PSerialize,
    VS: PSerialize,
    M: MessageStream<KS::Input, VS::Input>,
    S: MessageSink<KS, VS> + Send + Sync + 'static
{
    type Output = Sink<KS, VS, Self, M, S>;

    fn into_sink(self, topic: &str) -> Self::Output {
        self.sink(topic)    
    }
} */
use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll}, sync::Arc,
};

use futures::Stream;
use pin_project_lite::pin_project;

use crate::pipeline::message::{MessageStream, types::{FromMessage, PatchMessage}, PipelineStage, map::MapMessage};

use super::PipelineStream;

pin_project! {
    pub struct MapPipeline<S, M, K, V, F, E, R, RK, RV>
    where 
        S: PipelineStream<M, K, V>,
        M: MessageStream<K, V>,
        F: Fn(E) -> R,
        E: FromMessage<K, V>,
        R: PatchMessage<K, V, RK, RV>,
    {
        #[pin]
        inner: S,
        callback: Arc<F>,
        _pipeline_type: PhantomData<PipelineStage<M, K, V>>,
        _input_key_type: PhantomData<K>,
        _input_value_type: PhantomData<V>,
        _output_key_type: PhantomData<RK>,
        _output_value_type: PhantomData<RV>,
        _extractor_type: PhantomData<E>,
        _reassembler_type: PhantomData<R>,
    }
}

impl <S, M, K, V, F, E, R, RK, RV> MapPipeline<S, M, K, V, F, E, R, RK, RV>
where 
    S: PipelineStream<M, K, V>,
    M: MessageStream<K, V>,
    F: Fn(E) -> R,
    E: FromMessage<K, V>,
    R: PatchMessage<K, V, RK, RV>
{
    fn new(inner: S, callback: F) -> Self {
        Self {
            inner,
            callback: Arc::new(callback),
            _pipeline_type: PhantomData,
            _input_key_type: PhantomData,
            _input_value_type: PhantomData,
            _output_key_type: PhantomData,
            _output_value_type: PhantomData,
            _extractor_type: PhantomData,
            _reassembler_type: PhantomData,
        }
    }
}

impl<S, M, K, V, F, E, R, RK, RV> PipelineStream<MapMessage<M, F, E, R, K, V>, RK, RV> for MapPipeline<S, M, K, V, F, E, R, RK, RV>
where
    S: PipelineStream<M, K, V>,
    M: MessageStream<K, V>,
    F: Fn(E) -> R,
    E: FromMessage<K, V>,
    R: PatchMessage<K, V, RK, RV>,
{
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<PipelineStage<MapMessage<M, F, E, R, K, V>, RK, RV>>> {
        let this = self.project();

        let PipelineStage { queue_metadata, message_stream, .. } = match this.inner.poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(queue)) => queue,
        };

        let mapped_queue: MapMessage<M, F, E, R, K, V> = MapMessage::new(message_stream, this.callback.clone());

        Poll::Ready(
            Option::Some(
                PipelineStage::new(queue_metadata, mapped_queue)
        ))
    }
}

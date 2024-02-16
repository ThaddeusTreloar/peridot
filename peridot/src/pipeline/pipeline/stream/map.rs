use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll}, sync::Arc,
};

use pin_project_lite::pin_project;

use crate::pipeline::message::{stream::{MessageStream, PipelineStage}, types::{FromMessage, PatchMessage}, map::MapMessage};

use super::PipelineStream;

pin_project! {
    pub struct MapPipeline<S, K, V, RK, RV, M, F, E, R, >
    where 
        S: PipelineStream<K, V, M>,
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

impl <S, K, V, RK, RV, M, F, E, R, > MapPipeline<S, K, V, RK, RV, M, F, E, R, >
where 
    S: PipelineStream<K, V, M>,
    M: MessageStream<K, V>,
    F: Fn(E) -> R,
    E: FromMessage<K, V>,
    R: PatchMessage<K, V, RK, RV>
{
    pub fn new(inner: S, callback: F) -> Self {
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

impl<S, K, V, RK, RV, M, F, E, R, > PipelineStream<RK, RV, MapMessage<K, V, M, F, E, R>> for MapPipeline<S, K, V, RK, RV, M, F, E, R, >
where
    S: PipelineStream<K, V, M>,
    M: MessageStream<K, V>,
    F: Fn(E) -> R,
    E: FromMessage<K, V>,
    R: PatchMessage<K, V, RK, RV>,
{
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<PipelineStage<RK, RV, MapMessage<K, V, M, F, E, R>>>> {
        let this = self.project();

        match this.inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(queue)) => Poll::Ready(
                Option::Some(
                    queue.map(this.callback.clone())
                )   
            ),
        }
    }
}

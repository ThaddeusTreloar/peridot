use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll}, sync::Arc,
};

use pin_project_lite::pin_project;

use crate::pipeline::message::{stream::{MessageStream, PipelineStage}, types::{FromMessage, PatchMessage}, map::MapMessage};

use super::PipelineStream;

pin_project! {
    pub struct MapPipeline<S, F, E, R>
    where 
        S: PipelineStream,
    {
        #[pin]
        inner: S,
        callback: Arc<F>,
        _extractor: PhantomData<E>,
        _patcher: PhantomData<R>,
    }
}

impl <S, F, E, R> MapPipeline<S, F, E, R>
where 
    S: PipelineStream,
{
    pub fn new(inner: S, callback: F) -> Self {
        Self {
            inner,
            callback: Arc::new(callback),
            _extractor: PhantomData,
            _patcher: PhantomData,
        }
    }
}

impl<S, F, E, R> PipelineStream for MapPipeline<S, F, E, R>
where
    S: PipelineStream,
    F: Fn(E) -> R + Send + Sync,
    E: FromMessage<<<S as PipelineStream>::MStream as MessageStream>::KeyType, <<S as PipelineStream>::MStream as MessageStream>::ValueType> + Send,
    R: PatchMessage<<<S as PipelineStream>::MStream as MessageStream>::KeyType, <<S as PipelineStream>::MStream as MessageStream>::ValueType> + Send,
{
    type KeyType = R::RK;
    type ValueType = R::RV;
    type MStream = MapMessage<S::MStream, F, E, R>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<PipelineStage<Self::MStream>>> {
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

use std::{
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;

use crate::message::{
    filter::FilterMessage,
    map::MapMessage,
    stream::{MessageStream, PipelineStage},
    types::{FromMessage, PatchMessage},
};

use super::stream::PipelineStream;

pin_project! {
    pub struct FilterPipeline<S, F>
    where
        S: PipelineStream,
    {
        #[pin]
        inner: S,
        callback: Arc<F>,
    }
}

impl<S, F> FilterPipeline<S, F>
where
    S: PipelineStream,
{
    pub fn new(inner: S, callback: F) -> Self {
        Self {
            inner,
            callback: Arc::new(callback),
        }
    }
}

impl<S, F> PipelineStream for FilterPipeline<S, F>
where
    S: PipelineStream,
    F: Fn(
            &<<S as PipelineStream>::MStream as MessageStream>::KeyType,
            &<<S as PipelineStream>::MStream as MessageStream>::ValueType,
        ) -> bool
        + Send
        + Sync,
{
    type KeyType = <<S as PipelineStream>::MStream as MessageStream>::KeyType;
    type ValueType = <<S as PipelineStream>::MStream as MessageStream>::ValueType;
    type MStream = FilterMessage<S::MStream, F>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<PipelineStage<Self::MStream>>> {
        let this = self.project();

        match this.inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(PipelineStage(metadata, queue))) => {
                let filter = FilterMessage::new(queue, this.callback.clone());

                Poll::Ready(Option::Some(PipelineStage(metadata, filter)))
            }
        }
    }
}

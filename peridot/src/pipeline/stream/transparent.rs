use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
use tracing::info;

use crate::{
    engine::util::ExactlyOnce,
    message::stream::{ChannelStream, PipelineStage},
};

use super::{ChannelStreamPipeline, PipelineStream};

pin_project! {
    pub struct TransparentPipeline<K, V, G = ExactlyOnce>
    {
        #[pin]
        queue_stream: ChannelStreamPipeline<K, V>,
        _delivery_guarantee: PhantomData<G>
    }
}

impl<K, V, G> TransparentPipeline<K, V, G> {
    pub fn new(queue_stream: ChannelStreamPipeline<K, V>) -> Self {
        Self {
            queue_stream,
            _delivery_guarantee: PhantomData,
        }
    }
}

impl<K, V, G> PipelineStream for TransparentPipeline<K, V, G>
where
    K: Send + 'static,
    V: Send + 'static,
    G: Send + 'static,
{
    type KeyType = K;
    type ValueType = V;
    type MStream = ChannelStream<K, V>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<PipelineStage<Self::MStream>>> {
        let (metadata, queue) = match self.queue_stream.poll_recv(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(val)) => val,
        };

        info!(
            "Received new queue for topic: {}, parition: {}",
            metadata.source_topic(),
            metadata.partition()
        );

        Poll::Ready(Option::Some(PipelineStage::new(metadata, queue)))
    }
}

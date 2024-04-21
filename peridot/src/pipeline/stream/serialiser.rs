use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
use tracing::info;

use crate::{
    engine::wrapper::serde::PeridotDeserializer,
    engine::{util::ExactlyOnce, QueueReceiver},
    message::stream::{serialiser::QueueSerialiser, PipelineStage},
};

use super::PipelineStream;

pin_project! {
    pub struct SerialiserPipeline<KS, VS, G = ExactlyOnce>
    where KS: PeridotDeserializer,
        VS: PeridotDeserializer
    {
        #[pin]
        queue_stream: QueueReceiver,
        _key_serialiser: PhantomData<KS>,
        _value_serialiser: PhantomData<VS>,
        _delivery_guarantee: PhantomData<G>
    }
}

impl<KS, VS, G> SerialiserPipeline<KS, VS, G>
where
    KS: PeridotDeserializer,
    VS: PeridotDeserializer,
{
    pub fn new(queue_stream: QueueReceiver) -> Self {
        Self {
            queue_stream,
            _key_serialiser: PhantomData,
            _value_serialiser: PhantomData,
            _delivery_guarantee: PhantomData,
        }
    }
}

impl<KS, VS, G> PipelineStream for SerialiserPipeline<KS, VS, G>
where
    KS: PeridotDeserializer + Send,
    VS: PeridotDeserializer + Send,
{
    type KeyType = KS::Output;
    type ValueType = VS::Output;
    type MStream = QueueSerialiser<KS, VS>;

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

        Poll::Ready(Option::Some(PipelineStage::new(
            metadata,
            QueueSerialiser::<KS, VS>::new(queue),
        )))
    }
}

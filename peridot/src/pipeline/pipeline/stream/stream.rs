use std::{marker::PhantomData, pin::Pin, task::{Context, Poll}};

use pin_project_lite::pin_project;
use tracing::info;

use crate::{pipeline::{serde_ext::PDeserialize, message::stream::{connector::QueueConnector, PipelineStage}}, engine::{util::ExactlyOnce, QueueReceiver}};

use super::PipelineStream;

pin_project! {
    pub struct Pipeline<KS, VS, G = ExactlyOnce>
    where KS: PDeserialize,
        VS: PDeserialize
    {
        #[pin]
        queue_stream: QueueReceiver,
        _key_serialiser: PhantomData<KS>,
        _value_serialiser: PhantomData<VS>,
        _delivery_guarantee: PhantomData<G>
    }
}

impl <KS, VS, G> Pipeline<KS, VS, G>
where 
    KS: PDeserialize,
    VS: PDeserialize 
{
    pub fn new(queue_stream: QueueReceiver) -> Self {
        Self {
            queue_stream,
            _key_serialiser: PhantomData,
            _value_serialiser: PhantomData,
            _delivery_guarantee: PhantomData
        }
    }
}

impl<'a, KS, VS> PipelineStream<KS::Output, VS::Output, QueueConnector<KS, VS>> for Pipeline<KS, VS>
where
    KS: PDeserialize,
    VS: PDeserialize,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<PipelineStage<KS::Output, VS::Output, QueueConnector<KS, VS>>>> {
        let (metadata, queue) = match self.queue_stream.poll_recv(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(val)) => val,
        };

        info!("Received message new queue for topic: {}, parition: {}", metadata.source_topic(), metadata.partition());

        Poll::Ready(Option::Some(PipelineStage::new(
            metadata,
            QueueConnector::<KS, VS>::new(queue),
        )))
    }
}
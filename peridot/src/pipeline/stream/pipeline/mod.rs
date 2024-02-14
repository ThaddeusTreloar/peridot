use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};

use tokio::sync::mpsc::UnboundedReceiver;
use tracing::error;

use pin_project_lite::pin_project;

use crate::{
    app::PeridotPartitionQueue,
    pipeline::message::{Message, TryFromBorrowedMessage},
    serde_ext::PDeserialize,
};

use self::stage::{QueueMetadata, PipelineStage};

use super::MessageStream;

pub mod stage;

pin_project! {
    pub struct QueueConnector<KS, VS> {
        #[pin]
        input: PeridotPartitionQueue,
        _key_serialiser: PhantomData<KS>,
        _value_serialiser: PhantomData<VS>,
    }
}

impl<KS, VS> QueueConnector<KS, VS> {
    pub fn new(input: PeridotPartitionQueue) -> Self {
        QueueConnector {
            input,
            _key_serialiser: PhantomData,
            _value_serialiser: PhantomData,
        }
    }
}

impl<KS, VS> From<PeridotPartitionQueue> for QueueConnector<KS, VS> {
    fn from(input: PeridotPartitionQueue) -> Self {
        QueueConnector::new(input)
    }
}

impl<KS, VS> MessageStream<KS::Output, VS::Output> for QueueConnector<KS, VS>
where
    KS: PDeserialize,
    VS: PDeserialize,
{
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Message<KS::Output, VS::Output>>> {
        let this = self.project();

        let try_next_msg = match this.input.stream().poll_next_unpin(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(val)) => val,
        };

        let raw_msg = try_next_msg.expect("Error in message queue");

        let msg = match <Message<KS::Output, VS::Output> as TryFromBorrowedMessage<KS, VS>>::try_from_borrowed_message(raw_msg) {
            Err(e) => {
                error!("Failed to deser msg: {}", e);
                None
            }
            Ok(m) => Some(m)
        };

        Poll::Ready(msg)
    }
}

pin_project! {
    pub struct Pipeline<KS, VS>
    where KS: PDeserialize,
        VS: PDeserialize
    {
        #[pin]
        queue_stream: UnboundedReceiver<(QueueMetadata, PeridotPartitionQueue)>,
        _key_serialiser: PhantomData<KS>,
        _value_serialiser: PhantomData<VS>
    }
}

impl<KS, VS> Stream for Pipeline<KS, VS>
where
    KS: PDeserialize,
    VS: PDeserialize,
{
    type Item = PipelineStage<QueueConnector<KS, VS>, KS::Output, VS::Output>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (metadata, queue) = match self.queue_stream.poll_recv(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(val)) => val,
        };

        Poll::Ready(Option::Some(PipelineStage::new(
            metadata,
            QueueConnector::<KS, VS>::new(queue),
        )))
    }
}

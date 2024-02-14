use futures::{Stream, StreamExt};
use pin_project_lite::pin_project;
use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::error;

use crate::{
    app::PeridotPartitionQueue,
    engine::QueueReceiver,
    pipeline::serde_ext::PDeserialize,
};

use self::map::MapPipeline;

use super::message::{PipelineStage, MessageStream, types::{Message, TryFromBorrowedMessage, FromMessage, PatchMessage}};

pub mod map;

pub trait PipelineStream<M, K, V> 
where M: MessageStream<K, V>,
{
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<PipelineStage<M, K, V>>>;
}

pub trait PipelineStreamExt<M, K, V>: PipelineStream<M, K, V> 
where M: MessageStream<K, V> {

    fn map<E, R, F, RK, RV>(self, f: F) -> MapPipeline<Self, M, K, V, F, E, R, RK, RV>
    where
        F: Fn(E) -> R,
        E: FromMessage<K, V>,
        R: PatchMessage<K, V, RK, RV>,
        Self: Sized,
    {
        MapPipeline::new(self, f)
    }
}

impl <P, M, K, V> PipelineStreamExt<M, K, V> for P
where P: PipelineStream<M, K, V>,
    M: MessageStream<K, V>{}

pin_project! {
    pub struct Pipeline<KS, VS>
    where KS: PDeserialize,
        VS: PDeserialize
    {
        #[pin]
        queue_stream: QueueReceiver,
        _key_serialiser: PhantomData<KS>,
        _value_serialiser: PhantomData<VS>
    }
}

impl <KS, VS> Pipeline<KS, VS>
where 
    KS: PDeserialize,
    VS: PDeserialize 
{
    pub fn new(queue_stream: QueueReceiver) -> Self {
        Self {
            queue_stream,
            _key_serialiser: PhantomData,
            _value_serialiser: PhantomData,
        }
    }
}
impl<KS, VS> PipelineStream<QueueConnector<KS, VS>, KS::Output, VS::Output> for Pipeline<KS, VS>
where
    KS: PDeserialize,
    VS: PDeserialize,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<PipelineStage<QueueConnector<KS, VS>, KS::Output, VS::Output>>> {
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

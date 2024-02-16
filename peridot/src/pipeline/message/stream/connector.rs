use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{StreamExt, Stream};
use pin_project_lite::pin_project;
use tracing::{error, info};

use crate::{
    pipeline::{
        message::types::{Message, TryFromBorrowedMessage, TryFromOwnedMessage},
        serde_ext::PDeserialize,
    }, engine::partition_queue::StreamPeridotPartitionQueue,
};

use super::MessageStream;

pin_project! {
    pub struct QueueConnector<KS, VS> {
        #[pin]
        input: StreamPeridotPartitionQueue,
        _key_serialiser: PhantomData<KS>,
        _value_serialiser: PhantomData<VS>,
    }
}

impl<KS, VS> QueueConnector<KS, VS> {
    pub fn new(input: StreamPeridotPartitionQueue) -> Self {
        QueueConnector {
            input,
            _key_serialiser: PhantomData,
            _value_serialiser: PhantomData,
        }
    }
}

impl<KS, VS> From<StreamPeridotPartitionQueue> for QueueConnector<KS, VS> {
    fn from(input: StreamPeridotPartitionQueue) -> Self {
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

        info!("Polling kafka message stream.");

        let raw_msg = match this.input.poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(val)) => val,
        };

        info!("Found message. Deserialising and passing along.");

        let msg = match <Message<KS::Output, VS::Output> as TryFromOwnedMessage<KS, VS>>::try_from_owned_message(raw_msg) {
            Err(e) => {
                error!("Failed to deser msg: {}", e);
                None
            }
            Ok(m) => Some(m)
        };

        Poll::Ready(msg)
    }
}

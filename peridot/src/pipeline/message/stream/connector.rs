use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll}, sync::Arc,
};

use futures::StreamExt;
use pin_project_lite::pin_project;
use tracing::{error, info};

use crate::{
    app::{PeridotPartitionQueue, extensions::PeridotConsumerContext},
    pipeline::{
        message::types::{Message, TryFromBorrowedMessage},
        serde_ext::PDeserialize,
    },
};

use super::MessageStream;

pin_project! {
    pub struct QueueConnector<'a, KS, VS> {
        #[pin]
        input: PeridotPartitionQueue,
        #[pin]
        msg_stream: Option<Arc<rdkafka::consumer::MessageStream<'a, PeridotConsumerContext>>>,
        _key_serialiser: PhantomData<KS>,
        _value_serialiser: PhantomData<VS>,
    }
}

impl<'a, KS, VS> QueueConnector<'a, KS, VS> {
    pub fn new(input: PeridotPartitionQueue) -> Self {
        QueueConnector {
            input,
            msg_stream: None,
            _key_serialiser: PhantomData,
            _value_serialiser: PhantomData,
        }
    }
}

impl<'a, KS, VS> From<PeridotPartitionQueue> for QueueConnector<'a, KS, VS> {
    fn from(input: PeridotPartitionQueue) -> Self {
        QueueConnector::new(input)
    }
}

impl<'a, KS, VS> MessageStream<KS::Output, VS::Output> for QueueConnector<'a, KS, VS>
where
    KS: PDeserialize,
    VS: PDeserialize,
{
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Message<KS::Output, VS::Output>>> {
        let mut this = self.project();

        info!("Polling kafka message stream.");

        if let None = self.msg_stream {
            info!("Creating new message stream.");
            let msg_stream = Arc::new(this.input.as_ref().as_ref().stream());
            *this.msg_stream = Some(msg_stream);
        }
        let msg_stream = 

        let try_next_msg = match this.input.stream().poll_next_unpin(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(val)) => val,
        };

        info!("Found message. Deserialising and passing along.");

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

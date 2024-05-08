use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use pin_project_lite::pin_project;
use tracing::error;

use crate::{
    engine::{
        queue_manager::partition_queue::{QueueStreamPoll, StreamPeridotPartitionQueue},
        wrapper::serde::PeridotDeserializer,
    },
    message::types::{Message, TryFromOwnedMessage},
};

use super::{MessageStream, MessageStreamPoll};

pin_project! {
    pub struct QueueSerialiser<KS, VS> {
        #[pin]
        input: StreamPeridotPartitionQueue,
        _key_serialiser: PhantomData<KS>,
        _value_serialiser: PhantomData<VS>,
    }
}

impl<KS, VS> QueueSerialiser<KS, VS> {
    pub fn new(input: StreamPeridotPartitionQueue) -> Self {
        QueueSerialiser {
            input,
            _key_serialiser: PhantomData,
            _value_serialiser: PhantomData,
        }
    }
}

impl<KS, VS> From<StreamPeridotPartitionQueue> for QueueSerialiser<KS, VS> {
    fn from(input: StreamPeridotPartitionQueue) -> Self {
        QueueSerialiser::new(input)
    }
}

impl<KS, VS> MessageStream for QueueSerialiser<KS, VS>
where
    KS: PeridotDeserializer,
    VS: PeridotDeserializer,
{
    type KeyType = KS::Output;
    type ValueType = VS::Output;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<MessageStreamPoll<KS::Output, VS::Output>> {
        let this = self.project();

        match this.input.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(MessageStreamPoll::Closed),
            Poll::Ready(Some(poll)) => match poll {
                QueueStreamPoll::Commit(_) => Poll::Ready(MessageStreamPoll::Commit(Ok(()))),
                QueueStreamPoll::Message(raw_msg) => {
                    match <Message<KS::Output, VS::Output> as TryFromOwnedMessage<
                        KS,
                        VS,
                    >>::try_from_owned_message(raw_msg)
                    {
                        Err(e) => {
                            todo!("decide on behaviour");
                            panic!("Failed to deser msg: {}", e);
                        }
                        Ok(m) => Poll::Ready(MessageStreamPoll::Message(m)),
                    }
                }
            },
        }
    }
}

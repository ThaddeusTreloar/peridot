use core::panic;
use std::{
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll, Waker},
};

use futures::Stream;
use parking_lot::Mutex;
use pin_project_lite::pin_project;
use rdkafka::Message as rdMessage;
use tokio::time::Instant;
use tracing::error;

use crate::{
    engine::{
        queue_manager::partition_queue::StreamPeridotPartitionQueue,
        wrapper::serde::PeridotDeserializer,
    },
    message::{
        types::{Message, TryFromOwnedMessage},
        StreamState,
    },
};

use super::{MessageStream, MessageStreamPoll};

pub const COMMIT_INTERVAL: u128 = 100;

pin_project! {
    #[project=QueueHeadProjection]
    pub struct QueueHead<KS, VS> {
        #[pin]
        input: StreamPeridotPartitionQueue,
        interval: Option<Instant>,
        is_committed: bool,
        next_offset: i64,
        _key_serialiser: PhantomData<KS>,
        _value_serialiser: PhantomData<VS>,
    }
}

impl<KS, VS> QueueHead<KS, VS> {
    pub fn new(input: StreamPeridotPartitionQueue) -> Self {
        QueueHead {
            input,
            interval: None,
            is_committed: true,
            next_offset: 0,
            _key_serialiser: PhantomData,
            _value_serialiser: PhantomData,
        }
    }
}

impl<KS, VS> From<StreamPeridotPartitionQueue> for QueueHead<KS, VS> {
    fn from(input: StreamPeridotPartitionQueue) -> Self {
        QueueHead::new(input)
    }
}

impl<KS, VS> MessageStream for QueueHead<KS, VS>
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
        let QueueHeadProjection {
            input,
            interval,
            is_committed,
            next_offset,
            ..
        } = self.project();

        match interval.take() {
            None => {
                let _ = interval.replace(Instant::now());
            }
            Some(instant) => {
                if instant.elapsed().as_millis() > COMMIT_INTERVAL {
                    return Poll::Ready(MessageStreamPoll::Commit(Ok(*next_offset)));
                } else {
                    let _ = interval.replace(instant);
                }
            }
        }

        /*
        if is_closed {
            return Poll::Ready(MessageStreamPoll::Closed)
        }

        if sigint.poll() {
            *is_closed = true;

            return Poll::Ready(MessageStreamPoll::Commit(Ok(next_offset)))
        }
         */

        match input.poll_next(cx) {
            Poll::Pending => {
                let _ = interval.take();

                if *is_committed {
                    Poll::Pending
                } else {
                    *is_committed = true;

                    Poll::Ready(MessageStreamPoll::Commit(Ok(*next_offset)))
                }
            }
            Poll::Ready(None) => panic!("PartitionQueueStreams should never return none. If this panic is triggered, there must be an upstream change."),
            Poll::Ready(Some(raw_msg)) => {
                *next_offset = raw_msg.offset()+1;

                match <Message<KS::Output, VS::Output> as TryFromOwnedMessage<
                    KS,
                    VS,
                >>::try_from_owned_message(raw_msg)
                {
                    Err(e) => {
                        tracing::error!("Failed to deser msg: {}", e);
                        todo!("Propograte deserialisation error, drop, or route to dead letter queue.");
                    }
                    Ok(m) => Poll::Ready(MessageStreamPoll::Message(m)),
                }
            },
        }
    }
}

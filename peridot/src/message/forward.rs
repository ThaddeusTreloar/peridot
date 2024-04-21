use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{ready, Future};
use pin_project_lite::pin_project;
use rdkafka::{consumer::Consumer, producer::Producer, Offset, TopicPartitionList};
use tracing::info;

use crate::engine::QueueMetadata;

use super::{sink::MessageSink, stream::MessageStream};

pin_project! {
    #[project = ForwardProjection]
    pub struct Forward<M, Si>
    where
        M: MessageStream,
        Si: MessageSink<M::KeyType, M::ValueType>,
    {
        #[pin]
        message_stream: M,
        #[pin]
        message_sink: Si,
        queue_metadata: QueueMetadata,
        is_committing: bool,
    }
}

impl<M, Si> Forward<M, Si>
where
    M: MessageStream,
    Si: MessageSink<M::KeyType, M::ValueType>,
{
    pub fn new(message_stream: M, message_sink: Si, queue_metadata: QueueMetadata) -> Self {
        Self {
            message_stream,
            message_sink,
            queue_metadata,
            is_committing: false,
        }
    }
}

const BATCH_SIZE: usize = 1024;

impl<M, Si> Future for Forward<M, Si>
where
    M: MessageStream,
    Si: MessageSink<M::KeyType, M::ValueType>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ForwardProjection {
            mut message_stream,
            mut message_sink,
            queue_metadata,
            is_committing,
        } = self.project();

        info!("Forwarding messages from stream to sink...");

        // If we have entered a commit state, we need to commit the transaction before continuing.
        if *is_committing {
            // If the sink has not completed it's commit, we need to wait.
            ready!(message_sink.as_mut().poll_commit(cx)).expect("Failed to commit transaction.");

            // Otherwise, we can transition our commit state.
            *is_committing = false;
        }

        for _ in 0..BATCH_SIZE {
            ready!(message_sink.as_mut().poll_ready(cx)).expect("Failed to get sink ready.");

            match message_stream.as_mut().poll_next(cx) {
                Poll::Ready(None) => {
                    info!("No Messages left for stream, finishing...");

                    ready!(message_sink.as_mut().poll_close(cx)).expect("Failed to close.");

                    return Poll::Ready(());
                }
                Poll::Pending => {
                    info!("No messages available, waiting...");

                    // No messages available, we can transition to a commit state.
                    *is_committing = true;

                    // Propogate the pending state to the caller.
                    return Poll::Pending;
                }
                Poll::Ready(Some(message)) => {
                    let topic = String::from(message.topic());
                    let partition = message.partition();
                    let offset = message.offset();

                    message_sink
                        .as_mut()
                        .start_send(message)
                        .expect("Failed to send message to sink.");

                    let cgm = queue_metadata
                        .consumer()
                        .group_metadata()
                        .expect("No consumer group metadata present while committing transaction.");

                    let mut offsets = TopicPartitionList::new();

                    offsets
                        .add_partition_offset(&topic, partition, Offset::Offset(offset))
                        .expect("Failed to add partition offset.");

                    queue_metadata
                        .producer()
                        .send_offsets_to_transaction(&offsets, &cgm, Duration::from_millis(1000))
                        .expect("Failed to send offsets to transaction.");
                }
            }
        }

        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

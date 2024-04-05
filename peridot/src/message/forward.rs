use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{ready, Future};
use pin_project_lite::pin_project;
use rdkafka::{consumer::Consumer, producer::Producer, Offset, TopicPartitionList};
use tracing::info;

use crate::{engine::QueueMetadata, serde_ext::PSerialize};

use super::{sink::MessageSink, stream::MessageStream};

pin_project! {
    #[project = ForwardProjection]
    pub struct Forward<M, Si>
    where
        M: MessageStream,
        Si: MessageSink,
        Si::KeySerType: PSerialize<Input = <M as MessageStream>::KeyType>,
        Si::ValueSerType: PSerialize<Input = <M as MessageStream>::ValueType>,
    {
        #[pin]
        message_stream: M,
        #[pin]
        message_sink: Si,
        queue_metadata: QueueMetadata,
    }
}

impl<M, Si> Forward<M, Si>
where
    M: MessageStream,
    Si: MessageSink,
    Si::KeySerType: PSerialize<Input = <M as MessageStream>::KeyType>,
    Si::ValueSerType: PSerialize<Input = <M as MessageStream>::ValueType>,
{
    pub fn new(message_stream: M, message_sink: Si, queue_metadata: QueueMetadata) -> Self {
        Self {
            message_stream,
            message_sink,
            queue_metadata,
        }
    }
}

const BATCH_SIZE: usize = 1024;

impl<M, Si> Future for Forward<M, Si>
where
    M: MessageStream,
    Si: MessageSink,
    Si::KeySerType: PSerialize<Input = <M as MessageStream>::KeyType>,
    Si::ValueSerType: PSerialize<Input = <M as MessageStream>::ValueType>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ForwardProjection {
            mut message_stream,
            mut message_sink,
            queue_metadata,
        } = self.project();

        info!("Forwarding messages from stream to sink...");

        for _ in 0..BATCH_SIZE {
            ready!(message_sink.as_mut().poll_ready(cx))
                .expect("Failed to get sink ready.");

            match message_stream.as_mut().poll_next(cx) {
                Poll::Ready(None) => {
                    info!("No Messages left for stream, finishing...");

                    ready!(message_sink.as_mut().poll_close(cx)).expect("Failed to close.");

                    return Poll::Ready(());
                }
                Poll::Pending => {
                    info!("No messages available, waiting...");

                    queue_metadata
                        .producer()
                        .commit_transaction(Duration::from_millis(1000))
                        .expect("Failed to commit transaction.");

                    ready!(message_sink.as_mut().poll_commit(cx)).expect("Failed to commit.");

                    return Poll::Pending;
                }
                Poll::Ready(Some(message)) => {
                    message_sink
                        .as_mut()
                        .start_send(&message)
                        .expect("Failed to send message to sink.");

                    let cgm = queue_metadata
                        .consumer()
                        .group_metadata()
                        .expect("No consumer group metadata present while committing transaction.");

                    let mut offsets = TopicPartitionList::new();

                    offsets
                        .add_partition_offset(
                            message.topic(),
                            message.partition(),
                            Offset::Offset(message.offset()),
                        )
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

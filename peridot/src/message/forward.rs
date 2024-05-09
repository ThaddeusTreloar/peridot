use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::{ready, Future};
use pin_project_lite::pin_project;
use rdkafka::{
    consumer::Consumer,
    error::KafkaError,
    producer::{self, FutureProducer, Producer},
    Offset, TopicPartitionList,
};
use tokio::time::Instant;
use tracing::{event, info, instrument, trace_span, Level};

use crate::engine::queue_manager::queue_metadata::{self, QueueMetadata};

use super::{
    sink::MessageSink,
    stream::{MessageStream, MessageStreamPoll},
    StreamState, BATCH_SIZE,
};

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
        stream_state: StreamState,
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
            stream_state: Default::default(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ForwarderError {
    #[error(transparent)]
    UnknownError(#[from] Box<dyn std::error::Error + Send>),
}

impl<M, Si> Future for Forward<M, Si>
where
    M: MessageStream,
    Si: MessageSink<M::KeyType, M::ValueType>,
{
    type Output = Result<(), ForwarderError>;

    // TODO: changelog specific fork that injects the changelog offset into the packet
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ForwardProjection {
            mut message_stream,
            mut message_sink,
            queue_metadata,
            mut stream_state,
        } = self.project();

        let span = tracing::span!(
            Level::DEBUG,
            "Forward::poll",
            topic = queue_metadata.source_topic(),
            partition = queue_metadata.partition()
        )
        .entered();

        for _ in 0..BATCH_SIZE {
            match stream_state {
                StreamState::Committing | StreamState::CommittingClosing => {
                    // If the sink has not completed it's committing, we need to wait.
                    ready!(message_sink.as_mut().poll_commit(cx))
                        .expect("Failed to commit transaction.");
                    // TODO: match and if err, abort transaction.

                    let cgm = queue_metadata.engine_context().group_metadata();

                    let mut offsets = TopicPartitionList::new();

                    // Get from stored offset
                    let next_offset = 0;

                    offsets
                        .add_partition_offset(
                            queue_metadata.source_topic(),
                            queue_metadata.partition(),
                            Offset::Offset(next_offset),
                        )
                        .expect("Failed to add partition offset.");

                    match queue_metadata.producer_arc().send_offsets_to_transaction(
                        &offsets,
                        &cgm,
                        Duration::from_millis(1000),
                    ) {
                        Ok(r) => {
                            tracing::debug!("Successfully sent offsets, {} to producer transaction for source_topic: {}, partition: {}", next_offset, queue_metadata.source_topic(), queue_metadata.partition())
                        }
                        Err(KafkaError::Transaction(e)) => {
                            tracing::error!(
                                "Transaction error while committing transaction, caused by {}",
                                e
                            );
                            panic!(
                                "Transaction error while committing transaction, caused by {}",
                                e
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                "Unknown error while committing transaction, caused by {}",
                                e
                            );
                            panic!(
                                "Unknown error while committing transaction, caused by {}",
                                e
                            );
                        }
                    };

                    match queue_metadata
                        .producer()
                        .commit_transaction(Duration::from_millis(2500))
                    {
                        Ok(r) => {
                            tracing::debug!("Successfully committed producer transaction for source_topic: {}, partition: {}", queue_metadata.source_topic(), queue_metadata.partition())
                        }
                        Err(KafkaError::Transaction(e)) => {
                            tracing::error!(
                                "Transaction error while committing transaction, caused by {}",
                                e
                            );
                            panic!(
                                "Transaction error while committing transaction, caused by {}",
                                e
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                "Unknown error while committing transaction, caused by {}",
                                e
                            );
                            panic!(
                                "Unknown error while committing transaction, caused by {}",
                                e
                            );
                        }
                    }

                    match stream_state {
                        StreamState::Committing => {
                            *stream_state = StreamState::Committed;

                            match queue_metadata.producer().begin_transaction() {
                                Ok(r) => {
                                    tracing::debug!("Successfully begun producer transaction for source_topic: {}, partition: {}", queue_metadata.source_topic(), queue_metadata.partition())
                                }
                                Err(KafkaError::Transaction(e)) => {
                                    tracing::error!(
                                        "Transaction error while committing transaction, caused by {}",
                                        e
                                    );
                                    panic!(
                                        "Transaction error while committing transaction, caused by {}",
                                        e
                                    );
                                }
                                Err(e) => {
                                    tracing::error!(
                                        "Unknown error while committing transaction, caused by {}",
                                        e
                                    );
                                    panic!(
                                        "Unknown error while committing transaction, caused by {}",
                                        e
                                    );
                                }
                            };
                        }
                        StreamState::CommittingClosing => *stream_state = StreamState::Closing,
                        _ => (),
                    }
                }
                StreamState::Committed | StreamState::Uncommitted | StreamState::Sleeping => {
                    ready!(message_sink.as_mut().poll_ready(cx))
                        .expect("Failed to get sink ready.");

                    match message_stream.as_mut().poll_next(cx) {
                        Poll::Ready(MessageStreamPoll::Closed) => {
                            tracing::debug!(
                                "Upstream queue for topic: {}, partition: {}, has completed. Cleaning up...",
                                queue_metadata.source_topic(),
                                queue_metadata.partition()
                            );

                            match stream_state {
                                StreamState::Committed | StreamState::Sleeping => {
                                    *stream_state = StreamState::Closing;
                                }
                                StreamState::Uncommitted => {
                                    *stream_state = StreamState::CommittingClosing;
                                }
                                _ => (),
                            }
                        }
                        Poll::Pending | Poll::Ready(MessageStreamPoll::Commit(Ok(_))) => {
                            // TODO: Store commit offset from messagestreampoll

                            match stream_state {
                                StreamState::Uncommitted => {
                                    tracing::debug!(
                                        "No messages available for topic: {} partition: {}, committing...",
                                        queue_metadata.source_topic(),
                                        queue_metadata.partition()
                                    );
                                    *stream_state = StreamState::Committing;
                                }
                                StreamState::Committed => {
                                    tracing::debug!(
                                        "No messages available for topic: {} partition: {}, already comitted. Transitioning to Sleeping.",
                                        queue_metadata.source_topic(),
                                        queue_metadata.partition()
                                    );
                                    *stream_state = StreamState::Sleeping;
                                },
                                StreamState::Sleeping => {
                                    return Poll::Pending;
                                }
                                _ => (),
                            }
                        }
                        Poll::Ready(MessageStreamPoll::Commit(Err(e))) => {
                            todo!("Handle upstream commit error.");
                        }
                        Poll::Ready(MessageStreamPoll::Message(message)) => {
                            tracing::debug!("Message received in Fork, sending to sink.");
                            // If we were waiting for upstream nodes to finish committing, we can transition
                            // to our default state.
                            if let StreamState::Committed = stream_state {
                                *stream_state = Default::default();
                            }

                            // TODO: Some retry logic
                            message_sink
                                .as_mut()
                                .start_send(message)
                                .expect("Failed to send message to sink.");
                        }
                    }
                }
                StreamState::Closing => {
                    tracing::debug!("Stream state Closed, finishing...");

                    ready!(message_sink.as_mut().poll_close(cx)).expect("Failed to close");
                    return Poll::Ready(Ok(()));
                }
            }
        }

        cx.waker().wake_by_ref();

        Poll::Pending
    }
}

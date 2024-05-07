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
    producer::{FutureProducer, Producer},
    Offset, TopicPartitionList,
};
use tokio::time::Instant;
use tracing::{event, info, instrument, trace_span, Level};

use crate::engine::queue_manager::queue_metadata::{self, QueueMetadata};

use super::{sink::MessageSink, stream::MessageStream, StreamState, BATCH_SIZE};

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
        commit_state: StreamState,
        interval: Option<Instant>,
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
            commit_state: Default::default(),
            interval: None,
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
            mut commit_state,
            mut interval,
        } = self.project();

        let span = tracing::span!(
            Level::TRACE,
            "Forward::poll",
            topic = queue_metadata.source_topic(),
            partition = queue_metadata.partition()
        )
        .entered();

        if interval.is_none() {
            let _ = interval.replace(Instant::now());
        }

        // If we have entered a commit state, we need to commit the transaction before continuing.
        if commit_state.is_committing() {
            // If the sink has not completed it's committing, we need to wait.
            ready!(message_sink.as_mut().poll_commit(cx)).expect("Failed to commit transaction.");
            // TODO: match and if err, abort transaction.

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

            // Otherwise, we can transition our commit state.
            *commit_state = StreamState::Committed;

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

            let _ = interval.replace(Instant::now());
        }

        for _ in 0..BATCH_SIZE {
            ready!(message_sink.as_mut().poll_ready(cx)).expect("Failed to get sink ready.");

            match message_stream.as_mut().poll_next(cx) {
                Poll::Ready(None) => {
                    tracing::debug!(
                        "Upstream queue for topic: {}, partition: {}, has completed. Cleaning up...",
                        queue_metadata.source_topic(),
                        queue_metadata.partition()
                    );

                    match commit_state {
                        StreamState::Committed | StreamState::Sleeping => {
                            ready!(message_sink.as_mut().poll_close(cx)).expect("Failed to close");
                            return Poll::Ready(Ok(()));
                        }
                        StreamState::Uncommitted => {
                            *commit_state = StreamState::Committing;

                            cx.waker().wake_by_ref();

                            return Poll::Pending;
                        }
                        StreamState::Committing => panic!("This should not be possible"),
                    }
                }
                Poll::Pending => {
                    tracing::debug!(
                        "No messages available for topic: {} partition: {}, waiting...",
                        queue_metadata.source_topic(),
                        queue_metadata.partition()
                    );

                    // We have recieved no messages from upstream, they will have
                    // transitioned to a commit state.
                    // We can transition to a commit state if we haven't already.
                    if let StreamState::Uncommitted = commit_state {
                        *commit_state = StreamState::Committing;
                        cx.waker().wake_by_ref();
                    }

                    // Propogate the pending state to the caller.
                    return Poll::Pending;
                }
                Poll::Ready(Some(message)) => {
                    tracing::debug!("Message received in Fork, sending to sink.");
                    // If we were waiting for upstream nodes to finish committing, we can transition
                    // to our default state.
                    if let StreamState::Committed = commit_state {
                        *commit_state = Default::default();
                    }

                    let topic = String::from(message.topic());
                    let partition = message.partition();
                    let offset = message.offset();

                    message_sink
                        .as_mut()
                        .start_send(message)
                        .expect("Failed to send message to sink.");

                    if let Some(interval) = interval {
                        if interval.elapsed().as_millis() > 100 {
                            let cgm = queue_metadata.engine_context().group_metadata();

                            let mut offsets = TopicPartitionList::new();

                            let next_offset = offset + 1;

                            offsets
                                .add_partition_offset(
                                    &topic,
                                    partition,
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

                            // We have recieved no messages from upstream, they will have
                            // transitioned to a commit state.
                            // We can transition to a commit state if we haven't already.
                            if let StreamState::Uncommitted = commit_state {
                                *commit_state = StreamState::Committing;
                                cx.waker().wake_by_ref();
                            }

                            // Propogate the pending state to the caller.
                            return Poll::Pending;
                        }
                    }
                }
            }
        }

        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/*
 * Copyright 2024 Thaddeus Treloar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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

use crate::{
    engine::queue_manager::queue_metadata::{self, QueueMetadata}, error::ErrorType, message::sink::topic_sink::TopicSinkError
};

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
        next_offset: i64,
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
            next_offset: 0,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ForwarderError {
    #[error(transparent)]
    UnknownError(#[from] Box<dyn std::error::Error + Send>),
}

impl<M, Si> Forward<M, Si>
where
    M: MessageStream,
    Si: MessageSink<M::KeyType, M::ValueType>,
{
    fn commit(
        queue_metadata: &mut QueueMetadata,
        message_sink: &mut Pin<&mut Si>,
        stream_state: &mut StreamState,
        next_offset: &mut i64,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ForwarderError>> {
        tracing::debug!("Committing forward at offset: {}", *next_offset);

        match message_sink.as_mut().poll_commit(*next_offset, cx) {
            Poll::Pending => {
                tracing::debug!("Waiting on downstream commit...");
                return Poll::Pending;
            }
            Poll::Ready(r) => {
                r.expect("Failed to commit transaction.");
            }
        }
        // TODO: match and if err, abort transaction.

        let cgm = queue_metadata.engine_context().group_metadata();

        let mut offsets = TopicPartitionList::new();

        tracing::debug!("Adding offset: {} to transaction.", next_offset);

        if *next_offset < 0 {
            *stream_state = StreamState::Committed;

            return Poll::Ready(Ok(()));
        }

        offsets
            .add_partition_offset(
                queue_metadata.source_topic(),
                queue_metadata.partition(),
                Offset::Offset(*next_offset),
            )
            .expect("Failed to add partition offset");

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
                tracing::debug!("Successfully committed producer transaction for source_topic: {}, partition: {}, offset: {}", queue_metadata.source_topic(), queue_metadata.partition(), next_offset)
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

        info!("Transaction comitted.");
        *stream_state = StreamState::Committed;

        match queue_metadata.producer().begin_transaction() {
            Ok(r) => {
                tracing::debug!(
                    "Successfully begun producer transaction for source_topic: {}, partition: {}",
                    queue_metadata.source_topic(),
                    queue_metadata.partition()
                );

                Poll::Ready(Ok(()))
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
    }

    fn handle_poll(
        queue_metadata: &mut QueueMetadata,
        message_stream: &mut Pin<&mut M>,
        message_sink: &mut Pin<&mut Si>,
        stream_state: &mut StreamState,
        next_offset: &mut i64,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ForwarderError>> {
        ready!(message_sink.as_mut().poll_ready(cx)).expect("Failed to get sink ready.");

        match ready!(message_stream.as_mut().poll_next(cx)) {
            MessageStreamPoll::Closed => {
                tracing::debug!(
                    "Upstream queue for topic: {}, partition: {}, has completed. Cleaning up...",
                    queue_metadata.source_topic(),
                    queue_metadata.partition()
                );

                *stream_state = StreamState::Closing;

                Poll::Ready(Ok(()))
            }
            MessageStreamPoll::Commit(Ok(offset)) => {
                tracing::debug!(
                    "Recieved commit instruction for topic: {} partition: {}, offset: {}",
                    queue_metadata.source_topic(),
                    queue_metadata.partition(),
                    offset
                );

                if offset > *next_offset {
                    tracing::debug!(
                        "Consumer position increased, committing forward at offset: {}.",
                        offset
                    );

                    *next_offset = offset;

                    *stream_state = StreamState::Committing;

                    Self::commit(queue_metadata, message_sink, stream_state, next_offset, cx)
                } else {
                    tracing::debug!(
                        "No change in consumer offset, skipping forward commit at offset: {}.",
                        offset
                    );

                    Poll::Ready(Ok(()))
                }
            }
            MessageStreamPoll::Commit(Err(e)) => {
                todo!("Handle upstream commit error.");
            }
            MessageStreamPoll::Message(message) => {
                tracing::debug!("Message received in Fork, sending to sink.");
                // If we were waiting for upstream nodes to finish committing, we can transition
                // to our default state.
                if let StreamState::Committed = stream_state {
                    *stream_state = Default::default();
                }

                let topic = String::from(message.topic());
                let partition = message.partition();
                let offset = message.offset();

                // TODO: Some retry logic
                match message_sink.as_mut().start_send(message) {
                    Ok(_) => (),
                    //                    Err(e) => {}
                    Err(ErrorType::Fatal(e)) => panic!("Unknown error while sending record: {}", e),
                    Err(ErrorType::Retryable(e)) => todo!("Handle Retryable error"),
                };

                Poll::Ready(Ok(()))
            }
        }
    }
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
            stream_state,
            next_offset,
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
                StreamState::Committing => {
                    // TODO: Expand error propogation
                    ready!(Self::commit(
                        queue_metadata,
                        &mut message_sink,
                        stream_state,
                        next_offset,
                        cx
                    ))
                    .expect("Commit Error");
                }
                StreamState::Committed | StreamState::Uncommitted | StreamState::Sleeping => {
                    // TODO: Expand error propogation
                    ready!(Self::handle_poll(
                        queue_metadata,
                        &mut message_stream,
                        &mut message_sink,
                        stream_state,
                        next_offset,
                        cx,
                    ))
                    .expect("Poll error");
                }
                StreamState::Closing => {
                    ready!(message_sink.as_mut().poll_close(cx)).expect("Failed to close");
                    return Poll::Ready(Ok(()));
                }
            }
        }

        cx.waker().wake_by_ref();

        Poll::Pending
    }
}

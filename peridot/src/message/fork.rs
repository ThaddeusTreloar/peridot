use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::ready;
use pin_project_lite::pin_project;
use tracing::{info, Level};

use crate::engine::queue_manager::queue_metadata::{self, QueueMetadata};

use super::{sink::MessageSink, stream::{MessageStream, MessageStreamPoll}, StreamState, BATCH_SIZE};

pin_project! {
    #[project = ForkProjection]
    pub struct Fork<M, Si>
    where
        M: MessageStream,
        Si: MessageSink<M::KeyType, M::ValueType>,
    {
        queue_metadata: QueueMetadata,
        #[pin]
        message_stream: M,
        #[pin]
        message_sink: Si,
        stream_state: StreamState,
    }
}

impl<M, Si> Fork<M, Si>
where
    M: MessageStream,
    Si: MessageSink<M::KeyType, M::ValueType>,
{
    pub fn new(message_stream: M, message_sink: Si, queue_metadata: QueueMetadata) -> Self {
        Self {
            queue_metadata,
            message_stream,
            message_sink,
            stream_state: Default::default(),
        }
    }
}

impl<M, Si> MessageStream for Fork<M, Si>
where
    M: MessageStream,
    Si: MessageSink<M::KeyType, M::ValueType>,
    M::KeyType: Clone,
    M::ValueType: Clone,
{
    type KeyType = M::KeyType;
    type ValueType = M::ValueType;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<MessageStreamPoll<Self::KeyType, Self::ValueType>> {
        let ForkProjection {
            queue_metadata,
            mut message_stream,
            mut message_sink,
            stream_state,
        } = self.project();

        let span = tracing::span!(Level::DEBUG, "->Fork::poll",).entered();

        for _ in 0..BATCH_SIZE {
            match stream_state {
                StreamState::Committing | StreamState::CommittingClosing => {
                    // If we have transitioned to a committing state, we can start our
                    // sink commit process. Otherwise, we can continue to poll the stream.
                    tracing::debug!(
                        "Notifying fork sink of commit: {}",
                        std::any::type_name::<Si>()
                    );
                    ready!(message_sink.as_mut().poll_commit(cx)).expect("Failed to commit");

                    match stream_state {
                        StreamState::Committing => {
                            *stream_state = StreamState::Committed;
                        }
                        StreamState::CommittingClosing => {
                            *stream_state = StreamState::Closing;
                        }
                        _ => ()
                    }
                },
                StreamState::Closing => {
                    tracing::debug!("No Messages left for stream, finishing...");
                    ready!(message_sink.as_mut().poll_close(cx)).expect("Failed to close");
                    return Poll::Ready(MessageStreamPoll::Closed);
                },
                StreamState::Uncommitted | StreamState::Sleeping | StreamState::Committed => {
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
                            // We have recieved no messages from upstream, they will have
                            // transitioned to a commit state.
                            // We can transition to a commit state if we haven't already.
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
                            todo!("Handle upstream commit error")
                        }
                        Poll::Ready(MessageStreamPoll::Message(message)) => {
                            tracing::debug!("Message received in Fork, sending to sink.");
                            // If we were waiting for upstream nodes to commit, we can transition
                            // to our default state.
                            if let StreamState::Committed = stream_state {
                                *stream_state = Default::default();
                            }

                            message_sink
                                .as_mut()
                                .start_send(message.clone())
                                .expect("Failed to send message to sink.");

                            return Poll::Ready(MessageStreamPoll::Message(message));
                        }

                    }
                }
            }
        }

        cx.waker().wake_by_ref();

        Poll::Pending
    }
}

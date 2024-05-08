use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::ready;
use pin_project_lite::pin_project;
use tracing::{info, Level};

use crate::engine::queue_manager::queue_metadata::{self, QueueMetadata};

use super::{sink::MessageSink, stream::MessageStream, StreamState};

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
        commit_state: StreamState,
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
            commit_state: Default::default(),
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
    ) -> Poll<Option<super::types::Message<Self::KeyType, Self::ValueType>>> {
        let ForkProjection {
            queue_metadata,
            mut message_stream,
            mut message_sink,
            commit_state,
        } = self.project();

        let span = tracing::span!(Level::DEBUG, "->Fork::poll",).entered();

        // If we have transitioned to a committing state, we can start our
        // sink commit process. Otherwise, we can continue to poll the stream.
        if let StreamState::Committing = commit_state {
            tracing::debug!(
                "Notifying fork sink of commit: {}",
                std::any::type_name::<Si>()
            );
            ready!(message_sink.as_mut().poll_commit(cx)).expect("Failed to commut");
            *commit_state = StreamState::Committed;
        }

        // Poll the stream for the next message. If self.commit_state is
        // Committed, we know that we have committed our producer transaction,
        // and we will begin polling upstream nodes until they yield Poll::Ready
        // at which point we know that they have either committed or aborted.
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
                        Poll::Ready(None)
                    }
                    StreamState::Uncommitted => {
                        *commit_state = StreamState::Committing;

                        cx.waker().wake_by_ref();

                        Poll::Pending
                    }
                    StreamState::Committing => panic!("This should not be possible"),
                }
            }
            Poll::Pending => {
                tracing::debug!(
                    "No messages available for topic: {}, partition: {}, sleeping...",
                    queue_metadata.source_topic(),
                    queue_metadata.partition()
                );

                // We have recieved no messages from upstream, they will have
                // transitioned to a commit state.
                // We can transition to a commit state if we haven't already.
                if let StreamState::Uncommitted = commit_state {
                    *commit_state = StreamState::Committing;
                }

                Poll::Pending
            }
            Poll::Ready(Some(message)) => {
                tracing::debug!("Message received in Fork, sending to sink.");
                // If we were waiting for upstream nodes to commit, we can transition
                // to our default state.
                if let StreamState::Committed = commit_state {
                    *commit_state = Default::default();
                }

                message_sink
                    .as_mut()
                    .start_send(message.clone())
                    .expect("Failed to send message to sink.");

                Poll::Ready(Some(message))
            }
        }
    }
}

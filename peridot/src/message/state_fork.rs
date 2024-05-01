use std::{
    ops::DerefMut, pin::Pin, task::{Context, Poll}, time::Duration
};

use futures::ready;
use pin_project_lite::pin_project;
use serde::{de::DeserializeOwned, Deserialize};
use tracing::info;

use crate::{engine::wrapper::serde::native::NativeBytes, message::types::{Message, TryFromOwnedMessage}};

use super::{fork::CommitState, sink::MessageSink, stream::MessageStream};

#[derive(Debug, Default)]
enum StoreState {
    #[default]
    Empty,
    Rebuilding,
    Rebuilt,
}

pin_project! {
    #[project = StateSinkForkProjection]
    pub struct StateSinkFork<M, Si>
    where
        M: MessageStream,
        Si: MessageSink<M::KeyType, M::ValueType>,
    {
        #[pin]
        message_stream: M,
        #[pin]
        changelog_stream: M,
        #[pin]
        message_sink: Si,
        commit_state: CommitState,
        store_state: StoreState,
    }
}

impl<M, Si> StateSinkFork<M, Si>
where
    M: MessageStream,
    Si: MessageSink<M::KeyType, M::ValueType>,
{
    pub fn new(changelog_stream: M, message_stream: M, message_sink: Si) -> Self {
        Self {
            changelog_stream,
            message_stream,
            message_sink,
            commit_state: Default::default(),
            store_state: Default::default(),
        }
    }
}

impl<M, Si> MessageStream for StateSinkFork<M, Si>
where
    M: MessageStream,
    Si: MessageSink<M::KeyType, M::ValueType>,
    M::KeyType: Clone + DeserializeOwned,
    M::ValueType: Clone + DeserializeOwned,
{
    type KeyType = M::KeyType;
    type ValueType = M::ValueType;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<super::types::Message<Self::KeyType, Self::ValueType>>> {
        let StateSinkForkProjection {
            mut changelog_stream,
            mut message_stream,
            mut message_sink,
            commit_state,
            store_state,
        } = self.project();

        info!("ForkedSinking messages from stream to sink...");

        // If we have transitioned to a committing state, we can start our
        // sink commit process. Otherwise, we can continue to poll the stream.
        if let CommitState::Committing = commit_state {
            info!("Committing sink...");
            ready!(message_sink.as_mut().poll_close(cx)).expect("Failed to close");
            *commit_state = CommitState::Committed;
        }

        if let StoreState::Empty = store_state {
            match ready!(changelog_stream.poll_next(cx)) {
                None => {
                    unimplemented!("")
                },
                Some(message) => {
                    message_sink.as_mut().start_send(message)
                        .expect("Failed to start send.");
                },
            }
        }


        // Poll the stream for the next message. If self.commit_state is
        // Committed, we know that we have committed our producer transaction,
        // and we will begin polling upstream nodes until they yield Poll::Ready
        // at which point we know that they have either committed or aborted.
        match message_stream.as_mut().poll_next(cx) {
            Poll::Ready(None) => {
                info!("No Messages left for stream, finishing...");
                ready!(message_sink.as_mut().poll_close(cx)).expect("Failed to close");
                Poll::Ready(None)
            }
            Poll::Pending => {
                info!("No messages available, waiting...");

                // We have recieved no messages from upstream, they will have
                // transitioned to a commit state.
                // We can transition to a commit state if we haven't already.
                if let CommitState::NotCommitting = commit_state {
                    *commit_state = CommitState::Committing;
                    cx.waker().wake_by_ref();
                }

                Poll::Pending
            }
            Poll::Ready(Some(message)) => {
                // If we were waiting for upstream nodes to commit, we can transition
                // to our default state.
                if let CommitState::Committed = commit_state {
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

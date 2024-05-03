use std::{
    cmp::Ordering, marker::PhantomData, ops::DerefMut, pin::Pin, sync::Arc, task::{Context, Poll}, time::Duration
};

use futures::ready;
use pin_project_lite::pin_project;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::info;

use crate::{engine::{changelog_manager::ChangelogManager, client_manager::{self, ClientManager}, context::EngineContext, wrapper::serde::native::NativeBytes}, message::types::{Message, TryFromOwnedMessage}, state::{self, backend::StateBackend}};

use super::{fork::CommitState, sink::{state_sink::StateSink, MessageSink}, stream::MessageStream};

#[derive(Debug, Default, PartialEq, Eq)]
enum StoreState {
    #[default]
    Uninitialised,
    Rebuilding,
    Ready,
}

impl StoreState {
    pub(crate) fn is_ready(&self) -> bool {
        *self == StoreState::Ready
    }
}

pin_project! {
    #[project = StateSinkForkProjection]
    pub struct StateSinkFork<B, M>
    where
        M: MessageStream,
        B: StateBackend
    {
        #[pin]
        message_stream: M,
        #[pin]
        state_sink: StateSink<B, M::KeyType, M::ValueType>,
        engine_context: Arc<EngineContext>,
        #[pin]
        changelog_stream: Option<M>,
        table_name: String,
        partition: i32,
        commit_state: CommitState,
        store_state: StoreState,
        _state_backend: PhantomData<B>,
    }
}

impl<B, M> StateSinkFork<B, M>
where
    M: MessageStream,
    B: StateBackend,
{
    pub fn new(message_stream: M, state_sink: StateSink<B, M::KeyType, M::ValueType>, engine_context: Arc<EngineContext>, table_name: String, partition: i32) -> Self {
        Self {
            changelog_stream: None,
            message_stream,
            state_sink,
            engine_context,
            table_name,
            partition,
            store_state: StoreState::Ready,
            commit_state: Default::default(),
            _state_backend: Default::default(),
        }
    }

    pub fn new_with_changelog(changelog_stream: M, message_stream: M, state_sink: StateSink<B, M::KeyType, M::ValueType>, engine_context: Arc<EngineContext>, table_name: String, partition: i32) -> Self {
        Self {
            changelog_stream: Some(changelog_stream),
            message_stream,
            state_sink,
            engine_context,
            table_name,
            partition,
            store_state: Default::default(),
            commit_state: Default::default(),
            _state_backend: Default::default(),
        }
    }
}

impl<B, M> MessageStream for StateSinkFork<B, M>
where
    M: MessageStream,
    M::KeyType: Serialize + Send + Sync + Clone + DeserializeOwned + 'static,
    M::ValueType: Serialize + DeserializeOwned + Send + Sync + Clone + DeserializeOwned + 'static,
    B: StateBackend + Send + Sync + 'static,
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
            mut state_sink,
            commit_state,
            store_state,
            engine_context,
            table_name,
            partition,
            ..
        } = self.project();

        let mut changelog_stream_proj = changelog_stream.as_pin_mut();

        // If we have transitioned to a committing state, we can start our
        // sink commit process. Otherwise, we can continue to poll the stream.
        if let CommitState::Committing = commit_state {
            info!("Committing sink...");
            ready!(state_sink.as_mut().poll_commit(cx)).expect("Failed to close");
            *commit_state = CommitState::Committed;
        }

        if store_state.is_ready() && changelog_stream_proj.is_some() {
            let s = changelog_stream_proj.take();
        }


        if let Some(mut stream) = changelog_stream_proj {
            // If we were waiting for upstream nodes to commit, we can transition
            // to our default state.
            if let CommitState::Committed = commit_state {
                *commit_state = Default::default();
            }

            let checkpoint = state_sink.get_checkpoint()
                .expect("Failed to get checkpoint")
                .unwrap_or(0);

            let watermarks = engine_context.watermark_for_changelog(table_name, *partition);

            let mut beginning_offset = 0;

            if checkpoint > watermarks.high() || 
                checkpoint < watermarks.high() && checkpoint <= watermarks.low()
            {
                beginning_offset = watermarks.low();

                // reset state store
            }
            else {
                // If the check point is seekable, or the checkpoint is equal with the high watermark
                // Just use the checkpoint as the next offset.
                beginning_offset = checkpoint;
            }

            for _ in 0..1024 {
                match stream.as_mut().poll_next(cx) {
                    Poll::Ready(None) => panic!("Changelog stream closed before state store rebuilt."),
                    Poll::Ready(Some(message)) => state_sink.as_mut()
                        .start_send(message)
                        .expect("Failed to send message to sink."),
                    Poll::Pending => {
                        match state_sink.as_mut().poll_commit(cx) {
                            Poll::Pending => {
                                *commit_state = CommitState::Committing;
                                return Poll::Pending;
                            },
                            Poll::Ready(result) => {
                                result.expect("Sink failed to commit.");
                                
                                let checkpoint = state_sink.get_checkpoint()
                                    .expect("Failed to get checkpoint.")
                                    .expect("State sink not checkpointed after commit.");

                                match checkpoint.cmp(&watermarks.high()) {
                                    Ordering::Equal => {
                                        break;
                                    },
                                    Ordering::Greater => {
                                        panic!("Checkpoint greater than watermark after state store rebuild.")
                                    },
                                    Ordering::Less => (),
                                }
                            }
                        }
                    }
                }
            }

            *store_state = StoreState::Ready;

            engine_context.close_changelog_stream(table_name, *partition)
                .expect("Failed to close changelog stream.");
        }

        info!("ForkedSinking messages from stream to sink...");

        // Poll the stream for the next message. If self.commit_state is
        // Committed, we know that we have committed our producer transaction,
        // and we will begin polling upstream nodes until they yield Poll::Ready
        // at which point we know that they have either committed or aborted.
        match message_stream.as_mut().poll_next(cx) {
            Poll::Ready(None) => {
                info!("No Messages left for stream, finishing...");
                ready!(state_sink.as_mut().poll_close(cx)).expect("Failed to close");
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

                state_sink
                    .as_mut()
                    .start_send(message.clone())
                    .expect("Failed to send message to sink.");

                Poll::Ready(Some(message))
            }
        }
    }
}

use std::{
    cmp::Ordering,
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::ready;
use pin_project_lite::pin_project;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::info;

use crate::{
    engine::{
        changelog_manager::ChangelogManager,
        client_manager::{self, ClientManager},
        context::EngineContext,
        queue_manager::partition_queue::StreamPeridotPartitionQueue,
        wrapper::serde::native::NativeBytes,
    },
    message::types::{Message, TryFromOwnedMessage},
    state::{self, backend::StateBackend},
};

use super::{
    sink::{state_sink::StateSink, MessageSink},
    stream::{serialiser::QueueSerialiser, MessageStream},
    StreamState,
};

type DeserialiserQueue<K, V> = QueueSerialiser<NativeBytes<K>, NativeBytes<V>>;

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
        changelog_stream: Option<DeserialiserQueue<M::KeyType, M::ValueType>>,
        changelog_start: Option<i64>,
        store_name: String,
        partition: i32,
        commit_state: StreamState,
        store_state: StoreState,
        _state_backend: PhantomData<B>,
    }
}

impl<B, M> StateSinkFork<B, M>
where
    M: MessageStream,
    B: StateBackend,
{
    pub fn new(
        message_stream: M,
        state_sink: StateSink<B, M::KeyType, M::ValueType>,
        engine_context: Arc<EngineContext>,
        store_name: String,
        partition: i32,
    ) -> Self {
        Self {
            changelog_stream: None,
            message_stream,
            state_sink,
            engine_context,
            store_name,
            partition,
            changelog_start: None,
            store_state: StoreState::Ready,
            commit_state: Default::default(),
            _state_backend: Default::default(),
        }
    }

    pub fn new_with_changelog(
        changelog_stream: StreamPeridotPartitionQueue,
        message_stream: M,
        state_sink: StateSink<B, M::KeyType, M::ValueType>,
        engine_context: Arc<EngineContext>,
        store_name: String,
        partition: i32,
    ) -> Self {
        Self {
            changelog_stream: Some(QueueSerialiser::new(changelog_stream)),
            message_stream,
            state_sink,
            engine_context,
            store_name,
            partition,
            changelog_start: None,
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
            store_name,
            partition,
            changelog_start,
            ..
        } = self.project();

        if let StreamState::Sleeping = commit_state {
            *commit_state = Default::default();
        }

        let mut changelog_stream_proj = changelog_stream.as_pin_mut();

        // If we have transitioned to a committing state, we can start our
        // sink commit process. Otherwise, we can continue to poll the stream.
        if let StreamState::Committing = commit_state {
            tracing::debug!("Committing sink...");
            ready!(state_sink.as_mut().poll_commit(cx)).expect("Failed to close");
            *commit_state = StreamState::Committed;
        }

        if store_state.is_ready() && changelog_stream_proj.is_some() {
            let s = changelog_stream_proj.take();
        }

        if let Some(mut stream) = changelog_stream_proj {
            if let StreamState::Committed = commit_state {
                *commit_state = Default::default();
            }

            if changelog_start.is_none() {
                let checkpoint = state_sink
                    .get_checkpoint()
                    .expect("Failed to get checkpoint")
                    .unwrap_or(0);

                tracing::debug!("Rebuilding state store from checkpoint: {}", checkpoint);

                let watermarks = engine_context.watermark_for_changelog(store_name, *partition);

                tracing::debug!("State store changelog watermarks: {}", watermarks);

                let mut beginning_offset = 0;

                if checkpoint > watermarks.high() {
                    tracing::debug!(
                        "State store checkpoint higher than changelog high watermark..."
                    );
                    beginning_offset = watermarks.low();

                    // reset state store
                } else if checkpoint < watermarks.high() && checkpoint < watermarks.low() {
                    tracing::debug!("State store checkpoint lower than low watermark...");
                    beginning_offset = watermarks.low();

                    // reset state store
                } else {
                    // If the check point is seekable, or the checkpoint is equal with the high watermark
                    // Just use the checkpoint as the next offset.
                    tracing::debug!("Resuming changelog from checkpoint...");
                    beginning_offset = checkpoint;
                }

                tracing::debug!(
                    "Seeking changelog consumer for {}-{} to offset: {}",
                    store_name,
                    partition,
                    beginning_offset
                );

                engine_context
                    .seek_changlog_consumer(store_name, *partition, beginning_offset)
                    .expect("Failed to seek changelog consumer.");

                tracing::debug!(
                    "Reading changelog from current store position: {}",
                    beginning_offset
                );

                let _ = changelog_start.replace(beginning_offset);
            }

            for _ in 0..1024 {
                match stream.as_mut().poll_next(cx) {
                    Poll::Ready(None) => {
                        panic!("Changelog stream closed before state store rebuilt.")
                    }
                    Poll::Ready(Some(message)) => {
                        tracing::debug!("Sending changelog record to sink.");

                        state_sink
                            .as_mut()
                            .start_send(message)
                            .expect("Failed to send message to sink.")
                    }
                    Poll::Pending => {
                        tracing::debug!("No records for changelog.");

                        let consumer_position =
                            engine_context.get_changelog_consumer_position(store_name, *partition);

                        tracing::debug!("Checkpointing consumer position offset: {} for store: {}, partition: {}", consumer_position, store_name, partition);
                        state_sink.checkpoint_changelog_position(consumer_position);

                        match state_sink.as_mut().poll_commit(cx) {
                            Poll::Pending => {
                                tracing::debug!("State sink pending commit for checkpoint");

                                *commit_state = StreamState::Committing;
                                return Poll::Pending;
                            }
                            Poll::Ready(result) => {
                                result.expect("Sink failed to commit.");

                                let watermarks =
                                    engine_context.watermark_for_changelog(store_name, *partition);

                                let checkpoint = state_sink
                                    .get_checkpoint()
                                    .expect("Failed to get checkpoint.")
                                    .expect("State sink not checkpointed after commit.");

                                match checkpoint.cmp(&std::cmp::max(watermarks.high(), 0)) {
                                    Ordering::Equal => {
                                        tracing::debug!(
                                            "checkpoint: {} caught up to high watermark: {}",
                                            checkpoint,
                                            watermarks.high()
                                        );

                                        break;
                                    }
                                    Ordering::Greater => {
                                        panic!("Checkpoint greater than watermark after state store rebuild, checkpoint: {}, watermark: {}", checkpoint, watermarks.high())
                                    }
                                    Ordering::Less => {
                                        tracing::debug!(
                                            "checkpoint: {} still behind high watermark: {}",
                                            checkpoint,
                                            watermarks.high()
                                        );

                                        return Poll::Pending;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            tracing::debug!(
                "Setting state store status to StoreState::Ready for store: {}, partition: {}",
                store_name,
                partition
            );
            *store_state = StoreState::Ready;

            tracing::debug!(
                "Closing changelog stream for for store: {}, partition: {}",
                store_name,
                partition
            );
            engine_context
                .close_changelog_stream(store_name, *partition)
                .expect("Failed to close changelog stream.");
        }

        tracing::debug!("ForkedSinking messages from stream to sink...");

        // Poll the stream for the next message. If self.commit_state is
        // Committed, we know that we have committed our producer transaction,
        // and we will begin polling upstream nodes until they yield Poll::Ready
        // at which point we know that they have either committed or aborted.
        match message_stream.as_mut().poll_next(cx) {
            Poll::Ready(None) => {
                tracing::debug!("No Messages left for stream, finishing...");
                ready!(state_sink.as_mut().poll_close(cx)).expect("Failed to close");
                Poll::Ready(None)
            }
            Poll::Pending => {
                tracing::debug!("No messages available, waiting...");

                // We have recieved no messages from upstream, they will have
                // transitioned to a commit state.
                // We can transition to a commit state if we haven't already.
                match commit_state {
                    StreamState::Uncommitted => {
                        *commit_state = StreamState::Committing;
                        cx.waker().wake_by_ref();
                    }
                    StreamState::Committed => {
                        // Yield control so that when we wake dependents we will be
                        // behind any tasks that are pending wake.
                        cx.waker().wake_by_ref();
                    }
                    StreamState::Sleeping => {
                        // Before we sleep, wake any task that read the state lag
                        // before being updated for the final time, but got fenced
                        // from registering a waker while this task was waking
                        // dependants. Such tasks will now be ahead in the scheduler
                        // queue.
                        state_sink.wake_dependants();
                    }
                    _ => (),
                }

                Poll::Pending
            }
            Poll::Ready(Some(message)) => {
                // If we were waiting for upstream nodes to commit, we can transition
                // to our default state.
                if let StreamState::Committed = commit_state {
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

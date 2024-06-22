use std::{
    cmp::Ordering,
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use crossbeam::atomic::AtomicCell;
use futures::{ready, Future, FutureExt};
use pin_project_lite::pin_project;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::time::Sleep;
use tracing::info;

use crate::{
    engine::{
        changelog_manager::ChangelogManager,
        client_manager::{self, ClientManager},
        context::EngineContext,
        queue_manager::partition_queue::StreamPeridotPartitionQueue,
        wrapper::serde::native::NativeBytes,
    },
    message::{
        stream::MessageStreamPoll,
        types::{Message, TryFromOwnedMessage},
        BATCH_SIZE,
    },
    state::{self, backend::StateBackend},
};

use super::{
    sink::{
        state_sink::{StateSink, StateSinkError},
        MessageSink,
    },
    stream::{head::QueueHead, MessageStream},
    StreamState,
};

type DeserialiserQueue<K, V> = QueueHead<NativeBytes<K>, NativeBytes<V>>;
pub type StoreStateCell = AtomicCell<StoreState>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StoreState {
    #[default]
    Uninitialised,
    Rebuilding,
    Ready,
    Sleeping,
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
        store_state: Arc<StoreStateCell>,
        lso_sleep: Option<Pin<Box<Sleep>>>,
        consumer_next_offset: i64,
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
        store_state: Arc<StoreStateCell>,
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
            store_state,
            commit_state: Default::default(),
            lso_sleep: None,
            consumer_next_offset: 0,
            _state_backend: Default::default(),
        }
    }

    pub fn new_with_changelog(
        changelog_stream: StreamPeridotPartitionQueue,
        message_stream: M,
        state_sink: StateSink<B, M::KeyType, M::ValueType>,
        engine_context: Arc<EngineContext>,
        store_state: Arc<StoreStateCell>,
        store_name: String,
        partition: i32,
    ) -> Self {
        Self {
            changelog_stream: Some(QueueHead::new(changelog_stream)),
            message_stream,
            state_sink,
            engine_context,
            store_name,
            partition,
            changelog_start: None,
            store_state,
            commit_state: Default::default(),
            lso_sleep: None,
            consumer_next_offset: 0,
            _state_backend: Default::default(),
        }
    }
}

impl<B, M> StateSinkFork<B, M>
where
    M: MessageStream,
    M::KeyType: Serialize + Send + Sync + Clone + DeserializeOwned + 'static,
    M::ValueType: Serialize + DeserializeOwned + Send + Sync + Clone + DeserializeOwned + 'static,
    B: StateBackend + Send + Sync + 'static,
{
    fn set_changelog_start(
        state_sink: &mut Pin<&mut StateSink<B, M::KeyType, M::ValueType>>,
        engine_context: &mut Arc<EngineContext>,
        store_name: &mut String,
        partition: &mut i32,
        changelog_start: &mut Option<i64>,
        lso_sleep: &mut Option<Pin<Box<Sleep>>>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ()>> {
        let checkpoint = state_sink
            .as_mut()
            .get_checkpoint()
            .expect("Failed to get checkpoint")
            .unwrap_or(0);

        tracing::debug!("Rebuilding state store from checkpoint: {}", checkpoint);

        let watermarks = engine_context.watermark_for_changelog(store_name, *partition);

        let lso = match engine_context.get_changelog_lowest_stable_offset(store_name, *partition) {
            None => {
                info!("LSO not store, waiting...");

                let mut sleep = Box::pin(tokio::time::sleep(Duration::from_millis(100)));

                sleep.poll_unpin(cx);

                let _ = lso_sleep.replace(sleep);

                return Poll::Pending;
            }
            Some(lso) => lso,
        };

        tracing::debug!("State store changelog watermarks: {}", watermarks);

        let mut beginning_offset = 0;

        if checkpoint > lso {
            //watermarks.high() {
            tracing::debug!(
                "State store checkpoint higher than lso, checkpoint: {} lso: {}",
                checkpoint,
                lso
            );
            beginning_offset = watermarks.low();

            // reset state store
            // } else if checkpoint < watermarks.high() && checkpoint < watermarks.low() {
        } else if checkpoint < lso && checkpoint < watermarks.low() {
            tracing::debug!(
                "State store checkpoint lower than low watermark, checkpoint: {} lso: {}",
                checkpoint,
                lso
            );
            beginning_offset = watermarks.low();

            // reset state store
        } else {
            // If the check point is seekable, or the checkpoint is equal with the high watermark
            // Just use the checkpoint as the next offset.
            tracing::debug!(
                "Resuming changelog from checkpoint, checkpoint: {} lso: {}",
                checkpoint,
                lso
            );
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

        Poll::Ready(Ok(()))
    }

    fn commit(
        state_sink: &mut Pin<&mut StateSink<B, M::KeyType, M::ValueType>>,
        commit_state: &mut StreamState,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StateSinkError>> {
        ready!(state_sink.as_mut().poll_commit(cx))?;

        *commit_state = StreamState::Committed;

        Poll::Ready(Ok(()))
    }

    fn poll_changelog(
        changelog_stream: &mut Pin<
            &mut QueueHead<NativeBytes<M::KeyType>, NativeBytes<M::ValueType>>,
        >,
        state_sink: &mut Pin<&mut StateSink<B, M::KeyType, M::ValueType>>,
        engine_context: &mut Arc<EngineContext>,
        store_name: &mut String,
        partition: &mut i32,
        changelog_start: &mut Option<i64>,
        lso_sleep: &mut Option<Pin<Box<Sleep>>>,
        commit_state: &mut StreamState,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StateSinkError>> {
        match changelog_stream.as_mut().poll_next(cx) {
            Poll::Ready(MessageStreamPoll::Commit(i)) => {
                let consumer_position =
                    engine_context.get_changelog_consumer_position(store_name, *partition);

                state_sink.as_mut().set_consumer_position(consumer_position);

                tracing::debug!(
                    "Checkpointing consumer position offset: {} for store: {}, partition: {}",
                    consumer_position,
                    store_name,
                    partition
                );

                *commit_state = StreamState::Committing;

                Self::commit(state_sink, commit_state, cx)
            }
            Poll::Ready(MessageStreamPoll::Closed) => {
                panic!("Changelog stream closed before state store rebuilt.")
            }
            Poll::Ready(MessageStreamPoll::Message(message)) => {
                tracing::debug!("Sending changelog record to sink.");

                let _ = state_sink
                    .as_mut()
                    .start_send(message)
                    .expect("Failed to send message to sink.");

                Poll::Ready(Ok(()))
            }
            Poll::Pending => {
                panic!("No records for changelog.");
            }
        }
    }

    /// Only called after commit
    fn poll_state_rebuilt(
        state_sink: &mut Pin<&mut StateSink<B, M::KeyType, M::ValueType>>,
        engine_context: &mut Arc<EngineContext>,
        store_name: &mut str,
        partition: &mut i32,
        lso_sleep: &mut Option<Pin<Box<Sleep>>>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<bool, ()>> {
        // TODO: ALO and AMO semantics only
        //let watermarks =
        //    engine_context.watermark_for_changelog(store_name, *partition);

        let checkpoint = state_sink
            .get_checkpoint()
            .expect("Failed to get checkpoint.")
            .expect("State sink not checkpointed after commit.");

        let lso = match engine_context.get_changelog_lowest_stable_offset(store_name, *partition) {
            None => {
                info!("LSO not store, waiting...");

                let mut sleep = Box::pin(tokio::time::sleep(Duration::from_millis(100)));

                sleep.poll_unpin(cx);

                let _ = lso_sleep.replace(sleep);

                return Poll::Pending;
            }
            Some(lso) => lso,
        };

        match checkpoint.cmp(&std::cmp::max(lso, 0)) {
            Ordering::Equal => {
                tracing::debug!(
                    "checkpoint: {} caught up to changelog lso: {}",
                    checkpoint,
                    lso
                );

                Poll::Ready(Ok(true))
            }
            Ordering::Greater => {
                panic!("Checkpoint greater than lso after state store rebuild, checkpoint: {}, lso: {}", checkpoint, lso)
            }
            Ordering::Less => {
                tracing::debug!("checkpoint: {} still behind lso: {}", checkpoint, lso);

                Poll::Ready(Ok(false))
            }
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
    ) -> Poll<MessageStreamPoll<Self::KeyType, Self::ValueType>> {
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
            lso_sleep,
            consumer_next_offset,
            ..
        } = self.project();

        if let Some(mut w) = lso_sleep.take() {
            match w.as_mut().poll(cx) {
                Poll::Pending => {
                    let _ = lso_sleep.replace(w);
                    return Poll::Pending;
                }
                Poll::Ready(_) => (),
            }
        }

        let mut changelog_stream_proj = changelog_stream.as_pin_mut();

        while let Some(ref mut stream) = changelog_stream_proj {
            match *commit_state {
                StreamState::Committing => {
                    ready!(Self::commit(&mut state_sink, commit_state, cx))
                        .expect("Failed to commit");

                    if ready!(Self::poll_state_rebuilt(
                        &mut state_sink,
                        engine_context,
                        store_name,
                        partition,
                        lso_sleep,
                        cx,
                    ))
                    .expect("Failed to poll state store readiness")
                    {
                        tracing::debug!(
                            "Setting state store status to StoreState::Ready for store: {}, partition: {}",
                            store_name,
                            partition
                        );

                        store_state.store(StoreState::Ready);

                        tracing::debug!(
                            "Closing changelog stream for for store: {}, partition: {}",
                            store_name,
                            partition
                        );

                        engine_context
                            .close_changelog_stream(store_name, *partition)
                            .expect("Failed to close changelog stream.");

                        let _ = changelog_stream_proj.take();
                    }

                    *commit_state = StreamState::Committed;
                }
                StreamState::Closing => panic!("stream state closed"),
                StreamState::Committed | StreamState::Sleeping | StreamState::Uncommitted => {
                    if changelog_start.is_none() {
                        ready!(Self::set_changelog_start(
                            &mut state_sink,
                            engine_context,
                            store_name,
                            partition,
                            changelog_start,
                            lso_sleep,
                            cx,
                        ))
                        .expect("Failed to set changelog start.");
                    };

                    Self::poll_changelog(
                        stream,
                        &mut state_sink,
                        engine_context,
                        store_name,
                        partition,
                        changelog_start,
                        lso_sleep,
                        commit_state,
                        cx,
                    );
                }
            }
        }

        tracing::debug!("ForkedSinking messages from stream to sink...");

        for _ in 0..BATCH_SIZE {
            match *commit_state {
                StreamState::Committing => {
                    tracing::debug!("Committing sink...");
                    ready!(state_sink.as_mut().poll_commit(cx)).expect("Failed to close");
                    *commit_state = StreamState::Committed;

                    return Poll::Ready(MessageStreamPoll::Commit(Ok(0)));
                }
                StreamState::Closing => {
                    ready!(state_sink.as_mut().poll_close(cx)).expect("Failed to close");

                    return Poll::Ready(MessageStreamPoll::Closed);
                }
                StreamState::Committed | StreamState::Sleeping | StreamState::Uncommitted =>
                // Poll the stream for the next message. If self.commit_state is
                // Committed, we know that we have committed our producer transaction,
                // and we will begin polling upstream nodes until they yield Poll::Ready
                // at which point we know that they have either committed or aborted.
                {
                    match message_stream.as_mut().poll_next(cx) {
                        Poll::Ready(MessageStreamPoll::Closed) => {
                            *commit_state = StreamState::Closing;

                            return Poll::Ready(MessageStreamPoll::Closed);
                        }
                        Poll::Pending => {
                            tracing::debug!("No messages available, waiting...");

                            store_state.store(StoreState::Sleeping);
                            tracing::debug!("Entering sleep, waking all dependents...");
                            // Before we sleep, wake any task that read the state lag
                            // before being updated for the final time, but got fenced
                            // from registering a waker while this task was waking
                            // dependants. Such tasks will now be ahead in the scheduler
                            // queue.
                            state_sink.wake_all();

                            return Poll::Pending;
                        }
                        Poll::Ready(MessageStreamPoll::Message(message)) => {
                            *commit_state = StreamState::Uncommitted;
                            store_state.store(StoreState::Ready);

                            let message = state_sink
                                .as_mut()
                                .start_send(message)
                                .expect("Failed to send message to sink.");

                            return Poll::Ready(MessageStreamPoll::Message(message));
                        }
                        Poll::Ready(MessageStreamPoll::Commit(Err(e))) => {
                            return Poll::Ready(MessageStreamPoll::Commit(Err(e)))
                        }
                        Poll::Ready(MessageStreamPoll::Commit(Ok(next_offset))) => {
                            *commit_state = StreamState::Committing;
                            *consumer_next_offset = next_offset;
                        }
                    }
                }
            }
        }

        Poll::Pending
    }
}

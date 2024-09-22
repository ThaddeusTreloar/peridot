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
use tracing::{debug, info, Level};

use crate::{
    engine::{
        changelog_manager::ChangelogManager,
        consumer_manager::{self, ConsumerManager},
        context::EngineContext,
        queue_manager::partition_queue::{PeridotPartitionQueue, StreamPeridotPartitionQueue},
        wrapper::serde::native::NativeBytes,
    },
    message::{
        stream::MessageStreamPoll,
        types::{Message, TryFromOwnedMessage},
        BATCH_SIZE,
    },
    state::{self, store::StateStore},
};

use super::{
    sink::{
        state_sink::{StateSink, StateSinkError},
        MessageSink,
    },
    stream::{head::QueueHead, MessageCommitError, MessageStream},
    StreamState,
};

type DeserialiserQueue<K, V> = QueueHead<NativeBytes<K>, NativeBytes<V>>;
pub type StoreStateCell = AtomicCell<StoreState>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, derive_more::Display)]
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
        B: StateStore
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
        changelog_next_offset: i64,
        _state_backend: PhantomData<B>,
    }
}

impl<B, M> StateSinkFork<B, M>
where
    M: MessageStream,
    B: StateStore,
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
            changelog_next_offset: 0,
            _state_backend: Default::default(),
        }
    }

    pub fn new_with_changelog(
        changelog_stream: PeridotPartitionQueue,
        message_stream: M,
        state_sink: StateSink<B, M::KeyType, M::ValueType>,
        engine_context: Arc<EngineContext>,
        store_state: Arc<StoreStateCell>,
        store_name: &str,
        source_topic: &str,
        partition: i32,
    ) -> Self {
        Self {
            changelog_stream: Some(
                QueueHead::new_changelog(
                    changelog_stream, 
                    engine_context.clone(),
                    store_name,
                    source_topic,
                    partition
                )
            ),
            message_stream,
            state_sink,
            engine_context,
            store_name: store_name.to_owned(),
            partition,
            changelog_start: None,
            store_state,
            commit_state: Default::default(),
            lso_sleep: None,
            consumer_next_offset: 0,
            changelog_next_offset: 0,
            _state_backend: Default::default(),
        }
    }
}

impl<B, M> StateSinkFork<B, M>
where
    M: MessageStream,
    M::KeyType: Serialize + Send + Sync + Clone + DeserializeOwned + 'static,
    M::ValueType: Serialize + DeserializeOwned + Send + Sync + Clone + DeserializeOwned + 'static,
    B: StateStore + Send + Sync + 'static,
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

        let lso = match engine_context.get_changelog_next_offset(store_name, *partition) {
            None => {
                info!("LSO not stored, waiting...");

                let mut sleep = Box::pin(tokio::time::sleep(Duration::from_millis(100)));

                sleep.poll_unpin(cx);

                let _ = lso_sleep.replace(sleep);

                return Poll::Pending;
            }
            Some(lso) => lso,
        };

        tracing::info!("State store changelog watermarks: {}", watermarks);

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
        next_offset: i64,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), MessageCommitError>> {
        tracing::debug!(
            "Committing state fork for sink: {}, at offset: {}",
            std::any::type_name::<StateSink<B, M::KeyType, M::ValueType>>(),
            next_offset,
        );

        ready!(state_sink.as_mut().poll_commit(next_offset, cx))
            .expect("Failed to commit state store.");

        tracing::debug!(
            "Commit completed for state fork, at offset: {}",
            next_offset,
        );

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
        changelog_next_offset: &mut i64,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), MessageCommitError>> {
        match changelog_stream.as_mut().poll_next(cx) {
            Poll::Ready(MessageStreamPoll::Commit(next_offset)) => {
                let position = next_offset?;

                *changelog_next_offset = position;

                tracing::debug!(
                    "Checkpointing consumer position next_offset: {} for store: {}, partition: {}",
                    position,
                    store_name,
                    partition
                );

                *commit_state = StreamState::Committing;

                Self::commit(state_sink, commit_state, position, cx)
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
            Poll::Pending => Poll::Pending,
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

        let _consumer_position =
            engine_context.get_changelog_consumer_position(store_name, *partition);

        info!("Consumer position: {}", _consumer_position);

        let checkpoint = state_sink
            .get_checkpoint()
            .expect("Failed to get checkpoint.")
            .expect("State sink not checkpointed after commit.");

        let lso = match engine_context.get_changelog_lso(store_name, *partition) {
            None => {
                info!("LSO not store, waiting...");

                let mut sleep = Box::pin(tokio::time::sleep(Duration::from_millis(100)));

                sleep.poll_unpin(cx);

                let _ = lso_sleep.replace(sleep);

                return Poll::Pending;
            }
            Some(lso) => lso,
        };

        let changelog_next_offset =
            match engine_context.get_changelog_next_offset(store_name, *partition) {
                None => {
                    info!("LSO not store, waiting...");

                    let mut sleep = Box::pin(tokio::time::sleep(Duration::from_millis(100)));

                    sleep.poll_unpin(cx);

                    let _ = lso_sleep.replace(sleep);

                    return Poll::Pending;
                }
                Some(cno) => cno,
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
                tracing::debug!(
                    "checkpoint: {} still behind lso: {}, changelog_next_offset: {}",
                    checkpoint,
                    lso,
                    changelog_next_offset
                );

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
    B: StateStore + Send + Sync + 'static,
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
            changelog_next_offset,
            ..
        } = self.project();

        let span = tracing::span!(Level::DEBUG, "->StateSinkFork::poll",).entered();

        if let Some(mut w) = lso_sleep.take() {
            match w.as_mut().poll(cx) {
                Poll::Pending => {
                    let _ = lso_sleep.replace(w);
                    return Poll::Pending;
                }
                Poll::Ready(_) => (),
            }
        }

        match store_state.load() {
            StoreState::Rebuilding | StoreState::Uninitialised => {

                let mut changelog_stream_proj = changelog_stream.as_pin_mut();

                while let Some(ref mut stream) = changelog_stream_proj {
                    match *commit_state {
                        StreamState::Committing => {
                            ready!(Self::commit(
                                &mut state_sink,
                                commit_state,
                                *changelog_next_offset,
                                cx
                            ))
                            .expect("Failed to commit");

                            *commit_state = StreamState::Committed;
                        }
                        StreamState::Closing => panic!("stream state closed"),
                        StreamState::Committed => {
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

                            *commit_state = StreamState::Uncommitted;
                        }
                        StreamState::Sleeping | StreamState::Uncommitted => {
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

                            if Self::poll_changelog(
                                stream,
                                &mut state_sink,
                                engine_context,
                                store_name,
                                partition,
                                changelog_start,
                                lso_sleep,
                                commit_state,
                                changelog_next_offset,
                                cx,
                            )
                            .is_pending()
                            {
                                
                                /**commit_state = StreamState::Committing;

                                let mut sleep =
                                    Box::pin(tokio::time::sleep(Duration::from_millis(250)));

                                sleep.poll_unpin(cx);

                                let _ = lso_sleep.replace(sleep);*/

                                debug!("Waiting for changelog messages...");

                                return Poll::Pending;
                            }
                        }
                    }
                }
            }
            _ => {
                tracing::debug!("Taking changelog stream.");
    
                let _ = changelog_stream.take();
            },
        }

        tracing::debug!("Resuming state fork processing...");

        for _ in 0..BATCH_SIZE {
            match *commit_state {
                StreamState::Committing => {
                    match ready!(Self::commit(
                        &mut state_sink,
                        commit_state,
                        *consumer_next_offset,
                        cx
                    )) {
                        Ok(_) => {
                            return Poll::Ready(MessageStreamPoll::Commit(Ok(
                                *consumer_next_offset,
                            )))
                        }
                        Err(e) => return Poll::Ready(MessageStreamPoll::Commit(Err(e))),
                    }
                }
                StreamState::Closing => {
                    // TODO: Migrate to state function
                    ready!(state_sink.as_mut().poll_close(cx)).expect("Failed to close");

                    return Poll::Ready(MessageStreamPoll::Closed);
                }
                StreamState::Committed | StreamState::Sleeping | StreamState::Uncommitted => {
                    // TODO: Movie this to static function.
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
                            tracing::info!("Got message in state fork offset: {}", message.offset());

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

        cx.waker().wake_by_ref();

        Poll::Pending
    }
}

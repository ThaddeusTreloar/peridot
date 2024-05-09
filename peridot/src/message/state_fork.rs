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
    sink::{state_sink::StateSink, MessageSink},
    stream::{serialiser::QueueSerialiser, MessageStream},
    StreamState,
};

type DeserialiserQueue<K, V> = QueueSerialiser<NativeBytes<K>, NativeBytes<V>>;
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
        store_name: String,
        partition: i32,
        stream_state: StreamState,
        store_state: Arc<StoreStateCell>,
        wait: Option<Pin<Box<Sleep>>>,
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
            store_state,
            stream_state: Default::default(),
            wait: None,
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
            changelog_stream: Some(QueueSerialiser::new(changelog_stream)),
            message_stream,
            state_sink,
            engine_context,
            store_name,
            partition,
            store_state,
            stream_state: Default::default(),
            wait: None,
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
    ) -> Poll<MessageStreamPoll<Self::KeyType, Self::ValueType>> {
        let StateSinkForkProjection {
            mut changelog_stream,
            mut message_stream,
            mut state_sink,
            stream_state,
            store_state,
            engine_context,
            store_name,
            partition,
            wait,
            ..
        } = self.project();

        if wait.is_some() {
            let mut w = wait.take().unwrap();

            match w.as_mut().poll(cx) {
                Poll::Pending => {
                    let _ = wait.replace(w);
                    return Poll::Pending;
                }
                Poll::Ready(_) => (),
            }
        }

        let mut changelog_stream_proj = changelog_stream.as_pin_mut();

        for _ in 0..BATCH_SIZE {
            match (store_state.load(), stream_state) {
                (StoreState::Uninitialised, _) => {
                    let checkpoint = state_sink
                        .get_checkpoint()
                        .expect("Failed to get checkpoint")
                        .unwrap_or(0);
    
                    tracing::debug!("Rebuilding state store from checkpoint: {}", checkpoint);
    
                    let watermarks = engine_context.watermark_for_changelog(store_name, *partition);
    
                    let lso = match engine_context.get_changelog_lso(store_name, *partition) {
                        None => {
                            info!("LSO not store, waiting...");
    
                            let mut sleep = Box::pin(tokio::time::sleep(Duration::from_millis(100)));
    
                            sleep.poll_unpin(cx);
    
                            let _ = wait.replace(sleep);
    
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
    
                    store_state.store(StoreState::Rebuilding);
                }
                (_, StreamState::Committing | StreamState::CommittingClosing) => {
                    tracing::debug!("Committing sink...");
                    ready!(state_sink.as_mut().poll_commit(cx)).expect("Failed to close");

                    match stream_state {
                        StreamState::Committing => {
                            *stream_state = StreamState::Committed;
                        }
                        StreamState::CommittingClosing => {
                            *stream_state = StreamState::Closing;
                        }
                        _ => ()
                    }
                }
                (_, StreamState::Closing) => {

                }
                (StoreState::Rebuilding, StreamState::Uncommitted | StreamState::Committed | StreamState::Sleeping) => {
                    if let Some(mut stream) = changelog_stream_proj {
                        match stream.as_mut().poll_next(cx) {
                            Poll::Ready(MessageStreamPoll::Closed) => {
                                todo!("Changelog stream closed before state store rebuilt.")
                            }
                            Poll::Ready(MessageStreamPoll::Commit(val)) => {
                                match stream_state {
                                    StreamState::Uncommitted => {
                                        *stream_state = StreamState::Committing;
                                        cx.waker().wake_by_ref();
                                    }
                                    StreamState::Committed => {
                                        tracing::debug!("Transitioning to sleeping...");
                                        *stream_state = StreamState::Sleeping;
                
                                        // Yield control so that when we wake dependents we will be
                                        // behind any tasks that are pending wake.
                                        cx.waker().wake_by_ref();
                                    }
                                    StreamState::Sleeping => {
                                        todo!("Sleeping while rebuilding")
                                    }
                                    _ => (),
                                }
                                todo!("Changelog stream closed before state store rebuilt.")
                            }
                            Poll::Ready(MessageStreamPoll::Message(message)) => {
                                tracing::debug!("Sending changelog record to sink.");
        
                                let _ = state_sink
                                    .as_mut()
                                    .start_send(message)
                                    .expect("Failed to send message to sink.");
                            }
                            Poll::Pending => {
                                tracing::debug!("No records for changelog.");
        
                                let lso = match engine_context.get_changelog_lso(store_name, *partition) {
                                    None => {
                                        info!("LSO not stored, waiting...");
        
                                        let mut sleep =
                                            Box::pin(tokio::time::sleep(Duration::from_millis(100)));
        
                                        sleep.poll_unpin(cx);
        
                                        let _ = wait.replace(sleep);
        
                                        return Poll::Pending;
                                    }

                                    Some(lso) => lso,
                                };
        
                                let consumer_position =
                                    engine_context.get_changelog_consumer_position(store_name, *partition);
        
                                tracing::debug!("Checkpointing consumer position offset: {} for store: {}, partition: {}", consumer_position, store_name, partition);
        
                                state_sink.as_mut().set_consumer_position(consumer_position);
        
                                match state_sink.as_mut().poll_commit(cx) {
                                    Poll::Pending => {
                                        // Handle potential sleeping state.
                                        tracing::debug!("State sink pending commit for checkpoint");
        
                                        *stream_state = StreamState::Committing;
                                        return Poll::Pending;
                                    }
                                    Poll::Ready(result) => {
                                        result.expect("Sink failed to commit.");
        
                                        // TODO: ALO and AMO semantics only
                                        //let watermarks =
                                        //    engine_context.watermark_for_changelog(store_name, *partition);
        
                                        let lso = engine_context
                                            .get_changelog_lso(store_name, *partition)
                                            .unwrap();
        
                                        let checkpoint = state_sink
                                            .get_checkpoint()
                                            .expect("Failed to get checkpoint.")
                                            .expect("State sink not checkpointed after commit.");
        
                                        match checkpoint.cmp(&std::cmp::max(lso, 0)) {
                                            Ordering::Equal => {
                                                tracing::debug!(
                                                    "checkpoint: {} caught up to changelog lso: {}",
                                                    checkpoint,
                                                    lso
                                                );
        
                                                break;
                                            }
                                            Ordering::Greater => {
                                                panic!("Checkpoint greater than lso after state store rebuild, checkpoint: {}, lso: {}", checkpoint, lso)
                                            }
                                            Ordering::Less => {
                                                tracing::debug!(
                                                    "checkpoint: {} still behind lso: {}",
                                                    checkpoint,
                                                    lso
                                                );
        
                                                return Poll::Pending;
                                            }
                                        }
                                    }
                                }
                            }
                        }


                    let _ = changelog_stream_proj.take();
                }
                (StoreState::Ready | StoreState::Sleeping, StreamState::Uncommitted | StreamState::Committed | StreamState::Sleeping) => {

                }
            }
        }


        

        

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
        }

        tracing::debug!("ForkedSinking messages from stream to sink...");

        // Poll the stream for the next message. If self.commit_state is
        // Committed, we know that we have committed our producer transaction,
        // and we will begin polling upstream nodes until they yield Poll::Ready
        // at which point we know that they have either committed or aborted.
        match message_stream.as_mut().poll_next(cx) {
            Poll::Ready(MessageStreamPoll::Closed) => {
                tracing::debug!("No Messages left for stream, finishing...");
                ready!(state_sink.as_mut().poll_close(cx)).expect("Failed to close");
                Poll::Ready(None)
            }
            Poll::Pending => {
                tracing::debug!("No messages available, waiting...");

                // We have recieved no messages from upstream, they will have
                // transitioned to a commit state.
                // We can transition to a commit state if we haven't already.
                match stream_state {
                    StreamState::Uncommitted => {
                        *stream_state = StreamState::Committing;
                        cx.waker().wake_by_ref();
                    }
                    StreamState::Committed => {
                        tracing::debug!("Transitioning to sleeping...");
                        *stream_state = StreamState::Sleeping;

                        // Yield control so that when we wake dependents we will be
                        // behind any tasks that are pending wake.
                        cx.waker().wake_by_ref();
                    }
                    StreamState::Sleeping => {
                        store_state.store(StoreState::Sleeping);
                        tracing::debug!("Entering sleep, waking all dependents...");
                        // Before we sleep, wake any task that read the state lag
                        // before being updated for the final time, but got fenced
                        // from registering a waker while this task was waking
                        // dependants. Such tasks will now be ahead in the scheduler
                        // queue.
                        state_sink.wake_all();
                    }
                    _ => (),
                }

                Poll::Pending
            }
            Poll::Ready(MessageStreamPoll::Message(message)) => {
                // If we were waiting for upstream nodes to commit, we can transition
                // to our default state.
                if let StreamState::Committed | StreamState::Sleeping = stream_state {
                    *stream_state = Default::default();
                    store_state.store(StoreState::Ready)
                }

                let message = state_sink
                    .as_mut()
                    .start_send(message)
                    .expect("Failed to send message to sink.");

                Poll::Ready(Some(message))
            }
            Poll::Ready(MessageStreamPoll::Commit(Ok(()))) => {
                if let StreamState::Uncommitted = stream_state {
                    *stream_state = StreamState::Committing;
                    cx.waker().wake_by_ref();
                }

                Poll::Ready(Some(message))
            }
        }
    }
}

use std::{
    pin::Pin,
    sync::{atomic::AtomicI64, Arc},
    task::{ready, Context, Poll},
    time::Duration,
};

use futures::Future;
use pin_project_lite::pin_project;
use rdkafka::error::KafkaError;
use serde::{de::DeserializeOwned, Serialize};
use tracing::{debug, info};

use crate::{
    app::PeridotConsumer,
    engine::{
        queue_manager::queue_metadata::QueueMetadata,
        wrapper::serde::{json::Json, native::NativeBytes, PeridotDeserializer, PeridotSerializer},
    },
    message::{
        sink::{topic_sink::CHANGELOG_OFFSET_HEADER, MessageSink, NonCommittingSink},
        types::{Message, TryFromOwnedMessage},
    },
    state::backend::{
        facade::StateFacade,
        view::{ReadableStateView, WriteableStateView},
        StateBackend,
    },
};

type PendingCommit<E> = Option<Pin<Box<dyn Future<Output = Result<(), E>> + Send>>>;
type PendingOffsetCommit<E> = Option<Pin<Box<dyn Future<Output = Result<(), E>> + Send>>>;

pin_project! {
    pub struct StateSink<B, K, V>
    where
        B: StateBackend,
    {
        queue_metadata: QueueMetadata,
        state_facade: Arc<StateFacade<K, V, B>>,
        buffer: Vec<Message<K, V>>,
        _key_type: std::marker::PhantomData<K>,
        _value_type: std::marker::PhantomData<V>,
        pending_commit: PendingCommit<B::Error>,
        highest_offset: i64,
        highest_committed_offset: i64,
        consumer_position: Option<i64>,
    }
}

impl<B, K, V> StateSink<B, K, V>
where
    B: StateBackend + Send + Sync + 'static,
    K: Serialize + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(queue_metadata: QueueMetadata, state_facade: StateFacade<K, V, B>) -> Self {
        Self {
            queue_metadata,
            state_facade: Arc::new(state_facade),
            buffer: Default::default(),
            _key_type: Default::default(),
            _value_type: Default::default(),
            pending_commit: None,
            highest_offset: 0,
            highest_committed_offset: 0,
            consumer_position: None,
        }
    }

    pub fn get_checkpoint(&self) -> Result<Option<i64>, B::Error> {
        self.state_facade
            .get_checkpoint()
            .map(|r| r.map(|c| c.offset))
    }

    // Only for when the state is rebuilding.
    pub fn set_consumer_position(&mut self, offset: i64) {
        let _ = self.consumer_position.replace(offset);
    }

    pub fn wake_all(&self) {
        self.state_facade.wake_all();
    }

    pub fn wake(&self) {
        self.state_facade.wake();
    }
}

impl<B, K, V> StateSink<B, K, V>
where
    B: StateBackend + Send + Sync,
    K: Serialize + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
{
    pub fn checkpoint_changelog_position(&mut self, offset: i64) {
        self.highest_offset = std::cmp::max(self.highest_offset, offset);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StateSinkError {
    #[error("Failed to run: {0}")]
    EnqueueFailed(#[from] KafkaError),
}

impl<B, K, V> NonCommittingSink for StateSink<B, K, V>
where
    K: Clone,
    V: Clone,
    B: StateBackend + Send,
{
}

impl<B, K, V> MessageSink<K, V> for StateSink<B, K, V>
where
    B: StateBackend + Send + Sync + 'static,
    K: Clone + Serialize + Send + Sync + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Error = StateSinkError;

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_commit(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<i64, Self::Error>> {
        let this = self.project();

        tracing::debug!("Committing state sink.");

        if this.pending_commit.is_none() {
            if this.buffer.is_empty() {
                return Poll::Ready(Ok(*this.highest_committed_offset));
            }

            let range = this
                .buffer
                .drain(..)
                // Swap source offset for changelog offset. Otherwise our checkpoint
                // is derived from the source offset which may be different.
                .map(|mut message| {
                    if let Some(values) = message.headers().get(CHANGELOG_OFFSET_HEADER) {
                        let bytes = values.iter().next().unwrap();

                        let new_offset =
                            <i64 as PeridotDeserializer>::deserialize(bytes.as_slice())
                                .expect("Failed to parse changelog offset.");

                        message.override_offset(new_offset);
                    }

                    if let Some(offset) = this.consumer_position {
                        message.override_offset(*offset)
                    }

                    *this.highest_committed_offset =
                        std::cmp::max(*this.highest_committed_offset, message.offset());

                    message
                })
                .collect();

            let facade = this.state_facade.clone();

            let commit = Box::pin(facade.put_range(range));

            let _ = this.pending_commit.replace(commit);
        }

        if let Some(ref mut task) = this.pending_commit {
            ready!(task.as_mut().poll(cx)).expect("Failed to commit sink buffer.");

            tracing::debug!("Sink committed.");

            let _ = this.pending_commit.take();
            let _ = this.consumer_position.take();

            this.state_facade.wake();
        }

        Poll::Ready(Ok(*this.highest_committed_offset))
    }

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        message: Message<K, V>,
    ) -> Result<Message<K, V>, Self::Error> {
        let this = self.project();

        tracing::debug!(
            "Buffering state sink message: topic: {}, partition: {}, offset: {}",
            message.topic(),
            message.partition(),
            message.offset()
        );

        let offset = std::cmp::max(message.offset + 1, *this.highest_offset);

        *this.highest_offset = offset;

        this.buffer.push(message.clone());

        Ok(message)
    }
}

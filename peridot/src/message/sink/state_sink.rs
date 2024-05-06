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
        wrapper::serde::{json::Json, native::NativeBytes, PeridotSerializer},
    },
    message::{
        sink::{MessageSink, NonCommittingSink},
        types::{Message, TryFromOwnedMessage},
    },
    state::backend::{facade::StateFacade, ReadableStateView, StateBackend, WriteableStateView},
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
        }
    }

    pub fn get_checkpoint(&self) -> Result<Option<i64>, B::Error> {
        self.state_facade
            .get_checkpoint()
            .map(|r| r.map(|c| c.offset))
    }

    pub fn wake_dependants(&self) {
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
    K: Serialize + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Error = StateSinkError;

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_commit(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();

        tracing::debug!("Committing state sink.");

        if this.pending_commit.is_none() {
            if this.buffer.is_empty() {
                debug!("Nothing to commit...");

                if this
                    .state_facade
                    .get_checkpoint()
                    .expect("Error while checking checkpoint.")
                    .is_none()
                {
                    let offset = std::cmp::max(this.highest_offset, this.highest_committed_offset);

                    debug!(
                        "Checkpointing state store: {}-{} with offset: {}",
                        this.state_facade.store_name(),
                        this.state_facade.partition(),
                        offset
                    );

                    this.state_facade
                        .create_checkpoint(*offset)
                        .expect("Failed to store commit.");
                }

                return Poll::Ready(Ok(()));
            }

            let range: Vec<(K, V)> = this.buffer.drain(..).map(|m| (m.key, m.value)).collect();

            let facade = this.state_facade.clone();

            let commit = Box::pin(facade.put_range(range));

            let _ = this.pending_commit.replace(commit);
        }

        if let Some(ref mut task) = this.pending_commit {
            ready!(task.as_mut().poll(cx)).expect("Failed to commit sink buffer.");

            tracing::debug!("Sink committed.");

            let _ = this.pending_commit.take();
        }

        let offset = std::cmp::max(this.highest_offset, this.highest_committed_offset);

        tracing::debug!("Checkpointing state store.");

        this.state_facade
            .create_checkpoint(*offset)
            .expect("Failed to store commit.");

        // Check lag,
        //

        Poll::Ready(Ok(()))
    }

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, message: Message<K, V>) -> Result<(), Self::Error> {
        let this = self.project();

        let raw_key = Json::serialize(message.key()).unwrap();
        let ser_key = String::from_utf8_lossy(&raw_key);
        let raw_value = Json::serialize(message.value()).unwrap();
        let ser_value = String::from_utf8_lossy(&raw_value);

        tracing::debug!("Buffering state sink message: topic: {}, partition: {}, offset: {}, key: {}, value: {}", message.topic(), message.partition(), message.offset(), ser_key, ser_value);

        let offset = std::cmp::max(message.offset + 1, *this.highest_offset);

        *this.highest_offset = offset;

        this.buffer.push(message);

        Ok(())
    }
}

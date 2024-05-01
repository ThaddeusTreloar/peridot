use std::{
    pin::Pin,
    sync::{atomic::AtomicI64, Arc},
    task::{ready, Context, Poll}, time::Duration,
};

use futures::Future;
use pin_project_lite::pin_project;
use rdkafka::error::KafkaError;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    app::PeridotConsumer, engine::{queue_manager::queue_metadata::QueueMetadata, wrapper::serde::native::NativeBytes}, message::{
        sink::{MessageSink, NonCommittingSink},
        types::{Message, TryFromOwnedMessage},
    }, state::backend::{facade::StateFacade, StateBackend, WriteableStateView}
};

type PendingCommit<E> = Option<Pin<Box<dyn Future<Output=Result<(), E>> + Send>>>;
type PendingOffsetCommit<E> = Option<Pin<Box<dyn Future<Output=Result<(), E>> + Send>>>;

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
        pending_offset_commit: PendingOffsetCommit<B::Error>,
        highest_offset: i64,
        highest_committed_offset: i64,
    }
}

impl<B, K, V> StateSink<B, K, V>
where
    B: StateBackend,
{
    pub fn new(queue_metadata: QueueMetadata, state_facade: StateFacade<K, V, B>) -> Self {
        Self {
            queue_metadata,
            state_facade: Arc::new(state_facade),
            buffer: Default::default(),
            _key_type: Default::default(),
            _value_type: Default::default(),
            pending_commit: None,
            pending_offset_commit: None,
            highest_offset: Default::default(),
            highest_committed_offset: Default::default(),
        }
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
    V: Serialize + DeserializeOwned + Send + Sync + 'static
{
    type Error = StateSinkError;

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_commit(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();

        if let Some(commit) = this.pending_offset_commit {
            ready!(commit.as_mut().poll(cx))
                .expect("Failed to create checkpoint.");

            return Poll::Ready(Ok(()))
        }

        if this.pending_commit.is_none() {
            if this.buffer.is_empty() {
                // Nothing to commit
                return Poll::Ready(Ok(()));
            }

            let range: Vec<(K, V)> = this.buffer.drain(..).map(|m| (m.key, m.value)).collect();

            let facade = this.state_facade.clone();

            let commit = Box::pin(facade.put_range(range));

            let _ = this.pending_commit.replace(commit);
        }

        if let Some(ref mut task) = this.pending_commit {
            ready!(task.as_mut().poll(cx)).expect("Failed to commit sink buffer.");

            let _ = this.pending_commit.take();
        }

        let offset = std::cmp::max(this.highest_offset, this.highest_committed_offset);

        let offset_commit = this.state_facade.clone().create_checkpoint(*offset);
        
        let _ = this.pending_offset_commit.replace(Box::pin(offset_commit));

        cx.waker().wake_by_ref();

        Poll::Pending
    }

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        message: Message<K, V>,
    ) -> Result<(), Self::Error> {
        let this = self.project();

        let offset = std::cmp::max(message.offset, *this.highest_offset);

        *this.highest_offset = offset;

        this.buffer.push(message);

        Ok(())
    }
}

use std::{
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use futures::Future;
use pin_project_lite::pin_project;
use rdkafka::error::KafkaError;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    engine::QueueMetadata,
    message::{
        sink::{MessageSink, NonCommittingSink},
        types::Message,
    },
    state::backend::{facade::StateFacade, StateBackend, WriteableStateView},
};

type PendingCommit<E> = Option<Pin<Box<dyn Future<Output=Result<(), E>> + Send>>>;

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

        match this.pending_commit {
            Some(ref mut task) => {
                ready!(task.as_mut().poll(cx)).expect("Failed to commit message");

                let _ = this.pending_commit.take();

                Poll::Ready(Ok(()))
            }
            None => {
                let range: Vec<(K, V)> = this.buffer.drain(..).map(|m| (m.key, m.value)).collect();

                let facade = this.state_facade.clone();

                let commit = Box::pin(facade.put_range(range));

                let _ = this.pending_commit.replace(commit);

                match this.pending_commit.as_mut() {
                    Some(task) => ready!(task.as_mut().poll(cx)).expect("Failed to commit message"),
                    _ => panic!("This should not be possible"),
                }

                let _ = this.pending_commit.take();

                Poll::Ready(Ok(()))
            }
        }
    }

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        message: Message<K, V>,
    ) -> Result<(), Self::Error> {
        let this = self.project();

        this.buffer.push(message);

        Ok(())
    }
}

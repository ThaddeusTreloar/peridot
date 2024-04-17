use std::{
    collections::VecDeque,
    pin::Pin,
    task::{ready, Context, Poll},
};

use futures::Future;
use pin_project_lite::pin_project;
use rdkafka::error::KafkaError;

use crate::{
    engine::QueueMetadata,
    message::{
        sink::{MessageSink, NonCommittingSink},
        types::Message,
    },
    state::backend::{facade::StateFacade, StateBackend, WriteableStateView},
};

pin_project! {
    pub struct StateSink<B, K, V>
    where
        B: StateBackend,
    {
        queue_metadata: QueueMetadata,
        state_facade: StateFacade<K, V, B>,
        buffer: VecDeque<Message<K, V>>,
        _key_type: std::marker::PhantomData<K>,
        _value_type: std::marker::PhantomData<V>,
        pending_commit: Option<
            Pin<Box<
                dyn Future<
                    Output=Result<
                        (),
                        B::Error
        >>>>>,
    }
}

impl<B, K, V> StateSink<B, K, V>
where
    B: StateBackend,
{
    pub fn new(queue_metadata: QueueMetadata, state_facade: StateFacade<K, V, B>) -> Self {
        Self {
            queue_metadata,
            state_facade,
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

impl<B, K, V> MessageSink for StateSink<B, K, V>
where
    B: StateBackend + Send,
{
    type Error = StateSinkError;
    type KeyType = K;
    type ValueType = V;

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
                let range: Vec<(&K, &V)> = this
                    .buffer
                    .iter()
                    .map(|msg| (msg.key(), msg.value()))
                    .collect();

                let commit = Box::pin(this.state_facade.put_range(range));

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
        message: Message<Self::KeyType, Self::ValueType>,
    ) -> Result<(), Self::Error> {
        let this = self.project();

        this.buffer.push_back(message);

        Ok(())
    }
}
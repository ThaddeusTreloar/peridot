use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use futures::{Future, FutureExt};
use pin_project_lite::pin_project;
use rdkafka::error::KafkaError;

use crate::{
    engine::{AppEngine, QueueMetadata},
    message::{sink::MessageSink, types::Message},
    serde_ext::{Delegate, PSerialize},
    state::backend::{ReadableStateBackend, WriteableStateBackend},
};

use super::MessageSinkFactory;

pub struct StateSinkFactory<B, K, V> {
    engine_ref: Arc<AppEngine<B>>,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<B, K, V> StateSinkFactory<B, K, V> {
    pub fn from_backend_ref(backend: Arc<AppEngine<B>>) -> Self {
        Self {
            engine_ref: backend,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

impl<B, K, V> MessageSinkFactory for StateSinkFactory<B, K, V>
where
    K: Clone,
    V: Clone,
    B: ReadableStateBackend + WriteableStateBackend<K, V> + Send + Sync + 'static,
{
    type SinkType = StateSink<B, K, V>;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType {
        let partition = queue_metadata.partition();
        let source_topic = queue_metadata.source_topic().to_owned();

        StateSink::<B, K, V>::from_queue_metadata_and_backend_ref(
            queue_metadata,
            self.engine_ref.get_state_store(source_topic, partition),
        )
    }
}

pin_project! {
    pub struct StateSink<B, K, V>
    where
        B: WriteableStateBackend<K, V>,
        K: Clone,
        V: Clone,
    {
        queue_metadata: QueueMetadata,
        backend_ref: Arc<B>,
        buffer: VecDeque<Message<K, V>>,
        _key_type: std::marker::PhantomData<K>,
        _value_type: std::marker::PhantomData<V>,
        pending_commit: Option<
            Pin<Box<
                dyn Future<
                    Output=Result<
                        Option<Message<K, V>>,
                        B::Error
        >>>>>,
    }
}

impl<B, K, V> StateSink<B, K, V>
where
    K: Clone,
    V: Clone,
    B: WriteableStateBackend<K, V>,
{
    pub fn from_queue_metadata_and_backend_ref(
        queue_metadata: QueueMetadata,
        backend_ref: Arc<B>,
    ) -> Self {
        Self {
            queue_metadata,
            backend_ref,
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

impl<B, K, V> MessageSink for StateSink<B, K, V>
where
    K: Clone,
    V: Clone,
    B: WriteableStateBackend<K, V> + Send,
{
    type Error = StateSinkError;
    type KeySerType = Delegate<K>;
    type ValueSerType = Delegate<V>;

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_commit(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();

        loop {
            // Check if a pending task is present
            if let Some(task) = this.pending_commit {
                // If the task is not ready, return
                // Otherwise, check if the task is successful
                ready!(task.poll_unpin(cx)).expect("Failed to commit message");
            };
            // We know that after this point there is either no task or the task is finished
            // So we can pop the next message from the buffer
            match this.buffer.pop_front() {
                // If there is no message, we can clear any completed task and break out of the loop
                None => {
                    this.pending_commit.take();
                    break;
                }
                // If there is a message, we can commit it
                Some(message) => {
                    let commit = Box::pin(this.backend_ref.clone().commit_update(message));

                    let _ = this.pending_commit.replace(commit);
                }
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        message: &Message<
            <Self::KeySerType as PSerialize>::Input,
            <Self::ValueSerType as PSerialize>::Input,
        >,
    ) -> Result<(), Self::Error> {
        let this = self.project();

        this.buffer.push_back(message.clone());

        Ok(())
    }
}

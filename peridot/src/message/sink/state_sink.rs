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
    borrow::BorrowMut,
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
    state::{facade::{state_facade::StateStoreFacade, FacadeError, ReadableStateFacade, WriteableStateFacade}, store::StateStore},
};

type PendingCommit<E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send>>;
type PendingOffsetCommit<E> = Option<Pin<Box<dyn Future<Output = Result<(), E>> + Send>>>;

pin_project! {
    pub struct StateSink<B, K, V>
    where
        B: StateStore,
    {
        queue_metadata: QueueMetadata,
        state_facade: Arc<StateStoreFacade<K, V, B>>,
        buffer: Vec<PendingCommit<FacadeError>>,
        _key_type: std::marker::PhantomData<K>,
        _value_type: std::marker::PhantomData<V>,
        consumer_position: Option<i64>,
    }
}

impl<B, K, V> StateSink<B, K, V>
where
    B: StateStore + Send + Sync + 'static,
    K: Serialize + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(queue_metadata: QueueMetadata, state_facade: StateStoreFacade<K, V, B>) -> Self {
        Self {
            queue_metadata,
            state_facade: Arc::new(state_facade),
            buffer: Default::default(),
            _key_type: Default::default(),
            _value_type: Default::default(),
            consumer_position: None,
        }
    }

    pub fn get_checkpoint(&self) -> Result<Option<i64>, FacadeError> {
        self.state_facade
            .get_checkpoint()
            .map(|r| r.map(|c| c.offset))
    }

    pub fn wake_all(&self) {
        self.state_facade.wake_all();
    }

    pub fn wake(&self) {
        self.state_facade.wake();
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
    B: StateStore + Send,
{
}

impl<B, K, V> MessageSink<K, V> for StateSink<B, K, V>
where
    B: StateStore + Send + Sync + 'static,
    K: Clone + Serialize + Send + Sync + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Error = StateSinkError;

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    /// Will always return the changelog offset
    fn poll_commit(
        self: Pin<&mut Self>,
        mut consumer_position: i64,
        cx: &mut Context<'_>,
    ) -> Poll<Result<i64, Self::Error>> {
        let this = self.project();

        tracing::debug!("Committing state sink.");

        let pending_commits = this
            .buffer
            .drain(..)
            .filter_map(|mut commit| match commit.as_mut().poll(cx) {
                Poll::Pending => Some(commit),
                Poll::Ready(Ok(())) => None,
                Poll::Ready(Err(e)) => todo!("{}", e),
            })
            .collect::<Vec<PendingCommit<FacadeError>>>();

        this.buffer.extend(pending_commits);

        let changelog_write_position = this
            .queue_metadata
            .engine_context()
            .get_changelog_write_position(
                this.state_facade.store_name(),
                this.state_facade.partition(),
            );

        if let Some(offset) = changelog_write_position {
            consumer_position = offset + 1;
        }

        if this.buffer.is_empty() {
            // TODO: Evaluate whether it is better to wake after every commit attempt
            // or only after the commit is completed.
            // Waking after every commit attempt will decrease latency but increase
            // the async scheduler contention.
            this.state_facade.wake();

            this.state_facade
                .clone()
                .create_checkpoint(consumer_position);

            Poll::Ready(Ok(consumer_position))
        } else {
            Poll::Pending
        }
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

        let facade = this.state_facade.clone();

        let commit = Box::pin(facade.put(message.clone()));

        this.buffer.push(commit);

        Ok(message)
    }
}

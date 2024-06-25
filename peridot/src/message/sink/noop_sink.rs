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
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use pin_project_lite::pin_project;
use rdkafka::{consumer::Consumer, producer::Producer, Offset, TopicPartitionList};
use tracing::info;

use crate::{
    engine::{queue_manager::queue_metadata::QueueMetadata, wrapper::serde::PeridotSerializer},
    message::types::Message,
};

use super::{CommitingSink, MessageSink};

pin_project! {
    #[project = NoopProjection]
    pub struct NoopSink<K, V> {
        queue_metadata: QueueMetadata,
        _key_type: std::marker::PhantomData<K>,
        _value_type: std::marker::PhantomData<V>,
    }
}

impl<K, V> NoopSink<K, V> {
    pub fn from_queue_metadata(queue_metadata: QueueMetadata) -> Self {
        Self {
            queue_metadata,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

impl<K, V> CommitingSink for NoopSink<K, V> {}

#[derive(Debug, thiserror::Error)]
pub enum NoopSinkError {}

impl<KS, VS> MessageSink<KS::Input, VS::Input> for NoopSink<KS, VS>
where
    KS: PeridotSerializer,
    VS: PeridotSerializer,
{
    type Error = NoopSinkError;

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_commit(
        self: Pin<&mut Self>,
        consumer_position: i64,
        _: &mut Context<'_>,
    ) -> Poll<Result<i64, Self::Error>> {
        Poll::Ready(Ok(consumer_position))
    }

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        message: crate::message::types::Message<KS::Input, VS::Input>,
    ) -> Result<Message<KS::Input, VS::Input>, Self::Error> {
        let this = self.project();

        Ok(message)
    }
}

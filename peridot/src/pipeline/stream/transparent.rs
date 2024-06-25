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
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
use tracing::info;

use crate::{
    engine::util::ExactlyOnce,
    message::stream::{ChannelStream, PipelineStage},
};

use super::{ChannelStreamPipeline, PipelineStream};

pin_project! {
    pub struct TransparentPipeline<K, V, G = ExactlyOnce>
    {
        #[pin]
        queue_stream: ChannelStreamPipeline<K, V>,
        _delivery_guarantee: PhantomData<G>
    }
}

impl<K, V, G> TransparentPipeline<K, V, G> {
    pub fn new(queue_stream: ChannelStreamPipeline<K, V>) -> Self {
        Self {
            queue_stream,
            _delivery_guarantee: PhantomData,
        }
    }
}

impl<K, V, G> PipelineStream for TransparentPipeline<K, V, G>
where
    K: Send + 'static,
    V: Send + 'static,
    G: Send + 'static,
{
    type KeyType = K;
    type ValueType = V;
    type MStream = ChannelStream<K, V>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<PipelineStage<Self::MStream>>> {
        let (metadata, queue) = match self.queue_stream.poll_recv(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(val)) => val,
        };

        tracing::debug!(
            "Received new queue for topic: {}, parition: {}",
            metadata.source_topic(),
            metadata.partition()
        );

        Poll::Ready(Option::Some(PipelineStage::new(metadata, queue)))
    }
}

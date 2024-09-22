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
    engine::{
        queue_manager::QueueReceiver, util::ExactlyOnce, wrapper::serde::PeridotDeserializer,
    },
    message::stream::{head::QueueHead, PipelineStage},
};

use super::PipelineStream;

pin_project! {
    pub struct HeadPipeline<KS, VS, G = ExactlyOnce>
    where KS: PeridotDeserializer,
        VS: PeridotDeserializer
    {
        #[pin]
        queue_stream: QueueReceiver,
        _key_serialiser: PhantomData<KS>,
        _value_serialiser: PhantomData<VS>,
        _delivery_guarantee: PhantomData<G>
    }
}

impl<KS, VS, G> HeadPipeline<KS, VS, G>
where
    KS: PeridotDeserializer,
    VS: PeridotDeserializer,
{
    pub fn new(queue_stream: QueueReceiver) -> Self {
        Self {
            queue_stream,
            _key_serialiser: PhantomData,
            _value_serialiser: PhantomData,
            _delivery_guarantee: PhantomData,
        }
    }
}

impl<KS, VS, G> PipelineStream for HeadPipeline<KS, VS, G>
where
    KS: PeridotDeserializer + Send,
    VS: PeridotDeserializer + Send,
{
    type KeyType = KS::Output;
    type ValueType = VS::Output;
    type MStream = QueueHead<KS, VS>;

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

        let head = QueueHead::<KS, VS>::new(
            queue, 
            metadata.engine_context_arc(),
            metadata.source_topic(),
            metadata.partition()
        );

        Poll::Ready(Option::Some(PipelineStage::new(
            metadata,
            head,
        )))
    }
}

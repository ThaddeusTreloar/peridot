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
    fmt::Display,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
use rdkafka::{consumer::Consumer, TopicPartitionList};
use tracing::info;

use crate::{
    engine::{queue_manager::queue_metadata::QueueMetadata, wrapper::serde::PeridotSerializer},
    message::types::Message,
};

use super::MessageSink;

pin_project! {
    pub struct DebugSink<KS, VS>
    where
        KS: PeridotSerializer,
        VS: PeridotSerializer,
    {
        queue_metadata: QueueMetadata,
        _key_serialiser_type: PhantomData<KS>,
        _value_serialiser_type: PhantomData<VS>,
    }
}

impl<KS, VS> DebugSink<KS, VS>
where
    KS: PeridotSerializer,
    VS: PeridotSerializer,
{
    pub fn new(queue_metadata: QueueMetadata) -> Self {
        Self {
            queue_metadata,
            _key_serialiser_type: PhantomData,
            _value_serialiser_type: PhantomData,
        }
    }

    pub fn from_queue_metadata(queue_metadata: QueueMetadata) -> Self {
        Self::new(queue_metadata)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PrintSinkError {}

impl<KS, VS> MessageSink<KS::Input, VS::Input> for DebugSink<KS, VS>
where
    KS: PeridotSerializer,
    VS: PeridotSerializer,
    KS::Input: Display,
    VS::Input: Display,
{
    type Error = PrintSinkError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_commit(
        self: Pin<&mut Self>,
        consumer_position: i64,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<i64, Self::Error>> {
        Poll::Ready(Ok(consumer_position))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        message: Message<KS::Input, VS::Input>,
    ) -> Result<Message<KS::Input, VS::Input>, Self::Error> {
        let ser_key = KS::serialize(message.key()).expect("Failed to serialise key.");
        let ser_value = VS::serialize(message.value()).expect("Failed to serialise value.");

        tracing::info!(
            "Debug Sink: topic: {}, partition: {}, offset: {}, headers: {:?}, key: {:?}, value: {:?}", 
            message.topic(),
            message.partition(),
            message.offset(),
            message.headers(),
            String::from_utf8(ser_key).unwrap(),
            String::from_utf8(ser_value).unwrap(),
        );

        Ok(message)
    }
}

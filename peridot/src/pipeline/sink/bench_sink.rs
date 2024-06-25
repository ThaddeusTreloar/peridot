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

use std::sync::Arc;

use serde::Serialize;

use crate::{
    engine::{
        context::EngineContext, queue_manager::queue_metadata::QueueMetadata,
        wrapper::serde::PeridotSerializer,
    },
    message::sink::topic_sink::{TopicSink, TopicType},
};

use super::MessageSinkFactory;

pub struct BenchSinkFactory<KS, VS> {
    topic: String,
    waker: tokio::sync::mpsc::Sender<(i32, i64)>,
    _key_ser_type: std::marker::PhantomData<KS>,
    _value_ser_type: std::marker::PhantomData<VS>,
}

impl<KS, VS> BenchSinkFactory<KS, VS> {
    pub fn new(topic: &str, waker: tokio::sync::mpsc::Sender<(i32, i64)>) -> Self {
        Self {
            topic: topic.to_owned(),
            waker,
            _key_ser_type: Default::default(),
            _value_ser_type: Default::default(),
        }
    }
}

impl<KS, VS> MessageSinkFactory<KS::Input, VS::Input> for BenchSinkFactory<KS, VS>
where
    KS: PeridotSerializer,
    KS::Input: Serialize,
    VS: PeridotSerializer,
    VS::Input: Serialize,
{
    type SinkType = TopicSink<KS, VS>;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType {
        TopicSink::from_queue_metadata(queue_metadata, &self.topic)
            .with_topic_type(TopicType::Bench(self.waker.clone()))
    }
}

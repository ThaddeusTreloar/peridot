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
        wrapper::serde::native::NativeBytes,
    },
    message::sink::topic_sink::{TopicSink, TopicType},
};

use super::MessageSinkFactory;

pub struct ChangelogSinkFactory<K, V> {
    store_name: String,
    engine_context: Arc<EngineContext>,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<K, V> ChangelogSinkFactory<K, V> {
    pub fn new(store_name: &str, engine_context: Arc<EngineContext>) -> Self {
        Self {
            store_name: store_name.to_owned(),
            engine_context,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

impl<K, V> MessageSinkFactory<K, V> for ChangelogSinkFactory<K, V>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    type SinkType = TopicSink<NativeBytes<K>, NativeBytes<V>>;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType {
        let changlog_topic = self
            .engine_context
            .get_changelog_topic_name(&self.store_name);

        TopicSink::<NativeBytes<K>, NativeBytes<V>>::from_queue_metadata(
            queue_metadata,
            &changlog_topic,
        )
        .with_topic_type(TopicType::Changelog)
    }
}

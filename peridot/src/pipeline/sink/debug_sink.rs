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

use std::{fmt::Display, marker::PhantomData};

use crate::{
    engine::{
        queue_manager::queue_metadata::QueueMetadata,
        wrapper::serde::{PeridotSerializer, PeridotStatefulSerializer},
    },
    message::sink::debug_sink::DebugSink,
};

use super::{DynamicSerialiserSinkFactory, MessageSinkFactory};

#[derive(Default)]
pub struct DebugSinkFactory<KS, VS> {
    _key_ser_type: PhantomData<KS>,
    _value_ser_type: PhantomData<VS>,
}

impl<KS, VS> DebugSinkFactory<KS, VS> {
    pub fn new() -> Self {
        Self {
            _key_ser_type: PhantomData,
            _value_ser_type: PhantomData,
        }
    }
}

impl<KS, VS> MessageSinkFactory<KS::Input, VS::Input> for DebugSinkFactory<KS, VS>
where
    KS: PeridotSerializer,
    KS::Input: Display,
    VS: PeridotSerializer,
    VS::Input: Display,
{
    type SinkType = DebugSink<KS, VS>;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType {
        DebugSink::from_queue_metadata(queue_metadata)
    }
}

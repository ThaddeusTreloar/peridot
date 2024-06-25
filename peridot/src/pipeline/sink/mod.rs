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

use crate::{
    engine::{
        queue_manager::queue_metadata::QueueMetadata,
        wrapper::serde::{serializers::Serializers, PeridotStatefulSerializer},
    },
    message::sink::MessageSink,
};

pub mod bench_sink;
pub(crate) mod changelog_sink;
pub mod debug_sink;
pub(crate) mod noop_sink;
pub(crate) mod state_sink;
pub mod topic_sink;

/*
Currently undocumented is the design behaviour of different sink types.
In order to maintain only one controller over the producer transaction
all sinks in a topic-partition pipeline share the same producer.
Then, it is the responsibility of the forwarded sink to ensure that when
poll_commit is called, that the producer has committed its' transaction.

All other sinks do not need to worry about producer transaction management.

For sinks that do not have a producer (eg: StateSink) but instead commit to
some attached or internal store, it is not currently possible to bundle the
attached store transaction with the producer transaction. So instead, the
following state transition has been employed to ensure that state stores are
committed to, only all topic backed states have been committed:

*/

pub trait MessageSinkFactory<K, V> {
    type SinkType: MessageSink<K, V>;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType;
}

pub trait DynamicSerialiserSinkFactory<KS, VS>: MessageSinkFactory<KS::Input, VS::Input>
where
    KS: PeridotStatefulSerializer,
    VS: PeridotStatefulSerializer,
{
    fn new_sink_with_serialisers(serialisers: Serializers<KS, VS>) -> Self::SinkType;
}

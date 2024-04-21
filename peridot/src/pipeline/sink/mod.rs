use crate::{engine::{wrapper::serde::{serializers::Serializers, PeridotStatefulSerializer}, QueueMetadata}, message::sink::MessageSink};

pub(crate) mod changelog_sink;
pub mod print_sink;
pub(crate) mod state_sink;
pub mod topic_sink;
pub(crate) mod noop_sink;

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
    VS: PeridotStatefulSerializer
{
    fn new_sink_with_serialisers(serialisers: Serializers<KS, VS>) -> Self::SinkType;
}
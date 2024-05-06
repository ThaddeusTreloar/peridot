use std::{fmt::Display, marker::PhantomData};

use crate::{
    engine::{
        queue_manager::queue_metadata::QueueMetadata,
        wrapper::serde::{PeridotSerializer, PeridotStatefulSerializer},
    },
    message::sink::{debug_sink::DebugSink, noop_sink::NoopSink},
};

use super::{DynamicSerialiserSinkFactory, MessageSinkFactory};

#[derive(Default)]
pub struct NoopSinkFactory<KS, VS> {
    _key_ser_type: PhantomData<KS>,
    _value_ser_type: PhantomData<VS>,
}

impl<KS, VS> NoopSinkFactory<KS, VS> {
    pub fn new() -> Self {
        Self {
            _key_ser_type: PhantomData,
            _value_ser_type: PhantomData,
        }
    }
}

impl<KS, VS> MessageSinkFactory<KS::Input, VS::Input> for NoopSinkFactory<KS, VS>
where
    KS: PeridotSerializer,
    VS: PeridotSerializer,
{
    type SinkType = NoopSink<KS, VS>;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType {
        NoopSink::from_queue_metadata(queue_metadata)
    }
}

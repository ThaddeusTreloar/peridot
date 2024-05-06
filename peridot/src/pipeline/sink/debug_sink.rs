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

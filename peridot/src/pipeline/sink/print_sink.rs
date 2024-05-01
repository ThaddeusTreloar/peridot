use std::{fmt::Display, marker::PhantomData};

use crate::{
    engine::{queue_manager::queue_metadata::QueueMetadata, wrapper::serde::{PeridotSerializer, PeridotStatefulSerializer}},
    message::sink::print_sink::PrintSink,
};

use super::{DynamicSerialiserSinkFactory, MessageSinkFactory};

#[derive(Default)]
pub struct PrintSinkFactory<KS, VS> {
    _key_ser_type: PhantomData<KS>,
    _value_ser_type: PhantomData<VS>,
}

impl<KS, VS> PrintSinkFactory<KS, VS> {
    pub fn new() -> Self {
        Self {
            _key_ser_type: PhantomData,
            _value_ser_type: PhantomData,
        }
    }
}

impl<KS, VS> MessageSinkFactory<KS::Input, VS::Input> for PrintSinkFactory<KS, VS>
where
    KS: PeridotSerializer,
    KS::Input: Display,
    VS: PeridotSerializer,
    VS::Input: Display,
{
    type SinkType = PrintSink<KS, VS>;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType {
        PrintSink::from_queue_metadata(queue_metadata)
    }
}
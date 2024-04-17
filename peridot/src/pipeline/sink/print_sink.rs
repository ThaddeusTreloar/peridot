use std::{fmt::Display, marker::PhantomData};

use crate::{
    engine::{wrapper::serde::PSerialize, QueueMetadata},
    message::sink::print_sink::PrintSink,
};

use super::MessageSinkFactory;

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

impl<KS, VS> MessageSinkFactory for PrintSinkFactory<KS, VS>
where
    KS: PSerialize,
    KS::Input: Display,
    VS: PSerialize,
    VS::Input: Display,
{
    type SinkType = PrintSink<KS, VS>;

    fn new_sink(&self, queue_metadata: QueueMetadata) -> Self::SinkType {
        PrintSink::from_queue_metadata(queue_metadata)
    }
}

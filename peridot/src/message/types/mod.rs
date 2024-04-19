mod extractors;
mod headers;
mod offset;
mod message;
mod partial_message;
mod timestamp;

pub use timestamp::*;
pub use headers::*;
pub use message::*;
use partial_message::*;
pub use extractors::{
    headers::*,
    key_value::*,
    value::*,
};

pub trait FromMessage<K, V> {
    fn from_message(msg: Message<K, V>) -> (Self, PartialMessage<K, V>) where Self: Sized;
}

pub trait PatchMessage<K, V> {
    type RK;
    type RV;

    fn patch(self, msg: PartialMessage<K, V>) -> Message<Self::RK, Self::RV> where Self: Sized;
}
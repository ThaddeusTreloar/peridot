mod extractors;
mod headers;
mod message;
mod offset;
mod partial_message;
mod timestamp;

pub use extractors::{headers::*, key_value::*, value::*};
pub use headers::*;
pub use message::*;
pub(crate) use partial_message::*;
pub use timestamp::*;

pub trait FromMessage<K, V> {
    fn from_message(msg: Message<K, V>) -> (Self, PartialMessage<K, V>)
    where
        Self: Sized;
}

pub trait PatchMessage<K, V> {
    type RK;
    type RV;

    fn patch(self, msg: PartialMessage<K, V>) -> Message<Self::RK, Self::RV>
    where
        Self: Sized;
}

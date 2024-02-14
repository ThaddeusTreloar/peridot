use std::marker::PhantomData;

use crate::pipeline::{
    message::{PatchMessage, FromMessage},
    stream::{map::MessageMap, MessageStream},
};

#[derive(Debug)]
pub struct QueueMetadata {}

pub struct PipelineStage<M, K, V> {
    md: QueueMetadata,
    s: M,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<M, K, V> PipelineStage<M, K, V>
where
    M: MessageStream<K, V>,
{
    pub fn new(md: QueueMetadata, s: M) -> Self {
        PipelineStage {
            md,
            s,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }

    pub fn map<E, R, F, RK, RV>(self, f: F) -> PipelineStage<MessageMap<M, F, E, R, K, V>, RK, RV>
    where
        F: FnMut(E) -> R,
        E: FromMessage<K, V>,
        R: PatchMessage<K, V, RK, RV>,
        Self: Sized,
    {
        let wrapped = MessageMap::new(self.s, f);

        PipelineStage::new(self.md, wrapped)
    }
}

/*
pub trait PStage<K, V, M>
where M: MessageStream<K, V>
{
    fn into_state_parts(self) -> (QueueMetadata, M);
}

*/

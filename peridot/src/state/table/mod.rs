use std::{any::Any, marker::PhantomData, pin, sync::Arc, task::{Context, Waker}};

use crossbeam::{atomic::AtomicCell, epoch::Pointable};
use serde::Serialize;

use crate::{
    engine::{util::ExactlyOnce, EngineState},
    pipeline::{sink::{changelog_sink::ChangelogSinkFactory, state_sink::StateSinkFactory}, stream::{PipelineStream, PipelineStreamExt}}, serde_ext::PSerialize,
};

use super::backend::{BackendView, ReadableStateBackend, WriteableStateBackend};

pub mod backend_sink;

pub struct PeridotTable<B, KS, VS> {
    _name: String,
    backend: Arc<B>,
    state: Arc<AtomicCell<EngineState>>,
    _changelog_key_ser_type: PhantomData<KS>,
    _changelog_val_ser_type: PhantomData<VS>
}

impl<B, KS, VS> PeridotTable<B, KS, VS>
where
    B: ReadableStateBackend
        + WriteableStateBackend<
            <B as ReadableStateBackend>::KeyType,
            <B as ReadableStateBackend>::ValueType,
        > + BackendView<
            KeyType = <B as ReadableStateBackend>::KeyType,
            ValueType = <B as ReadableStateBackend>::ValueType,
        >,
    <B as ReadableStateBackend>::KeyType: Serialize + Clone + Send,
    <B as ReadableStateBackend>::ValueType: Serialize + Clone + Send,
    KS: PSerialize<Input = <B as ReadableStateBackend>::KeyType> + Send + 'static,
    VS: PSerialize<Input = <B as ReadableStateBackend>::ValueType> + Send + 'static,
{
    pub fn new<P>(name: String, backend: B, stream_queue: P) -> Self
    where
        B: Send + Sync + 'static,
        P: PipelineStream<
                KeyType = <B as ReadableStateBackend>::KeyType,
                ValueType = <B as ReadableStateBackend>::ValueType,
            > + Send
            + 'static,
    {
        let backend_ref = Arc::new(backend);
        
        let changelog_sink_factory = ChangelogSinkFactory
            ::<KS, VS>
            ::new(format!("{}-changelog", name));
        
        let changelog_output = stream_queue
            .fork::<_, ExactlyOnce>(changelog_sink_factory);
        
        let backend_sink_factory = StateSinkFactory
            ::<KS, VS, B>
            ::from_backend_ref(backend_ref.clone());

        let backend_output = changelog_output
            .fork::<_, ExactlyOnce>(backend_sink_factory);

        Self {
            _name: name,
            backend: backend_ref,
            state: Default::default(),
            _changelog_key_ser_type: Default::default(),
            _changelog_val_ser_type: Default::default(),
        }
    }

    pub fn get_table_state(&self) -> Arc<AtomicCell<EngineState>> {
        self.state.clone()
    }

    pub fn view(
        &self,
    ) -> Arc<
        impl BackendView<
            KeyType = <B as ReadableStateBackend>::KeyType,
            ValueType = <B as ReadableStateBackend>::ValueType,
        >,
    > {
        self.backend.clone()
    }
}

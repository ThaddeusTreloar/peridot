use std::sync::Arc;

use crossbeam::atomic::AtomicCell;
use serde::Serialize;

use crate::{
    engine::{util::ExactlyOnce, EngineState},
    pipeline::{
        forward::PipelineForward,
        stream::{transparent::TransparentPipeline, PipelineStream},
    },
};

use self::queue_handler::QueueReceiverHandler;

use super::backend::{BackendView, ReadableStateBackend, WriteableStateBackend};

pub mod partition_handler;
pub mod queue_handler;

pub struct PeridotTable<B> {
    _name: String,
    backend: Arc<B>,
    state: Arc<AtomicCell<EngineState>>,
}

impl<B> PeridotTable<B>
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
        /*
        let backend_ref = Arc::new(backend);
        //
        // changelog_sink_factory: format!("{}-changelog", name)
        //
        let (changelog_forwarder, backend_downstream) =
            stream_queue.forked_sink(changelog_sink_factory);
        //
        // backend_sink_factory: backend,
        //
        let (backend_forwarder, downstream) = backend_downstream.forked_sink(backend_sink_factory);

        app.job(backend_forwarder);
        app.job(changelog_forwarder);

        Self {
            _name: name,
            backend: backend_ref,
            state: Default::default(),
        } */

        unimplemented!("")
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

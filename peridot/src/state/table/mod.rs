use std::sync::Arc;

use crossbeam::atomic::AtomicCell;
use serde::Serialize;

use crate::{
    engine::{util::ExactlyOnce, EngineState},
    pipeline::{sink::PipelineForward, stream::{PipelineStream, PipelineStreamSinkExt}},
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
        let backend_ref = Arc::new(backend);

        let queue_handler = QueueReceiverHandler::new(format!("{}-changelog", name));

        stream_queue.sink(queue_handler);

        let pipeline_forwarder =
            PipelineForward::<_, _, ExactlyOnce>::new(stream_queue, queue_handler);

        tokio::spawn(pipeline_forwarder);

        Self {
            _name: name,
            backend: backend_ref,
            state: Default::default(),
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

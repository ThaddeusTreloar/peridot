use std::sync::Arc;

use crossbeam::atomic::AtomicCell;
use serde::Serialize;

use crate::{
    engine::{util::ExactlyOnce, EngineState},
    pipeline::{
        fork::PipelineFork,
        sink::{changelog_sink::ChangelogSinkFactory, state_sink::StateSinkFactory},
        stream::{PipelineStream, PipelineStreamExt},
    },
};

type TableDownstream<P, B, K, V> =
    PipelineFork<PipelineFork<P, ChangelogSinkFactory<K, V>>, StateSinkFactory<B, K, V>>;

use super::backend::{BackendView, ReadableStateBackend, WriteableStateBackend};

pub mod backend_sink;

pub struct PeridotTable<P, B>
where
    P: PipelineStream + Send + 'static,
    P::MStream: Send + 'static,
    P::KeyType: Clone + Send + 'static,
    P::ValueType: Clone + Send + 'static,
{
    _name: String,
    backend: Arc<B>,
    backend_output: TableDownstream<P, B, P::KeyType, P::ValueType>,
    state: Arc<AtomicCell<EngineState>>,
}

pub trait IntoTable<P, B>
where
    P: PipelineStream + Send + 'static,
    P::MStream: Send + 'static,
    P::KeyType: Clone + Send + 'static,
    P::ValueType: Clone + Send + 'static,
{
    fn into_table(self, backend: Arc<B>) -> PeridotTable<P, B>;
}

impl<P, B> IntoTable<P, B> for P
where
    P: PipelineStream<
            KeyType = <B as ReadableStateBackend>::KeyType,
            ValueType = <B as ReadableStateBackend>::ValueType,
        > + Send
        + 'static,
    B: ReadableStateBackend
        + WriteableStateBackend<
            <B as ReadableStateBackend>::KeyType,
            <B as ReadableStateBackend>::ValueType,
        > + BackendView<
            KeyType = <B as ReadableStateBackend>::KeyType,
            ValueType = <B as ReadableStateBackend>::ValueType,
        > + Send
        + 'static,
    <B as ReadableStateBackend>::KeyType: Serialize + Clone + Send,
    <B as ReadableStateBackend>::ValueType: Serialize + Clone + Send,
{
    fn into_table(self, backend: Arc<B>) -> PeridotTable<P, B> {
        PeridotTable::new("table".to_string(), backend, self)
    }
}

impl<P, B> PeridotTable<P, B>
where
    P: PipelineStream<
            KeyType = <B as ReadableStateBackend>::KeyType,
            ValueType = <B as ReadableStateBackend>::ValueType,
        > + Send
        + 'static,
    B: ReadableStateBackend
        + WriteableStateBackend<
            <B as ReadableStateBackend>::KeyType,
            <B as ReadableStateBackend>::ValueType,
        > + BackendView<
            KeyType = <B as ReadableStateBackend>::KeyType,
            ValueType = <B as ReadableStateBackend>::ValueType,
        > + Send
        + 'static,
    <B as ReadableStateBackend>::KeyType: Serialize + Clone + Send,
    <B as ReadableStateBackend>::ValueType: Serialize + Clone + Send,
{
    pub fn new(name: String, backend: Arc<B>, stream_queue: P) -> Self {
        let changelog_sink_factory = ChangelogSinkFactory::new(format!("{}-changelog", name));

        let changelog_output = stream_queue.fork::<_, ExactlyOnce>(changelog_sink_factory);

        let backend_sink_factory = StateSinkFactory::<
            B,
            <B as ReadableStateBackend>::KeyType,
            <B as ReadableStateBackend>::ValueType,
        >::from_backend_ref(backend.clone());

        let backend_output = changelog_output.fork::<_, ExactlyOnce>(backend_sink_factory);

        Self {
            _name: name,
            backend,
            backend_output,
            state: Default::default(),
        }
    }

    pub fn get_table_state(&self) -> Arc<AtomicCell<EngineState>> {
        self.state.clone()
    }
}

pub trait IntoView {
    type KeyType;
    type ValueType;

    fn into_view(&self) -> Arc<impl BackendView>;
}

impl<P, B> IntoView for PeridotTable<P, B>
where
    P: PipelineStream + Send + 'static,
    P::MStream: Send + 'static,
    P::KeyType: Clone + Send + 'static,
    P::ValueType: Clone + Send + 'static,
    B: BackendView + ReadableStateBackend,
{
    type KeyType = <B as ReadableStateBackend>::KeyType;
    type ValueType = <B as ReadableStateBackend>::ValueType;

    fn into_view(&self) -> Arc<impl BackendView> {
        self.backend.clone()
    }
}

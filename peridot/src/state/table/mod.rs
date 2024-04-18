use std::sync::Arc;

use crossbeam::atomic::AtomicCell;

use crate::{
    engine::{util::ExactlyOnce, AppEngine, EngineState},
    pipeline::{
        fork::PipelineFork,
        sink::{changelog_sink::ChangelogSinkFactory, state_sink::StateSinkFactory},
        stream::{PipelineStream, PipelineStreamExt},
    },
};

use super::backend::StateBackend;

type TableDownstream<P, B, K, V> =
    PipelineFork<PipelineFork<P, ChangelogSinkFactory<K, V>>, StateSinkFactory<B, K, V>>;

pub struct Table<P, B>
where
    P: PipelineStream + Send + 'static,
    P::MStream: Send + 'static,
    P::KeyType: Clone + Send + 'static,
    P::ValueType: Clone + Send + 'static,
{
    _name: String,
    app_engine_ref: Arc<AppEngine<B>>,
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
    fn into_table(self, app_engine_ref: Arc<AppEngine<B>>) -> Table<P, B>;
}

impl<P, B> IntoTable<P, B> for P
where
    P: PipelineStream + Send + 'static,
    B: StateBackend + Send + 'static,
    P::KeyType: Clone + Send + 'static,
    P::ValueType: Clone + Send + 'static,
{
    fn into_table(self, app_engine_ref: Arc<AppEngine<B>>) -> Table<P, B> {
        Table::new("table".to_string(), app_engine_ref, self)
    }
}

impl<P, B> Table<P, B>
where
    P: PipelineStream + Send + 'static,
    B: StateBackend + Send + 'static,
    P::KeyType: Clone + Send + 'static,
    P::ValueType: Clone + Send + 'static,
{
    pub fn new(name: String, app_engine_ref: Arc<AppEngine<B>>, stream_queue: P) -> Self {
        let changelog_sink_factory = ChangelogSinkFactory::new(format!("{}-changelog", name));

        let changelog_output = stream_queue.fork::<_, ExactlyOnce>(changelog_sink_factory);

        let backend_sink_factory =
            StateSinkFactory::<B, P::KeyType, P::ValueType>::from_backend_ref(
                app_engine_ref.clone(),
                name.clone(),
            );

        let backend_output = changelog_output.fork::<_, ExactlyOnce>(backend_sink_factory);

        Self {
            _name: name,
            app_engine_ref,
            backend_output,
            state: Default::default(),
        }
    }

    pub fn get_table_state(&self) -> Arc<AtomicCell<EngineState>> {
        self.state.clone()
    }
}

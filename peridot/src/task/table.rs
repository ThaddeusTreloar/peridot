use std::{marker::PhantomData, sync::Arc};

use crossbeam::atomic::AtomicCell;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    app::PeridotApp, engine::{util::{DeliveryGuaranteeType, ExactlyOnce}, EngineState}, pipeline::{
        fork::PipelineFork,
        sink::{changelog_sink::ChangelogSinkFactory, state_sink::StateSinkFactory},
        stream::{PipelineStream, PipelineStreamExt},
    }, state::backend::StateBackend
};

use super::Task;

pub type TableOutput<K, V, P, B> = PipelineFork<PipelineFork<P, ChangelogSinkFactory<K, V>>, StateSinkFactory<B, K, V>>;

type TableDownstream<P, B, K, V> =
    PipelineFork<PipelineFork<P, ChangelogSinkFactory<K, V>>, StateSinkFactory<B, K, V>>;

pub struct TableTask<'a, P, B, G>
where
    G: DeliveryGuaranteeType,
    P: PipelineStream + Send + 'static,
    P::MStream: Send + 'static,
    P::KeyType: Clone + Send + 'static,
    P::ValueType: Clone + Send + 'static,
{
    app: &'a PeridotApp<B, G>,
    _name: String,
    backend_output: TableDownstream<P, B, P::KeyType, P::ValueType>,
    state: Arc<AtomicCell<EngineState>>,
    _delivery_guarantee: PhantomData<G>,
}

/*
pub trait IntoTable<P, B, G>
where
    P: PipelineStream + Send + 'static,
    P::MStream: Send + 'static,
    P::KeyType: Clone + Send + 'static,
    P::ValueType: Clone + Send + 'static,
{
    fn into_table(self, app_engine_ref: Arc<AppEngine<B>>) -> Table<P, B, G>;
}

impl<P, B, G> IntoTable<P, B, G> for P
where
    P: PipelineStream + Send + 'static,
    B: StateBackend + Send + 'static,
    P::KeyType: Clone + Send + 'static,
    P::ValueType: Clone + Send + 'static,
{
    fn into_table(self, app_engine_ref: Arc<AppEngine<B>>) -> Table<P, B, G> {
        Table::new("table".to_string(), app_engine_ref, self)
    }
} */

impl<'a, P, B, G> TableTask<'a, P, B, G>
where
    G: DeliveryGuaranteeType,
    P: PipelineStream + Send + 'static,
    B: StateBackend + Send + Sync + 'static,
    P::KeyType: Clone + Send + 'static,
    P::ValueType: Clone + Send + 'static,
{
    pub fn new(app: &'a PeridotApp<B, G>, name: String, stream_queue: P) -> Self {
        let changelog_sink_factory = ChangelogSinkFactory::new(format!("{}-changelog", name));

        let changelog_output = stream_queue.fork::<_, ExactlyOnce>(changelog_sink_factory);

        let backend_sink_factory =
            StateSinkFactory::<B, P::KeyType, P::ValueType>::from_backend_ref(
                app.engine_ref().clone(),
                name.clone(),
            );

        let backend_output = changelog_output.fork::<_, ExactlyOnce>(backend_sink_factory);

        Self {
            app,
            _name: name,
            backend_output,
            state: Default::default(),
            _delivery_guarantee: Default::default(),
        }
    }

    pub fn get_table_state(&self) -> Arc<AtomicCell<EngineState>> {
        self.state.clone()
    }
}


impl<'a, P, B, G> Task<'a> for TableTask<'a, P, B, G> 
where
    P: PipelineStream + Send + 'static,
    B: StateBackend + Send + Sync + 'static,
    B::Error: Send,
    P::KeyType: Serialize + Clone + Send + Sync + 'static,
    P::ValueType: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    G: DeliveryGuaranteeType,
{
    type G = G;
    type B =  B;
    type R = TableOutput<P::KeyType, P::ValueType, P, B>;

    fn into_pipeline(self) -> Self::R {
        let Self {
            backend_output,
            ..
        } = self;

        backend_output
    }

    fn into_parts(self) -> (&'a crate::app::PeridotApp<Self::B, Self::G>, Self::R) {
        let Self {
            app,
            backend_output,
            ..
        } = self;

        (app, backend_output)
    }
}
use std::{marker::PhantomData, sync::Arc};

use crossbeam::atomic::AtomicCell;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    app::PeridotApp,
    engine::{
        context::EngineContext,
        engine_state::EngineState,
        metadata_manager::table_metadata,
        util::{DeliveryGuaranteeType, ExactlyOnce},
        wrapper::serde::json::Json,
        AppEngine,
    },
    pipeline::{
        fork::PipelineFork,
        forward::PipelineForward,
        sink::{
            changelog_sink::ChangelogSinkFactory, noop_sink::NoopSinkFactory,
            state_sink::StateSinkFactory,
        },
        state_fork::StateForkPipeline,
        stream::{PipelineStream, PipelineStreamExt},
    },
    state::backend::{
        facade::{FacadeDistributor, StateFacade},
        view::GetViewDistributor,
        StateBackend,
    },
};

use super::{PipelineParts, Task};

pub type TableOutput<K, V, P, B> =
    StateForkPipeline<PipelineFork<P, ChangelogSinkFactory<K, V>, ExactlyOnce>, B>;

type TableDownstream<P, B, K, V> =
    StateForkPipeline<PipelineFork<P, ChangelogSinkFactory<K, V>, ExactlyOnce>, B>;

#[must_use = "Tables do not run unless they are finished or they have downsream tasks."]
pub struct TableTask<'a, P, B, G>
where
    G: DeliveryGuaranteeType + Send + Sync + 'static,
    P: PipelineStream + Send + 'static,
    P::MStream: Send + 'static,
    P::KeyType: Clone + Serialize + Send + 'static,
    P::ValueType: Clone + Serialize + Send + 'static,
{
    app: &'a PeridotApp<B, G>,
    source_topic: String,
    name: String,
    backend_output: TableDownstream<P, B, P::KeyType, P::ValueType>,
    state: Arc<AtomicCell<EngineState>>,
    _delivery_guarantee: PhantomData<G>,
}

impl<'a, P, B, G> TableTask<'a, P, B, G>
where
    G: DeliveryGuaranteeType + Send + Sync + 'static,
    P: PipelineStream + Send + 'static,
    B: StateBackend + Send + Sync + 'static,
    P::KeyType: Clone + Serialize + Send + 'static,
    P::ValueType: Clone + Serialize + Send + 'static,
{
    // TODO: considering refactoring this so that is returns (Self, TableHandle) where
    // TableHandle is what is used for joins, views, etc
    pub fn new(
        app: &'a PeridotApp<B, G>,
        source_topic: String,
        store_name: String,
        stream_queue: P,
    ) -> Self {
        let table_metadata = app
            .engine()
            .engine_context()
            .register_topic_store(&source_topic, &store_name)
            .expect("Failed to register table.");

        let changelog_sink_factory =
            ChangelogSinkFactory::new(&store_name, app.engine().engine_context());

        let changelog_output = PipelineFork::new(stream_queue, changelog_sink_factory);

        let backend_sink_factory =
            StateSinkFactory::<B, P::KeyType, P::ValueType>::from_state_store_manager(
                app.engine().state_store_context(),
                &store_name,
                &source_topic,
            );

        let backend_output = StateForkPipeline::new_with_changelog(
            changelog_output,
            backend_sink_factory,
            store_name.clone(),
            app.engine().engine_context(),
        );

        Self {
            app,
            source_topic,
            name: store_name,
            backend_output,
            state: Default::default(),
            _delivery_guarantee: Default::default(),
        }
    }

    pub fn get_table_state(&self) -> Arc<AtomicCell<EngineState>> {
        self.state.clone()
    }
}

impl<'a, P, B, G> TableTask<'a, P, B, G>
where
    P: PipelineStream + Send + 'static,
    B: StateBackend + Send + Sync + 'static,
    B::Error: Send,
    P::KeyType: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    P::ValueType: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    G: DeliveryGuaranteeType + Send + Sync + 'static,
{
    pub fn finish(self) {
        let sink_factory = NoopSinkFactory::<Json<P::KeyType>, Json<P::ValueType>>::new();
        let PipelineParts(app, _, output) = self.into_parts();
        let job = PipelineForward::<_, _, G>::new(output, sink_factory);
        app.job(Box::pin(job));
    }
}

impl<'a, P, B, G> Task<'a> for TableTask<'a, P, B, G>
where
    P: PipelineStream + Send + 'static,
    B: StateBackend + Send + Sync + 'static,
    B::Error: Send,
    P::KeyType: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    P::ValueType: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    G: DeliveryGuaranteeType + Send + Sync + 'static,
{
    type G = G;
    type B = B;
    type R = TableOutput<P::KeyType, P::ValueType, P, B>;

    fn into_pipeline(self) -> Self::R {
        let Self { backend_output, .. } = self;

        backend_output
    }

    fn into_parts(self) -> PipelineParts<'a, Self::B, Self::G, Self::R> {
        let Self {
            app,
            source_topic,
            backend_output,
            ..
        } = self;

        PipelineParts(app, source_topic, backend_output)
    }
}

impl<'a, P, B, G> GetViewDistributor for &TableTask<'a, P, B, G>
where
    P: PipelineStream + Send + 'static,
    B: StateBackend + Send + Sync + 'static,
    B::Error: Send,
    P::KeyType: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    P::ValueType: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    G: DeliveryGuaranteeType + Send + Sync + 'static,
{
    type Error = B::Error;
    type KeyType = P::KeyType;
    type ValueType = P::ValueType;
    type Backend = B;

    fn get_view_distributor(
        &self,
    ) -> FacadeDistributor<Self::KeyType, Self::ValueType, Self::Backend> {
        FacadeDistributor::new(self.app.engine(), self.name.clone())
    }
}

pub struct TableHandle<B, G> {
    engine: Arc<AppEngine<B, G>>,
    store_name: String,
}

impl<B, G> GetViewDistributor for TableHandle<B, G> {
    type Backend = B;
    type KeyType = ();
    type ValueType = ();
    type Error = std::io::Error;

    fn get_view_distributor(
        &self,
    ) -> FacadeDistributor<Self::KeyType, Self::ValueType, Self::Backend> {
        todo!("")
    }
}

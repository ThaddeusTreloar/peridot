/*
 * Copyright 2024 Thaddeus Treloar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

use std::{marker::PhantomData, sync::Arc};

use crossbeam::atomic::AtomicCell;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    app::PeridotApp,
    engine::{
        context::EngineContext, engine_state::EngineState, metadata_manager::table_metadata, state_store_manager::StateStoreManager, util::{DeliveryGuaranteeType, ExactlyOnce}, wrapper::serde::json::Json, AppEngine
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
    state::{facade::{facade_distributor::FacadeDistributor, state_facade::StateStoreFacade, FacadeError, GetFacade, GetFacadeDistributor}, store::StateStore, view::GetViewDistributor},
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
    store_name: String,
    backend_output: TableDownstream<P, B, P::KeyType, P::ValueType>,
    state: Arc<AtomicCell<EngineState>>,
    _delivery_guarantee: PhantomData<G>,
}

impl<'a, P, B, G> TableTask<'a, P, B, G>
where
    G: DeliveryGuaranteeType + Send + Sync + 'static,
    P: PipelineStream + Send + 'static,
    B: StateStore + Send + Sync + 'static,
    P::KeyType: Clone + Serialize + Send + 'static,
    P::ValueType: Clone + Serialize + Send + 'static,
{
    // TODO: considering refactoring this so that is returns (Self, TableHandle) where
    // TableHandle is what is used for joins, views, etc
    pub async fn new(
        app: &'a PeridotApp<B, G>,
        source_topic: String,
        store_name: String,
        stream_queue: P,
    ) -> Self {
        let table_metadata = app
            .engine()
            .engine_context()
            .register_topic_store(&source_topic, &store_name)
            .await
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
            store_name,
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
    B: StateStore + Send + Sync + 'static,
    P::KeyType: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    P::ValueType: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    G: DeliveryGuaranteeType + Send + Sync + 'static,
{
    pub fn finish(self) -> TableHandle<B, P::KeyType, P::ValueType> {
        let sink_factory = NoopSinkFactory::<Json<P::KeyType>, Json<P::ValueType>>::new();
        let store_name = self.store_name.clone();
        let PipelineParts(app, _, output) = self.into_parts();
        let job = PipelineForward::<_, _, G>::new(output, sink_factory);
        app.job(Box::pin(job));

        TableHandle::new(app.engine(), store_name)
    }
}

impl<'a, P, B, G> Task<'a> for TableTask<'a, P, B, G>
where
    P: PipelineStream + Send + 'static,
    B: StateStore + Send + Sync + 'static,
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

impl<'a, P, B, G> GetFacadeDistributor for &TableTask<'a, P, B, G>
where
    P: PipelineStream + Send + 'static,
    B: StateStore + Send + Sync + 'static,
    P::KeyType: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    P::ValueType: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    G: DeliveryGuaranteeType + Send + Sync + 'static,
{
    type KeyType = P::KeyType;
    type ValueType = P::ValueType;
    type Backend = B;

    fn get_facade_distributor(
        &self,
    ) -> Result<FacadeDistributor<Self::KeyType, Self::ValueType, Self::Backend>, FacadeError> {
        Ok(FacadeDistributor::new(self.app.engine(), self.store_name.clone()))
    }
}

#[derive(Clone)]
pub struct TableHandle<B, K, V> {
    engine_context: Arc<EngineContext>,
    state_store_manager: Arc<StateStoreManager<B>>,
    store_name: String,
    key_type: PhantomData<K>,
    value_type: PhantomData<V>
}

impl <B, K, V> TableHandle<B, K, V> 
where 
    B: StateStore + Send + Sync + 'static
{
    fn new<G>(engine: &AppEngine<B, G>, store_name: String) -> Self
    where G: DeliveryGuaranteeType + Send + Sync + 'static
    {
        TableHandle { engine_context: engine.engine_context(), state_store_manager: engine.state_store_context(), store_name, key_type: Default::default(), value_type: Default::default() }
    }

    fn fetch_backend(&self, partition: i32) -> Arc<B> {
        let table_metadata = self.engine_context.store_metadata(&self.store_name);

        self.state_store_manager
            .get_state_store(table_metadata.source_topic(), partition)
            .expect("Failed to get state store for facade distributor.")
    }

    fn store_name(&self) -> &str {
        &self.store_name
    }
}

impl<B, K, V> GetFacade for TableHandle<B, K, V> 
where 
    B: StateStore + Send + Sync + 'static
{
    type Backend = B;
    type KeyType = K;
    type ValueType = V;

    fn get_facade(
            &self,
            partition: i32,
        ) -> Result<crate::state::facade::state_facade::StateStoreFacade<Self::KeyType, Self::ValueType, Self::Backend>, FacadeError> {
        let backend = self.fetch_backend(partition);

        Ok(StateStoreFacade::new(
            backend,
            self.state_store_manager.clone(),
            self.store_name().to_owned(),
            partition,
        ))
    }

}

impl<B, K, V> GetFacadeDistributor for TableHandle<B, K, V> {
    type Backend = B;
    type KeyType = K;
    type ValueType = V;

   fn get_facade_distributor(
        &self,
    ) -> Result<FacadeDistributor<Self::KeyType, Self::ValueType, Self::Backend>, FacadeError> {
        todo!("")
    }
}

impl<'a, P, B, G> GetViewDistributor for TableTask<'a, P, B, G>
where
    G: DeliveryGuaranteeType + Send + Sync + 'static,
    P: PipelineStream + Send + 'static,
    P::MStream: Send + 'static,
    P::KeyType: Clone + Serialize + Send + 'static,
    P::ValueType: Clone + Serialize + Send + 'static,
{
    type Backend = B;
    type KeyType = P::KeyType;
    type ValueType = P::ValueType;
    type Error = std::io::Error;

    fn get_view_distributor(
        &self,
    ) -> crate::state::view::view_distributor::ViewDistributor<
        Self::KeyType,
        Self::ValueType,
        Self::Backend,
    > {
        todo!("")
    }
}

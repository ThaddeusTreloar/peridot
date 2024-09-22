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

use std::{
    pin::Pin,
    sync::{Arc, Mutex},
};

use futures::{
    future::{join_all, select_all},
    Future,
};
use rdkafka::{consumer::BaseConsumer, ClientConfig};
use serde::Serialize;
use tracing::info;

use crate::{
    app::extensions::PeridotConsumerContext,
    engine::{
        util::{DeliveryGuaranteeType, ExactlyOnce},
        wrapper::serde::PeridotDeserializer,
        AppEngine, Job,
    },
    pipeline::stream::head::HeadPipeline,
    state::store::{in_memory::InMemoryStateStore, StateStore},
    task::{table::TableTask, transparent::TransparentTask, Task},
};

use self::{
    config::PeridotConfig,
    error::{PeridotAppCreationError, PeridotAppRuntimeError},
};

pub mod builder;
pub mod config;
pub mod error;
pub mod extensions;

pub type PeridotConsumer = BaseConsumer<PeridotConsumerContext>;

#[derive(Debug, Clone)]
pub(crate) struct CompletedQueueMetadata {
    source_topic: String,
    partition: i32,
}

impl CompletedQueueMetadata {
    fn new(source_topic: &str, partition: i32) -> Self {
        Self {
            source_topic: source_topic.to_owned(),
            partition,
        }
    }
}

type DirectTableTask<'a, KS, VS, B, G> = TableTask<'a, HeadPipeline<KS, VS>, B, G>;

#[derive()]
pub struct PeridotApp<B = InMemoryStateStore, G = ExactlyOnce>
where
    G: DeliveryGuaranteeType + Send + Sync + 'static,
{
    _config: PeridotConfig,
    engine: AppEngine<B, G>,
    _phantom: std::marker::PhantomData<G>,
}

impl<B, G> PeridotApp<B, G>
where
    G: DeliveryGuaranteeType + Send + Sync + 'static,
    B: StateStore + Send + Sync + 'static,
{
    fn stream<KS, VS>(
        &self,
        topic: &str,
    ) -> Result<HeadPipeline<KS, VS, ExactlyOnce>, PeridotAppRuntimeError>
    where
        KS: PeridotDeserializer,
        VS: PeridotDeserializer,
    {
        tracing::debug!("Creating stream for topic: {}", topic);
        Ok(self.engine.input_stream(topic.to_string())?)
    }

    pub async fn table<'a, KS, VS>(
        &'a self,
        topic: &'a str,
        store_name: &'a str,
    ) -> Result<DirectTableTask<KS, VS, B, G>, PeridotAppRuntimeError>
    where
        KS: PeridotDeserializer + Send + 'static,
        VS: PeridotDeserializer + Send + 'static,
        KS::Output: Clone + Serialize + Send,
        VS::Output: Clone + Serialize + Send,
    {
        let input: HeadPipeline<KS, VS, ExactlyOnce> =
            self.stream(topic)?;

        Ok(TransparentTask::new(self, topic, input)
            .into_table(store_name)
            .await)
    }

    pub fn task<'a, KS, VS>(
        &'a self,
        topic: &'a str,
    ) -> TransparentTask<'a, HeadPipeline<KS, VS, ExactlyOnce>, B, G>
    where
        KS: PeridotDeserializer + Send + 'static,
        VS: PeridotDeserializer + Send + 'static,
    {
        let input: HeadPipeline<KS, VS, ExactlyOnce> = self
            .stream(topic)
            .expect("Failed to create input stream from source topic");

        TransparentTask::new(self, topic, input)
    }

    pub fn from_config(mut config: PeridotConfig) -> Result<Self, PeridotAppCreationError> {
        let engine = AppEngine::from_config(&config)?;

        Ok(Self {
            _config: config.clone(),
            engine: engine,
            _phantom: std::marker::PhantomData,
        })
    }

    pub(crate) fn job(&self, job: Job) {
        self.engine.submit(job)
    }

    pub(crate) fn engine(&self) -> &AppEngine<B, G> {
        &self.engine
    }

    // TODO: Consider having an alternate method that returns (AppHandle, RuntimeFuture)
    // Where the RuntimeFuture runs the app to completion, and the
    // AppHandle allows app management and operational views.
    pub async fn run(self) -> Result<(), PeridotAppRuntimeError> {
        self.engine.run().await?;

        Ok(())
    }
}

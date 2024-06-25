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

use crate::{
    app::PeridotApp,
    engine::util::DeliveryGuaranteeType,
    message::types::{KeyValue, Message, PatchMessage},
    pipeline::stream::PipelineStream,
    state::backend::StateBackend,
};

use super::Task;

#[must_use = "pipelines do nothing unless patched to a topic"]
pub struct ImportTask<'a, S, B, G>
where
    B: StateBackend,
    G: DeliveryGuaranteeType + Send + Sync + 'static,
{
    app: &'a PeridotApp<B, G>,
    imports: Vec<S>,
}

impl<'a, S, B, G> ImportTask<'a, S, B, G>
where
    S: futures::Stream,
    B: StateBackend,
    G: DeliveryGuaranteeType + Send + Sync + 'static,
{
    pub fn new(app: &'a PeridotApp<B, G>, imports: Vec<S>) -> Self {
        Self { app, imports }
    }

    pub fn new_single(app: &'a PeridotApp<B, G>, import: S) -> Self {
        Self {
            app,
            imports: vec![import],
        }
    }

    pub fn filter<F>(&self, callback: F) {}

    pub fn map<F>(&self, callback: F) {}

    pub fn integrate<F, RM>(&self, callback: F)
    where
        F: Fn(S::Item) -> RM,
        RM: PatchMessage<(), ()>,
    {
    }
}

#[must_use = "pipelines do nothing unless patched to a topic"]
pub struct IntegratedImportTask<'a, S, B, G>
where
    B: StateBackend,
    G: DeliveryGuaranteeType + Send + Sync + 'static,
{
    app: &'a PeridotApp<B, G>,
    imports: Vec<S>,
}

impl<'a, S, B, G> IntegratedImportTask<'a, S, B, G>
where
    B: StateBackend,
    G: DeliveryGuaranteeType + Send + Sync + 'static,
{
    pub fn new(app: &'a PeridotApp<B, G>, imports: Vec<S>) -> Self {
        Self { app, imports }
    }

    pub fn new_single(app: &'a PeridotApp<B, G>, import: S) -> Self {
        Self {
            app,
            imports: vec![import],
        }
    }
}

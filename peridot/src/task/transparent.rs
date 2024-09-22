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
    app::PeridotApp, engine::util::DeliveryGuaranteeType, pipeline::stream::PipelineStream,
    state::store::StateStore,
};

use super::{PipelineParts, Task};

#[must_use = "pipelines do nothing unless patched to a topic"]
pub struct TransparentTask<'a, R, B, G>
where
    R: PipelineStream,
    B: StateStore,
    G: DeliveryGuaranteeType + Send + Sync + 'static,
{
    app: &'a PeridotApp<B, G>,
    source_topic: String,
    output: R,
}

impl<'a, R, B, G> TransparentTask<'a, R, B, G>
where
    R: PipelineStream + 'static,
    B: StateStore,
    G: DeliveryGuaranteeType + Send + Sync + 'static,
{
    pub fn new(app: &'a PeridotApp<B, G>, source_topic: &str, handler: R) -> Self {
        Self {
            app,
            source_topic: source_topic.to_owned(),
            output: handler,
        }
    }
}

impl<'a, R, B, G> Task<'a> for TransparentTask<'a, R, B, G>
where
    R: PipelineStream + Send + 'static,
    R::MStream: Send,
    B: StateStore + Send + Sync + 'static,
    G: DeliveryGuaranteeType + Send + Sync + 'static,
{
    type G = G;
    type R = R;
    type B = B;

    fn into_pipeline(self) -> Self::R {
        self.output
    }

    fn into_parts(self) -> PipelineParts<'a, Self::B, Self::G, Self::R> {
        let Self {
            app,
            source_topic,
            output,
        } = self;

        PipelineParts(app, source_topic, output)
    }
}

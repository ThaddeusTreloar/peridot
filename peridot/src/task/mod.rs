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

use std::fmt::Display;

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    app::PeridotApp,
    engine::{
        util::DeliveryGuaranteeType,
        wrapper::serde::{PeridotSerializer, PeridotStatefulSerializer},
    },
    message::{
        join::Combiner,
        types::{FromMessage, PatchMessage},
    },
    pipeline::{
        filter::FilterPipeline,
        forward::PipelineForward,
        join::JoinPipeline,
        map::MapPipeline,
        sink::{
            bench_sink::BenchSinkFactory, debug_sink::DebugSinkFactory,
            topic_sink::TopicSinkFactory,
        },
        stream::{PipelineStream, PipelineStreamExt},
    },
    state::backend::{
        facade::{FacadeDistributor, StateFacade},
        view::GetViewDistributor,
        StateBackend,
    },
};

use self::{table::TableTask, transform::TransformTask};

pub mod import;
pub mod table;
pub mod transform;
pub mod transparent;

pub struct PipelineParts<'a, B, G, R>(&'a PeridotApp<B, G>, String, R)
where
    G: DeliveryGuaranteeType + Send + Sync + 'static;

impl<'a, B, G, R> PipelineParts<'a, B, G, R>
where
    G: DeliveryGuaranteeType + Send + Sync + 'static,
{
    pub fn app(&self) -> &'a PeridotApp<B, G> {
        self.0
    }

    pub fn source_topic(&self) -> String {
        self.1.clone()
    }

    pub fn output(self) -> R {
        self.2
    }
}

pub trait Task<'a> {
    type G: DeliveryGuaranteeType + Send + Sync + 'static;
    type B: StateBackend + Send + Sync + 'static;
    type R: PipelineStream + Send + 'static;

    fn and_then<F1, R1>(self, next: F1) -> TransformTask<'a, F1, Self::R, R1, Self::B, Self::G>
    where
        F1: Fn(Self::R) -> R1,
        R1: PipelineStream + Send + 'static,
        R1::MStream: Send + 'static,
        Self: Sized,
    {
        let parts = self.into_parts();

        TransformTask::<'a>::new(parts.app(), parts.source_topic(), next, parts.output())
    }

    fn join<T, C>(
        self,
        table: T,
        combiner: C,
    ) -> TransformTask<
        'a,
        impl FnOnce(
            Self::R,
        )
            -> JoinPipeline<Self::R, FacadeDistributor<T::KeyType, T::ValueType, T::Backend>, C>,
        Self::R,
        JoinPipeline<Self::R, FacadeDistributor<T::KeyType, T::ValueType, T::Backend>, C>,
        Self::B,
        Self::G,
    >
    where
        T: GetViewDistributor<KeyType = <Self::R as PipelineStream>::KeyType> + Send + 'a,
        T::KeyType: Send + Sync + 'static,
        T::ValueType: DeserializeOwned + Send + Sync + 'static,
        T::Backend: StateBackend + Sync + 'static,
        C: Combiner<<Self::R as PipelineStream>::ValueType, T::ValueType> + 'static,
        C::Output: Send + Sync + 'static,
        <Self::R as PipelineStream>::KeyType: PartialEq<T::KeyType> + Serialize + Clone + Send,
        <Self::R as PipelineStream>::ValueType: Send + Sync,
        Self: Sized,
    {
        let parts = self.into_parts();

        let view = table.get_view_distributor();

        let transform = move |input: Self::R| JoinPipeline::new(input, view, combiner);

        TransformTask::<'a>::new(parts.app(), parts.source_topic(), transform, parts.output())
    }

    fn map<MF, ME, MR>(
        self,
        callback: MF,
    ) -> TransformTask<
        'a,
        impl FnOnce(Self::R) -> MapPipeline<Self::R, MF, ME, MR>,
        Self::R,
        MapPipeline<Self::R, MF, ME, MR>,
        Self::B,
        Self::G,
    >
    where
        MF: Fn(ME) -> MR + Send + Sync + Clone + 'static,
        ME: FromMessage<
                <Self::R as PipelineStream>::KeyType,
                <Self::R as PipelineStream>::ValueType,
            > + Send
            + 'static,
        MR: PatchMessage<
                <Self::R as PipelineStream>::KeyType,
                <Self::R as PipelineStream>::ValueType,
            > + Send
            + 'static,
        Self: Sized,
        Self::R: PipelineStreamExt,
    {
        let parts = self.into_parts();

        TransformTask::<'a>::new(
            parts.app(),
            parts.source_topic(),
            move |input| MapPipeline::new(input, callback),
            parts.output(),
        )
    }

    fn filter<F>(
        self,
        callback: F,
    ) -> TransformTask<
        'a,
        impl FnOnce(Self::R) -> FilterPipeline<Self::R, F>,
        Self::R,
        FilterPipeline<Self::R, F>,
        Self::B,
        Self::G,
    >
    where
        F: Fn(
                &<Self::R as PipelineStream>::KeyType,
                &<Self::R as PipelineStream>::ValueType,
            ) -> bool
            + Send
            + Sync
            + Clone
            + 'static,
        Self: Sized,
        Self::R: PipelineStreamExt,
    {
        let parts = self.into_parts();

        TransformTask::<'a>::new(
            parts.app(),
            parts.source_topic(),
            move |inner| FilterPipeline::new(inner, callback),
            parts.output(),
        )
    }

    async fn into_table(self, store_name: &str) -> TableTask<'a, Self::R, Self::B, Self::G>
    where
        <Self::R as PipelineStream>::KeyType: Clone + Serialize + Send + 'static,
        <Self::R as PipelineStream>::ValueType: Clone + Serialize + Send + 'static,
        Self: Sized + 'a,
    {
        let parts = self.into_parts();

        TableTask::new(
            parts.app(),
            parts.source_topic(),
            store_name.to_owned(),
            parts.output(),
        )
        .await
    }

    fn into_pipeline(self) -> Self::R;

    fn into_parts(self) -> PipelineParts<'a, Self::B, Self::G, Self::R>;

    fn into_topic<KS, VS>(self, topic: &str)
    where
        KS: PeridotSerializer<Input = <Self::R as PipelineStream>::KeyType> + Send + 'static,
        VS: PeridotSerializer<Input = <Self::R as PipelineStream>::ValueType> + Send + 'static,
        KS::Input: Send + Serialize + 'static,
        VS::Input: Send + Serialize + 'static,
        Self: Sized + 'a,
        Self::R: PipelineStreamExt,
    {
        let PipelineParts(app, _, output) = self.into_parts();
        let sink_factory = TopicSinkFactory::<KS, VS>::new(topic, app.engine().engine_context());
        let job = PipelineForward::<_, _, Self::G>::new(output, sink_factory);
        app.job(Box::pin(job));
    }

    fn into_bench<KS, VS>(self, topic: &str, waker: tokio::sync::mpsc::Sender<(i32, i64)>)
    where
        KS: PeridotSerializer<Input = <Self::R as PipelineStream>::KeyType> + Send + 'static,
        VS: PeridotSerializer<Input = <Self::R as PipelineStream>::ValueType> + Send + 'static,
        KS::Input: Send + Serialize + 'static,
        VS::Input: Send + Serialize + 'static,
        Self: Sized + 'a,
        Self::R: PipelineStreamExt,
    {
        let PipelineParts(app, _, output) = self.into_parts();
        let sink_factory = BenchSinkFactory::<KS, VS>::new(topic, waker);
        let job = PipelineForward::<_, _, Self::G>::new(output, sink_factory);
        app.job(Box::pin(job));
    }

    fn into_debug<KS, VS>(self)
    where
        KS: PeridotSerializer<Input = <Self::R as PipelineStream>::KeyType> + Send + 'static,
        VS: PeridotSerializer<Input = <Self::R as PipelineStream>::ValueType> + Send + 'static,
        KS::Input: Send + Display + 'static,
        VS::Input: Send + Display + 'static,
        Self: Sized + 'a,
        Self::R: PipelineStreamExt,
    {
        let sink_factory = DebugSinkFactory::<KS, VS>::new();
        let PipelineParts(app, _, output) = self.into_parts();
        let job = PipelineForward::<_, _, Self::G>::new(output, sink_factory);
        app.job(Box::pin(job));
    }

    /*
    fn into_topic_with_ser<KS, VS>(self, _topic: &str, key_serialiser: KS, value_serialiser: VS)
    where
        KS: PeridotStatefulSerializer<Input = <Self::R as PipelineStream>::KeyType> + Send + 'static,
        VS: PeridotStatefulSerializer<Input = <Self::R as PipelineStream>::ValueType> + Send + 'static,
        KS::Input: Send + Display + 'static,
        VS::Input: Send + Display + 'static,
        Self: Sized + 'a,
        Self::R: PipelineStreamExt,
    {
        let sink_factory = PrintSinkFactory::<KS, VS>::new();
        let (app, output) = self.into_parts();
        let job = output.forward(sink_factory);
        app.job(Box::pin(job));
    } */
}

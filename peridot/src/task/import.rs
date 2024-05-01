use crate::{app::PeridotApp, engine::util::DeliveryGuaranteeType, message::types::{KeyValue, Message, PatchMessage}, pipeline::stream::PipelineStream, state::backend::{StateBackend, StateBackendContext}};

use super::Task;

#[must_use = "pipelines do nothing unless patched to a topic"]
pub struct ImportTask<'a, S, B, G>
where
    B: StateBackend,
    G: DeliveryGuaranteeType,
{
    app: &'a PeridotApp<B, G>,
    imports: Vec<S>,
}

impl<'a, S, B, G> ImportTask<'a, S, B, G>
where
    S: futures::Stream,
    B: StateBackend,
    G: DeliveryGuaranteeType,
{
    pub fn new(app: &'a PeridotApp<B, G>, imports: Vec<S>) -> Self {
        Self {
            app,
            imports,
        }
    }

    pub fn new_single(app: &'a PeridotApp<B, G>, import: S) -> Self {
        Self {
            app,
            imports: vec![import],
        }
    }

    pub fn filter<F>(&self, callback: F) {

    }

    pub fn map<F>(&self, callback: F) {

    }

    pub fn integrate<F, RM>(&self, callback: F) 
    where
        F: Fn(S::Item) -> RM,
        RM: PatchMessage<(), ()>
    {

    }
}

#[must_use = "pipelines do nothing unless patched to a topic"]
pub struct IntegratedImportTask<'a, S, B, G>
where
    B: StateBackend,
    G: DeliveryGuaranteeType,
{
    app: &'a PeridotApp<B, G>,
    imports: Vec<S>,
}

impl<'a, S, B, G> IntegratedImportTask<'a, S, B, G>
where
    B: StateBackend,
    G: DeliveryGuaranteeType,
{
    pub fn new(app: &'a PeridotApp<B, G>, imports: Vec<S>) -> Self {
        Self {
            app,
            imports,
        }
    }

    pub fn new_single(app: &'a PeridotApp<B, G>, import: S) -> Self {
        Self {
            app,
            imports: vec![import],
        }
    }
}

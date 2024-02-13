use std::sync::Arc;

use crate::app::pstream::PStream;

use super::{AppEngine, error::PeridotEngineRuntimeError, util::{ExactlyOnce, DeliveryGuaranteeType, AtLeastOnce, AtMostOnce}};


#[derive()]
pub struct StreamBuilder<G=ExactlyOnce> 
where G: DeliveryGuaranteeType
{
    engine: Arc<AppEngine<G>>,
    topic: String,
}

impl <G>  StreamBuilder<G>
where G: DeliveryGuaranteeType
{
    pub fn new(topic: &str, engine: Arc<AppEngine<G>>) -> Self {
        Self {
            engine,
            topic: topic.to_string(),
        }
    }
}

impl StreamBuilder<AtLeastOnce> {
    pub async fn build<'a>(self) -> Result<PStream<AtLeastOnce>, PeridotEngineRuntimeError> {
        let Self { engine, topic } = self;

        AppEngine::<AtLeastOnce>::stream(engine, topic).await
    }
}

impl StreamBuilder<ExactlyOnce> {
    pub async fn build<'a>(self) -> Result<PStream<ExactlyOnce>, PeridotEngineRuntimeError> {
        let Self { engine, topic } = self;

        AppEngine::<ExactlyOnce>::stream(engine, topic).await
    }
}

impl StreamBuilder<AtMostOnce> {
    pub async fn build<'a>(self) -> Result<PStream<AtMostOnce>, PeridotEngineRuntimeError> {
        let Self { engine, topic } = self;

        AppEngine::<AtMostOnce>::stream(engine, topic).await
    }
}
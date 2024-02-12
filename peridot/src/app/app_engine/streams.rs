use std::sync::Arc;

use crate::app::pstream::PStream;

use super::{AppEngine, error::PeridotEngineRuntimeError};


#[derive()]
pub struct StreamBuilder {
    engine: Arc<AppEngine>,
    topic: String,
}

impl StreamBuilder
{
    pub fn new(topic: &str, engine: Arc<AppEngine>) -> Self {
        StreamBuilder {
            engine,
            topic: topic.to_string(),
        }
    }

    pub async fn build<'a>(self) -> Result<PStream, PeridotEngineRuntimeError> {
        let Self { engine, topic } = self;

        AppEngine::stream(engine, topic).await
    }
}

/*pub struct NewTable<K, V, B = PersistentStateBackend<V>> {
    _topic: String,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
    _backend_type: std::marker::PhantomData<B>,
}

impl From<StreamBuilder> for NewTable {
    fn from(builder: StreamBuilder) -> Self {
        NewTable {
            _topic: builder.topic,
            _key_type: Default::default(),
            _value_type: Default::default(),
            _backend_type: Default::default(),
        }
    }
}*/
use std::{sync::Arc, pin::Pin, task::{Context, Poll}, future::IntoFuture};

use futures::{Stream, stream::empty, StreamExt, Sink, Future};
use rdkafka::{
    consumer::{stream_consumer::StreamPartitionQueue, StreamConsumer},
    ClientConfig, message::BorrowedMessage,
};
use tracing::info;

use crate::{
    app::extensions::PeridotConsumerContext,
    state::{
        backend::{
            persistent::PersistentStateBackend, ReadableStateBackend, StateBackend,
            WriteableStateBackend,
        },
        StateStore,
    }, stream::{types::{KeyValue, IntoRecordParts}},
};

use self::{
    app_engine::{AppEngine, tables::TableBuilder, streams::StreamBuilder},
    error::{PeridotAppCreationError, PeridotAppRuntimeError}, ptable::PTable, pstream::PStream, psink::PSink,
};

pub mod app_engine;
pub mod error;
pub mod extensions;
pub mod psink;
pub mod pstream;
pub mod ptable;

pub type PeridotConsumer = StreamConsumer<PeridotConsumerContext>;
pub type PeridotPartitionQueue = StreamPartitionQueue<PeridotConsumerContext>;

#[derive()]
pub struct PeridotApp {
    _config: ClientConfig,
    engine: Arc<AppEngine>,
}

impl PeridotApp {
    pub fn from_config(config: &ClientConfig) -> Result<Self, PeridotAppCreationError> {
        let engine = AppEngine::from_config(config)?;

        Ok(PeridotApp {
            _config: config.clone(),
            engine: Arc::new(engine),
        })
    }
}

pub async fn run(app: &PeridotApp) -> Run {
    info!("Running PeridotApp");
    app.engine.run().await.unwrap();
    Run::default()
}

#[derive(Default)]
pub struct Run {
}

impl Future for Run {
    type Output = Result<(), PeridotAppRuntimeError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        Poll::Pending
    }
}



impl PeridotApp {
    //type Error = PeridotAppRuntimeError;

    pub async fn run(&self) -> Result<(), PeridotAppRuntimeError> {
        info!("Running PeridotApp");
        self.engine.run().await?;
        Ok(())
    }

    pub async fn table<K, V, B>(
        &self,
        topic: &str,
    ) -> Result<PTable<K, V, B>, PeridotAppRuntimeError>
    where
        B: StateBackend
            + ReadableStateBackend<V>
            + WriteableStateBackend<V>
            + Send
            + Sync
            + 'static,
        K: Send + Sync + 'static,
        V: Send + Sync + 'static + for<'de> serde::Deserialize<'de>,
    {
        let engine = self.engine.clone();

        Ok(AppEngine::table(engine, topic.to_string()).await?)
    }

    pub async fn arc_table<K, V, B>(
        self: Arc<Self>,
        topic: &str,
    ) -> Result<PTable<K, V, B>, PeridotAppRuntimeError>
    where
        B: StateBackend
            + ReadableStateBackend<V>
            + WriteableStateBackend<V>
            + Send
            + Sync
            + 'static,
        K: Send + Sync + 'static,
        V: Send + Sync + 'static + for<'de> serde::Deserialize<'de>,
    {
        let engine = self.engine.clone();

        Ok(AppEngine::table(engine, topic.to_string()).await?)
    }

    pub async fn stream(&self, topic: &str) -> Result<PStream, PeridotAppRuntimeError> {
        info!("Creating stream for topic: {}", topic);
        Ok(AppEngine::stream(self.engine.clone(), topic.to_string()).await?)
    }

    pub async fn stream_builder(&self, topic: &str) -> StreamBuilder {
        StreamBuilder::new(topic, self.engine.clone())
    }

    pub async fn sink(&self, topic: &str) -> Result<PSink, PeridotAppRuntimeError> {
        info!("Creating sink for topic: {}", topic);
        Ok(AppEngine::sink(self.engine.clone(), topic.to_string()).await?)
    }
}

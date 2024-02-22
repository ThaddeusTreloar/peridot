use std::{collections::HashMap, time::Duration};

use eap::{config::Config, environment::Environment};
use futures::StreamExt;
use peridot::{
    app::{error::PeridotAppRuntimeError, App, PeridotApp},
    init::init_tracing,
    state::{
        backend::{self, in_memory::InMemoryStateBackend, persistent::PersistentStateBackend},
        ReadableStateStore, StateStore,
    },
};
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    ClientConfig,
};
use tracing::{info, level_filters::LevelFilter};

#[derive(Debug, eap::Config)]
struct AppConfig {}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Topic {
    topic_name: String,
    scope: Vec<String>,
    consent_required: bool,
    consent_owner_type: String,
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct ConsentGrant {
    owner_type: String,
    owner: String,
    map: HashMap<String, HashMap<String, HashMap<String, bool>>>,
}

#[tokio::main]
async fn main() -> Result<(), PeridotAppRuntimeError> {
    init_tracing(LevelFilter::INFO);

    let mut source = ClientConfig::new();

    source
        .set("bootstrap.servers", "servicesaustralia.com.au:29092")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "true")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", "5D5PMQEIB2VD633V")
        .set(
            "sasl.password",
            "ee5DtvJYWFXYJ/MF+bCJVBil8+xEH5vuZ6c8Fk2qjD0xSGhlDnXr9w4D9LTUQv2t",
        )
        .set("group.id", "rust-test4")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug);

    let backend: PersistentStateBackend<ConsentGrant> = PersistentStateBackend::try_from_file(
        std::path::Path::new("/tmp/peridot.gw.state_store.db"),
    )
    .await
    .unwrap();

    let state_store: StateStore<PersistentStateBackend<_>, ConsentGrant> =
        StateStore::from_consumer_config_and_backend("consent.Client", &source, backend).unwrap();

    let primary_stream: StreamConsumer = source.create().unwrap();

    primary_stream.subscribe(&["changeOfAddress"]).unwrap();

    primary_stream.stream().for_each(|message| async {});

    let app = PeridotApp::from_client_config(&source).unwrap();

    let some_table = app.table::<(), ()>("test.topic");

    let some_stream = app.stream::<(), ()>("test.topic");

    app.run().await
}

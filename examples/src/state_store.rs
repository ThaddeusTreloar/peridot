use std::{collections::HashMap, time::Duration};

use eap::{config::Config, environment::Environment};
use peridot::{
    init::init_tracing,
    state::{
        backend::{in_memory::InMemoryStateBackend, persistent::PersistentStateBackend},
        ReadableStateStore, StateStore,
    },
};
use rdkafka::{config::RDKafkaLogLevel, ClientConfig};
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

async fn test_persistent_store(source: &ClientConfig) {
    let persistent_backend =
        PersistentStateBackend::try_from_file(std::path::Path::new("/tmp/peridot.state_store.db"))
            .await
            .unwrap();

    let state_store: StateStore<PersistentStateBackend<_>, ConsentGrant> =
        StateStore::from_consumer_config_and_backend("consent.Client", &source, persistent_backend)
            .unwrap();

    loop {
        match state_store.get("jon").await {
            Some(value) => info!("Value: {:?}", value),
            None => info!("No value found"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn test_in_memory_store(source: &ClientConfig) {
    let state_store: StateStore<InMemoryStateBackend<_>, ConsentGrant> =
        StateStore::from_consumer_config("consent.Client", source).unwrap();

    loop {
        match state_store.get("jon").await {
            Some(value) => info!("Value: {:?}", value),
            None => info!("No value found"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[tokio::main]
async fn main() {
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

    //test_in_memory_store(&source).await;
    test_persistent_store(&source).await;
}

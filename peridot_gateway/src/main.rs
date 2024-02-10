use std::{time::Duration, default, sync::Arc, collections::HashMap};

use crossbeam::atomic::AtomicCell;
use peridot::{types::Domain, state::{ReadableStateStore, StateStore, backend::{in_memory::InMemoryStateBackend, persistent::PersistantStateBackend, ReadableStateBackend}}, init::init_tracing};
use rdkafka::{ClientConfig, config::RDKafkaLogLevel};
use tokio::time::sleep;
use eap::{
    config::Config,
    environment::Environment
};
use tracing::{info, level_filters::LevelFilter};

#[derive(Debug, eap::Config)]
struct AppConfig {
}

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

async fn test_persistence() {
    let persistent_backend = PersistantStateBackend::try_from_file(std::path::Path::new("./state_store")).await.unwrap();

    let grant: Option<ConsentGrant> = persistent_backend.get("jon").await;

    match grant {
        Some(value) => info!("Value: {:?}", value),
        None => info!("No value found"),
    }
}

async fn test_persistent_store(source: &ClientConfig) {
    let persistent_backend = PersistantStateBackend::try_from_file(std::path::Path::new("./state_store")).await.unwrap();

    let state_store: Arc<StateStore<PersistantStateBackend<_>, ConsentGrant>> = StateStore::from_consumer_config_and_backend(
        "consent.Client",
        &source,
        persistent_backend
    ).unwrap();

    loop {
        match state_store.get("jon").await {
            Some(value) => info!("Value: {:?}", value),
            None => info!("No value found"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn test_in_memory_store(source: &ClientConfig) {
    let state_store: Arc<StateStore<InMemoryStateBackend<_>, ConsentGrant>> = StateStore::from_consumer_config(
        "consent.Client",
        source,
    ).unwrap();

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
        .set(
            "bootstrap.servers",
            "servicesaustralia.com.au:29092",
        )
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "false")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", "5D5PMQEIB2VD633V")
        .set(
            "sasl.password",
            "ee5DtvJYWFXYJ/MF+bCJVBil8+xEH5vuZ6c8Fk2qjD0xSGhlDnXr9w4D9LTUQv2t",
        )
        .set("group.id", "rust-test50")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug);

    test_in_memory_store(&source).await;
    // test_persistent_store(&source).await;
    // test_persistence().await;
}

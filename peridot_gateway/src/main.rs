use std::{time::Duration, default};

use crossbeam::atomic::AtomicCell;
use peridot::{types::Domain, state::{InMemoryStateStore, ReadableStateStore}, init::init_tracing};
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

#[derive(Debug, Clone, Copy, Default)]
enum Test {
    #[default]
    Hello,
    Goodbye
}

#[tokio::main]
async fn main() {
    init_tracing(LevelFilter::INFO);

    let mut source = ClientConfig::new();

    println!("Is lock free: {}", AtomicCell::<Test>::is_lock_free());

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
        .set("group.id", "rust-test1")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug);

    let state_store: InMemoryStateStore<'_, Topic> = InMemoryStateStore::from_consumer_config(
        &source,
        "topicStore.Global",
    ).unwrap();

    match state_store.get("changeOfAddress").await {
        Some(value) => info!("Value: {:?}", value),
        None => info!("No value found"),
    }
}

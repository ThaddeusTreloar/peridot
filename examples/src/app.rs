use std::collections::HashMap;
use std::sync::Arc;

use peridot::app::ptable::{PTable, PeridotTable};
use peridot::init::init_tracing;
use peridot::app::PeridotApp;
use peridot::state::backend::in_memory::InMemoryStateBackend;
use rdkafka::ClientConfig;

use peridot::state::ReadableStateStore;
use rdkafka::config::RDKafkaLogLevel;
use tracing::info;
use tracing::level_filters::LevelFilter;


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
async fn main() -> Result<(), anyhow::Error> {
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
        .set("group.id", "rust-test19")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug);

    let app = Arc::new(PeridotApp::from_client_config(&source)?);

    info!("Creating table");

    let consent_table: PTable<'_, String, ConsentGrant> = app
        .table("consent.Client").await?;

    let topic_table: PTable<'_, String, Topic> = app
        .table("topicStore.Global").await?;

    info!("Creating stream");

    //let _stream = app.stream("changeOfAddress").await?;

    info!("Running app");

    app.run().await?;

    loop {
        match consent_table.get_store()?.get("jon").await {
            Some(value) => info!("Consent Value: {:?}", value),
            None => info!("No value found"),
        }

        match topic_table.get_store()?.get("changeOfAddress").await {
            Some(value) => info!("Topic Store Value: {:?}", value),
            None => info!("No value found"),
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
use std::collections::HashMap;

use peridot::init::init_tracing;
use peridot::state::backend::persistent::PersistentStateBackend;
use peridot::{app::PeridotAppBuilder, state::backend::in_memory::InMemoryStateBackend};
use rdkafka::ClientConfig;

use peridot::app::PeridotTable;
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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing(LevelFilter::INFO);

    let mut source = ClientConfig::new();

    source
        .set("bootstrap.servers", "servicesaustralia.com.au:29092")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "false")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", "5D5PMQEIB2VD633V")
        .set(
            "sasl.password",
            "ee5DtvJYWFXYJ/MF+bCJVBil8+xEH5vuZ6c8Fk2qjD0xSGhlDnXr9w4D9LTUQv2t",
        )
        .set("group.id", "rust-test10")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug);

    let app_builder = PeridotAppBuilder::new(&source).unwrap();

    info!("Creating table");

    let consent_table = app_builder.table::<String, ConsentGrant, PersistentStateBackend<ConsentGrant>>("consent.Client").unwrap()
        .build()
        .await
        .unwrap();

    let topic_table = app_builder.table::<String, Topic, PersistentStateBackend<Topic>>("topicStore.Global").unwrap()
        .build()
        .await
        .unwrap();

    info!("Creating stream");

    let stream = app_builder.stream::<String, String>("changeOfAddress").unwrap();

    info!("Running app");

    let _ = app_builder
        .run()
        .await?;

    loop {
        match consent_table.get_store().unwrap().get("jon").await {
            Some(value) => info!("Consent Value: {:?}", value),
            None => info!("No value found"),
        }

        match topic_table.get_store().unwrap().get("changeOfAddress").await {
            Some(value) => info!("Topic Store Value: {:?}", value),
            None => info!("No value found"),
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    Ok(())
}
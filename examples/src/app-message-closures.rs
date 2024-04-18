use std::collections::HashMap;
use std::fmt::Display;

use peridot::app::builder::AppBuilder;
use peridot::engine::wrapper::serde::Json;
use peridot::init::init_tracing;
use peridot::message::types::KeyValue;
use peridot::task::Task;
use rdkafka::ClientConfig;

use rdkafka::config::RDKafkaLogLevel;
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

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChangeOfAddress {
    #[serde(alias = "Address")]
    address: String,
    #[serde(alias = "City")]
    city: String,
    #[serde(alias = "State")]
    state: String,
    #[serde(alias = "Postcode")]
    postcode: String,
}

impl Display for ChangeOfAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ChangeOfAddress {{ address: {}, city: {}, state: {}, postcode: {} }}",
            self.address, self.city, self.state, self.postcode
        )
    }
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct Client {
    owner_type: String,
    owner: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing(LevelFilter::INFO);

    let mut client_config = ClientConfig::new();

    let group = "rust-test35";
    let group_instance = "peridot-instance-1";

    client_config
        .set("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "false")
        .set("group.id", group)
        .set("group.instance.id", group_instance)
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug);

    let mut app = AppBuilder::new()
        .with_client_config(client_config)
        .build()
        .expect("Failed to build app.");

    app.task::<String, Json<ChangeOfAddress>>("changeOfAddress")
        .map(|kv: KeyValue<String, ChangeOfAddress>| KeyValue::from((kv.key, kv.value.address)))
        .map(|kv: KeyValue<String, String>| KeyValue::from((kv.key, kv.value)))
        .into_topic::<String, String>("genericTopic");

    app.run().await?;

    Ok(())
}

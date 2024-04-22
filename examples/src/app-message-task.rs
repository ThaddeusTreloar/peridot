use std::collections::HashMap;
use std::fmt::Display;

use peridot::app::builder::AppBuilder;
use peridot::app::config::builder::PeridotConfigBuilder;
use peridot::engine::util::ExactlyOnce;
use peridot::engine::wrapper::serde::json::Json;
use peridot::init::init_tracing;
use peridot::message::types::{KeyValue, Value};
use peridot::pipeline::stream::{PipelineStream, PipelineStreamExt};
use peridot::state::backend::in_memory::InMemoryStateBackend;
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

fn partial_task(
    input: impl PipelineStream<KeyType = String, ValueType = ChangeOfAddress> + Send,
) -> impl PipelineStream<KeyType = String, ValueType = String> + Send {
    input
        .map(|Value(coa)| coa)
        .map(|KeyValue(key, value)| KeyValue(key, value.address))
        .map(|KeyValue(key, value)| KeyValue(key, value))
}

fn filtering_task(
    input: impl PipelineStream<KeyType = String, ValueType = String> + Send,
) -> impl PipelineStream<KeyType = String, ValueType = String> + Send {
    input.map(|KeyValue(key, value): KeyValue<String, String>| KeyValue(key, value))
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing(LevelFilter::INFO);

    let mut client_config = ClientConfig::new();

    let group = "rust-test71";
    let group_instance = "peridot-instance-1";

    client_config
        .set("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "false")
        .set("group.id", group)
        .set("group.instance.id", group_instance)
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug);

    let app = AppBuilder::new()
        .with_config(PeridotConfigBuilder::from(&client_config).build().expect("Failed to build config."))
        .with_delivery_guarantee::<ExactlyOnce>()
        .with_state_backend::<InMemoryStateBackend>()
        .build()
        .expect("Failed to build app.");

    let _consent_table = app.table::<String, Json<ConsentGrant>>("consent.Client", "consent_table");

    app.task::<String, Json<ChangeOfAddress>>("changeOfAddress")
        .and_then(partial_task)
        .and_then(filtering_task)
        .into_topic::<String, String>("genericTopic");

    app.run().await?;

    Ok(())
}

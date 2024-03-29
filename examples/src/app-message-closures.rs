use std::collections::HashMap;
use std::fmt::Display;

use peridot::app::PeridotApp;
use peridot::engine::util::ExactlyOnce;
use peridot::init::init_tracing;
use peridot::message::types::{KeyValue, Value};
use peridot::pipeline::stream::{PipelineStream, PipelineStreamExt};
use peridot::serde_ext::Json;
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
        .map(|kv: KeyValue<String, ChangeOfAddress>| KeyValue::from((kv.key, kv.value.address)))
        .map(|(key, value)| (key, value))
        .map(|value: String| Value::from(value))
}

fn filtering_task(
    input: impl PipelineStream<KeyType = String, ValueType = String> + Send,
) -> impl PipelineStream<KeyType = String, ValueType = String> + Send {
    input.map(|kv: KeyValue<String, String>| KeyValue::from((kv.key, kv.value)))
}

fn join_task(
    input1: impl PipelineStream<KeyType = String, ValueType = String> + Send,
    _input2: impl PipelineStream<KeyType = String, ValueType = String> + Send,
) -> impl PipelineStream<KeyType = String, ValueType = String> + Send {
    input1.map(|kv: KeyValue<String, String>| KeyValue::from((kv.key, kv.value)))
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing(LevelFilter::INFO);

    let mut source = ClientConfig::new();

    let group = "rust-test35";
    let group_instance = "peridot-instance-1";

    source
        .set("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "false")
        .set("group.id", group)
        .set("group.instance.id", group_instance)
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug);

    let mut app: PeridotApp<ExactlyOnce> = PeridotApp::from_client_config(&source)?;

    app.task::<String, Json<ChangeOfAddress>>("changeOfAddress")
        .map(|kv: KeyValue<String, ChangeOfAddress>| KeyValue::from((kv.key, kv.value.address)))
        .map(|kv: KeyValue<String, String>| KeyValue::from((kv.key, kv.value)))
        .into_topic::<String, String>("genericTopic");

    app.run().await?;

    Ok(())
}

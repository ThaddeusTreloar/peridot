use std::collections::HashMap;
use std::fmt::Display;

use peridot::engine::util::ExactlyOnce;
use peridot::init::init_tracing;
use peridot::app::{PeridotApp, Task, IntoTask};
use peridot::pipeline::message::sink::PrintSink;
use peridot::pipeline::message::types::{Value, KeyValue};
use peridot::pipeline::pipeline::stream::{PipelineStreamExt, PipelineStream};
use peridot::pipeline::serde_ext::Json;
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
    #[serde(alias="Address")]
    address: String,
    #[serde(alias="City")]
    city: String,
    #[serde(alias="State")]
    state: String,
    #[serde(alias="Postcode")]
    postcode: String,
}

impl Display for ChangeOfAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ChangeOfAddress {{ address: {}, city: {}, state: {}, postcode: {} }}", self.address, self.city, self.state, self.postcode)
    }
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct Client {
    owner_type: String,
    owner: String,
}

//type PeridotStream = impl PipelineStream<KeyType = String, ValueType = String> + Send;

fn partial_task(
    input: impl PipelineStream<KeyType = String, ValueType = ChangeOfAddress> + Send,
) -> impl PipelineStream<KeyType = String, ValueType = String> + Send
{
    input.map(|kv: KeyValue<String, ChangeOfAddress>| {
        KeyValue::from((kv.key, kv.value.address))
    }).map(|(key, value)| {
        (key, value)
    }).map(|value: String|{
        Value::from(value)
    })
}

fn filtering_task(
    input: impl PipelineStream<KeyType = String, ValueType = String> + Send,
) -> impl PipelineStream<KeyType = String, ValueType = String> + Send
{
    input.map(|kv: KeyValue<String, String>| {
        KeyValue::from((kv.key, kv.value))
    })
}

fn join_task(
    input1: impl PipelineStream<KeyType = String, ValueType = String> + Send,
    _input2: impl PipelineStream<KeyType = String, ValueType = String> + Send,
) -> impl PipelineStream<KeyType = String, ValueType = String> + Send
{
    input1.map(|kv: KeyValue<String, String>| {
        KeyValue::from((kv.key, kv.value))
    })
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing(LevelFilter::INFO);

    let mut source = ClientConfig::new();

    let group = "rust-test71";
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

    app.stream_builder()
        .stream::<String, Json<ChangeOfAddress>>("changeOfAddress").expect("Failed to build pipeline")
        .map(
            |val: Value<ChangeOfAddress>| Value::from(val.value.address)
        ).into_task(&mut app)
        .into_topic::<String, String>("generic");

    app.run().await?;

    Ok(())
}
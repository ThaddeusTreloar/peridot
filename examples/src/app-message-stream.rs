use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use peridot::app::pstream::PStream;
use peridot::app::ptable::{PTable, PeridotTable};
use peridot::engine::util::ExactlyOnce;
use peridot::init::init_tracing;
use peridot::app::PeridotApp;
use peridot::pipeline::message::sink::PrintSink;
use peridot::pipeline::message::types::{Value, KeyValue};
use peridot::pipeline::pipeline::stream::{PipelineStreamExt, PipelineStreamSinkExt};
use peridot::pipeline::serde_ext::Json;
use peridot::state::backend::in_memory::InMemoryStateBackend;
use peridot::state::backend::persistent::PersistentStateBackend;
use rdkafka::ClientConfig;

use peridot::state::ReadableStateStore;
use rdkafka::config::{RDKafkaLogLevel, FromClientConfig};
use rdkafka::producer::{BaseProducer, Producer};
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

fn extract_city(Value{ value }: Value<ChangeOfAddress>) -> Value<String> {
    Value::from(value.city)
}

fn passthrough(Value{ value }: Value<String>) -> Value<String> {
    Value::from(value)
}

fn kv_passthrough(kv: KeyValue<String, String>) -> KeyValue<String, String> {
    kv
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing(LevelFilter::INFO);

    let mut source = ClientConfig::new();

    let group = "rust-test28";

    source
        .set("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "false")
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug);

    let app = Arc::new(PeridotApp::<ExactlyOnce>::from_client_config(&source)?);

    //let _table = app
    //    .table::<String, ConsentGrant, InMemoryStateBackend<_>>("consent.Client")
    //    .await?;

    app.run().await?;

    let _mappped = app.stream::<String, Json<ChangeOfAddress>>("changeOfAddress")?
        .map(extract_city)
        .sink::<PrintSink<String, String>>("someTopic").await;

    Ok(())
}
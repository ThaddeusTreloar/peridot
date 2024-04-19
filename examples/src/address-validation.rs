use std::collections::HashMap;
use std::fmt::Display;

use peridot::app::builder::AppBuilder;
use peridot::engine::util::ExactlyOnce;
use peridot::engine::wrapper::serde::Json;
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

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct ValidatedAddress {
    address: String,
    city: String,
    state: String,
    postcode: String,
    is_valid: bool,
}

impl Display for ValidatedAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unimplemented!("")
    }
}

fn validate_address(Value(address): Value<ChangeOfAddress>) -> Value<ValidatedAddress> {
    let valid = false;

    // some validation logic

    let ChangeOfAddress {
        address,
        city,
        state,
        postcode
    } = address;

    let validated = ValidatedAddress {
        address,
        city,
        state,
        postcode,
        is_valid: valid
    };

    Value(validated)
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

    let mut app = AppBuilder::new()
        .with_client_config(client_config)
        .with_delivery_guarantee::<ExactlyOnce>()
        .with_state_backend::<InMemoryStateBackend>()
        .build()
        .expect("Failed to build app.");

    let s = app.table::<String, String>("asd", "table_name");

    app.task::<String, Json<ChangeOfAddress>>("changeOfAddress")
        .map(validate_address)
        .into_topic::<String, Json<_>>("genericTopic");

    s.map(|KeyValue(k, v)|KeyValue(k, v.len()))
        .into_topic::<String, Json<usize>>("topic");

    app.run().await?;

    Ok(())
}

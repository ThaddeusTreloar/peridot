use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use peridot::app::pstream::PStream;
use peridot::app::ptable::{PTable, PeridotTable};
use peridot::engine::util::ExactlyOnce;
use peridot::init::init_tracing;
use peridot::app::PeridotApp;
use peridot::pipeline::message::types::{Value, KeyValue};
use peridot::pipeline::pipeline::PipelineStreamExt;
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

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct Client {
    owner_type: String,
    owner: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing(LevelFilter::INFO);

    let mut source = ClientConfig::new();

    let group = "rust-test25";

    source
        .set("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "false")
        .set("group.id", group)
        .set("auto.offset.reset", "latest")
        .set_log_level(RDKafkaLogLevel::Debug);

    let app = Arc::new(PeridotApp::<ExactlyOnce>::from_client_config(&source)?);

    let table = app
        .table::<String, ConsentGrant, InMemoryStateBackend<_>>("consent.Client")
        .await?;

    let stream = app
        .stream("changeOfAddress").await?
        .stream()
        .map(|f| async {
            f
        }).map(
            |f| {
                f
            }
        ).filter_map(
            |s| {
                Some(s)
            }
        );

    let engine = app.engine_ref();

    let pstream = PStream::<ExactlyOnce>::new_new::<String, Json<ChangeOfAddress>>();

    let mappped = pstream.map(
        |v: Value<ChangeOfAddress>| {
            KeyValue::from((String::from("Asd"), v.value.city))
        }
    );

    //let mut sink = app
    //    .sink::<String, Json<ChangeOfAddress>>("genericTopic").await?;
    
    app.run().await?;
    

    Ok(())
}
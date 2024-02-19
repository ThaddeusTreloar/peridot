use std::collections::HashMap;
use std::fmt::Display;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::{StreamExt, Future};
use peridot::app::error::PeridotAppRuntimeError;
use peridot::app::pstream::PStream;
use peridot::app::ptable::{PTable, PeridotTable};
use peridot::engine::util::{ExactlyOnce, DeliveryGuaranteeType};
use peridot::init::init_tracing;
use peridot::app::{PeridotApp, AppBuilder};
use peridot::pipeline::message::sink::PrintSink;
use peridot::pipeline::message::types::{Value, KeyValue};
use peridot::pipeline::pipeline::sink::SinkError;
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

fn map_changes_of_address_to_city(
    app: &AppBuilder<ExactlyOnce>,
) -> Pin<Box<dyn Future<Output = Result<(), PeridotAppRuntimeError>> + Send>> 
{
    Box::pin(
        app.stream::<String, Json<ChangeOfAddress>>("changeOfAddress").expect("Failed to create stream")
            .map(|kv: KeyValue<String, ChangeOfAddress>| {
                KeyValue::from((kv.key, kv.value.address))
            })
            .sink::<PrintSink<String, String>>("someTopic")
    )
}

fn sink_handler(s: Stream<String, Json<Client>>) -> IntoStream {
    unimplemented!("")
}

fn task_handler(s: Stream<String, Json<Client>>) -> IntoStream {
    unimplemented!("")
}

fn task_joiner(left: Stream<String, String>, right: Stream<String, String>) -> IntoStream {
    unimplemented!("")
}

trait FromBuilder {}

fn task<F, S, T>(task_name: &str, source_topic: T, cb: F)
where
    F: Fn(S) -> IntoStream,
    S: FromBuilder,
    T: FromTopicPool {}

fn chain<F, T, S>(task_name: &str, source_task: T, cb: F)
where
    F: Fn(S) -> IntoStream,
    S: FromBuilder,
    T: FromTaskPool {}


fn job<S>(source_task: &str, sink_topic: &str) 
where S: Sink {}


fn trace_task(s: impl Pipeline<K, V>) -> impl Pipeline<K, V> {

}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing(LevelFilter::INFO);

    let mut source = ClientConfig::new();

    let group = "rust-test31";

    source
        .set("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "false")
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug);

    let mut app: PeridotApp<ExactlyOnce> = PeridotApp::from_client_config(&source)?;

    let table_a = app.table("my_cool_app");

    let task_a = app.task("addressTopic", task_handler);
    let task_b = app.task("addressTopic", task_handler)
        .and_then(filter_handler);

    app.job(task_b(task_a, task_b, table_a));

    //let _table = app
    //    .table::<String, ConsentGrant, InMemoryStateBackend<_>>("consent.Client")
    //    .await?;

    app.run().await?;

    Ok(())
}
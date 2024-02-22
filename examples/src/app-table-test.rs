use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use futures::StreamExt;
use peridot::app::ptable::{PTable, PeridotTable};
use peridot::app::PeridotApp;
use peridot::engine::util::ExactlyOnce;
use peridot::init::init_tracing;
use peridot::pipeline::message::sink::PrintSink;
use peridot::pipeline::pipeline::sink::PipelineSinkExt;
use peridot::pipeline::pipeline::stream::PipelineStreamSinkExt;
use peridot::pipeline::serde_ext::Json;
use peridot::state::backend::in_memory::InMemoryStateBackend;
use rdkafka::ClientConfig;

use peridot::state::ReadableStateStore;
use rdkafka::config::RDKafkaLogLevel;
use tokio::select;
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

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing(LevelFilter::INFO);

    let mut source = ClientConfig::new();

    let group = "rust-test53";

    source
        .set("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "false")
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug);

    let app = PeridotApp::<ExactlyOnce>::from_client_config(&source)?;

    let table = app
        .table::<String, Json<ConsentGrant>, InMemoryStateBackend<_, _>>("consent.Client")
        .await?;

    app.run().await?;

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        match table
            .get_store()
            .unwrap()
            .get(&String::from("Oliver"))
            .await
        {
            Some(consent_grant) => {
                info!("Got consent grant: {:?}", consent_grant);
            }
            None => {
                info!("No consent grant found");
            }
        };
    }

    Ok(())
}

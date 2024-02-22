use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use peridot::app::engine::util::{AtLeastOnce, ExactlyOnce};
use peridot::app::ptable::{PTable, PeridotTable};
use peridot::app::wrappers::{
    KeyValueMessage, MessageKey, MessageValue, TransferMessageContext,
    TransferMessageContextAndKey, ValueMessage,
};
use peridot::app::PeridotApp;
use peridot::init::init_tracing;
use peridot::serde_ext::Json;
use peridot::state::backend::in_memory::InMemoryStateBackend;
use peridot::state::backend::persistent::PersistentStateBackend;
use peridot::stream::types::{KeyValue, StringKeyValue};
use rdkafka::ClientConfig;

use peridot::state::ReadableStateStore;
use rdkafka::config::{FromClientConfig, RDKafkaLogLevel};
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

    let stream = app.stream("changeOfAddress").await?;

    let mut sink = app
        .sink::<String, Json<ChangeOfAddress>>("genericTopic")
        .await?;

    app.run().await?;

    let table_state = table.get_store()?;

    let filter_func = |kv: &KeyValueMessage<String, (ChangeOfAddress, ConsentGrant)>| {
        let maybe_grant = kv
            .value()
            .1
            .map
            .get("DomainAlpha")
            .and_then(|f| f.get("DomainBravo"))
            .and_then(|f| f.get("changeOfAddress"));

        let result = match maybe_grant {
            None => {
                info!("No consent found for key: {}", kv.key());
                false
            }
            Some(consent) => {
                info!("Consent '{}' found for key: {}", *consent, kv.key());
                *consent
            }
        };

        async move { result }
    };

    match stream
        .stream::<KeyValueMessage<_, _>, (String, Json<ChangeOfAddress>)>()
        .filter_map(|kv: KeyValueMessage<String, ChangeOfAddress>| {
            let key = kv.key().clone();
            let value = kv.value().clone();

            async {
                info!("Getting consent for key: {}", key);
                match table_state.get(&key).await {
                    Some(consent_grant) => {
                        let s = kv.map(key, (value, consent_grant));

                        Some(s)
                    }
                    None => {
                        info!("Failed to get consent for key: {}", key);
                        None
                    }
                }
            }
        })
        .filter(filter_func)
        .map(|kv| {
            let value = kv.value().0.clone();

            Ok(kv.map_value::<KeyValueMessage<_, _>>(value))
        })
        .forward(&mut sink)
        .await
    {
        Ok(_) => info!("Stream completed"),
        Err(e) => info!("Stream failed: {:?}", e),
    };

    Ok(())
}

use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use peridot::app::ptable::{PTable, PeridotTable};
use peridot::init::init_tracing;
use peridot::app::PeridotApp;
use peridot::run;
use peridot::state::backend::in_memory::InMemoryStateBackend;
use peridot::state::backend::persistent::PersistentStateBackend;
use peridot::stream::types::{KeyValue, StringKeyValue};
use rdkafka::ClientConfig;

use peridot::state::ReadableStateStore;
use rdkafka::config::RDKafkaLogLevel;
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

    source
        .set("bootstrap.servers", "servicesaustralia.com.au:29092")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "true")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", "5D5PMQEIB2VD633V")
        .set(
            "sasl.password",
            "ee5DtvJYWFXYJ/MF+bCJVBil8+xEH5vuZ6c8Fk2qjD0xSGhlDnXr9w4D9LTUQv2t",
        )
        .set("group.id", "rust-test21")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug);

    let app = Arc::new(PeridotApp::from_config(&source)?);

    let table = app
        .table::<String, ConsentGrant, PersistentStateBackend<_>>("consent.Client")
        .await?;

    let stream = app
        .stream("changeOfAddress").await?;

    let mut sink = app
        .sink("orgTopic.a").await?;
    
    app.run().await?;
    
    let table_state = table.get_store()?;

    let filter_func = |kv: &StringKeyValue<(ChangeOfAddress, ConsentGrant)>| {
        let key = kv.key.clone();
        let msg = kv.value.0.clone();
        let consent_grant = kv.value.1.clone();

        let result = match consent_grant.map.get("servicesaustralia.com.au")
            .and_then(|f|f.get("medicare.com.au"))
            .and_then(|f|f.get("changeOfAddress")) {
            None => {
                info!("No consent found for key: {}", key);
                false
            },
            Some(consent) => {
                info!("Consent '{}' found for key: {}", *consent, key);
                *consent
            }
        };

        async move {result}
    };

    match stream
        .stream::<StringKeyValue<ChangeOfAddress>>().then(
            |kv: StringKeyValue<ChangeOfAddress>| {
                let key = kv.key.clone();
                let value = kv.value.clone();

                async {
                    info!("Getting consent for key: {}", key);
                    match table_state.get(&key).await {
                        Some(consent_grant) => Some(StringKeyValue::new(key, (value, consent_grant))),
                        None => {
                            info!("Failed to get consent for key: {}", key);
                            None
                        }
                    }
                }
        })
        .filter_map(|o|async{o})
        .filter(filter_func)
        .map(|kv|Ok(StringKeyValue::new(kv.key, kv.value.0)))
        .forward(&mut sink).await {
        Ok(_) => info!("Stream completed"),
        Err(e) => info!("Stream failed: {:?}", e),
    };

    Ok(())
}
use std::collections::HashMap;
use std::fmt::Display;
use std::io::{BufRead, Write};
use std::sync::Arc;
use std::time::Duration;

use peridot::app::builder::AppBuilder;
use peridot::app::config::builder::PeridotConfigBuilder;
use peridot::engine::wrapper::serde::json::Json;
use peridot::init::init_tracing;
use peridot::message::types::KeyValue;
use peridot::state::backend::view::{GetView, GetViewDistributor, ReadableStateView};
use peridot::task::Task;
use rdkafka::ClientConfig;

use rdkafka::config::RDKafkaLogLevel;
use tracing::info;
use tracing::level_filters::LevelFilter;

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

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing(LevelFilter::TRACE);

    let mut client_config = ClientConfig::new();

    let group = "rust-test35";
    let group_instance = "peridot-instance-1";

    client_config
        .set("bootstrap.servers", "kafka1:9092,kafka2:9093,kafka3:9094")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "false")
        .set("application.id", "app-message-closures")
        .set("group.id", group)
        .set("group.instance.id", group_instance)
        .set("auto.offset.reset", "earliest")
        //.set("statistics.interval.ms", "1")
        .set_log_level(RDKafkaLogLevel::Error);

    let app = AppBuilder::new()
        .with_config(
            PeridotConfigBuilder::from(&client_config)
                .build()
                .expect("Failed to build config."),
        )
        .build()
        .expect("Failed to build app.");

    let table = app
        .table::<String, Json<ConsentGrant>>("consent.Client", "consent_table")
        .await;

    let table_ref = &table;

    let dist = table_ref.get_view_distributor();

    app.task::<String, Json<ChangeOfAddress>>("changeOfAddress")
        .map(|kv: KeyValue<String, ChangeOfAddress>| KeyValue(kv.0, kv.1.address))
        .map(|kv: KeyValue<String, String>| KeyValue(kv.0, kv.1))
        .into_topic::<String, String>("genericTopic");

    table.finish();

    tokio::spawn(app.run());

    // Very dodgy test of the state store
    // Using the internal facade distributor rather than a user facing version.
    tokio::time::sleep(Duration::from_millis(1000)).await;

    use std::io::stdin;

    let facade_0 = Arc::new(dist.get_view(0));
    let facade_1 = Arc::new(dist.get_view(1));

    let stdin = stdin();

    print!("Enter key: ");
    std::io::stdout().flush();

    for line in stdin.lock().lines() {
        let name = line.unwrap();

        match facade_0.clone().get(name.clone()).await {
            Ok(Some(value)) => {
                info!("{}: {:?}", name, value);
                print!("Enter key: ");
                std::io::stdout().flush();
                continue;
            }
            Ok(None) => (),
            Err(e) => panic!("Facade failure: {}", e),
        }

        match facade_1.clone().get(name.clone()).await {
            Ok(Some(value)) => {
                info!("{}: {:?}", name, value);
                print!("Enter key: ");
                std::io::stdout().flush();
                continue;
            }
            Ok(None) => (),
            Err(e) => panic!("Facade failure: {}", e),
        }

        tracing::error!("This key, {},  is not found", name);
        print!("Enter key: ");
        std::io::stdout().flush();
    }

    Ok(())
}

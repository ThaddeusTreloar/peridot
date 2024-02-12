use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Query, State},
    routing::get,
    Json,
};
use eap::{config::Config, environment::Environment};
use peridot::{
    app::{PTable, PeridotApp, PeridotTable},
    init::init_tracing,
    state::{
        backend::{
            persistent::PersistentStateBackend, ReadableStateBackend, StateBackend,
            WriteableStateBackend,
        },
        ReadableStateStore,
    },
};
use rdkafka::{config::RDKafkaLogLevel, ClientConfig};
use tracing::level_filters::LevelFilter;

#[derive(Debug, eap::Config)]
struct AppConfig {}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct ConsentGrant {
    owner_type: String,
    owner: String,
    map: HashMap<String, HashMap<String, HashMap<String, bool>>>,
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct ConsentQuery {
    owner: String,
}

impl ConsentQuery {
    pub fn owner(&self) -> &str {
        &self.owner
    }
}

#[derive()]
struct AppState<'a, T> {
    state_store: Arc<PTable<'a, String, ConsentGrant, T>>,
}

impl<'a, T> Clone for AppState<'a, T> {
    fn clone(&self) -> Self {
        Self {
            state_store: self.state_store.clone(),
        }
    }
}

impl<'a, T> AppState<'a, T> {
    pub fn _new(state_store: PTable<'a, String, ConsentGrant, T>) -> Self {
        Self {
            state_store: Arc::new(state_store),
        }
    }

    pub fn from_arc(state_store: Arc<PTable<'a, String, ConsentGrant, T>>) -> Self {
        Self { state_store }
    }

    pub fn _get_state(&self) -> Arc<PTable<String, ConsentGrant, T>> {
        self.state_store.clone()
    }
}

async fn get_consent<T>(
    state: State<Arc<AppState<'_, T>>>,
    param: Query<ConsentQuery>,
) -> impl axum::response::IntoResponse
where
    T: StateBackend
        + ReadableStateBackend<ConsentGrant>
        + WriteableStateBackend<ConsentGrant>
        + Send
        + Sync
        + 'static,
{
    let item: ConsentGrant = state
        .state_store
        .get_store()
        .unwrap()
        .get(param.owner())
        .await
        .unwrap();

    Json(item)
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_tracing(LevelFilter::INFO);

    let mut source = ClientConfig::new();

    source
        .set("bootstrap.servers", "servicesaustralia.com.au:29092")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "false")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", "5D5PMQEIB2VD633V")
        .set(
            "sasl.password",
            "ee5DtvJYWFXYJ/MF+bCJVBil8+xEH5vuZ6c8Fk2qjD0xSGhlDnXr9w4D9LTUQv2t",
        )
        .set("group.id", "rust-test61")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug);

    let app = PeridotApp::from_config(&source)?;

    let consent_table: Arc<PTable<String, ConsentGrant, PersistentStateBackend<_>>> =
        Arc::new(app.table("consent.Client")?.build().await?);

    let app_state = Arc::new(AppState::from_arc(consent_table));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();

    let routes = axum::Router::new()
        .route("/", get(get_consent))
        .with_state(app_state);

    app.run().await?;

    Ok(axum::serve(listener, routes).await?)
}

use std::{collections::HashMap, time::Duration, sync::Arc};

use axum::{routing::get, extract::{State, Query}, Json};
use eap::{config::Config, environment::Environment};
use peridot::{
    init::init_tracing,
    state::{
        backend::{in_memory::InMemoryStateBackend, persistent::PersistantStateBackend},
        ReadableStateStore, StateStore,
    },
};
use rdkafka::{config::RDKafkaLogLevel, ClientConfig};
use tracing::{info, level_filters::LevelFilter};

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

#[derive(Debug)]
struct AppState<T> 
where T: ReadableStateStore<ConsentGrant> {
    state_store: Arc<T>,
}

impl <T> Clone for AppState<T> 
where T: ReadableStateStore<ConsentGrant> {
    fn clone(&self) -> Self {
        Self {
            state_store: self.state_store.clone()
        }
    }
}

impl<T> AppState<T> 
where T: ReadableStateStore<ConsentGrant> {
    pub fn new(state_store: T) -> Self {
        Self {
            state_store: Arc::new(state_store)
        }
    }

    pub fn get_state(&self) -> Arc<T> {
        self.state_store.clone()
    }
}

type PersistentStateStore<'a> = StateStore<'a, PersistantStateBackend<ConsentGrant>, ConsentGrant>;

async fn get_consent(state: State<Arc<AppState<PersistentStateStore<'_>>>>, param: Query<ConsentQuery>) -> impl axum::response::IntoResponse {
    let item: ConsentGrant = state
        .state_store
        .get(param.owner()).await.unwrap();

    Json(item)
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
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
        .set("group.id", "rust-test4")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug);

    let backend: PersistantStateBackend<ConsentGrant> = PersistantStateBackend::
        try_from_file(
            std::path::Path::new("/tmp/peridot.api.state_store.db")
        )
        .await
        .unwrap();

    let state_store: StateStore<PersistantStateBackend<_>, ConsentGrant> = StateStore::from_consumer_config_and_backend("consent.Client", &source, backend)
        .unwrap();

    let app_state = Arc::new(AppState::new(state_store));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();

    let routes = axum::Router::new()
        .route("/", get(get_consent))
        .with_state(app_state);

    axum::serve(listener, routes).await
}

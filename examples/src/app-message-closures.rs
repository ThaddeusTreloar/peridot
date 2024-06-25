/*
 * Copyright 2024 Thaddeus Treloar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

use std::collections::HashMap;
use std::fmt::Display;

use peridot::app::builder::AppBuilder;
use peridot::app::config::builder::PeridotConfigBuilder;
use peridot::engine::wrapper::serde::json::Json;
use peridot::init::init_tracing;
use peridot::task::Task;

use serde::Serialize;
use tracing::level_filters::LevelFilter;

#[derive(Debug, Serialize)]
struct CombinedValues(ChangeOfAddress, ConsentGrant);

impl Display for CombinedValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CombinedValues {{ ChangeOfAddress: {}, ConsentGrant: {} }}",
            self.0, self.1
        )
    }
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct ConsentGrant {
    owner_type: String,
    owner: String,
    map: HashMap<String, HashMap<String, HashMap<String, bool>>>,
}

impl Display for ConsentGrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConsentGrant {{ owner_type: {}, owner: {}, map: {:?} }}",
            self.owner_type, self.owner, self.map
        )
    }
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
    init_tracing(LevelFilter::DEBUG);

    let mut peridot_config = PeridotConfigBuilder::new();

    peridot_config
        .set("bootstrap.servers", "kafka1:9092,kafka2:9093,kafka3:9094")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "false")
        .set("application.id", "app-message-closures")
        .set("auto.offset.reset", "earliest");

    let app = AppBuilder::new()
        .with_config(peridot_config.build()?)
        .build()
        .expect("Failed to build app.");

    let table = app
        .table::<String, Json<ConsentGrant>>("consent.Client", "consent_table")
        .await;

    app.task::<String, Json<ChangeOfAddress>>("changeOfAddress")
        .join(&table, CombinedValues)
        .into_topic::<String, Json<CombinedValues>>("genericTopic");

    table.finish();

    Ok(app.run().await?)
}

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
use peridot::bencher::Bencher;
use peridot::engine::wrapper::serde::json::Json;
use peridot::init::init_tracing;
use peridot::task::Task;
use rdkafka::ClientConfig;

use rdkafka::config::RDKafkaLogLevel;
use tracing::level_filters::LevelFilter;

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

    let mut client_config = ClientConfig::new();

    let group = "rust-test35";
    let group_instance = "peridot-instance-1";

    client_config
        .set("bootstrap.servers", "kafka1:9092,kafka2:9093,kafka3:9094")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "false")
        .set("application.id", "app-message-bench")
        .set("group.id", group)
        .set("group.instance.id", group_instance)
        .set("auto.offset.reset", "earliest")
        .set("statistics.interval.ms", "50")
        .set("queue.buffering.max.messages", "500000")
        .set("fetch.queue.backoff.ms", "10")
        .set_log_level(RDKafkaLogLevel::Error);

    let app = AppBuilder::new()
        .with_config(
            PeridotConfigBuilder::from(&client_config)
                .build()
                .expect("Failed to build config."),
        )
        .build()
        .expect("Failed to build app.");

    let message_count: i64 = 1_000_000;

    let (sender, receiver) = tokio::sync::mpsc::channel(message_count as usize);

    let bencher = Bencher::new(message_count, receiver);

    app.task::<String, Json<ChangeOfAddress>>("inputTopic")
        .into_bench::<String, Json<ChangeOfAddress>>("outputTopic", sender);

    //app.task::<String, Json<ChangeOfAddress>>("inputTopic")
    //    .into_topic::<String, Json<ChangeOfAddress>>("outputTopic");

    //Ok(app.run().await?)

    tokio::spawn(app.run());
    bencher.await;
    Ok(())
}

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

use std::{fmt::Display, process::exit, time::Duration};

use futures::StreamExt;
use peridot::{
    engine::wrapper::serde::{json::Json, PeridotDeserializer, PeridotSerializer},
    init::init_tracing,
};
use rdkafka::{
    config::{FromClientConfig, RDKafkaLogLevel},
    consumer::{BaseConsumer, Consumer, StreamConsumer},
    message::{BorrowedMessage, OwnedMessage},
    producer::{BaseProducer, FutureProducer, FutureRecord, Producer},
    util::DefaultRuntime,
    ClientConfig, Message, TopicPartitionList,
};
use tokio::time::Instant;
use tracing::{debug, info, level_filters::LevelFilter};

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
async fn main() {
    init_tracing(LevelFilter::INFO);

    let mut client_config = ClientConfig::new();

    let group = "baseline_bench";

    client_config
        .set("bootstrap.servers", "kafka1:9092,kafka2:9093,kafka3:9094")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "false")
        .set("group.id", group)
        .set("enable.idempotence", "true")
        .set("isolation.level", "read_committed")
        .set("group.instance.id", "baseline_bench")
        .set("transactional.id", "baseline_bench")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Error);

    let consumer = StreamConsumer::<_, DefaultRuntime>::from_config(&client_config)
        .expect("Failed to create consumer");

    consumer
        .subscribe(&["inputTopic"])
        .expect("Failed to subscribe.");

    let mut message_stream = consumer.stream();

    let producer = FutureProducer::<_, DefaultRuntime, _>::from_config(&client_config)
        .expect("Failed to create producer.");

    producer
        .init_transactions(Duration::from_millis(1000))
        .expect("init");

    producer.begin_transaction().expect("begin");

    let mut counter = 0;

    let start = Instant::now();

    let partitions = 6;
    let message_count = 100_000;

    let mut offsets = vec![0; partitions];
    let mut interval = Instant::now();

    while let Some(record) = message_stream.next().await {
        let message = record.expect("Consumer error");

        let deser_start = Instant::now();

        let key = String::deserialize(message.key().unwrap()).expect("Deser failure");
        let value = Json::<ChangeOfAddress>::deserialize(message.payload().unwrap())
            .expect("Deser failure");

        debug!("Deser time: {}µs", deser_start.elapsed().as_micros());

        let ser_start = Instant::now();
        let ser_key = <String as PeridotSerializer>::serialize(&key).unwrap();
        let ser_value = Json::<ChangeOfAddress>::serialize(&value).unwrap();
        debug!("Ser time: {}µs", ser_start.elapsed().as_micros());

        let timestamp = message.timestamp().to_millis().unwrap();

        let record_build = Instant::now();
        let record = FutureRecord {
            topic: "outputTopic",
            partition: None,
            key: Some(&ser_key),
            payload: Some(&ser_value),
            timestamp: Some(timestamp),
            headers: None,
        };
        debug!("Ser time: {}µs", record_build.elapsed().as_micros());

        let producer_time = Instant::now();
        producer.send_result(record).expect("Failed to produce");
        debug!("Produce time: {}µs", producer_time.elapsed().as_millis());

        let _ = std::mem::replace(&mut offsets[message.partition() as usize], message.offset());

        counter += 1;

        if interval.elapsed().as_millis() > 100 {
            let send_offsets = Instant::now();
            let mut tpl = TopicPartitionList::new();

            tpl.add_partition_offset(
                "inputTopic",
                message.partition(),
                rdkafka::Offset::Offset(message.offset() + 1),
            )
            .expect("add partition");

            producer
                .send_offsets_to_transaction(
                    &tpl,
                    &consumer.group_metadata().unwrap(),
                    Duration::from_millis(1000),
                )
                .expect("Send offsets");

            debug!(
                "Send offsets time: {}ms",
                send_offsets.elapsed().as_millis()
            );
            let commit_time = Instant::now();
            producer
                .commit_transaction(Duration::from_millis(1000))
                .expect("Commit transaction");

            info!("Commit time: {}ms", commit_time.elapsed().as_millis());
            producer.begin_transaction().expect("begin");

            info!("");
            info!(
                "Committed transaction, total: {}, time_taken: {}s",
                offsets.iter().sum::<i64>(),
                start.elapsed().as_secs(),
            );

            if offsets.iter().sum::<i64>() > message_count {
                let time_taken = start.elapsed().as_millis();
                let mps = offsets.iter().sum::<i64>() as f64 / (time_taken as f64 / 1000_f64);

                info!("Time taken: {}ms", time_taken);
                info!("Messages per sec: {:.2}m/s", mps);
                exit(0);
            } else {
                offsets
                    .iter()
                    .enumerate()
                    .for_each(|(p, o)| info!("partition: {}, offset: {}", p, o));
            }

            interval = Instant::now()
        }
    }
}

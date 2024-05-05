use std::time::Duration;

use peridot::init::init_tracing;
use rdkafka::{
    config::{FromClientConfig, RDKafkaLogLevel},
    consumer::{BaseConsumer, Consumer},
    producer::{BaseProducer, FutureProducer, FutureRecord, Producer},
    util::DefaultRuntime,
    ClientConfig, Message, TopicPartitionList,
};
use tracing::{info, level_filters::LevelFilter};

#[tokio::main]
async fn main() {
    init_tracing(LevelFilter::DEBUG);

    let mut client_config = ClientConfig::new();

    let group = "transaction-test";

    client_config
        .set("bootstrap.servers", "kafka1:9092,kafka2:9093,kafka3:9094")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.auto.commit", "false")
        .set("group.id", group)
        .set("enable.idempotence", "true")
        .set("isolation.level", "read_committed")
        .set("transactional.id", "changeOfAddress")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Error);

    let consumer = BaseConsumer::from_config(&client_config).expect("Failed to create consumer");

    consumer
        .subscribe(&["changeOfAddress"])
        .expect("Failed to subscribe.");

    let msg = loop {
        match consumer.poll(Duration::from_millis(2000)) {
            Some(result) => {
                break result.unwrap();
            }
            None => continue,
        };
    };

    let producer = FutureProducer::<_, DefaultRuntime, _>::from_config(&client_config)
        .expect("Failed to create producer.");

    producer
        .init_transactions(Duration::from_millis(1000))
        .expect("init");

    producer.begin_transaction().expect("begin");

    let key = String::from("SomeKey");
    let value = String::from("SomeValue");
    let timestamp = msg.timestamp().to_millis().unwrap();

    let record = FutureRecord {
        topic: "genericTopic",
        partition: None,
        key: Some(&key),
        payload: Some(&value),
        timestamp: Some(timestamp),
        headers: None,
    };

    let (partition, offset) = match producer.send(record, Duration::from_millis(100)).await {
        Ok(po) => {
            info!("success, partition: {}, offset, {}", po.0, po.1);
            po
        }
        Err(e) => {
            tracing::error!("Error: {}", e.0);
            panic!("")
        }
    };

    let mut tpl = TopicPartitionList::new();

    tpl.add_partition_offset(
        "changeOfAddress",
        partition,
        rdkafka::Offset::Offset(offset + 1),
    )
    .expect("add partition");

    info!("Send offsets");

    producer
        .send_offsets_to_transaction(
            &tpl,
            &consumer.group_metadata().unwrap(),
            Duration::from_millis(1000),
        )
        .expect("Send offsets");

    info!("commit transactions");

    //producer.abort_transaction(Duration::from_millis(1000))
    //    .expect("Failed to abort transaction.");

    //info!("aborted");
    producer
        .commit_transaction(Duration::from_millis(1000))
        .expect("Commit transaction");

    info!("committed");
}

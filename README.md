# Peridot

A streams library for the Rust ecosytem. This library is heavily inspired by Kafka Streams and aims to provide a similar API for Rust.
It is currently in early development and is not yet feature complete.

## Features

This library aims to provide a simple and elegant API, similar to those provided by the likes of Axum, SurrealDb, and other excellent Rust libraries.
Currently, the Rust ecosystem is lacking solid, easy to use Kafka Libraries.

Below is a list of the features currently being architected:

- ✅ Streams DSL architecture
- ✅ State store architecture
- ✅ Exactly once semantics (Streams)
- ✅ Exactly once semantics (Tables)
- ✅ Changelog state rebuilding
- 🚧 Automatic topic creation (changelogs etc.)
- 🚧 Joins
- 🚧 Unified Service layer API 
- 🚧 Event time processing
- 🚧 Import/Export integration API for any types that implement futures::Stream 
- ❌ Timestamp extraction
- ❌ Feature complete DSL operations (akin to Kafka Streams DSL, Joins, Aggregations, etc)
- ❌ Windowed operations (Stream-Stream Joins)
- ❌ Result 'style' dead letter support (impl FromResidual)
- ❌ Schema Registry Integration

Peridot Adjacent Features:
- ❌ Schema registry build dependency for codegen

The architecture for these features are unstable and subject to change.

## Limitations

This section details the forseen limitations of the Rust implementation. These can be due to language restrictions or upstream library limitations.  

### Standby Replicas

Currently, the upstream C++ library, librdkafka, does not expose the consumer assignment API in the same way the Java based Kafka Clients library does. As such, neither do the Rust bindings in rdkafka. Because of this it would be incredibly difficult, if not impossible to implement standby replicas assignment without escaping the Kafka ecosystem. There are currently pull requests to expose this API in the original C++ library but they have not been accepted yet.

### Mixing State Backend Implementations

This library uses static dispatch when dealing with state store implementations, and therefore the engine can only store a collection of exact typed state backend. What this means is that when the app is initially configured to utilise a particular type that Implements StateBackend, all state store instances are created with that type. Due to the way the StateBackend trait is designed, StateBackend implementations are not object safe and cannot be stored with dynamic dispatch, eg: Vec< Arc< dyn StateBackend >>. It will be evaluated later in development if there is a use case for dynamic state backend types, but for the moment, this is not being considered.

## Performance

Although it is still early and some features aren't implemented, performance is looking quite good. Using a basic timing tool (--bin app-message-bench). On a 1 million message log and a input_topic > deser(json) > ser(json) > output_topic pipeline, peridot is achieving throughput of:
```
INFO peridot::bencher: peridot/src/bencher/mod.rs:44: Time taken: 1515ms
INFO peridot::bencher: peridot/src/bencher/mod.rs:45: Messages per sec: 682514.85m/s
```
The test was performed with transactional producers and a commit interval of 100ms.

For reference, a similar test on Kafka Streams yielded ~130k m/s.

Benchmark performed on a Framework 13 Ryzen™ 7 7840U 32GB (Fedora Linux 39). The clusters were hosted on docker, and on the same machine that was running the test. The cluster contained 3 nodes and the topics were configured as:
``` 
Topic: outputTopic      TopicId: *  PartitionCount: 6       ReplicationFactor: 3    
Configs: 
        Topic: outputTopic      Partition: 0    Leader: 3       Replicas: 3,1,2 Isr: 3,1,2
        Topic: outputTopic      Partition: 1    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3
        Topic: outputTopic      Partition: 2    Leader: 2       Replicas: 2,3,1 Isr: 2,3,1
        Topic: outputTopic      Partition: 3    Leader: 2       Replicas: 2,3,1 Isr: 2,3,1
        Topic: outputTopic      Partition: 4    Leader: 3       Replicas: 3,1,2 Isr: 3,1,2
        Topic: outputTopic      Partition: 5    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3
```

## Example

```
use peridot::prelude::*;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct Client {
    owner_type: String,
    owner: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut peridot_config = PeridotConfigBuilder::new();

    peridot_config
        .set("bootstrap.servers", "kafka1:9092,kafka2:9093,kafka3:9094")

    ... some normal kafka configuration ...

    let app = AppBuilder::new()
        .with_config(peridot_config.build()?)
        .build()
        .expect("Failed to build app.");

    app.task::<String, Json<Client>>("clientTopic")
        .map(|(key, value)| (key, value.len()))
        .into_topic::<String, Json<u64>>("genericTopic");

    app.run().await?;

    Ok(())
}
```

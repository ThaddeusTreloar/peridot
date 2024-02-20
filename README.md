# Peridot

A streams library for the Rust ecosytem. This library is heavily inspired by Kafka Streams and aims to provide a similar API for Rust.
It is currently in early development and is not yet feature complete.

## Features

This library aims to provide a simple and elegant API, similar to those provided by the likes of Axum, SurrealDb, and other excellent Rust libraries.
Currently, the Rust ecosystem is lacking solid, easy to use Kafka Libraries.

To be considered an MVP this library should provide the following features:

- [x] Streams
- [x] Tables
- [x] Exactly once semantics (Streams)
- [ ] Exactly once semantics (Tables)
- [ ] Changelogs
- [ ] Feature complete DSL operations (akin to Kafka Streams DSL, Joins, Aggregations, etc)
- [ ] Windowed operations
- [ ] Result 'style' dead letter support (impl FromResidual)
- [ ] Schema Registry Integration 

Peridot Adjacent Features:
- [ ] Schema registry build dependency for codegen

## Example

Tasks can be created with pipeline templates as such:
```
use peridot::prelude::*;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct Client {
    owner_type: String,
    owner: String,
}

fn partial_task(
    input: impl PipelineStream<KeyType = String, ValueType = ChangeOfAddress> + Send,
) -> impl PipelineStream<KeyType = String, ValueType = String> + Send
{
    input.map(|kv: KeyValue<String, ChangeOfAddress>| {
        KeyValue::from((kv.key, kv.value.address))
    })
}


#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut source = ClientConfig::new();

    ... some normal kafka configuration ...

    let app = PeridotApp::from_client_config(&source)?;

    app.task::<String, Json<Client>, _, _>("clientTopic", partial_task)
        .into_topic::<String, String, PrintSink<String, String, _, _>>("genericTopic");

    app.run().await?;

    Ok(())
}
```

You can also use closures directly:

```
use peridot::prelude::*;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct Client {
    owner_type: String,
    owner: String,
}

fn partial_task(
    input: impl PipelineStream<KeyType = String, ValueType = ChangeOfAddress> + Send,
) -> impl PipelineStream<KeyType = String, ValueType = String> + Send
{
    input.map(|kv: KeyValue<String, ChangeOfAddress>| {
        KeyValue::from((kv.key, kv.value.address))
    })
}


#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut source = ClientConfig::new();

    ... some normal kafka configuration ...

    let app = PeridotApp::from_client_config(&source)?;

    app.head_task::<String, Json<Client>, _, _>("clientTopic")
        .map(
            |kv: KeyValue<String, ChangeOfAddress>| KeyValue::from((kv.key, kv.value.address))
        )
        .into_topic::<String, String, PrintSink<String, String, _, _>>("genericTopic");

    app.run().await?;

    Ok(())
}
```
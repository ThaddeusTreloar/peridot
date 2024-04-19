# Peridot

A streams library for the Rust ecosytem. This library is heavily inspired by Kafka Streams and aims to provide a similar API for Rust.
It is currently in early development and is not yet feature complete.

## Features

This library aims to provide a simple and elegant API, similar to those provided by the likes of Axum, SurrealDb, and other excellent Rust libraries.
Currently, the Rust ecosystem is lacking solid, easy to use Kafka Libraries.

To be considered an MVP this library should provide the following features:

- ✅ Streams DSL architecture
- 🚧 State store architecture
- 🚧 Tables
- 🚧 Exactly once semantics (Streams)
- ❌ Exactly once semantics (Tables)
- 🚧 Changelogs
- ❌ Feature complete DSL operations (akin to Kafka Streams DSL, Joins, Aggregations, etc)
- ❌ Windowed operations
- ❌ Result 'style' dead letter support (impl FromResidual)
- ❌ Schema Registry Integration
- ❌ Preprocess service layers

Peridot Adjacent Features:
- ❌ Schema registry build dependency for codegen

Stretch Goals:
- ❌ Lazy pipeline initialisation
- ❌ Lazy message deserialisation

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
) -> impl PipelineStream<KeyType = String, ValueType = usize> + Send
{
    input.map(|KeyValue(key, value)| {
        KeyValue(key, value.len())
    })
}


#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut client_config = ClientConfig::new();

    ... some normal kafka configuration ...

    let app = PeridotApp::from_client_config(&client_config)?;

    app.task::<String, Json<Client>>("clientTopic")
        .and_then(partial_task)
        .into_topic::<String, Json<usize>>("genericTopic");

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

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut client_config = ClientConfig::new();

    ... some normal kafka configuration ...

    let app = PeridotApp::from_client_config(&client_config)?;

    app.task::<String, Json<Client>>("clientTopic")
        .map(
            |(key, value)| (key, value.len())
        )
        .into_topic::<String, Json<usize>>("genericTopic");

    app.run().await?;

    Ok(())
}
```

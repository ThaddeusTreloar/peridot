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

## Example

Using tables:

```
use peridot::prelude::*;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct Client {
    owner_type: String,
    owner: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut source = ClientConfig::new();

    ... some normal kafka configuration ...

    let app = PeridotApp::from_client_config(&source)?;

    let table = app
        .table::<String, Json<ConsentGrant>, InMemoryStateBackend<_, _>>("clientsTopic")
        .await?;

    app.run().await?;

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        match table.get_store()
            .unwrap()
            .get(&String::from("Oliver")).await 
        {
            Some(client) => {
                info!("Got client record: {:?}", client);
            },
            None => {
                info!("No client record found");
            }
        };
    }
}
```

Using streams:

```
use peridot::prelude::*;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct Client {
    owner_type: String,
    owner: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut source = ClientConfig::new();

    ... some normal kafka configuration ...

    let app = PeridotApp::from_client_config(&source)?;

    app.run().await?;

    let _ = app.stream::<String, Json<Client>>("clientTopic")?
        .map(|kv: Value<Client>| {
            Value::from(kv.value.owner_type)
        })
        .sink::<PrintSink<String, String>>("clientTypesTopic").await;

    Ok(())
}
```
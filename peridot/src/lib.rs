/*!
# Peridot

A streams library for the Rust ecosytem. This library is heavily inspired by Kafka Streams and aims to provide a similar API for Rust.
It is currently in early development and is not yet feature complete.

## Features

This library aims to provide a simple and elegant API, similar to those provided by the likes of Axum, SurrealDb, and other excellent Rust libraries.
Currently, the Rust ecosystem is lacking solid, easy to use Kafka Libraries.

Tasks can be created with pipeline templates as such:
...
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

    // some normal kafka configuration 

    let app = PeridotApp::from_client_config(&client_config)?;

    app.task::<String, Json<Client>>("clientTopic")
        .and_then(partial_task)
        .into_topic::<String, Json<usize>>("genericTopic");

    app.run().await?;

    Ok(())
}
...
You can also use closures directly:
...
use peridot::prelude::*;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
struct Client {
    owner_type: String,
    owner: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut client_config = ClientConfig::new();

    // some normal kafka configuration 

    let app = PeridotApp::from_client_config(&client_config)?;

    app.task::<String, Json<Client>>("clientTopic")
        .map(
            |(key, value)| (key, value.len())
        )
        .into_topic::<String, Json<usize>>("genericTopic");

    app.run().await?;

    Ok(())
}
...
 */
#![allow(refining_impl_trait)]

pub mod app;
pub mod engine;
pub mod init;
pub mod message;
pub mod pipeline;
pub mod state;
pub mod task;
pub(crate) mod util;

use tracing::info;


pub const HELP_URL: &str = "https://github.com/ThaddeusTreloar/peridot/blob/master/docs";

pub fn help(help_topic: &str) -> String {
    let resource = format!("{}/{}.md", HELP_URL, help_topic);

    info!("More information can be found at {}", resource);

    resource
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_help() {
        assert_eq!(
            help("auto-commit"),
            String::from(
                "https://github.com/ThaddeusTreloar/peridot/blob/master/docs/auto-commit.md"
            )
        );
    }
}

use std::{collections::HashMap, sync::Arc, time::Duration, marker::PhantomData};

use rdkafka::{consumer::{StreamConsumer, Consumer}, ClientConfig, Message, message::OwnedMessage};
use futures::{StreamExt, Stream};
use tokio::sync::RwLock;

use self::error::StateStoreCreationError;

pub mod error;

pub trait ReadableStateStore<T> {
    async fn get(&self, key: &str) -> Option<T>;
}

pub trait WriteableStateStore<T> {
    async fn set(&self, key: &str, value: T) -> Option<T>;
    async fn delete(&self, key: &str) -> Option<T>;
}

pub struct InMemoryStateStore<'a, T> 
where T: serde::Deserialize<'a>
{
    consumer: Arc<StreamConsumer>,
    store: Arc<RwLock<HashMap<String, T>>>,
    lifetime: &'a PhantomData<()>
}


async fn start_update_thread<T>(consumer: Arc<StreamConsumer>, store: Arc<RwLock<HashMap<String, T>>>)
where T: serde::de::DeserializeOwned
{
    consumer.stream()
        .filter_map(|item| async {
                match item {
                    Ok(i) => Some(i),
                    Err(_) => None
            }
        }).for_each(|msg| {
            let store_ref = store.clone();
            async move {
                let raw_key = msg.key().expect("No key");
                //println!("Raw key: {:?}", String::from_utf8_lossy(raw_key));
                let key = String::from_utf8_lossy(raw_key).to_string();
                let raw_value = msg.payload().expect("No value");
                //println!("Raw value: {:?}", String::from_utf8_lossy(raw_value));
                let value = serde_json::from_slice::<T>(raw_value).expect("Failed to deserialize value");

                store_ref.write().await.insert(key, value);
        }}).await;
}

impl <'a, T> InMemoryStateStore<'a, T> 
where T: serde::de::DeserializeOwned + Send + Sync + 'static
{
    pub fn from_consumer(consumer: StreamConsumer, topic: &str) -> Result<Self, StateStoreCreationError> {
        let store = InMemoryStateStore {
            consumer: Arc::new(consumer),
            store: Default::default(),
            lifetime: &PhantomData
        };

        store.consumer.subscribe(&[topic])?;

        store.start_update_thread();

        Ok(store)
    }

    pub fn from_consumer_config(config: &ClientConfig, topic: &str) -> Result<Self, StateStoreCreationError> {
        let client = config.create()?;

        InMemoryStateStore::from_consumer(client, topic)
    }

    fn start_update_thread(&self) {
        let consumer_ref = self.consumer.clone();
        let store_ref = self.store.clone();

        tokio::spawn(start_update_thread(consumer_ref, store_ref));
    }
}

impl <'a, T> ReadableStateStore<T> for InMemoryStateStore<'a, T> 
where T: serde::de::DeserializeOwned + Clone
{
    async fn get(&self, key: &str) -> Option<T> {
        Some(
            self.store
                .read().await
                .get(key)?
                .clone()
        )
    }
}
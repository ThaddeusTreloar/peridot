use std::{sync::Arc, task::Waker};

use crossbeam::atomic::AtomicCell;
use dashmap::DashMap;
use tracing::info;

use crate::{
    message::{state_fork::StoreStateCell, StreamState},
    state::backend::StateBackend,
};

use self::timestamped_waker::TimestampedWaker;

pub mod timestamped_waker;

// Key: (Topic, Partition)
type StateStoreMap<B> = Arc<DashMap<(String, i32), Arc<B>>>;

// Key: (Store, Partition)
type StateStreamStateMap = Arc<DashMap<(String, i32), Arc<StoreStateCell>>>;
type WakerMap = Arc<DashMap<(String, i32), Vec<TimestampedWaker>>>;

#[derive(Debug, thiserror::Error)]
pub enum StateStoreManagerError {
    #[error("State store exists for {}:{}", topic, partition)]
    StateStoreExists { topic: String, partition: i32 },
    #[error(
        "Failed to create state store for {}:{} caused by {}",
        topic,
        partition,
        err
    )]
    StateStoreCreation {
        topic: String,
        partition: i32,
        err: Box<dyn std::error::Error>,
    },
}

#[derive()]
pub(crate) struct StateStoreManager<B> {
    state_stores: StateStoreMap<B>,
    state_stream_states: StateStreamStateMap,
    wakers: WakerMap,
}

impl<B> Default for StateStoreManager<B> {
    fn default() -> Self {
        Self {
            state_stores: Default::default(),
            state_stream_states: Default::default(),
            wakers: Default::default(),
        }
    }
}

impl<B> StateStoreManager<B>
where
    B: StateBackend,
    B::Error: 'static,
{
    pub(super) fn new() -> Self {
        Default::default()
    }

    pub(crate) fn get_state_store(&self, source_topic: &str, partition: i32) -> Option<Arc<B>> {
        self.state_stores
            .get(&(source_topic.to_owned(), partition))
            .map(|s| s.clone())
    }

    pub(crate) fn create_state_store(
        &self,
        source_topic: &str,
        partition: i32,
    ) -> Result<(), StateStoreManagerError> {
        tracing::debug!(
            "Created state store for source: {}, partition: {}",
            source_topic,
            partition
        );

        if self
            .state_stores
            .contains_key(&(source_topic.to_owned(), partition))
        {
            Err(StateStoreManagerError::StateStoreExists {
                topic: source_topic.to_owned(),
                partition,
            })?
        }

        let backend =
            B::with_source_topic_name_and_partition(source_topic, partition).map_err(|err| {
                StateStoreManagerError::StateStoreCreation {
                    topic: source_topic.to_owned(),
                    partition,
                    err: Box::new(err),
                }
            })?;

        self.state_stores
            .insert((source_topic.to_owned(), partition), Arc::new(backend));

        Ok(())
    }

    pub(crate) fn create_state_store_for_topic(
        &self,
        source_topic: &str,
        state_name: &str,
        partition: i32,
    ) -> Result<Arc<StoreStateCell>, StateStoreManagerError> {
        tracing::debug!(
            "Created state store for source: {}, partition: {}",
            source_topic,
            partition
        );

        match self.state_stores.get(&(source_topic.to_owned(), partition)) {
            None => panic!("Partition store doesn't exist"),
            Some(store) => store.init_state(source_topic, state_name, partition),
        };

        let new_state = Arc::new(AtomicCell::new(Default::default()));

        self.state_stream_states
            .insert((state_name.to_owned(), partition), new_state.clone());

        self.wakers
            .insert((state_name.to_owned(), partition), Default::default());

        Ok(new_state)
    }

    pub(crate) fn create_state_store_if_not_exists(
        &self,
        source_topic: &str,
        partition: i32,
    ) -> Result<(), StateStoreManagerError> {
        match self.create_state_store(source_topic, partition) {
            Ok(_) | Err(StateStoreManagerError::StateStoreExists { .. }) => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub(crate) fn get_stream_state(
        &self,
        store_name: &str,
        partition: i32,
    ) -> Option<Arc<StoreStateCell>> {
        self.state_stream_states
            .get(&(store_name.to_owned(), partition))
            .map(|arc| arc.clone())
    }

    pub(crate) fn store_waker(&self, store_name: &str, partition: i32, time: i64, waker: Waker) {
        tracing::debug!(
            "Storing waker for store_name: {}, partition: {}",
            store_name,
            partition
        );
        match self.wakers.get_mut(&(store_name.to_owned(), partition)) {
            Some(mut wakers) => wakers.push(TimestampedWaker::new(time, waker)),
            None => todo!(""),
        }
    }

    pub(crate) fn wake_for_time(&self, store_name: &str, partition: i32, time: i64) {
        tracing::debug!("Waking dependencies...");

        match self.wakers.get_mut(&(store_name.to_owned(), partition)) {
            Some(mut wakers) => {
                // TODO: tracking stabilisation of:
                //  - https://github.com/rust-lang/rust/issues/43244https://doc.rust-lang.org/std/vec/struct.Vec.html#method.extract_if
                //  - https://github.com/rust-lang/rust/issues/43244
                // change logic to this when api stablised.
                wakers
                    .iter()
                    .filter(|w| *w <= &time)
                    .for_each(|w| w.wake_by_ref());

                wakers.retain(|w| w > &time);
            }
            None => todo!(""),
        }
    }

    pub(crate) fn wake_all(&self, store_name: &str, partition: i32) {
        tracing::debug!("Waking all dependencies...");

        match self.wakers.get_mut(&(store_name.to_owned(), partition)) {
            Some(mut wakers) => wakers.drain(..).for_each(|w| w.wake()),
            None => todo!(""),
        }
    }
}

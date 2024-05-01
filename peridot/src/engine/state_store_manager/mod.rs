use std::sync::Arc;

use dashmap::DashMap;


pub type StateStoreMap<B> = Arc<DashMap<(String, i32), Arc<B>>>;

#[derive()]
pub(crate) struct StateStoreManager<B> {
    state_stores: StateStoreMap<B>,
}

impl <B> Default for StateStoreManager<B> {
    fn default() -> Self {
        Self {
            state_stores: Default::default(),
        }
    }
}

impl <B> StateStoreManager<B> {
    fn new() -> Self {
        Default::default()
    }
}
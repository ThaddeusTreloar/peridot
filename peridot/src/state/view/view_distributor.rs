use std::sync::Arc;

use crate::{
    engine::{context::EngineContext, state_store_manager::StateStoreManager},
    state::store::StateStore,
};

use super::{GetView, ViewError};

pub struct ViewDistributor<K, V, B> {
    engine_context: Arc<EngineContext>,
    state_store_manager: Arc<StateStoreManager<B>>,
    store_name: String,
    _key_type: std::marker::PhantomData<K>,
    _value_type: std::marker::PhantomData<V>,
}

impl<K, V, B> GetView for ViewDistributor<K, V, B>
where
    B: StateStore,
{
    type Backend = B;
    type KeyType = K;
    type ValueType = V;

    fn get_view(
        &self,
        partition: i32,
    ) -> Result<super::state_view::StateView<Self::KeyType, Self::ValueType, Self::Backend>, ViewError> {
        unimplemented!("")
    }
}

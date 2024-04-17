use std::sync::Arc;

use crate::state::backend::IntoView;

use super::stream::PipelineStream;

pub struct Join<S, T, C> {
    inner: S,
    table: Arc<T>,
    combiner: Arc<C>,
}

impl<S, T, C> Join<S, T, C>
where
    S: PipelineStream,
    T: IntoView,
    S::KeyType: PartialEq<T::KeyType>,
{
    pub fn new(inner: S, table: T, combiner: C) -> Self {
        Self {
            inner,
            table: Arc::new(table),
            combiner: Arc::new(combiner),
        }
    }
}

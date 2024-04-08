use std::sync::Arc;

pub struct JoinBy<S, T, J, C> {
    inner: S,
    table: Arc<T>,
    joiner: Arc<J>,
    combiner: Arc<C>,
}

impl<S, T, J, C> JoinBy<S, T, J, C> {
    pub fn new(inner: S, table: T, joiner: J, combiner: C) -> Self {
        Self {
            inner,
            table: Arc::new(table),
            joiner: Arc::new(joiner),
            combiner: Arc::new(combiner),
        }
    }
}

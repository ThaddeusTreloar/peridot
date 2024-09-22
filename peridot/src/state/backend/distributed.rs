pub trait StateBackend {
    fn get_state_store(&self, source_topic: &str, partition: i32) -> Option<Arc<B>> {
        self.state_stores
            .get(&(source_topic.to_owned(), partition))
            .map(|s| s.clone())
    }
    fn create_state_store(
        &self,
        source_topic: &str,
        partition: i32,
    ) -> Result<(), StateBackendError> {
        tracing::debug!(
            "Created state store for source: {}, partition: {}",
            source_topic,
            partition
        );

        if self
            .state_stores
            .contains_key(&(source_topic.to_owned(), partition))
        {
            Err(StateBackendError::StateStoreExists {
                topic: source_topic.to_owned(),
                partition,
            })?
        }

        let backend =
            B::with_source_topic_name_and_partition(source_topic, partition).map_err(|err| {
                StateBackendError::StateStoreCreation {
                    topic: source_topic.to_owned(),
                    partition,
                    err: Box::new(err),
                }
            })?;

        self.state_stores
            .insert((source_topic.to_owned(), partition), Arc::new(backend));

        Ok(())
    }

    fn create_state_store_for_topic(
        &self,
        source_topic: &str,
        state_name: &str,
        partition: i32,
    ) -> Result<Arc<StoreStateCell>, StateBackendError> {
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

    fn create_state_store_if_not_exists(
        &self,
        source_topic: &str,
        partition: i32,
    ) -> Result<(), StateBackendError> {
        match self.create_state_store(source_topic, partition) {
            Ok(_) | Err(StateBackendError::StateStoreExists { .. }) => Ok(()),
            Err(err) => Err(err),
        }
    }
}

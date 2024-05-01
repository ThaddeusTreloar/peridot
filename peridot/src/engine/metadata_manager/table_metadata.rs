use dashmap::DashMap;

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TableState {
    #[default]
    Uninitialised,
    Rebuilding,
    Ready,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct TableStateAggregate {
    inner: DashMap<i32, TableState>,
}

impl TableStateAggregate {
    pub(crate) fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub(crate) fn table_state(&self) -> TableState {
        self.inner.iter()
            .map(|s|s.value().clone())
            .min()
            .unwrap_or(Default::default())
    }
}

#[derive(Debug, Default, Clone)]
pub struct TableMetadata {
    source_topic: String,
    changelog_topic: Option<String>,
    table_state: TableStateAggregate,
}

impl TableMetadata {
    pub(crate) fn new(source_topic: &str) -> Self {
        Self {
            source_topic: source_topic.to_owned(),
            ..Default::default()
        }
    }

    pub(crate) fn new_with_changelog(source_topic: &str, changelog_topic: &str) -> Self {
        let mut new = Self::new(source_topic);

        let _ = new.set_changelog_topic(changelog_topic);

        new
    }

    pub(crate) fn set_changelog_topic(&mut self, changelog_topic: &str) -> Option<String> {
        self.changelog_topic.replace(changelog_topic.to_owned())
    }

    pub(crate) fn set_table_state(&mut self, partition: i32, state: TableState) {
        self.table_state.inner.insert(partition, state);
    }

    pub(crate) fn table_state(&mut self) -> TableState {
        self.table_state.table_state()
    }

    pub(crate) fn changelog_topic(&self) -> Option<&String> {
        self.changelog_topic.as_ref()
    }

    pub(crate) fn source_topic(&self) -> &str{
        &self.source_topic
    }
}
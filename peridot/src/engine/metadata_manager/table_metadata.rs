/*
 * Copyright 2024 Thaddeus Treloar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

use std::fmt::Display;

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
        self.inner
            .iter()
            .map(|s| s.value().clone())
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

impl Display for TableMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.changelog_topic {
            None => f.write_str(&format!(
                "{{ source_topic: {}, changelog_topic: None }}",
                self.source_topic
            )),
            Some(c_topic) => f.write_str(&format!(
                "{{ source_topic: {}, changelog_topic: {} }}",
                self.source_topic, c_topic
            )),
        }
    }
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

    pub(crate) fn source_topic(&self) -> &str {
        &self.source_topic
    }
}

use std::collections::HashMap;

use crate::{types::{MessageContext, Domain}, state::ReadableStateStore};

trait Filter {
    async fn filter(&self, msg: Option<MessageContext>) -> Option<MessageContext>;
}

pub struct SoureDestTopicFilter<T> {
    rules: T
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceDestRule {
    id: String,
    sources: HashMap<String, Vec<Domain>>,
}

impl SourceDestRule {
    pub fn new(id: String, sources: HashMap<String, Vec<Domain>>) -> Self {
        SourceDestRule {
            id,
            sources
        }
    }

    pub fn allowed(&self, source: &str, target: &str, topic: &str) -> bool {
        if let Some(destinations) =  self.sources.get(source) {
            if let Some(dest) = destinations.iter().find(|d| d.eq(&target)) {
                dest.contains_topic(topic)
            } else {
                false
            }
        } else {
            false
        }
    }
}

impl<T: ReadableStateStore<SourceDestRule>> Filter for SoureDestTopicFilter<T> {
    async fn filter(&self, msg: Option<MessageContext>) -> Option<MessageContext> {
        if let Some(msg) = msg {
            let rule = self.rules.get(msg.key.as_str()).await?;

            if rule.allowed(
                msg.source.as_str(), 
                msg.target.as_str(), 
                msg.topic.as_str()
            ) {
                Some(msg)
            } else {
                None
            }
        } else {
            msg
        }
    }
}

/*pub struct FilterChain {
    filters: Vec<Box<dyn Filter>>,
}

impl Filter for FilterChain {
    async fn filter(&self, msg: Option<MessageContext>) -> Option<MessageContext> {
        self.filters.iter().fold(msg, |acc, filter| filter.filter(acc))
    }
}*/
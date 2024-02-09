use serde_json::Value;



#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Domain {
    name: String,
    topics: Vec<String>,
}

impl PartialEq<str> for Domain {
    fn eq(&self, other: &str) -> bool {
        self.name == *other
    }
}

impl Domain {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn contains_topic(&self, topic: &str) -> bool {
        self.topics.contains(&topic.to_string())
    }

    pub fn topics(&self) -> &Vec<String> {
        &self.topics
    }
}

impl Default for Domain {
    fn default() -> Self {
        Domain {
            name: "default".to_string(),
            topics: vec!["default".to_string()],
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Topic {
    name: String,
    filters: Vec<String>,
}

pub struct MessageContext {
    pub source: String,
    pub target: String,
    pub topic: String,
    pub key: String,
    pub headers: Vec<String>,
    pub message: Value
}
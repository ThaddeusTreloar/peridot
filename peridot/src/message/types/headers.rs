use std::collections::HashMap;

use rdkafka::message::{BorrowedHeaders, Header, Headers as KafkaHeaders, OwnedHeaders};

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct MessageHeaders {
    headers: HashMap<String, Vec<Vec<u8>>>,
}

impl MessageHeaders {
    pub fn get(&self, key: &str) -> Option<&Vec<Vec<u8>>> {
        self.headers.get(key)
    }

    pub fn set(&mut self, key: &str, value: Vec<u8>) {
        match self.headers.get_mut(key) {
            None => {
                let values = vec![value];
                self.headers.insert(key.to_owned(), values);
            }
            Some(values) => values.push(value),
        };
    }

    pub fn remove(&mut self, key: &str) {
        self.headers.remove(key);
    }

    pub fn into_owned_headers(&self) -> OwnedHeaders {
        let out = self
            .headers
            .iter()
            .fold(OwnedHeaders::new(), |mut out, (key, values)| {
                values.iter().fold(out, |acc, value| {
                    acc.insert(Header {
                        key,
                        value: Some(value),
                    })
                })
            });

        out
    }
}

impl From<&BorrowedHeaders> for MessageHeaders {
    fn from(from: &BorrowedHeaders) -> Self {
        let mut headers = Self::default();

        from.iter()
            .map(|h| (String::from(h.key), h.value.unwrap_or_default().to_vec()))
            .for_each(|(key, value)| headers.set(&key, value));

        headers
    }
}

impl From<&OwnedHeaders> for MessageHeaders {
    fn from(from: &OwnedHeaders) -> Self {
        let mut headers = Self::default();

        from.iter()
            .map(|h| (String::from(h.key), h.value.unwrap_or_default().to_vec()))
            .for_each(|(key, value)| headers.set(&key, value));

        headers
    }
}

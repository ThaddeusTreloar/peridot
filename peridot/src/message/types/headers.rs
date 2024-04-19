use rdkafka::message::{BorrowedHeaders, Header, OwnedHeaders,Headers as KafkaHeaders};

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct MessageHeaders {
    headers: Vec<(String, Vec<u8>)>,
}

impl MessageHeaders {
    fn iter(&self) -> impl Iterator<Item = &(String, Vec<u8>)> {
        self.headers.iter()
    }

    fn list(&self, key: &str) -> Vec<&Vec<u8>> {
        self.headers
            .iter()
            .filter_map(|(k, v)| if k == key { Some(v) } else { None })
            .collect()
    }

    pub fn into_owned_headers(&self) -> OwnedHeaders {
        let out = self
            .headers
            .iter()
            .fold(OwnedHeaders::new(), |out, (key, value)| {
                out.insert(Header {
                    key,
                    value: Some(value),
                })
            });

        out
    }
}

impl From<&BorrowedHeaders> for MessageHeaders {
    fn from(headers: &BorrowedHeaders) -> Self {
        let headers: Vec<_> = headers
            .iter()
            .map(|h| (String::from(h.key), h.value.unwrap_or_default().to_vec()))
            .collect::<Vec<(String, Vec<u8>)>>();

        Self {
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_vec()))
                .collect(),
        }
    }
}

impl From<&OwnedHeaders> for MessageHeaders {
    fn from(headers: &OwnedHeaders) -> Self {
        let headers: Vec<_> = headers
            .iter()
            .map(|h| (String::from(h.key), h.value.unwrap_or_default().to_vec()))
            .collect::<Vec<(String, Vec<u8>)>>();

        Self {
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_vec()))
                .collect(),
        }
    }
}
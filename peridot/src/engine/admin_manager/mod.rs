use rdkafka::{admin::AdminClient, client::DefaultClientContext, config::FromClientConfig};

use crate::app::config::PeridotConfig;

#[derive(Debug, thiserror::Error)]
pub enum AdminManagerError {
    #[error("ClientManagerError::CreateClientError: Failed to create client while initialising AdminManager -> {}", 0)]
    CreateClientError(rdkafka::error::KafkaError),
}

pub struct AdminManager {
    client: AdminClient<DefaultClientContext>,
}

impl AdminManager {
    pub(super) fn new(config: &PeridotConfig) -> Result<Self, AdminManagerError> {
        let client = AdminClient::from_config(config.client_config())
            .map_err(AdminManagerError::CreateClientError)?;

        Ok(Self { client })
    }
}

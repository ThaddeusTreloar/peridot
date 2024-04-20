#![allow(refining_impl_trait)]

pub mod app;
pub mod engine;
pub mod init;
pub mod message;
pub mod pipeline;
pub mod state;
pub mod task;
pub(crate) mod util;

use tracing::info;

pub const HELP_URL: &str = "https://github.com/ThaddeusTreloar/peridot/blob/master/docs";

pub fn help(help_topic: &str) -> String {
    let resource = format!("{}/{}.md", HELP_URL, help_topic);

    info!("More information can be found at {}", resource);

    resource
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_help() {
        assert_eq!(
            help("auto-commit"),
            String::from(
                "https://github.com/ThaddeusTreloar/peridot/blob/master/docs/auto-commit.md"
            )
        );
    }
}

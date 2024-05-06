use std::sync::atomic::AtomicI64;

use peridot::init::init_tracing;
use tracing::{info, level_filters::LevelFilter};

#[tokio::main]
async fn main() {
    init_tracing(LevelFilter::DEBUG);

    let a_i64 = AtomicI64::from(10);

    a_i64.fetch_max(12, std::sync::atomic::Ordering::Relaxed);

    info!(
        "After 12: {}",
        a_i64.load(std::sync::atomic::Ordering::Relaxed)
    );

    a_i64.fetch_max(8, std::sync::atomic::Ordering::Relaxed);

    info!(
        "After 8: {}",
        a_i64.load(std::sync::atomic::Ordering::Relaxed)
    );
}

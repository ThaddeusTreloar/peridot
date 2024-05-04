use tracing::level_filters::LevelFilter;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;

pub fn init_tracing(log_level: LevelFilter) {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_line_number(true);

    let subscriber = tracing_subscriber::Registry::default()
        .with(log_level)
        .with(fmt_layer);

    tracing::subscriber::set_global_default(subscriber).unwrap();
}

pub fn init_json_tracing(log_level: LevelFilter) {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .flatten_event(true)
        .with_file(true)
        .with_line_number(true)
        .with_span_list(false);

    let subscriber = tracing_subscriber::Registry::default()
        .with(log_level)
        .with(fmt_layer);

    tracing::subscriber::set_global_default(subscriber).unwrap();
}

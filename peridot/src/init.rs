use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;

pub fn init_tracing() {
    let filter_layer = tracing_subscriber::filter::LevelFilter::INFO;
    
    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .flatten_event(true)
        .with_file(true)
        .with_line_number(true)
        .with_span_list(false);

    let subscriber = tracing_subscriber::Registry::default()
        .with(filter_layer)
        .with(fmt_layer);

    tracing::subscriber::set_global_default(subscriber).unwrap();
}
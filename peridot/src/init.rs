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

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{fmt::format::FmtSpan, prelude::__tracing_subscriber_SubscriberExt};

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

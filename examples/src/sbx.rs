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

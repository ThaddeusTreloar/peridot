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

pub mod async_map;
pub mod filter;
pub mod fork;
pub mod forward;
pub mod join;
pub mod map;
pub mod par_map;
pub mod sink;
pub mod state_fork;
pub mod stream;
pub mod types;

// Derive from partition_nodes/max_producer_msg
const BATCH_SIZE: usize = 65535;

#[derive(Debug, Default, PartialEq, Eq, derive_more::Display)]
pub(crate) enum StreamState {
    #[default]
    Uncommitted,
    Committing,
    Committed,
    Sleeping,
    Closing,
}

impl StreamState {
    fn is_committing(&self) -> bool {
        *self == StreamState::Committing
    }
}

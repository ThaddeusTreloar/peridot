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

use std::sync::Arc;

pub struct JoinBy<S, T, J, C> {
    inner: S,
    table: Arc<T>,
    joiner: Arc<J>,
    combiner: Arc<C>,
}

impl<S, T, J, C> JoinBy<S, T, J, C> {
    pub fn new(inner: S, table: T, joiner: J, combiner: C) -> Self {
        Self {
            inner,
            table: Arc::new(table),
            joiner: Arc::new(joiner),
            combiner: Arc::new(combiner),
        }
    }
}

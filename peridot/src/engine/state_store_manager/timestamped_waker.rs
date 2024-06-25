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

use std::task::Waker;

pub(super) struct TimestampedWaker {
    time: i64,
    waker: Waker,
}

impl TimestampedWaker {
    pub(super) fn new(time: i64, waker: Waker) -> Self {
        Self { time, waker }
    }

    pub(super) fn wake(self) {
        self.waker.wake()
    }

    pub(super) fn wake_by_ref(&self) {
        self.waker.wake_by_ref()
    }
}

impl PartialEq<Self> for TimestampedWaker {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

impl Eq for TimestampedWaker {}

impl PartialOrd<Self> for TimestampedWaker {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimestampedWaker {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time.cmp(&other.time)
    }
}

impl PartialEq<i64> for TimestampedWaker {
    fn eq(&self, other: &i64) -> bool {
        &self.time == other
    }
}

impl PartialOrd<i64> for TimestampedWaker {
    fn partial_cmp(&self, other: &i64) -> Option<std::cmp::Ordering> {
        Some(self.time.cmp(other))
    }
}

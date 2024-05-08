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

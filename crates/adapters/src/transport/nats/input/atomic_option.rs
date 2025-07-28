use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct AtomicOptionNonZeroU64 {
    inner: AtomicU64,
}

impl AtomicOptionNonZeroU64 {
    pub fn new(val: Option<NonZeroU64>) -> Self {
        let raw = val.map_or(0, NonZeroU64::get);
        Self {
            inner: AtomicU64::new(raw),
        }
    }

    pub fn load(&self, order: Ordering) -> Option<NonZeroU64> {
        NonZeroU64::new(self.inner.load(order))
    }

    pub fn store(&self, val: Option<NonZeroU64>, order: Ordering) {
        let raw = val.map_or(0, NonZeroU64::get);
        self.inner.store(raw, order);
    }
}

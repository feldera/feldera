//! Counts entries into and exits from calls.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

/// The source of [TransitionCounter] identifiers.
static NEXT_COUNTER_ID: AtomicU64 = AtomicU64::new(0);

/// Counts entries into and exits from calls.
pub struct TransitionCounter {
    /// Identifies the counter.  No other counter in the process has the same
    /// identifier.
    id: u64,

    /// The number of entries into and exits from calls.  Shared with the
    /// [TransitionReader]s of the counter.
    value: Arc<AtomicU64>,
}

impl Default for TransitionCounter {
    fn default() -> Self {
        Self {
            id: NEXT_COUNTER_ID.fetch_add(1, Ordering::Relaxed),
            value: Arc::default(),
        }
    }
}

impl TransitionCounter {
    /// Increments the value.
    fn increment(&mut self) {
        // `TransitionCounter` is not `Clone`, and `&mut self` gives exclusive
        // access to it, so this is the only writer: a load and a store do not
        // race.
        let value = self.value.load(Ordering::Relaxed);
        self.value.store(value + 1, Ordering::Relaxed);
    }

    /// Increments the value, calls `f`, and increments the value again.
    pub fn count<T>(&mut self, f: impl FnOnce() -> T) -> T {
        self.increment();
        let result = f();
        self.increment();
        result
    }

    /// Returns a reader of the value.
    pub fn reader(&self) -> TransitionReader {
        TransitionReader {
            counter_id: self.id,
            value: Arc::downgrade(&self.value),
        }
    }
}

/// The value of a [TransitionCounter] at one read.
///
/// Two values are equal only if they come from the same [TransitionCounter],
/// and no call started or returned between the two reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Transitions {
    /// The identifier of the [TransitionCounter] that was read.
    counter_id: u64,

    /// The value of the [TransitionCounter] at the read.
    value: u64,
}

impl Transitions {
    /// True if a call was in progress at the read.
    ///
    /// The counter adds 1 when a call starts and 1 when the call returns, and
    /// starts at 0.  So the value is even between calls, and odd during a
    /// call.
    pub fn in_call(self) -> bool {
        self.value % 2 == 1
    }
}

/// Reads the value of a [TransitionCounter] through a weak reference to the
/// value.
///
/// The value can be changed from only one place, the [TransitionCounter], and
/// read from many places, by [TransitionReader]s.  The weak reference does
/// not keep the value alive: after the [TransitionCounter] is dropped,
/// [Self::get] returns `None`.
#[derive(Clone)]
pub struct TransitionReader {
    /// The identifier of the [TransitionCounter] that the reader reads.
    counter_id: u64,

    /// A weak reference to the value of the [TransitionCounter].
    value: Weak<AtomicU64>,
}

impl TransitionReader {
    /// Returns the value, or `None` if the [TransitionCounter] was dropped.
    ///
    /// The value is a snapshot.  A call can start or return right after the
    /// read, so the value can be out of date when the caller uses it.
    pub fn get(&self) -> Option<Transitions> {
        self.value.upgrade().map(|value| Transitions {
            counter_id: self.counter_id,
            value: value.load(Ordering::Relaxed),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::TransitionCounter;

    #[test]
    fn transitions() {
        let mut counter = TransitionCounter::default();
        let reader = counter.reader();

        let before = reader.get().unwrap();
        let during = counter.count(|| reader.get().unwrap());
        let after = reader.get().unwrap();
        assert!(!before.in_call());
        assert!(during.in_call());
        assert!(!after.in_call());
        assert_ne!(before, after);

        // A new counter has the same value as `before`, but a different
        // identifier.
        let other = TransitionCounter::default();
        assert_ne!(before, other.reader().get().unwrap());

        drop(counter);
        assert_eq!(reader.get(), None);
    }
}

use crate::{CacheEntry, LruCache};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::sync::Arc;
use std::thread;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Weighted<T> {
    value: T,
    charge: usize,
}

impl<T> Weighted<T> {
    fn new(value: T, charge: usize) -> Self {
        Self { value, charge }
    }
}

impl<T: Send + Sync + 'static> CacheEntry for Weighted<T> {
    fn cost(&self) -> usize {
        self.charge
    }
}

type TestCache<K, V> = LruCache<K, Weighted<V>>;

#[test]
fn basic_insert_get_remove() {
    let cache = TestCache::<u64, String>::new(1024);
    cache.insert(1, Weighted::new("a".to_string(), 8));
    assert_eq!(&cache.get(&1).unwrap().value, "a");
    assert_eq!(&cache.remove(&1).unwrap().value, "a");
    assert!(cache.get(&1).is_none());
    cache.validate_invariants();
}

#[test]
fn replacement_updates_value_and_charge() {
    let cache = TestCache::<u64, String>::new(64);
    cache.insert(1, Weighted::new("a".to_string(), 10));
    cache.insert(1, Weighted::new("b".to_string(), 7));
    assert_eq!(&cache.get(&1).unwrap().value, "b");
    assert_eq!(cache.total_charge(), 7);
    cache.validate_invariants();
}

#[test]
fn weighted_eviction_happens_by_charge() {
    let cache = TestCache::<u64, &'static str>::new(10);
    cache.insert(1, Weighted::new("a", 6));
    cache.insert(2, Weighted::new("b", 6));
    assert!(!cache.contains_key(&1));
    assert_eq!(cache.get(&2).unwrap().value, "b");
    assert_eq!(cache.total_charge(), 6);
    cache.validate_invariants();
}

#[test]
fn get_updates_recency() {
    let cache = TestCache::<u64, &'static str>::new(2);
    cache.insert(1, Weighted::new("a", 1));
    cache.insert(2, Weighted::new("b", 1));
    let _ = cache.get(&1);
    cache.insert(3, Weighted::new("c", 1));
    assert!(cache.contains_key(&1));
    assert!(!cache.contains_key(&2));
    assert!(cache.contains_key(&3));
    cache.validate_invariants();
}

#[test]
fn remove_if_keeps_structure_consistent() {
    let cache = TestCache::<u64, &'static str>::new(16);
    cache.insert(1, Weighted::new("a", 4));
    cache.insert(2, Weighted::new("b", 4));
    cache.insert(3, Weighted::new("c", 4));
    cache.remove_if(|key| key % 2 == 1);
    assert_eq!(cache.len(), 1);
    assert!(!cache.contains_key(&1));
    assert!(cache.contains_key(&2));
    assert!(!cache.contains_key(&3));
    cache.validate_invariants();
}

#[test]
fn remove_range_keeps_structure_consistent() {
    let cache = TestCache::<u64, &'static str>::new(32);
    for key in 0..6 {
        cache.insert(key, Weighted::new("v", 4));
    }
    assert_eq!(cache.remove_range(2..5), 3);
    assert!(cache.contains_key(&0));
    assert!(cache.contains_key(&1));
    assert!(!cache.contains_key(&2));
    assert!(!cache.contains_key(&3));
    assert!(!cache.contains_key(&4));
    assert!(cache.contains_key(&5));
    cache.validate_invariants();
}

/// This behavior is somewhat specific to our implementation
/// so we encode it here.
#[test]
fn oversize_entry_replaces_cache_contents() {
    let cache = TestCache::<u64, &'static str>::new(8);
    cache.insert(1, Weighted::new("big", 32));
    assert_eq!(cache.get(&1).unwrap().value, "big");
    assert_eq!(cache.total_charge(), 32);
    cache.validate_invariants();
}

#[test]
fn backend_reports_one_shard() {
    let cache = TestCache::<u64, u64>::new(10);
    cache.insert(1, Weighted::new(10, 3));
    assert_eq!(cache.shard_count(), 1);
    assert_eq!(cache.shard_usage(0), (3, 10));
    cache.validate_invariants();
}

#[test]
fn concurrency_smoke_test() {
    let cache = Arc::new(TestCache::<u64, u64>::new(4096));
    let mut threads = Vec::new();
    for tid in 0..8 {
        let cache = cache.clone();
        threads.push(thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(1234 + tid);
            for _ in 0..10_000 {
                let key = rng.gen_range(0..256);
                match rng.gen_range(0..3) {
                    0 => {
                        cache.insert(key, Weighted::new(key, rng.gen_range(1..32)));
                    }
                    1 => {
                        let _ = cache.get(&key);
                    }
                    _ => {
                        let _ = cache.remove(&key);
                    }
                }
            }
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    cache.validate_invariants();
}

use crate::{CacheEntry, S3FifoCache};
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

type TestCache<K, V> = S3FifoCache<K, Weighted<V>>;

#[test]
fn basic_insert_get_remove() {
    let cache = TestCache::<u64, String>::with_shards(1024, 4);
    cache.insert(1, Weighted::new("a".to_string(), 8));
    assert_eq!(&cache.get(&1).unwrap().value, "a");
    assert_eq!(&cache.remove(&1).unwrap().value, "a");
    assert!(cache.get(&1).is_none());
    cache.validate_invariants();
}

#[test]
fn replacement_updates_value_and_charge() {
    let cache = TestCache::<u64, String>::with_shards(64, 2);
    cache.insert(1, Weighted::new("a".to_string(), 10));
    cache.insert(1, Weighted::new("b".to_string(), 7));
    assert_eq!(&cache.get(&1).unwrap().value, "b");
    assert_eq!(cache.total_charge(), 7);
    cache.validate_invariants();
}

#[test]
fn weighted_eviction_happens_by_charge() {
    let cache = TestCache::<u64, &'static str>::with_shards(10, 1);
    cache.insert(1, Weighted::new("a", 6));
    cache.insert(2, Weighted::new("b", 6));
    assert_eq!(cache.total_charge(), 6);
    assert_eq!(cache.len(), 1);
    assert!(!cache.contains_key(&1) || !cache.contains_key(&2));
    cache.validate_invariants();
}

#[test]
fn remove_keeps_cache_consistent() {
    let cache = TestCache::<u64, &'static str>::with_shards(8, 1);
    cache.insert(1, Weighted::new("a", 4));
    cache.insert(2, Weighted::new("b", 4));
    cache.remove(&1);
    cache.insert(3, Weighted::new("c", 4));
    assert!(cache.contains_key(&2) && cache.contains_key(&3));
    cache.validate_invariants();
}

#[test]
fn shard_selection_stability() {
    let cache = TestCache::<u64, u64>::with_shards(1024, 8);
    for key in 0..100 {
        cache.insert(key, Weighted::new(key, 1));
        assert_eq!(cache.shard_index(&key), cache.shard_index(&key));
    }
    cache.validate_invariants();
}

/// This is different from our LRU cache, I think it's fine.
#[test]
fn oversize_entry_policy_is_insert_then_evict() {
    let cache = TestCache::<u64, &'static str>::with_shards(8, 1);
    cache.insert(1, Weighted::new("big", 32));
    assert_eq!(cache.total_charge(), 0);
    assert!(!cache.contains_key(&1));
    cache.validate_invariants();
}

#[test]
fn concurrency_smoke_test() {
    let cache = Arc::new(TestCache::<u64, u64>::with_shards(4096, 16));
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

#[test]
fn charge_accounting_after_replacement_and_remove() {
    let cache = TestCache::<u64, &'static str>::with_shards(100, 2);
    cache.insert(1, Weighted::new("a", 20));
    cache.insert(1, Weighted::new("b", 30));
    assert_eq!(cache.total_charge(), 30);
    cache.remove(&1);
    assert_eq!(cache.total_charge(), 0);
    cache.validate_invariants();
}

#[test]
fn basic_sequence() {
    let cache = TestCache::<String, String>::with_shards(3, 1);
    cache.insert(
        "foo".to_string(),
        Weighted::new("foocontent".to_string(), 1),
    );
    cache.insert(
        "bar".to_string(),
        Weighted::new("barcontent".to_string(), 1),
    );
    assert_eq!(
        cache.remove(&"bar".to_string()).map(|value| value.value),
        Some("barcontent".to_string())
    );
    cache.insert(
        "bar2".to_string(),
        Weighted::new("bar2content".to_string(), 1),
    );
    cache.insert(
        "bar3".to_string(),
        Weighted::new("bar3content".to_string(), 1),
    );
    assert_eq!(
        cache.get(&"foo".to_string()).map(|value| value.value),
        Some("foocontent".to_string())
    );
    assert_eq!(cache.get(&"bar".to_string()), None);
    assert_eq!(
        cache.get(&"bar2".to_string()).map(|value| value.value),
        Some("bar2content".to_string())
    );
    assert_eq!(
        cache.get(&"bar3".to_string()).map(|value| value.value),
        Some("bar3content".to_string())
    );
    cache.validate_invariants();
}

#[test]
fn insert_doesnt_exceed_capacity() {
    let total_capacity = 2usize;
    let cache = TestCache::<String, u64>::with_shards(total_capacity, 1);
    cache.insert("a".to_string(), Weighted::new(1, 1));
    cache.insert("b".to_string(), Weighted::new(2, 1));
    assert!(cache.get(&"a".to_string()).is_some());
    assert!(cache.get(&"b".to_string()).is_some());
    cache.insert("c".to_string(), Weighted::new(3, 1));
    assert!(cache.len() <= 2);
    assert!(cache.total_charge() <= cache.total_capacity());
    cache.validate_invariants();
}

/// This is behavior of quick-cache.
#[test]
fn backend_shard_capacity_rounds_up_evenly() {
    let cache = TestCache::<u64, u64>::with_shards(10, 4);
    let caps: Vec<usize> = (0..4).map(|i| cache.shard_usage(i).1).collect();
    assert_eq!(caps, vec![3, 3, 3, 3]);
    assert_eq!(cache.total_capacity(), 12);
    cache.validate_invariants();
}

/// We do not use this atm, but it could be useful to "pin" entries.
#[test]
fn zero_charge_entries_do_not_increase_budget_usage() {
    let cache = TestCache::<u64, u64>::with_shards(1, 1);
    cache.insert(1, Weighted::new(10, 0));
    cache.insert(2, Weighted::new(20, 0));
    cache.insert(3, Weighted::new(30, 0));
    assert_eq!(cache.total_charge(), 0);
    assert_eq!(cache.len(), 3);
    assert_eq!(cache.get(&1).unwrap().value, 10);
    cache.validate_invariants();
}

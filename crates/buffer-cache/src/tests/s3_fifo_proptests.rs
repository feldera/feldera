//! Property tests for `S3FifoCache`.
//!
//! We sanity check that the crate we're using behaves like we expect it to.
//!
//! High-level invariants:
//! 1. Successful `get`/`remove` only return values previously inserted for that key.
//! 2. The cache never mutates keys or values, and never cross-wires one key to another key's value.
//! 3. Global accounting stays consistent: shard usage sums match total usage and capacity is never exceeded.
//! 4. `remove_if` removes exactly the live entries matching the predicate.

use crate::{CacheEntry, S3FifoCache};
use proptest::prelude::*;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TestValue {
    key: u8,
    version: u32,
    token: u64,
    charge: u8,
}

impl CacheEntry for TestValue {
    fn cost(&self) -> usize {
        self.charge as usize
    }
}

#[derive(Clone, Copy, Debug)]
enum TestOp {
    Insert {
        key: u8,
        version: u32,
        token: u64,
        charge: u8,
    },
    Get {
        key: u8,
    },
    Remove {
        key: u8,
    },
}

#[derive(Clone, Copy, Debug)]
enum RemovePredicate {
    EvenKeys,
    KeyLessThan(u8),
    ExactKey(u8),
}

fn remove_predicate_matches(predicate: RemovePredicate, key: u8) -> bool {
    match predicate {
        RemovePredicate::EvenKeys => key.is_multiple_of(2),
        RemovePredicate::KeyLessThan(cutoff) => key < cutoff,
        RemovePredicate::ExactKey(exact) => key == exact,
    }
}

fn assert_global_invariants(cache: &S3FifoCache<u8, TestValue>) {
    cache.validate_invariants();
    let shard_total_usage: usize = (0..cache.shard_count())
        .map(|i| cache.shard_usage(i).0)
        .sum();
    let shard_total_capacity: usize = (0..cache.shard_count())
        .map(|i| cache.shard_usage(i).1)
        .sum();
    assert_eq!(shard_total_usage, cache.total_charge());
    assert_eq!(shard_total_capacity, cache.total_capacity());
    assert!(cache.total_charge() <= cache.total_capacity());
}

fn assert_resident_entries_match_model(
    cache: &S3FifoCache<u8, TestValue>,
    model: &BTreeMap<u8, TestValue>,
) {
    let mut live_count = 0usize;
    let mut live_charge = 0usize;

    for (&key, &expected) in model {
        match cache.get(&key) {
            Some(actual) => {
                assert_eq!(
                    actual, expected,
                    "cache returned mutated or stale value for key {key}"
                );
                assert_eq!(actual.key, key, "cache returned value for wrong key");
                live_count += 1;
                live_charge += actual.charge as usize;
            }
            None => {
                assert!(!cache.contains_key(&key));
            }
        }
    }

    assert_eq!(cache.len(), live_count);
    assert_eq!(cache.total_charge(), live_charge);
}

fn run_model_sequence(
    shards: usize,
    capacity: usize,
    ops: &[TestOp],
    remove_predicates: &[RemovePredicate],
) {
    let cache = S3FifoCache::<u8, TestValue>::with_shards(capacity, shards);
    let mut model = BTreeMap::<u8, TestValue>::new();

    for op in ops {
        match *op {
            TestOp::Insert {
                key,
                version,
                token,
                charge,
            } => {
                let value = TestValue {
                    key,
                    version,
                    token,
                    charge,
                };
                cache.insert(key, value);
                model.insert(key, value);
            }
            TestOp::Get { key } => match cache.get(&key) {
                Some(actual) => {
                    let expected = model
                        .get(&key)
                        .expect("cache returned a value for a never-inserted key");
                    assert_eq!(
                        actual, *expected,
                        "cache returned mutated, cross-key, or stale value for key {key}"
                    );
                    assert_eq!(actual.key, key, "cache returned value for wrong key");
                }
                None => {
                    assert!(
                        !cache.contains_key(&key),
                        "contains_key disagrees with get for absent key {key}"
                    );
                }
            },
            TestOp::Remove { key } => {
                let removed = cache.remove(&key);
                let expected = model.remove(&key);
                match (removed, expected) {
                    (Some(actual), Some(expected_value)) => {
                        assert_eq!(
                            actual, expected_value,
                            "remove returned mutated or wrong value for key {key}"
                        );
                        assert_eq!(actual.key, key, "remove returned value for wrong key");
                    }
                    (None, _) => {}
                    (Some(_), None) => {
                        panic!("remove returned a value for key {key} absent in model");
                    }
                }
                assert!(cache.get(&key).is_none());
                assert!(!cache.contains_key(&key));
            }
        }

        assert_global_invariants(&cache);
        assert_resident_entries_match_model(&cache, &model);
    }

    for &predicate in remove_predicates {
        let expected_removed = model
            .keys()
            .filter(|key| remove_predicate_matches(predicate, **key) && cache.contains_key(key))
            .count();
        let len_before_removal = cache.len();
        match predicate {
            RemovePredicate::EvenKeys => cache.remove_if(|key| key % 2 == 0),
            RemovePredicate::KeyLessThan(cutoff) => cache.remove_if(|key| *key < cutoff),
            RemovePredicate::ExactKey(exact) => cache.remove_if(|key| *key == exact),
        };
        let removed = len_before_removal - cache.len();
        assert_eq!(removed, expected_removed);
        model.retain(|key, _| !remove_predicate_matches(predicate, *key));
        assert_global_invariants(&cache);
        assert_resident_entries_match_model(&cache, &model);
    }
}

fn op_strategy() -> impl Strategy<Value = TestOp> {
    prop_oneof![
        (0u8..32, any::<u32>(), any::<u64>(), 0u8..8).prop_map(|(key, version, token, charge)| {
            TestOp::Insert {
                key,
                version,
                token,
                charge,
            }
        }),
        (0u8..32).prop_map(|key| TestOp::Get { key }),
        (0u8..32).prop_map(|key| TestOp::Remove { key }),
    ]
}

fn predicate_strategy() -> impl Strategy<Value = RemovePredicate> {
    prop_oneof![
        Just(RemovePredicate::EvenKeys),
        (0u8..32).prop_map(RemovePredicate::KeyLessThan),
        (0u8..32).prop_map(RemovePredicate::ExactKey),
    ]
}

proptest! {
    #[test]
    fn single_shard_equals_model(
        ops in prop::collection::vec(op_strategy(), 1..200),
        remove_predicates in prop::collection::vec(predicate_strategy(), 0..8),
        capacity in 0usize..48,
    ) {
        run_model_sequence(1, capacity, &ops, &remove_predicates);
    }

    #[test]
    fn multi_shard_equals_model(
        ops in prop::collection::vec(op_strategy(), 1..200),
        remove_predicates in prop::collection::vec(predicate_strategy(), 0..8),
        shard_pow in 0u8..=3,
        capacity in 0usize..96,
    ) {
        let shards = 1usize << shard_pow;
        run_model_sequence(shards, capacity, &ops, &remove_predicates);
    }
}

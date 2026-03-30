use crate::{
    BufferCacheAllocationStrategy, BufferCacheBuilder, BufferCacheStrategy, CacheEntry, ThreadType,
};
use std::sync::Arc;

#[derive(Clone)]
struct TestEntry;

impl CacheEntry for TestEntry {
    fn cost(&self) -> usize {
        1
    }
}

type TestBuilder = BufferCacheBuilder<u64, TestEntry>;

#[test]
fn s3_fifo_builder_shares_caches_per_worker_pair_by_default() {
    let caches = TestBuilder::new().build(2, 1024);
    assert!(Arc::ptr_eq(
        &caches[0][ThreadType::Foreground],
        &caches[0][ThreadType::Background]
    ));
    assert!(Arc::ptr_eq(
        &caches[1][ThreadType::Foreground],
        &caches[1][ThreadType::Background]
    ));
    assert!(!Arc::ptr_eq(
        &caches[0][ThreadType::Foreground],
        &caches[1][ThreadType::Foreground]
    ));
}

#[test]
fn s3_fifo_builder_can_share_per_worker_pair() {
    let caches = TestBuilder::new()
        .with_buffer_cache_allocation_strategy(BufferCacheAllocationStrategy::SharedPerWorkerPair)
        .build(2, 1024);
    assert!(Arc::ptr_eq(
        &caches[0][ThreadType::Foreground],
        &caches[0][ThreadType::Background]
    ));
    assert!(Arc::ptr_eq(
        &caches[1][ThreadType::Foreground],
        &caches[1][ThreadType::Background]
    ));
    assert!(!Arc::ptr_eq(
        &caches[0][ThreadType::Foreground],
        &caches[1][ThreadType::Foreground]
    ));
}

#[test]
fn s3_fifo_builder_can_share_globally() {
    let caches = TestBuilder::new()
        .with_buffer_cache_allocation_strategy(BufferCacheAllocationStrategy::Global)
        .build(2, 1024);
    assert!(Arc::ptr_eq(
        &caches[0][ThreadType::Foreground],
        &caches[0][ThreadType::Background]
    ));
    assert!(Arc::ptr_eq(
        &caches[0][ThreadType::Foreground],
        &caches[1][ThreadType::Foreground]
    ));
    assert!(Arc::ptr_eq(
        &caches[0][ThreadType::Foreground],
        &caches[1][ThreadType::Background]
    ));
}

/// The LRU cache has a single mutex, sharing it would be harmful.
#[test]
fn lru_builder_keeps_separate_caches_even_when_sharing_is_requested() {
    let caches = TestBuilder::new()
        .with_buffer_cache_strategy(BufferCacheStrategy::Lru)
        .with_buffer_cache_allocation_strategy(BufferCacheAllocationStrategy::Global)
        .build(2, 1024);
    assert!(!Arc::ptr_eq(
        &caches[0][ThreadType::Foreground],
        &caches[0][ThreadType::Background]
    ));
    assert!(!Arc::ptr_eq(
        &caches[0][ThreadType::Foreground],
        &caches[1][ThreadType::Foreground]
    ));
    assert!(!Arc::ptr_eq(
        &caches[0][ThreadType::Foreground],
        &caches[1][ThreadType::Background]
    ));
}

#[test]
fn lru_builder_uses_separate_caches_by_default() {
    let caches = TestBuilder::new()
        .with_buffer_cache_strategy(BufferCacheStrategy::Lru)
        .build(2, 1024);
    assert!(!Arc::ptr_eq(
        &caches[0][ThreadType::Foreground],
        &caches[0][ThreadType::Background]
    ));
    assert!(!Arc::ptr_eq(
        &caches[1][ThreadType::Foreground],
        &caches[1][ThreadType::Background]
    ));
    assert!(!Arc::ptr_eq(
        &caches[0][ThreadType::Foreground],
        &caches[1][ThreadType::Foreground]
    ));
}

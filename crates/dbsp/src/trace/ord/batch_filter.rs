//! Key prefilters for file-backed exact seeks.
//!
//! These filters only answer "definitely not present" versus "might still be
//! present", so callers can reject cheap cases before doing the exact lookup.

use crate::{
    dynamic::DataTrait,
    storage::file::reader::{ColumnSpec, Reader},
    trace::ord::key_range::KeyRange,
};
use dyn_clone::{DynClone, clone_box};
use size_of::SizeOf;
use std::sync::Arc;

/// A cheap, in-memory precheck used by `seek_key_exact`.
///
/// Each filter may only reject a key early. Returning `true` means "the key
/// might still be present", so the caller must continue with the next filter or
/// the exact lookup.
pub(crate) trait BatchFilter<K>: DynClone + Send + Sync
where
    K: DataTrait + ?Sized,
{
    /// Returns `false` only when the key is definitely absent.
    ///
    /// Filters that need a hash compute it through `hash`, so a chain of
    /// filters pays that cost at most once.
    fn maybe_contains_key(&self, key: &K, hash: &mut Option<u64>) -> bool;
}

/// Ordered key filters for file-backed batches.
///
/// The key range is kept separately from the trait objects because we need to
/// reuse it when a batch builds a new reader, e.g. in `neg_by_ref()`. All other
/// filters are stored behind a trait object so we can swap bloom for bitmap or
/// another single post-range filter without changing the batch structs again.
#[derive(SizeOf)]
pub(crate) struct BatchFilters<K>
where
    K: DataTrait + ?Sized,
{
    range_filter: Option<KeyRange<K>>,
    #[size_of(skip)]
    filter: Box<dyn BatchFilter<K>>,
}

impl<K> BatchFilters<K>
where
    K: DataTrait + ?Sized,
{
    pub(crate) fn new(range_filter: Option<KeyRange<K>>, filter: Box<dyn BatchFilter<K>>) -> Self {
        Self {
            range_filter,
            filter,
        }
    }

    /// Builds the current file-backed filter chain.
    ///
    /// The range check runs before the bloom filter so out-of-range keys never
    /// pay the hash or bloom lookup cost.
    pub(crate) fn from_file<A, N>(
        key_range: Option<KeyRange<K>>,
        file: Arc<Reader<(&'static K, &'static A, N)>>,
    ) -> Self
    where
        A: DataTrait + ?Sized,
        N: 'static,
        (&'static K, &'static A, N): ColumnSpec,
    {
        Self::new(key_range, Box::new(file))
    }

    pub(crate) fn key_range(&self) -> Option<&KeyRange<K>> {
        self.range_filter.as_ref()
    }

    pub(crate) fn maybe_contains_key(&self, key: &K, mut hash: Option<u64>) -> bool {
        if !self.range_filter.maybe_contains_key(key, &mut hash) {
            return false;
        }

        self.filter.maybe_contains_key(key, &mut hash)
    }
}

impl<K> Clone for BatchFilters<K>
where
    K: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            range_filter: self.range_filter.clone(),
            filter: clone_box(self.filter.as_ref()),
        }
    }
}

impl<K> BatchFilter<K> for Option<KeyRange<K>>
where
    K: DataTrait + ?Sized,
{
    fn maybe_contains_key(&self, key: &K, _hash: &mut Option<u64>) -> bool {
        // `None` means the batch is empty, so no key can match.
        self.as_ref().is_some_and(|range| range.contains(key))
    }
}

impl<K, A, N> BatchFilter<K> for Arc<Reader<(&'static K, &'static A, N)>>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    N: 'static,
    (&'static K, &'static A, N): ColumnSpec,
{
    fn maybe_contains_key(&self, key: &K, hash: &mut Option<u64>) -> bool {
        let hash = hash.get_or_insert_with(|| key.default_hash());
        self.as_ref().maybe_contains_key(*hash)
    }
}

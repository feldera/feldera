//! In-memory key bounds cached for ordered batches.

use crate::dynamic::DataTrait;
use dyn_clone::clone_box;
use size_of::SizeOf;

/// Closed key interval for a batch.
///
/// We materialize the endpoints once and keep them in memory so exact-key seeks
/// can reject out-of-range keys before touching slower filters.
#[derive(Debug, SizeOf)]
pub(crate) struct KeyRange<K>
where
    K: DataTrait + ?Sized,
{
    min: Box<K>,
    max: Box<K>,
}

impl<K> KeyRange<K>
where
    K: DataTrait + ?Sized,
{
    /// Creates a range from owned endpoints.
    pub(crate) fn new(min: Box<K>, max: Box<K>) -> Self {
        assert!(min.as_ref() <= max.as_ref());
        Self { min, max }
    }

    /// Clones a range from borrowed endpoints.
    pub(crate) fn from_refs(min: &K, max: &K) -> Self {
        Self::new(clone_box(min), clone_box(max))
    }

    /// Returns `true` when `key` is inside the closed interval.
    pub(crate) fn contains(&self, key: &K) -> bool {
        self.min.as_ref() <= key && key <= self.max.as_ref()
    }
}

impl<K> From<(Box<K>, Box<K>)> for KeyRange<K>
where
    K: DataTrait + ?Sized,
{
    fn from((min, max): (Box<K>, Box<K>)) -> Self {
        Self::new(min, max)
    }
}

impl<K> Clone for KeyRange<K>
where
    K: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self::from_refs(self.min.as_ref(), self.max.as_ref())
    }
}

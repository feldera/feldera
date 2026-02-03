//! Filtering predicates for traces.
//!
//! Filters are used by the garbage collector to discard unused records.
//! We support different several types of filters for keys and values.

use dyn_clone::DynClone;

use crate::{circuit::metadata::MetaItem, dynamic::Factory};

pub trait FilterFunc<V: ?Sized>: Fn(&V) -> bool + DynClone + Send + Sync {}

impl<V: ?Sized, F> FilterFunc<V> for F where F: Fn(&V) -> bool + Clone + Send + Sync + 'static {}

dyn_clone::clone_trait_object! {<V: ?Sized> FilterFunc<V>}

pub struct Filter<V: ?Sized> {
    filter_func: Box<dyn FilterFunc<V>>,
    metadata: MetaItem,
}

impl<V: ?Sized> Filter<V> {
    pub fn new(filter_func: Box<dyn FilterFunc<V>>) -> Self {
        Self {
            filter_func,
            metadata: MetaItem::String(String::new()),
        }
    }

    pub fn with_metadata(mut self, metadata: MetaItem) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn filter_func(&self) -> &dyn FilterFunc<V> {
        self.filter_func.as_ref()
    }

    pub fn metadata(&self) -> &MetaItem {
        &self.metadata
    }

    pub fn include(this: &Option<Filter<V>>, value: &V) -> bool {
        this.as_ref().is_none_or(|f| (f.filter_func)(value))
    }
}

impl<V: ?Sized> Clone for Filter<V> {
    fn clone(&self) -> Self {
        Self {
            filter_func: self.filter_func.clone(),
            metadata: self.metadata.clone(),
        }
    }
}

/// A filter over a group of values associated with a key.
///
/// * `Simple` - retain all values that satisfy a predicate. Doesn't make
///   any assumptions about the ordering of values.
/// * `LastN` - retains all values that satisfy a predicate and up to a
///   constant number of values preceding the first value that satisfies the
///   predicate. If no value in the group satisfies the predicate, retains the
///   last N values in the group.
///   Assumes that the predicate is monotonic: once it is satisfied for a value,
///   it is also satisfied for all subsequent values for the same key.
///   Also assumed that the values are ordered in some way, so that the last N
///   values under the cursor ate the ones that need to be preserved.
/// * `TopN` - retains all values that satisfy a predicate and up to a
///   constant number of largest values that do not satisfy the predicate.
///   This is similar to `LastN`, but it does not assume that the predicate is
///   monotonic.
///
///
/// Note that the `LastN`, `TopN` and `BottomN` filters can not be evaluated against
/// an individual batch and require access to the complete spine that the batch belongs
/// to.  The reason is that some of the last N values in the batch may not be present
/// in the trace because there may exist retractions for them in other batches within
/// the spine.
///
/// Therefore, these filters are only evaluated as part of a background merge.
/// See `BatchReader::merge_batches_with_snapshot` for more details.
pub enum GroupFilter<V: ?Sized + 'static> {
    Simple(Filter<V>),
    LastN(usize, Filter<V>),
    TopN(usize, Filter<V>, &'static dyn Factory<V>),
    BottomN(usize, Filter<V>, &'static dyn Factory<V>),
}

impl<V: ?Sized + 'static> GroupFilter<V> {
    /// Returns true if the filter cannot be evaluated against an individual batch and requires
    /// access to the complete spine that the batch belongs to.
    pub fn requires_snapshot(&self) -> bool {
        match self {
            Self::Simple(..) => false,
            Self::LastN(..) => true,
            Self::TopN(..) => true,
            Self::BottomN(..) => true,
        }
    }
}

impl<V: ?Sized> Clone for GroupFilter<V> {
    fn clone(&self) -> Self {
        match self {
            Self::Simple(filter) => Self::Simple(filter.clone()),
            Self::LastN(n, filter) => Self::LastN(*n, filter.clone()),
            Self::TopN(n, filter, vals_factory) => Self::TopN(*n, filter.clone(), *vals_factory),
            Self::BottomN(n, filter, vals_factory) => {
                Self::BottomN(*n, filter.clone(), *vals_factory)
            }
        }
    }
}

impl<V: ?Sized> GroupFilter<V> {
    pub fn metadata(&self) -> MetaItem {
        match self {
            Self::Simple(filter) => filter.metadata().clone(),
            Self::LastN(_n, filter) => filter.metadata().clone(),
            Self::TopN(_n, filter, _vals_factory) => filter.metadata().clone(),
            Self::BottomN(_n, filter, _vals_factory) => filter.metadata().clone(),
        }
    }
}

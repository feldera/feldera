use crate::trace::{
    consolidation::consolidate_from,
    layers::{
        erased::{ErasedLayer, IntoErasedData, IntoErasedDiff, TypedLayer},
        Builder, MergeBuilder, Trie, TupleBuilder,
    },
};
use size_of::SizeOf;
use std::marker::PhantomData;

/// A builder for ordered values
#[derive(SizeOf)]
pub struct TypedLayerBuilder<K, R> {
    layer: ErasedLayer,
    __type: PhantomData<(K, R)>,
}

impl<K, R> Builder for TypedLayerBuilder<K, R>
where
    K: IntoErasedData,
    R: IntoErasedDiff,
{
    type Trie = TypedLayer<K, R>;

    fn boundary(&mut self) -> usize {
        self.layer.len()
    }

    fn done(self) -> Self::Trie {
        TypedLayer {
            layer: self.layer,
            __type: PhantomData,
        }
    }
}

impl<K, R> MergeBuilder for TypedLayerBuilder<K, R>
where
    K: IntoErasedData,
    R: IntoErasedDiff,
{
    fn with_capacity(left: &Self::Trie, right: &Self::Trie) -> Self {
        let capacity = Trie::keys(left) + Trie::keys(right);
        Self::with_key_capacity(capacity)
    }

    fn with_key_capacity(capacity: usize) -> Self {
        Self {
            layer: ErasedLayer::with_capacity(
                &<K as IntoErasedData>::DATA_VTABLE,
                &<R as IntoErasedDiff>::DIFF_VTABLE,
                capacity,
            ),
            __type: PhantomData,
        }
    }

    fn reserve(&mut self, additional: usize) {
        self.layer.reserve(additional);
    }

    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
        // Safety: The current builder's layer and the trie layer types are the same
        unsafe { self.layer.extend_from_range(&other.layer, lower, upper) };
    }

    fn push_merge<'a>(
        &'a mut self,
        lhs_cursor: <Self::Trie as Trie>::Cursor<'a>,
        rhs_cursor: <Self::Trie as Trie>::Cursor<'a>,
    ) -> usize {
        let (lhs, rhs) = (lhs_cursor.storage(), rhs_cursor.storage());
        let (lhs_bounds, rhs_bounds) = (lhs_cursor.bounds(), rhs_cursor.bounds());
        self.layer.push_merge(lhs, lhs_bounds, rhs, rhs_bounds)
    }
}

impl<K, R> TupleBuilder for TypedLayerBuilder<K, R>
where
    K: IntoErasedData,
    R: IntoErasedDiff,
{
    type Item = (K, R);

    fn new() -> Self {
        Self {
            layer: ErasedLayer::new(
                &<K as IntoErasedData>::DATA_VTABLE,
                &<R as IntoErasedDiff>::DIFF_VTABLE,
            ),
            __type: PhantomData,
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            layer: ErasedLayer::with_capacity(
                &<K as IntoErasedData>::DATA_VTABLE,
                &<R as IntoErasedDiff>::DIFF_VTABLE,
                capacity,
            ),
            __type: PhantomData,
        }
    }

    fn reserve_tuples(&mut self, additional: usize) {
        self.layer.reserve(additional);
    }

    fn tuples(&self) -> usize {
        self.layer.len()
    }

    fn push_tuple(&mut self, tuple: (K, R)) {
        self.layer.push(tuple);
    }
}

/// A builder for unordered values
#[derive(Debug, Clone, SizeOf)]
pub struct UnorderedTypedLayerBuilder<K, R> {
    tuples: Vec<(K, R)>,
    boundary: usize,
}

impl<K, R> UnorderedTypedLayerBuilder<K, R> {
    /// Create a new `UnorderedTypedLayerBuilder`
    pub const fn new() -> Self {
        Self {
            tuples: Vec::new(),
            boundary: 0,
        }
    }

    /// Get the length of the current builder
    pub(crate) fn len(&self) -> usize {
        self.tuples.len()
    }
}

impl<K, R> Builder for UnorderedTypedLayerBuilder<K, R>
where
    K: IntoErasedData,
    R: IntoErasedDiff,
{
    type Trie = TypedLayer<K, R>;

    fn boundary(&mut self) -> usize {
        consolidate_from(&mut self.tuples, self.boundary);
        self.boundary = self.len();
        self.boundary
    }

    fn done(mut self) -> Self::Trie {
        self.boundary();

        let mut layer =
            ErasedLayer::with_capacity(&K::DATA_VTABLE, &R::DIFF_VTABLE, self.tuples.len());
        layer.extend(self.tuples);

        // TODO: The tuples buffer is dropped here, can we reuse it for other builders?
        TypedLayer {
            layer,
            __type: PhantomData,
        }
    }
}

impl<K, R> TupleBuilder for UnorderedTypedLayerBuilder<K, R>
where
    K: IntoErasedData,
    R: IntoErasedDiff,
{
    type Item = (K, R);

    fn new() -> Self {
        Self {
            tuples: Vec::new(),
            boundary: 0,
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            tuples: Vec::with_capacity(capacity),
            boundary: 0,
        }
    }

    fn reserve_tuples(&mut self, additional: usize) {
        self.tuples.reserve(additional);
    }

    fn tuples(&self) -> usize {
        self.len()
    }

    fn push_tuple(&mut self, tuple: (K, R)) {
        self.tuples.push(tuple);
    }

    fn extend_tuples<I>(&mut self, tuples: I)
    where
        I: IntoIterator<Item = Self::Item>,
    {
        self.tuples.extend(tuples);
    }
}

impl<K, R> Default for UnorderedTypedLayerBuilder<K, R> {
    fn default() -> Self {
        Self::new()
    }
}

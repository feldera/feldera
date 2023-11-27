use crate::{
    trace::{
        consolidation::consolidate_from,
        layers::{
            erased::{ErasedKeyLeaf, IntoErasedData, TypedErasedKeyLeaf},
            Builder, Cursor, MergeBuilder, Trie, TupleBuilder,
        },
    },
    DBData, DBWeight,
};
use size_of::SizeOf;
use std::marker::PhantomData;

/// A builder for ordered values
#[derive(SizeOf)]
pub struct TypedErasedKeyLeafBuilder<K, R> {
    layer: ErasedKeyLeaf<R>,
    __type: PhantomData<(K, R)>,
}

impl<K, R> Builder for TypedErasedKeyLeafBuilder<K, R>
where
    K: DBData + IntoErasedData,
    R: DBWeight,
{
    type Trie = TypedErasedKeyLeaf<K, R>;

    fn boundary(&mut self) -> usize {
        self.layer.len()
    }

    fn done(self) -> Self::Trie {
        TypedErasedKeyLeaf {
            layer: self.layer,
            __type: PhantomData,
        }
    }
}

impl<K, R> MergeBuilder for TypedErasedKeyLeafBuilder<K, R>
where
    K: DBData + IntoErasedData,
    R: DBWeight,
{
    fn with_capacity(left: &Self::Trie, right: &Self::Trie) -> Self {
        let capacity = Trie::keys(left) + Trie::keys(right);
        Self::with_key_capacity(capacity)
    }

    fn with_key_capacity(capacity: usize) -> Self {
        Self {
            layer: ErasedKeyLeaf::with_capacity(&<K as IntoErasedData>::DATA_VTABLE, capacity),
            __type: PhantomData,
        }
    }

    fn reserve(&mut self, additional: usize) {
        self.layer.reserve(additional);
    }

    fn keys(&self) -> usize {
        self.layer.keys.len()
    }

    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
        // Safety: The current builder's layer and the trie layer types are the same
        unsafe { self.layer.extend_from_range(&other.layer, lower, upper) };
    }

    fn copy_range_retain_keys<'a, F>(
        &mut self,
        _other: &'a Self::Trie,
        _lower: usize,
        _upper: usize,
        _filter: &F,
    ) where
        F: Fn(&<<Self::Trie as Trie>::Cursor<'a> as Cursor<'a>>::Key) -> bool,
    {
        todo!()
    }

    fn push_merge<'a>(
        &'a mut self,
        lhs_cursor: <Self::Trie as Trie>::Cursor<'a>,
        rhs_cursor: <Self::Trie as Trie>::Cursor<'a>,
    ) {
        let (lhs, rhs) = (lhs_cursor.storage(), rhs_cursor.storage());
        let lhs_bounds = (lhs_cursor.position(), lhs_cursor.bounds().1);
        let rhs_bounds = (rhs_cursor.position(), rhs_cursor.bounds().1);
        self.layer.push_merge(lhs, lhs_bounds, rhs, rhs_bounds);
    }

    fn push_merge_retain_keys<'a, F>(
        &'a mut self,
        _other1: <Self::Trie as Trie>::Cursor<'a>,
        _other2: <Self::Trie as Trie>::Cursor<'a>,
        _filter: &F,
    ) where
        F: Fn(&<<Self::Trie as Trie>::Cursor<'a> as Cursor<'a>>::Key) -> bool,
    {
        todo!()
    }
}

impl<K, R> TupleBuilder for TypedErasedKeyLeafBuilder<K, R>
where
    K: DBData + IntoErasedData,
    R: DBWeight,
{
    type Item = (K, R);

    fn new() -> Self {
        Self {
            layer: ErasedKeyLeaf::new(&<K as IntoErasedData>::DATA_VTABLE),
            __type: PhantomData,
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            layer: ErasedKeyLeaf::with_capacity(&<K as IntoErasedData>::DATA_VTABLE, capacity),
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
    K: DBData + IntoErasedData,
    R: DBWeight,
{
    type Trie = TypedErasedKeyLeaf<K, R>;

    fn boundary(&mut self) -> usize {
        consolidate_from(&mut self.tuples, self.boundary);
        self.boundary = self.len();
        self.boundary
    }

    fn done(mut self) -> Self::Trie {
        self.boundary();

        let mut layer = ErasedKeyLeaf::with_capacity(&K::DATA_VTABLE, self.tuples.len());
        layer.extend(self.tuples);

        // TODO: The tuples buffer is dropped here, can we reuse it for other builders?
        TypedErasedKeyLeaf {
            layer,
            __type: PhantomData,
        }
    }
}

impl<K, R> TupleBuilder for UnorderedTypedLayerBuilder<K, R>
where
    K: DBData + IntoErasedData,
    R: DBWeight,
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

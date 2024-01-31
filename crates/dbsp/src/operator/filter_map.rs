//! Filter and transform data record-by-record.

use crate::{
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, OwnershipPreference, Scope, Stream,
    },
    trace::{
        ord::{FileIndexedZSet, FileZSet, VecIndexedZSet, VecZSet},
        Batch, BatchReader, Builder, Consumer, Cursor, ValueConsumer,
    },
    DBData, DBWeight, OrdIndexedZSet, OrdZSet,
};
use std::{
    any::TypeId,
    borrow::Cow,
    marker::PhantomData,
    mem::{transmute_copy, ManuallyDrop},
};

/// This trait abstracts away a stream of records that can be filtered
/// and transformed on a record-by-record basis.
///
/// The notion of a record is determined by each implementer and can be either
/// a `(key, value)` tuple for a stream of key/value pairs or just `key`
/// for a stream of singleton values.
///
/// # Background
///
/// DBSP represents relational data using the [`Batch`](`crate::trace::Batch`)
/// trait. A batch is conceptually a set of `(key, value, weight)` tuples.  When
/// processing batches, we often need to filter and/or transform tuples one at a
/// time with a user-provided closure, e.g., we may want to create a batch with
/// only the tuples that satisfy a predicate of the form `Fn(K, V) -> bool`.
///
/// In practice we often work with specialized implementations of `Batch` where
/// the value type is `()`, e.g., [`OrdZSet`](`crate::OrdZSet`).  We call such
/// batches **non-indexed batches**, in contrast to **indexed batches** like
/// [`OrdIndexedZSet`](`crate::OrdIndexedZSet`), which support arbitrary `value`
/// types.  When filtering or transforming non-indexed batches we want to ignore
/// the value and work with keys only, e.g., to write filtering predicates of
/// the form `Fn(K) -> bool` rather than `Fn(K, ()) -> bool`.
///
/// This trait enables both use cases by allowing the implementer to define
/// their own record type, which can be either `(K, V)` or `K`.
///
/// These methods are equally suitable for [streams of data or streams of
/// deltas](Stream#data-streams-versus-delta-streams).
///
/// This trait uses the same [naming
/// convention](Stream#operator-naming-convention) as [`Stream`], for `_index`
/// and `_generic` suffixes.
pub trait FilterMap<C> {
    /// Record type of the input stream, e.g., `(K, V)` for a stream of `(key,
    /// value, weight)` tuples or just `K` if the value type is `()`.
    type Item: 'static;

    /// A borrowed version of the record type, e.g., `(&K, &V)` for a stream of
    /// `(key, value, weight)` tuples or `&K` if the value type is `()`.
    type ItemRef<'a>;

    /// Type of the `weight` component of the `(key, value, weight)` tuple.
    type R: DBWeight;

    /// Filter input stream only retaining records that satisfy the
    /// `filter_func` predicate.
    fn filter<F>(&self, filter_func: F) -> Self
    where
        F: Fn(Self::ItemRef<'_>) -> bool + 'static;

    /// Applies `map_func` to each record in the input stream.  Assembles output
    /// record into `OrdZSet` batches.
    fn map<F, V>(&self, map_func: F) -> Stream<C, OrdZSet<V, Self::R>>
    where
        V: DBData,
        F: Fn(Self::ItemRef<'_>) -> V + Clone + 'static,
    {
        self.map_generic(map_func)
    }

    /// Like [`Self::map`], but can return any batch type.
    fn map_generic<F, T, O>(&self, map_func: F) -> Stream<C, O>
    where
        F: Fn(Self::ItemRef<'_>) -> T + Clone + 'static,
        O: Batch<Key = T, Val = (), Time = (), R = Self::R>;

    /// Behaves as [`Self::map`] followed by [`index`](`crate::Stream::index`),
    /// but is more efficient.  Assembles output records into
    /// `OrdIndexedZSet` batches.
    fn map_index<F, K, V>(&self, map_func: F) -> Stream<C, OrdIndexedZSet<K, V, Self::R>>
    where
        K: DBData,
        V: DBData,
        F: Fn(Self::ItemRef<'_>) -> (K, V) + 'static,
    {
        self.map_index_generic(map_func)
    }

    /// Like [`Self::map_index`], but can return any batch type.
    fn map_index_generic<F, K, V, O>(&self, map_func: F) -> Stream<C, O>
    where
        F: Fn(Self::ItemRef<'_>) -> (K, V) + 'static,
        O: Batch<Key = K, Val = V, Time = (), R = Self::R>;

    /// Applies `func` to each record in the input stream.  Assembles output
    /// records into `OrdZSet` batches.
    ///
    /// The output of `func` can be any type that implements `trait
    /// IntoIterator`, e.g., `Option<>` or `Vec<>`.
    fn flat_map<F, I>(&self, func: F) -> Stream<C, OrdZSet<I::Item, Self::R>>
    where
        F: FnMut(Self::ItemRef<'_>) -> I + 'static,
        I: IntoIterator + 'static,
        I::Item: DBData,
    {
        self.flat_map_generic(func)
    }

    /// Like [`Self::flat_map`], but can return any batch type.
    fn flat_map_generic<F, I, O>(&self, func: F) -> Stream<C, O>
    where
        F: FnMut(Self::ItemRef<'_>) -> I + 'static,
        I: IntoIterator + 'static,
        O: Batch<Key = I::Item, Val = (), Time = (), R = Self::R>;

    /// Behaves as [`Self::flat_map`] followed by
    /// [`index`](`crate::Stream::index`), but is more efficient.  Assembles
    /// output records into `OrdIndexedZSet` batches.
    fn flat_map_index<F, K, V, I>(&self, func: F) -> Stream<C, OrdIndexedZSet<K, V, Self::R>>
    where
        F: Fn(Self::ItemRef<'_>) -> I + 'static,
        I: IntoIterator<Item = (K, V)> + 'static,
        K: DBData,
        V: DBData,
    {
        self.flat_map_index_generic(func)
    }

    /// Like [`Self::flat_map_index`], but can return any batch type.
    fn flat_map_index_generic<F, K, V, I, O>(&self, func: F) -> Stream<C, O>
    where
        F: Fn(Self::ItemRef<'_>) -> I + 'static,
        I: IntoIterator<Item = (K, V)> + 'static,
        O: Batch<Key = K, Val = V, Time = (), R = Self::R> + Clone + 'static;
}

// This impl for VecZSet is identical to the one for FileZSet below.  There
// doesn't seem to be a good way to avoid the code duplication short of a macro.
impl<C, K, R> FilterMap<C> for Stream<C, VecZSet<K, R>>
where
    C: Circuit,
    K: DBData,
    R: DBWeight,
{
    type Item = K;
    type ItemRef<'a> = &'a K;
    type R = R;

    fn filter<F>(&self, filter_func: F) -> Self
    where
        F: Fn(Self::ItemRef<'_>) -> bool + 'static,
    {
        let filtered = self
            .circuit()
            .add_unary_operator(FilterKeys::new(filter_func), &self.try_sharded_version());
        filtered.mark_sharded_if(self);
        filtered
    }

    fn map_generic<F, T, O>(&self, map_func: F) -> Stream<C, O>
    where
        F: Fn(Self::ItemRef<'_>) -> T + Clone + 'static,
        O: Batch<Key = T, Val = (), Time = (), R = Self::R>,
    {
        self.circuit().add_unary_operator(
            MapKeys::new(map_func.clone(), move |x| (map_func)(&x)),
            self,
        )
    }

    fn map_index_generic<F, KT, VT, O>(&self, map_func: F) -> Stream<C, O>
    where
        F: Fn(Self::ItemRef<'_>) -> (KT, VT) + 'static,
        O: Batch<Key = KT, Val = VT, Time = (), R = Self::R>,
    {
        self.circuit().add_unary_operator(
            Map::new(move |kv: (Self::ItemRef<'_>, &())| map_func(kv.0)),
            self,
        )
    }

    fn flat_map_generic<F, I, O>(&self, mut func: F) -> Stream<C, O>
    where
        F: FnMut(Self::ItemRef<'_>) -> I + 'static,
        I: IntoIterator + 'static,
        O: Batch<Key = I::Item, Val = (), Time = (), R = Self::R>,
    {
        self.circuit().add_unary_operator(
            FlatMap::new(move |kv: (Self::ItemRef<'_>, &())| {
                func(kv.0).into_iter().map(|x| (x, ()))
            }),
            self,
        )
    }

    fn flat_map_index_generic<F, KT, VT, I, O>(&self, func: F) -> Stream<C, O>
    where
        F: Fn(Self::ItemRef<'_>) -> I + 'static,
        I: IntoIterator<Item = (KT, VT)> + 'static,
        O: Batch<Key = KT, Val = VT, Time = (), R = Self::R>,
    {
        self.circuit().add_unary_operator(
            FlatMap::new(move |kv: (Self::ItemRef<'_>, &())| func(kv.0)),
            self,
        )
    }
}

impl<C, K, R> FilterMap<C> for Stream<C, FileZSet<K, R>>
where
    C: Circuit,
    K: DBData,
    R: DBWeight,
{
    type Item = K;
    type ItemRef<'a> = &'a K;
    type R = R;

    fn filter<F>(&self, filter_func: F) -> Self
    where
        F: Fn(Self::ItemRef<'_>) -> bool + 'static,
    {
        let filtered = self
            .circuit()
            .add_unary_operator(FilterKeys::new(filter_func), &self.try_sharded_version());
        filtered.mark_sharded_if(self);
        filtered
    }

    fn map_generic<F, T, O>(&self, map_func: F) -> Stream<C, O>
    where
        F: Fn(Self::ItemRef<'_>) -> T + Clone + 'static,
        O: Batch<Key = T, Val = (), Time = (), R = Self::R>,
    {
        self.circuit().add_unary_operator(
            MapKeys::new(map_func.clone(), move |x| (map_func)(&x)),
            self,
        )
    }

    fn map_index_generic<F, KT, VT, O>(&self, map_func: F) -> Stream<C, O>
    where
        F: Fn(Self::ItemRef<'_>) -> (KT, VT) + 'static,
        O: Batch<Key = KT, Val = VT, Time = (), R = Self::R>,
    {
        self.circuit().add_unary_operator(
            Map::new(move |kv: (Self::ItemRef<'_>, &())| map_func(kv.0)),
            self,
        )
    }

    fn flat_map_generic<F, I, O>(&self, mut func: F) -> Stream<C, O>
    where
        F: FnMut(Self::ItemRef<'_>) -> I + 'static,
        I: IntoIterator + 'static,
        O: Batch<Key = I::Item, Val = (), Time = (), R = Self::R>,
    {
        self.circuit().add_unary_operator(
            FlatMap::new(move |kv: (Self::ItemRef<'_>, &())| {
                func(kv.0).into_iter().map(|x| (x, ()))
            }),
            self,
        )
    }

    fn flat_map_index_generic<F, KT, VT, I, O>(&self, func: F) -> Stream<C, O>
    where
        F: Fn(Self::ItemRef<'_>) -> I + 'static,
        I: IntoIterator<Item = (KT, VT)> + 'static,
        O: Batch<Key = KT, Val = VT, Time = (), R = Self::R>,
    {
        self.circuit().add_unary_operator(
            FlatMap::new(move |kv: (Self::ItemRef<'_>, &())| func(kv.0)),
            self,
        )
    }
}

// This impl for VecIndexedZSet is identical to the one for FileIndexedZSet
// below.  There doesn't seem to be a good way to avoid the code duplication
// short of a macro.
impl<C, K, V, R> FilterMap<C> for Stream<C, VecIndexedZSet<K, V, R>>
where
    C: Circuit,
    R: DBWeight,
    K: DBData,
    V: DBData,
{
    type Item = (K, V);
    type ItemRef<'a> = (&'a K, &'a V);
    type R = R;

    fn filter<F>(&self, filter_func: F) -> Self
    where
        F: Fn(Self::ItemRef<'_>) -> bool + 'static,
    {
        let filtered = self
            .circuit()
            .add_unary_operator(FilterVals::new(filter_func), &self.try_sharded_version());
        filtered.mark_sharded_if(self);
        filtered.mark_distinct_if(self);
        filtered
    }

    fn map_generic<F, T, O>(&self, map_func: F) -> Stream<C, O>
    where
        F: Fn(Self::ItemRef<'_>) -> T + Clone + 'static,
        O: Batch<Key = T, Val = (), Time = (), R = Self::R>,
    {
        self.circuit().add_unary_operator(
            Map::new(move |kv: Self::ItemRef<'_>| (map_func(kv), ())),
            self,
        )
    }

    fn map_index_generic<F, KT, VT, O>(&self, map_func: F) -> Stream<C, O>
    where
        F: Fn(Self::ItemRef<'_>) -> (KT, VT) + 'static,
        O: Batch<Key = KT, Val = VT, Time = (), R = Self::R>,
    {
        self.circuit().add_unary_operator(Map::new(map_func), self)
    }

    fn flat_map_generic<F, I, O>(&self, mut func: F) -> Stream<C, O>
    where
        F: FnMut(Self::ItemRef<'_>) -> I + 'static,
        I: IntoIterator + 'static,
        O: Batch<Key = I::Item, Val = (), Time = (), R = Self::R>,
    {
        self.circuit().add_unary_operator(
            FlatMap::new(move |kv: Self::ItemRef<'_>| func(kv).into_iter().map(|x| (x, ()))),
            self,
        )
    }

    fn flat_map_index_generic<F, KT, VT, I, O>(&self, func: F) -> Stream<C, O>
    where
        F: Fn(Self::ItemRef<'_>) -> I + 'static,
        I: IntoIterator<Item = (KT, VT)> + 'static,
        O: Batch<Key = KT, Val = VT, Time = (), R = Self::R>,
    {
        self.circuit().add_unary_operator(FlatMap::new(func), self)
    }
}

impl<C, K, V, R> FilterMap<C> for Stream<C, FileIndexedZSet<K, V, R>>
where
    C: Circuit,
    R: DBWeight,
    K: DBData,
    V: DBData,
{
    type Item = (K, V);
    type ItemRef<'a> = (&'a K, &'a V);
    type R = R;

    fn filter<F>(&self, filter_func: F) -> Self
    where
        F: Fn(Self::ItemRef<'_>) -> bool + 'static,
    {
        let filtered = self
            .circuit()
            .add_unary_operator(FilterVals::new(filter_func), &self.try_sharded_version());
        filtered.mark_sharded_if(self);
        filtered.mark_distinct_if(self);
        filtered
    }

    fn map_generic<F, T, O>(&self, map_func: F) -> Stream<C, O>
    where
        F: Fn(Self::ItemRef<'_>) -> T + Clone + 'static,
        O: Batch<Key = T, Val = (), Time = (), R = Self::R>,
    {
        self.circuit().add_unary_operator(
            Map::new(move |kv: Self::ItemRef<'_>| (map_func(kv), ())),
            self,
        )
    }

    fn map_index_generic<F, KT, VT, O>(&self, map_func: F) -> Stream<C, O>
    where
        F: Fn(Self::ItemRef<'_>) -> (KT, VT) + 'static,
        O: Batch<Key = KT, Val = VT, Time = (), R = Self::R>,
    {
        self.circuit().add_unary_operator(Map::new(map_func), self)
    }

    fn flat_map_generic<F, I, O>(&self, mut func: F) -> Stream<C, O>
    where
        F: FnMut(Self::ItemRef<'_>) -> I + 'static,
        I: IntoIterator + 'static,
        O: Batch<Key = I::Item, Val = (), Time = (), R = Self::R>,
    {
        self.circuit().add_unary_operator(
            FlatMap::new(move |kv: Self::ItemRef<'_>| func(kv).into_iter().map(|x| (x, ()))),
            self,
        )
    }

    fn flat_map_index_generic<F, KT, VT, I, O>(&self, func: F) -> Stream<C, O>
    where
        F: Fn(Self::ItemRef<'_>) -> I + 'static,
        I: IntoIterator<Item = (KT, VT)> + 'static,
        O: Batch<Key = KT, Val = VT, Time = (), R = Self::R>,
    {
        self.circuit().add_unary_operator(FlatMap::new(func), self)
    }
}

/// Internal implementation for filtering [`BatchReader`]s
pub struct FilterKeys<CI, CO, F> {
    filter: F,
    _type: PhantomData<*const (CI, CO)>,
}

impl<CI, CO, F> FilterKeys<CI, CO, F> {
    pub fn new(filter: F) -> Self {
        Self {
            filter,
            _type: PhantomData,
        }
    }

    fn filter_owned_generic(&mut self, input: CI) -> CO
    where
        CI: BatchReader<Time = ()>,
        CO: Batch<Key = CI::Key, Val = CI::Val, Time = (), R = CI::R>,
        F: Fn(&CI::Key) -> bool + 'static,
    {
        // We can use Builder because cursor yields ordered values.  This
        // is a nice property of the filter operation.
        //
        // Pre-allocating will create waste if most tuples get filtered out, since
        // the buffers allocated here can make it all the way to the output batch.
        // This is probably ok, because the batch will either get freed at the end
        // of the current clock tick or get added to the trace, where it will likely
        // get merged with other batches soon, at which point the waste is gone.
        let mut builder = CO::Builder::with_capacity((), input.len());

        let mut consumer = input.consumer();
        while consumer.key_valid() {
            let (key, mut values) = consumer.next_key();

            // FIXME: We assume that this operator will only be used with `OrdZSet` and that
            // it has either zero or one values, meaning that it'll skip any additional
            // values
            if (self.filter)(&key) && values.value_valid() {
                let (value, weight, ()) = values.next_value();
                builder.push((CO::item_from(key, value), weight));
            }
        }

        builder.done()
    }
}

impl<CI, CO, F> Operator for FilterKeys<CI, CO, F>
where
    CI: 'static,
    CO: 'static,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("FilterKeys")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<CI, CO, F> UnaryOperator<CI, CO> for FilterKeys<CI, CO, F>
where
    CI: BatchReader<Time = ()>,
    CO: Batch<Key = CI::Key, Val = CI::Val, Time = (), R = CI::R>,
    F: Fn(&CI::Key) -> bool + 'static,
{
    fn eval(&mut self, input: &CI) -> CO {
        // We can use Builder because cursor yields ordered values.  This
        // is a nice property of the filter operation.
        //
        // Pre-allocating will create waste if most tuples get filtered out, since
        // the buffers allocated here can make it all the way to the output batch.
        // This is probably ok, because the batch will either get freed at the end
        // of the current clock tick or get added to the trace, where it will likely
        // get merged with other batches soon, at which point the waste is gone.
        let mut builder = CO::Builder::with_capacity((), input.len());

        let mut cursor = input.cursor();
        while cursor.key_valid() {
            if (self.filter)(cursor.key()) {
                while cursor.val_valid() {
                    let val = cursor.val().clone();
                    let w = cursor.weight();
                    builder.push((CO::item_from(cursor.key().clone(), val), w.clone()));
                    cursor.step_val();
                }
            }
            cursor.step_key();
        }

        builder.done()
    }

    fn eval_owned(&mut self, input: CI) -> CO {
        // Bootleg specialization, we can do filtering in-place when the input and
        // output types are `OrdZSet`. I'd prefer to do this with "real" specialization
        // of some kind, but this'll work for now
        if TypeId::of::<CI>() == TypeId::of::<OrdZSet<CI::Key, CI::R>>()
            && TypeId::of::<CO>() == TypeId::of::<OrdZSet<CI::Key, CI::R>>()
        {
            // Safety: We've ensured that `CI` is an `OrdZSet`
            let mut output = unsafe {
                let input = ManuallyDrop::new(input);
                transmute_copy::<CI, OrdZSet<CI::Key, CI::R>>(&input)
            };

            // Filter the values within the `OrdZSet`
            output.retain(|key, _| (self.filter)(key));

            // Safety: We've ensured that `CO` is an `OrdZSet`
            unsafe {
                let output = ManuallyDrop::new(output);
                transmute_copy::<OrdZSet<CI::Key, CI::R>, CO>(&output)
            }
        } else {
            // Use a generic filter implementation
            self.filter_owned_generic(input)
        }
    }

    // Filtering *wants* owned values, but it's not critical to performance
    fn input_preference(&self) -> OwnershipPreference {
        if TypeId::of::<CI>() == TypeId::of::<OrdZSet<CI::Key, CI::R>>()
            && TypeId::of::<CO>() == TypeId::of::<OrdZSet<CI::Key, CI::R>>()
        {
            // Owned values matter a lot more when the input and output types are both
            // OrdZSet
            OwnershipPreference::PREFER_OWNED
        } else {
            OwnershipPreference::WEAKLY_PREFER_OWNED
        }
    }
}

/// Internal implementation for filtering [`BatchReader`]s
pub struct FilterVals<CI, CO, F>
where
    F: 'static,
{
    filter: F,
    _type: PhantomData<(CI, CO)>,
}

impl<CI, CO, F> FilterVals<CI, CO, F>
where
    F: 'static,
{
    pub fn new(filter: F) -> Self {
        Self {
            filter,
            _type: PhantomData,
        }
    }
}

impl<CI, CO, F> Operator for FilterVals<CI, CO, F>
where
    CI: 'static,
    CO: 'static,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("FilterVals")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<CI, CO, F> UnaryOperator<CI, CO> for FilterVals<CI, CO, F>
where
    CI: BatchReader<Time = ()>,
    CO: Batch<Key = CI::Key, Val = CI::Val, Time = (), R = CI::R>,
    for<'a> F: Fn((&'a CI::Key, &'a CI::Val)) -> bool + 'static,
{
    fn eval(&mut self, input: &CI) -> CO {
        // We can use Builder because cursor yields ordered values.  This
        // is a nice property of the filter operation.

        // This will create waste if most tuples get filtered out, since
        // the buffers allocated here can make it all the way to the output batch.
        // This is probably ok, because the batch will either get freed at the end
        // of the current clock tick or get added to the trace, where it will likely
        // get merged with other batches soon, at which point the waste is gone.
        let mut builder = CO::Builder::with_capacity((), input.len());

        let mut cursor = input.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                if (self.filter)((cursor.key(), cursor.val())) {
                    let val = cursor.val().clone();
                    let w = cursor.weight();
                    builder.push((CO::item_from(cursor.key().clone(), val), w.clone()));
                }
                cursor.step_val();
            }
            cursor.step_key();
        }

        builder.done()
    }

    fn eval_owned(&mut self, input: CI) -> CO {
        let mut builder = CO::Builder::with_capacity((), input.len());

        let mut consumer = input.consumer();
        while consumer.key_valid() {
            let (key, mut values) = consumer.next_key();

            while values.value_valid() {
                let (value, diff, ()) = values.next_value();

                if (self.filter)((&key, &value)) {
                    builder.push((CO::item_from(key.clone(), value), diff));
                }
            }
        }

        builder.done()
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::WEAKLY_PREFER_OWNED
    }
}

/// Internal implementation of `OrdIndexedZSet::map`,
/// `OrdIndexedZSet::map_index`.
pub struct Map<CI, CO, F> {
    map: F,
    _type: PhantomData<(CI, CO)>,
}

impl<CI, CO, F> Map<CI, CO, F>
where
    F: 'static,
{
    pub fn new(map: F) -> Self {
        Self {
            map,
            _type: PhantomData,
        }
    }
}

impl<CI, CO, F> Operator for Map<CI, CO, F>
where
    CI: 'static,
    CO: 'static,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("Map")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<CI, CO, F> UnaryOperator<CI, CO> for Map<CI, CO, F>
where
    CI: BatchReader<Time = ()>,
    CO: Batch<Time = (), R = CI::R>,
    for<'a> F: Fn((&'a CI::Key, &'a CI::Val)) -> (CO::Key, CO::Val) + 'static,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut batch = Vec::with_capacity(i.len());

        let mut cursor = i.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                let (k, v) = (self.map)((cursor.key(), cursor.val()));
                batch.push((CO::item_from(k, v), cursor.weight()));
                cursor.step_val();
            }
            cursor.step_key();
        }

        CO::from_tuples((), batch)
    }
}

/// Internal implementation of `OrdZSet::map`.
pub struct MapKeys<CI, CO, FB, FO> {
    map_borrowed: FB,
    map_owned: FO,
    _type: PhantomData<(CI, CO)>,
}

impl<CI, CO, FB, FO> MapKeys<CI, CO, FB, FO> {
    pub fn new(map_borrowed: FB, map_owned: FO) -> Self {
        Self {
            map_borrowed,
            map_owned,
            _type: PhantomData,
        }
    }
}

impl<CI, CO, FB, FO> Operator for MapKeys<CI, CO, FB, FO>
where
    CI: 'static,
    CO: 'static,
    FB: 'static,
    FO: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("MapKeys")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<CI, CO, FB, FO> UnaryOperator<CI, CO> for MapKeys<CI, CO, FB, FO>
where
    CI: BatchReader<Time = ()>,
    CO: Batch<Val = CI::Val, Time = (), R = CI::R>,
    FB: Fn(&CI::Key) -> CO::Key + 'static,
    FO: Fn(CI::Key) -> CO::Key + 'static,
{
    fn eval(&mut self, input: &CI) -> CO {
        let mut batch = Vec::with_capacity(input.len());

        let mut cursor = input.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                let w = cursor.weight();
                let v = cursor.val();
                let k = cursor.key();
                batch.push((CO::item_from((self.map_borrowed)(k), v.clone()), w.clone()));
                cursor.step_val();
            }
            cursor.step_key();
        }

        CO::from_tuples((), batch)
    }

    fn eval_owned(&mut self, input: CI) -> CO {
        let mut batch = Vec::with_capacity(input.len());

        let mut consumer = input.consumer();
        while consumer.key_valid() {
            let (key, mut values) = consumer.next_key();

            // FIXME: We assume that this operator will only be used with `OrdZSet` and that
            // it has either zero or one values, meaning that it'll skip any additional
            // values
            if values.value_valid() {
                let (value, weight, ()) = values.next_value();
                batch.push((CO::item_from((self.map_owned)(key), value), weight));
            }
        }

        CO::from_tuples((), batch)
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

/// Internal implementation of `flat_map` methods.
pub struct FlatMap<CI, CO, F, I> {
    map_func: F,
    _type: PhantomData<(CI, CO, I)>,
}

impl<CI, CO, F, I> FlatMap<CI, CO, F, I> {
    pub fn new(map_func: F) -> Self {
        Self {
            map_func,
            _type: PhantomData,
        }
    }
}

impl<CI, CO, F, I> Operator for FlatMap<CI, CO, F, I>
where
    CI: 'static,
    CO: 'static,
    F: 'static,
    I: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("FlatMap")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<CI, CO, F, I> UnaryOperator<CI, CO> for FlatMap<CI, CO, F, I>
where
    CI: BatchReader<Time = ()>,
    CO: Batch<Time = (), R = CI::R>,
    for<'a> F: FnMut((&'a CI::Key, &'a CI::Val)) -> I + 'static,
    I: IntoIterator<Item = (CO::Key, CO::Val)> + 'static,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut cursor = i.cursor();
        let mut batch = Vec::with_capacity(i.len());

        while cursor.key_valid() {
            while cursor.val_valid() {
                let weight = cursor.weight();
                let values = (self.map_func)((cursor.key(), cursor.val())).into_iter();

                // Reserve capacity for the given elements
                let (low, high) = values.size_hint();
                batch.reserve(high.unwrap_or(low));

                for (x, y) in values {
                    batch.push((CO::item_from(x, y), weight.clone()));
                }

                cursor.step_val();
            }

            cursor.step_key();
        }

        CO::from_tuples((), batch)
    }
}

#[cfg(test)]
mod test {
    use crate::utils::Tup2;
    use crate::{
        indexed_zset,
        operator::{FilterMap, Generator},
        trace::ord::OrdZSet,
        zset, Circuit, RootCircuit,
    };
    use std::vec;

    #[test]
    fn filter_map_test() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut input: vec::IntoIter<OrdZSet<Tup2<i64, String>, i64>> =
                vec![zset! { Tup2(1, "1".to_string()) => 1, Tup2(-1, "-1".to_string()) => 1, Tup2(5, "5 foo".to_string()) => 1, Tup2(-2, "-2".to_string()) => 1 }].into_iter();

            let mut filter_output =
                vec![zset! { 1 => 1, 5 => 1 }].into_iter();
            let mut i_filter_output =
                vec![indexed_zset! { 5 => {"5 foo".to_string() => 1} }].into_iter();
            let mut indexed_output =
                vec![indexed_zset! { 1 => {1 => 1}, -1 => {-1 => 1}, 5 => {5 => 1}, -2 => {-2 => 1} }].into_iter();
            let mut i_indexed_output =
                vec![indexed_zset! { 2 => {"1".to_string() => 1}, -2 => {"-1".to_string() => 1}, 10 => {"5 foo".to_string() => 1}, -4 => {"-2".to_string() => 1} }].into_iter();
            let mut times2_output =
                vec![zset! { 2 => 1, -2 => 1, 10 => 1, -4 => 1 }].into_iter();
            let mut i_times2_output =
                vec![zset! { 2 => 1, -2 => 1, 10 => 1, -4 => 1 }].into_iter();
            let mut times2_pos_output =
                vec![zset! { 2 => 1, 10 => 1 }].into_iter();
            let mut i_times2_pos_output =
                vec![zset! { 10 => 1 }].into_iter();
            let mut neg_output =
                vec![zset! { -1 => 1, 1 => 1, -5 => 1, 2 => 1}].into_iter();
            let mut i_neg_output =
                vec![zset! { -1 => 1, 1 => 1, -5 => 1, 2 => 1}].into_iter();
            let mut neg_pos_output =
                vec![zset! { -1 => 1, -5 => 1}].into_iter();
            let mut i_neg_pos_output =
                vec![zset! {-5 => 1}].into_iter();
            let mut abs_output =
                vec![zset! { 1 => 2, 5 => 1, 2 => 1 }].into_iter();
            let mut i_abs_output =
                vec![zset! { Tup2(1, "1".to_string()) => 1, Tup2(1, "-1".to_string()) => 1, Tup2(5, "5 foo".to_string()) => 1, Tup2(2, "-2".to_string()) => 1 }].into_iter();
            let mut abs_pos_output =
                vec![zset! { 1 => 1, 5 => 1 }].into_iter();
            let mut i_abs_pos_output =
                vec![zset! { Tup2(1, "1".to_string()) => 1, Tup2(5, "5 foo".to_string()) => 1 }].into_iter();
            let mut sqr_output =
                vec![zset! { 1 => 2, 25 => 1, 4 => 1 }].into_iter();
            let mut i_sqr_output =
                vec![zset! { 1 => 2, 25 => 1, 4 => 1 }].into_iter();
            let mut sqr_pos_output =
                vec![zset! { 1 => 1, 25 => 1 }].into_iter();
            let mut i_sqr_pos_output =
                vec![zset! { 25 => 1 }].into_iter();
            let mut sqr_pos_indexed_output =
                vec![indexed_zset! { 1 => {1 => 1}, 25 => {5 => 1} }].into_iter();
            let mut i_sqr_pos_indexed_output =
                vec![indexed_zset! { 1 => {"1".to_string() => 1}, 25 => {"5 foo".to_string() => 1} }].into_iter();

            let input =
                circuit.add_source(Generator::new(move || input.next().unwrap()));
            let input_indexed = input.index();
            let input_ints = input_indexed.map(|(&x, _)| x);

            let filter_pos = input_ints.filter(|&n| n > 0);
            let indexed = input_ints.map_index(|&n| (n, n));
            let times2 = input_ints.map(|&n| n * 2);
            let times2_pos = input_ints.flat_map(|&n| if n > 0 { Some(n * 2) } else { None });
            let neg = input_ints.map(|n| -n);
            let neg_pos = input_ints.flat_map(|n| if *n > 0 { Some(-n) } else { None });
            let abs = input_ints.map(|n| n.abs());
            let abs_pos = input_ints.flat_map(|n| if *n > 0 { Some(n.abs()) } else { None });
            let sqr = input_ints.map(|n| n * n);
            let sqr_pos = input_ints.flat_map(|n| if *n > 0 { Some(n * n) } else { None });
            let sqr_pos_indexed = input_ints.flat_map_index(|&n| if n > 0 { Some((n * n, n)) } else { None });

            let i_filter_pos = input_indexed.filter(|(&n, s)| n > 0 && s.contains("foo"));
            let i_indexed = input_indexed.map_index(move |(&n, s)| (2 * n, s.clone()));
            let i_times2 = input_indexed.map(|(&n, _)| n * 2);
            let i_times2_pos = input_indexed.flat_map(|(&n, s)| if n > 0 && s.contains("foo") { Some(n * 2) } else { None });
            let i_neg = input_indexed.map(|(n, _)| -n);
            let i_neg_pos = input_indexed.flat_map(|(&n, s)| if n > 0 && s.contains("foo") { Some(-n) } else { None });
            let i_abs = input_indexed.map(|(n, s)| Tup2(n.abs(), s.clone()));
            let i_abs_pos = input_indexed.flat_map(|(&n, s)| if n > 0 { Some(Tup2(n.abs(), s.clone())) } else { None });
            let i_sqr = input_indexed.map(|(n, _)| n * n);
            let i_sqr_pos = input_indexed.flat_map(|(&n, s)| if n > 0 && s.contains("foo") { Some(n * n) } else { None });
            let i_sqr_pos_indexed = input_indexed.flat_map_index(|(&n, s)| if n > 0 { Some((n * n, s.clone())) } else { None });

            filter_pos.inspect(move |n| {
                assert_eq!(*n, filter_output.next().unwrap());
            });
            indexed.inspect(move |n| {
                assert_eq!(*n, indexed_output.next().unwrap());
            });
            times2.inspect(move |n| {
                assert_eq!(*n, times2_output.next().unwrap());
            });
            times2_pos.inspect(move |n| {
                assert_eq!(*n, times2_pos_output.next().unwrap());
            });
            neg.inspect(move |n| {
                assert_eq!(*n, neg_output.next().unwrap());
            });
            neg_pos.inspect(move |n| {
                assert_eq!(*n, neg_pos_output.next().unwrap());
            });
            abs.inspect(move |n| {
                assert_eq!(*n, abs_output.next().unwrap());
            });
            abs_pos.inspect(move |n| {
                assert_eq!(*n, abs_pos_output.next().unwrap());
            });
            sqr.inspect(move |n| {
                assert_eq!(*n, sqr_output.next().unwrap());
            });
            sqr_pos.inspect(move |n| {
                assert_eq!(*n, sqr_pos_output.next().unwrap());
            });
            sqr_pos_indexed.inspect(move |n| {
                assert_eq!(*n, sqr_pos_indexed_output.next().unwrap());
            });
            i_filter_pos.inspect(move |n| {
                assert_eq!(*n, i_filter_output.next().unwrap());
            });
            i_indexed.inspect(move |n| {
                assert_eq!(*n, i_indexed_output.next().unwrap());
            });
            i_times2.inspect(move |n| {
                assert_eq!(*n, i_times2_output.next().unwrap());
            });
            i_times2_pos.inspect(move |n| {
                assert_eq!(*n, i_times2_pos_output.next().unwrap());
            });
            i_neg.inspect(move |n| {
                assert_eq!(*n, i_neg_output.next().unwrap());
            });
            i_neg_pos.inspect(move |n| {
                assert_eq!(*n, i_neg_pos_output.next().unwrap());
            });
            i_abs.inspect(move |n| {
                assert_eq!(*n, i_abs_output.next().unwrap());
            });
            i_abs_pos.inspect(move |n| {
                assert_eq!(*n, i_abs_pos_output.next().unwrap());
            });
            i_sqr.inspect(move |n| {
                assert_eq!(*n, i_sqr_output.next().unwrap());
            });
            i_sqr_pos.inspect(move |n| {
                assert_eq!(*n, i_sqr_pos_output.next().unwrap());
            });
            i_sqr_pos_indexed.inspect(move |n| {
                assert_eq!(*n, i_sqr_pos_indexed_output.next().unwrap());
            });
            Ok(())
        })
        .unwrap().0;

        for _ in 0..1 {
            circuit.step().unwrap();
        }
    }
}

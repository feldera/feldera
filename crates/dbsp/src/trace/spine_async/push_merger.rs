use itertools::Itertools;
use std::{cmp::Ordering, sync::Arc};

use ouroboros::self_referencing;

use crate::{
    algebra::Lattice,
    dynamic::{DynDataTyped, DynWeightedPairs, WeightTrait},
    time::Timestamp,
    trace::{
        cursor::{Pending, PushCursor},
        spine_async::index_set::IndexSet,
        Batch, BatchFactories, BatchReaderFactories, Builder, Filter, Weight,
    },
};

pub struct ArcPushMerger<B>(ArcPushMergerInner<B>)
where
    B: Batch;

#[self_referencing]
struct ArcPushMergerInner<B>
where
    B: Batch,
{
    batches: Vec<Arc<B>>,
    #[borrows(batches)]
    #[not_covariant]
    merger: PushMerger<Box<dyn PushCursor<B::Key, B::Val, B::Time, B::R> + Send + 'this>, B>,
}

impl<B> ArcPushMerger<B>
where
    B: Batch,
{
    pub fn new(
        factories: &B::Factories,
        batches: Vec<Arc<B>>,
        key_filter: &Option<Filter<B::Key>>,
        value_filter: &Option<Filter<B::Val>>,
    ) -> Self {
        Self(
            ArcPushMergerInnerBuilder {
                batches,
                merger_builder: |batches| {
                    PushMerger::new(
                        factories,
                        batches.iter().map(|b| b.push_cursor()).collect(),
                        key_filter.clone(),
                        value_filter.clone(),
                    )
                },
            }
            .build(),
        )
    }

    pub fn merge(
        &mut self,
        builder: &mut B::Builder,
        frontier: &B::Time,
        fuel: &mut isize,
    ) -> Result<(), Pending> {
        self.0
            .with_mut(|fields| fields.merger.merge(builder, frontier, fuel))
    }

    /// Gives all the cursors under this merger an opportunity to process I/O
    /// results and launch further I/O.  Should be called periodically.
    pub fn run(&mut self) {
        self.0.with_mut(|fields| fields.merger.run())
    }
}

/// Push-based merger.
///
/// Other mergers in DBSP do not take into account what data in their sources is
/// already in memory.  Instead, they read data from their sources blindly,
/// which results in blocking on I/O when they reach data that needs to be read
/// from disk.
///
/// `PushMerger` is different.  It takes [PushCursor]s, which distinguish data
/// that is currently readable from data that is pending I/O.  `PushMerger`
/// stops, instead of blocking, when it encounters data that is not yet
/// readable.  That is, the cursors "push" data into the merger, rather than the
/// merger pulling the data.  This allows the client to do other work while
/// further data is being fetched.
pub struct PushMerger<C, B>
where
    C: PushCursor<B::Key, B::Val, B::Time, B::R>,
    B: Batch,
{
    cursors: Vec<C>,
    key_filter: Option<Filter<B::Key>>,
    value_filter: Option<Filter<B::Val>>,
    any_values: bool,
    tmp_weight: Box<B::R>,
    time_diffs: Option<Box<DynWeightedPairs<DynDataTyped<B::Time>, B::R>>>,
}

impl<C, B> PushMerger<C, B>
where
    C: PushCursor<B::Key, B::Val, B::Time, B::R>,
    B: Batch,
{
    /// Creates a new merger for `cursors`.
    pub fn new(
        factories: &B::Factories,
        cursors: Vec<C>,
        key_filter: Option<Filter<B::Key>>,
        value_filter: Option<Filter<B::Val>>,
    ) -> Self {
        assert!(cursors.len() <= 64);
        Self {
            cursors,
            key_filter,
            value_filter,
            any_values: false,
            tmp_weight: factories.weight_factory().default_box(),
            time_diffs: factories.time_diffs_factory().map(|f| f.default_box()),
        }
    }

    /// Returns true if all merging is done.
    #[allow(dead_code)]
    pub fn is_done(&self) -> bool {
        self.cursors.iter().all(|cursor| cursor.key() == Ok(None))
    }

    /// Returns true if merging is not complete and there's enough data in
    /// memory to carry on.
    #[allow(dead_code)]
    pub fn is_ready(&self) -> bool {
        self.cursors.iter().all(|cursor| cursor.key().is_ok())
    }

    /// Continues merging, writing output to `builder` and reducing `*fuel`
    /// (which should initially be positive) by the amount of fuel used.  The
    /// possible results are:
    ///
    /// - `Ok(())`, if `*fuel > 0`: Merge is complete.
    ///
    /// - `Ok(())`, if `*fuel <= 0`: Merge is incomplete due to running out of fuel.
    ///
    /// - `Err(Pending)`: Merge is incomplete because one of the inputs is
    ///   waiting on I/O.
    fn merge(
        &mut self,
        builder: &mut B::Builder,
        frontier: &B::Time,
        fuel: &mut isize,
    ) -> Result<(), Pending> {
        debug_assert!(*fuel > 0);
        // We can drop all the cursors whose keys are at EOI.  If that
        // eliminates all of them, we're all done.
        //
        // If any keys or values are pending, then we can't do any work.
        assert!(self.cursors.len() <= 64);
        let mut remaining_cursors = IndexSet::empty();
        for (index, cursor) in self.cursors.iter_mut().enumerate() {
            skip_filtered_keys(cursor, &self.key_filter, &self.value_filter, fuel)?;
            if cursor.key()?.is_some() {
                cursor.val()?;
                remaining_cursors.add(index);
            }
        }
        if remaining_cursors.is_empty() || *fuel <= 0 {
            return Ok(());
        }

        let advance_func = |t: &mut DynDataTyped<B::Time>| t.join_assign(frontier);

        let time_map_func = if frontier == &B::Time::minimum() {
            None
        } else {
            Some(&advance_func as &dyn Fn(&mut DynDataTyped<B::Time>))
        };

        // As long as there are multiple cursors...
        while remaining_cursors.is_long() {
            if *fuel <= 0 {
                return Ok(());
            }

            // Find the indexes of the cursors with minimum keys, among the
            // remaining cursors.
            let orig_min_keys = find_min_indexes(
                remaining_cursors
                    .into_iter()
                    .map(|index| (index, self.cursors[index].key().unwrap())),
            );

            // As long as there is more than one cursor with minimum keys...
            let mut min_keys = orig_min_keys
                .into_iter()
                .filter(|index| self.cursors[*index].val().unwrap().is_some())
                .collect::<IndexSet>();
            while min_keys.is_long() {
                // ...Find the indexes of the cursors with minimum values, among
                // those with minimum keys, and copy their time-diff pairs and
                // value into the output.
                let min_vals = try_find_min_indexes(
                    min_keys
                        .into_iter()
                        .map(|index| (index, self.cursors[index].val())),
                )?;
                self.any_values =
                    self.copy_times(builder, time_map_func, min_vals, fuel) || self.any_values;

                // Then go on to the next value in each cursor, dropping the keys
                // for which we've exhausted the values.
                let mut pending = false;
                for index in min_vals {
                    match self.step_val(index, fuel) {
                        Err(Pending) => pending = true,
                        Ok(None) => min_keys.remove(index),
                        Ok(Some(_)) => (),
                    }
                }
                if pending {
                    return Err(Pending);
                }
                if *fuel <= 0 {
                    return Ok(());
                }
            }

            // If there's exactly one cursor left with minimum key, copy its
            // values into the output.
            if let Some(index) = min_keys.first() {
                loop {
                    self.cursors[index].val()?;
                    self.any_values =
                        self.copy_times(builder, time_map_func, min_keys, fuel) || self.any_values;
                    if self.step_val(index, fuel)?.is_none() {
                        break;
                    }
                    if *fuel <= 0 {
                        return Ok(());
                    }
                }
            }

            // If we wrote any values for these minimum keys, write the key.
            if self.any_values {
                let index = orig_min_keys.first().unwrap();
                builder.push_key(self.cursors[index].key().unwrap().unwrap());
                self.any_values = false;
            }

            // Advance each minimum-key cursor, dropping the cursors for which
            // we've exhausted the data.
            for index in orig_min_keys {
                if self.step_key(index, fuel)?.is_none() {
                    remaining_cursors.remove(index);
                }
            }
        }

        // If there is a cursor left (there's either one or none), copy it
        // directly to the output.
        if let Some(index) = remaining_cursors.first() {
            loop {
                loop {
                    self.cursors[index].val()?;
                    self.any_values =
                        self.copy_times(builder, time_map_func, remaining_cursors, fuel)
                            || self.any_values;
                    if self.step_val(index, fuel)?.is_none() {
                        break;
                    }
                    if *fuel <= 0 {
                        return Ok(());
                    }
                }
                debug_assert!(time_map_func.is_some() || self.any_values, "This assertion should fail only if B::Cursor is a spine or a CursorList, but we shouldn't be merging those");
                if self.any_values {
                    self.any_values = false;
                    builder.push_key(self.cursors[index].key().unwrap().unwrap());
                }
                if self.step_key(index, fuel)?.is_none() {
                    break;
                }
                if *fuel <= 0 {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Gives all the cursors under this merger an opportunity to process I/O
    /// results and launch further I/O.  Should be called periodically.
    pub fn run(&mut self) {
        for cursor in &mut self.cursors {
            cursor.run();
        }
    }

    fn step_val(&mut self, index: usize, fuel: &mut isize) -> Result<Option<&B::Val>, Pending> {
        *fuel -= 1;
        self.cursors[index].step_val();
        skip_filtered_values(&mut self.cursors[index], &self.value_filter, fuel)?;
        self.cursors[index].val()
    }

    fn step_key(&mut self, index: usize, fuel: &mut isize) -> Result<Option<&B::Key>, Pending> {
        *fuel -= 1;
        self.cursors[index].step_key();
        skip_filtered_keys(
            &mut self.cursors[index],
            &self.key_filter,
            &self.value_filter,
            fuel,
        )?;
        self.cursors[index].key()
    }

    fn copy_times(
        &mut self,
        builder: &mut B::Builder,
        map_func: Option<&dyn Fn(&mut DynDataTyped<B::Time>)>,
        indexes: IndexSet,
        fuel: &mut isize,
    ) -> bool {
        // All of the cursors must have a valid value (hence the `unwrap()`),
        // and they must be equal.
        debug_assert!(indexes
            .into_iter()
            .map(|index| self.cursors[index].val().unwrap())
            .all_equal());

        // If this is a timed batch, we must consolidate the (time, weight) array; otherwise we
        // simply compute the total weight of the current value.
        if let Some(time_diffs) = &mut self.time_diffs {
            if let Some(map_func) = map_func {
                time_diffs.clear();
                for i in indexes {
                    self.cursors[i].map_times(&mut |time, w| {
                        let mut time: B::Time = time.clone();
                        map_func(&mut time);

                        time_diffs.push_refs((&time, w));
                    });
                }
                time_diffs.consolidate();
                if time_diffs.is_empty() {
                    return false;
                }
                for (time, diff) in time_diffs.dyn_iter().map(|td| td.split()) {
                    builder.push_time_diff(time, diff);
                }
            } else if indexes.is_long() {
                time_diffs.clear();
                for i in indexes {
                    self.cursors[i].map_times(&mut |time, w| {
                        time_diffs.push_refs((time, w));
                    });
                }
                time_diffs.consolidate();
                if time_diffs.is_empty() {
                    return false;
                }
                for (time, diff) in time_diffs.dyn_iter().map(|td| td.split()) {
                    builder.push_time_diff(time, diff);
                }
            } else {
                debug_assert_eq!(indexes.len(), 1);
                for i in indexes {
                    self.cursors[i].map_times(&mut |time, w| {
                        builder.push_time_diff(time, w);
                    });
                }
            }
        } else {
            self.tmp_weight.set_zero();
            for i in indexes {
                self.cursors[i].map_times(&mut |_time, weight| {
                    self.tmp_weight.add_assign(weight);
                });
            }
            if self.tmp_weight.is_zero() {
                return false;
            }
            builder.push_time_diff_mut(&mut B::Time::default(), &mut self.tmp_weight);
        }

        let index = indexes.first().unwrap();
        builder.push_val(self.cursors[index].val().unwrap().unwrap());
        *fuel -= 1;
        true
    }
}

fn find_min_indexes<Item>(mut iterator: impl Iterator<Item = (usize, Item)>) -> IndexSet
where
    Item: Ord,
{
    let (min_index, mut min_value) = iterator.next().unwrap();
    let mut min_indexes = IndexSet::for_index(min_index);

    for (index, value) in iterator {
        match value.cmp(&min_value) {
            Ordering::Less => {
                min_value = value;
                min_indexes = IndexSet::for_index(index);
            }
            Ordering::Equal => {
                min_indexes.add(index);
            }
            Ordering::Greater => (),
        }
    }
    min_indexes
}

fn try_find_min_indexes<Item>(
    mut iterator: impl Iterator<Item = (usize, Result<Item, Pending>)>,
) -> Result<IndexSet, Pending>
where
    Item: Ord,
{
    let (min_index, min_value) = iterator.next().unwrap();
    let mut min_indexes = IndexSet::for_index(min_index);
    let mut min_value = min_value?;

    for (index, value) in iterator {
        let value = value?;
        match value.cmp(&min_value) {
            Ordering::Less => {
                min_value = value;
                min_indexes = IndexSet::for_index(index);
            }
            Ordering::Equal => {
                min_indexes.add(index);
            }
            Ordering::Greater => (),
        }
    }
    Ok(min_indexes)
}

fn skip_filtered_keys<C, K, V, T, R>(
    cursor: &mut C,
    key_filter: &Option<Filter<K>>,
    value_filter: &Option<Filter<V>>,
    fuel: &mut isize,
) -> Result<(), Pending>
where
    C: PushCursor<K, V, T, R>,
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
{
    if key_filter.is_some() || value_filter.is_some() {
        while let Some(key) = cursor.key()? {
            if Filter::include(key_filter, key) && skip_filtered_values(cursor, value_filter, fuel)?
            {
                return Ok(());
            }
            cursor.step_key();
            *fuel -= 1;
        }
    } else {
        if cursor.key()?.is_some() {
            cursor.val()?;
        }
    }
    Ok(())
}

fn skip_filtered_values<C, K, V, T, R>(
    cursor: &mut C,
    value_filter: &Option<Filter<V>>,
    fuel: &mut isize,
) -> Result<bool, Pending>
where
    C: PushCursor<K, V, T, R>,
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
{
    if value_filter.is_some() {
        while let Some(value) = cursor.val()? {
            if Filter::include(value_filter, value) {
                return Ok(true);
            }
            cursor.step_val();
            *fuel -= 1;
        }
        Ok(false)
    } else {
        debug_assert!(cursor.key().is_ok());
        cursor.val()?;
        Ok(true)
    }
}

#[cfg(test)]
mod test {
    use std::fmt::{Debug, Formatter};

    use itertools::Itertools;

    use crate::{
        dynamic::{DynData, DynWeight, Erase},
        trace::{
            cursor::{Pending, PushCursor},
            ord::vec::indexed_wset_batch::VecIndexedWSetBuilder,
            spine_async::{index_set::IndexSet, push_merger::PushMerger},
            Batch, BatchReader, BatchReaderFactories, Builder, TupleBuilder, VecIndexedWSet,
            VecIndexedWSetFactories,
        },
    };

    #[derive(Clone)]
    struct Value {
        barrier: bool,
        datum: u32,
    }

    impl Debug for Value {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
            if self.barrier {
                write!(f, "b")?;
            }
            write!(f, "{}", self.datum)
        }
    }

    #[derive(Clone)]
    struct TestCursor<'a> {
        data: &'a [(Value, Vec<(Value, i32)>)],
        key: usize,
        key_barrier: bool,
        value: usize,
        value_barrier: bool,
    }

    impl<'a> TestCursor<'a> {
        fn new(data: &'a [(Value, Vec<(Value, i32)>)]) -> Self {
            let (key_barrier, value_barrier) = match data.first() {
                Some((key, values)) => (key.barrier, values[0].0.barrier),
                None => (false, false),
            };
            Self {
                data,
                key: 0,
                key_barrier,
                value: 0,
                value_barrier,
            }
        }
    }

    impl PushCursor<DynData, DynData, (), DynWeight> for TestCursor<'_> {
        fn key(&self) -> Result<Option<&DynData>, Pending> {
            if self.key_barrier {
                Err(Pending)
            } else {
                Ok(self
                    .data
                    .get(self.key)
                    .map(|(key, _values)| key.datum.erase()))
            }
        }

        fn val(&self) -> Result<Option<&DynData>, Pending> {
            assert!(!self.key_barrier);
            match self.data.get(self.key) {
                Some((_key, values)) => {
                    if self.value_barrier {
                        Err(Pending)
                    } else {
                        Ok(values.get(self.value).map(|value| value.0.datum.erase()))
                    }
                }
                None => unreachable!(),
            }
        }

        fn map_times(&mut self, logic: &mut dyn FnMut(&(), &DynWeight)) {
            assert!(self.val().is_ok_and(|value| value.is_some()));
            let weight = self.weight();
            logic(&(), weight);
        }

        fn weight(&mut self) -> &DynWeight {
            assert!(!self.key_barrier);
            assert!(!self.value_barrier);
            self.data[self.key].1[self.value].1.erase()
        }

        fn step_key(&mut self) {
            assert!(self.key().is_ok_and(|value| value.is_some()));
            self.key += 1;
            self.value = 0;
            (self.key_barrier, self.value_barrier) = match self.data.get(self.key) {
                Some((key, values)) => (key.barrier, values[0].0.barrier),
                None => (false, false),
            }
        }

        fn step_val(&mut self) {
            assert!(self.val().is_ok_and(|value| value.is_some()));
            self.value += 1;
            self.value_barrier = self
                .data
                .get(self.key)
                .unwrap()
                .1
                .get(self.value)
                .is_some_and(|value| value.0.barrier);
        }

        fn run(&mut self) {
            if self.value_barrier {
                self.value_barrier = false;
            } else if self.key_barrier {
                self.key_barrier = false;
            }
        }
    }

    struct TestInput(Vec<(Value, Vec<(Value, i32)>)>);

    impl Debug for TestInput {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
            writeln!(f, "Pattern:")?;
            for (key, values) in &self.0 {
                write!(f, "  {key:?}(")?;
                for (index, (value, weight)) in values.iter().enumerate() {
                    if index > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "({value:?}, {weight})")?;
                }
                writeln!(f, ")")?;
            }
            Ok(())
        }
    }

    #[derive(Copy, Clone)]
    struct GeneratorParams {
        /// Number of keys in the batch.
        n_keys: usize,

        /// Number of values per key.
        n_values: usize,

        /// If set, all key barriers are set to the value.  Otherwise, all
        /// on/off possibilities are exhaustively tried.
        key_barriers: Option<bool>,

        /// If set, all value barriers are set to the value.  Otherwise, all
        /// on/off possibilities are exhaustively tried.
        value_barriers: Option<bool>,

        /// Whether to iterate all weight combinations.
        exhaustive_weights: bool,
    }

    struct IndexedZSetGenerator {
        keys: KeysGenerator,
        values: Vec<ValuesGenerator>,
    }

    impl IndexedZSetGenerator {
        fn new(params: GeneratorParams) -> Self {
            Self {
                keys: KeysGenerator::new(params),
                values: (0..params.n_keys)
                    .map(|_| ValuesGenerator::new(params))
                    .collect(),
            }
        }
        fn current(&self) -> TestInput {
            TestInput(
                self.values
                    .iter()
                    .enumerate()
                    .map(|(row, values)| (self.keys.current(row), values.current()))
                    .collect(),
            )
        }
        fn next(&mut self) -> bool {
            for values in self.values.iter_mut().rev() {
                if values.next() {
                    return true;
                }
                values.clear();
            }
            self.keys.next()
        }
        fn generate(mut self) -> Vec<TestInput> {
            let mut inputs = Vec::new();
            loop {
                inputs.push(self.current());
                if !self.next() {
                    return inputs;
                }
            }
        }
    }

    struct KeysGenerator {
        params: GeneratorParams,
        deltas: u32,
        barriers: u32,
    }

    impl KeysGenerator {
        fn new(params: GeneratorParams) -> Self {
            Self {
                params,
                deltas: 0,
                barriers: 0,
            }
        }
        fn current(&self, row: usize) -> Value {
            let datum = (0..=row).map(|index| self.delta(index)).sum();
            let barrier = self.barrier(row);
            Value { datum, barrier }
        }
        fn delta(&self, row: usize) -> u32 {
            if (self.deltas & (1 << row)) != 0 {
                2
            } else {
                1
            }
        }
        fn barrier(&self, row: usize) -> bool {
            self.params
                .key_barriers
                .unwrap_or_else(|| (self.barriers & (1 << row)) != 0)
        }
        fn next(&mut self) -> bool {
            if self.params.key_barriers.is_none() {
                self.barriers += 1;
                if self.barriers < (1 << self.params.n_keys) {
                    return true;
                }
                self.barriers = 0;
            }

            self.deltas += 1;
            self.deltas < (1 << self.params.n_keys)
        }
    }

    struct ValuesGenerator {
        params: GeneratorParams,
        mask: u32,
        barriers: u32,
        weights: u32,
    }

    impl ValuesGenerator {
        fn new(params: GeneratorParams) -> Self {
            Self {
                params,
                mask: 1,
                barriers: 0,
                weights: 0,
            }
        }
        fn current(&self) -> Vec<(Value, i32)> {
            let barriers = match self.params.value_barriers {
                Some(true) => u32::MAX,
                Some(false) => 0,
                None => self.barriers,
            };
            let weights = if self.params.exhaustive_weights {
                self.weights
            } else {
                !barriers
            };
            IndexSet::for_mask(self.mask)
                .into_iter()
                .enumerate()
                .map(|(index, datum)| {
                    (
                        Value {
                            datum: datum as u32,
                            barrier: (barriers & (1 << index)) != 0,
                        },
                        if (weights & (1 << index)) != 0 { -1 } else { 1 },
                    )
                })
                .collect()
        }
        fn next(&mut self) -> bool {
            if self.params.exhaustive_weights {
                self.weights += 1;
                if self.weights < (1 << self.mask.count_ones()) {
                    return true;
                }
                self.weights = 0;
            }

            if self.params.value_barriers.is_none() {
                self.barriers += 1;
                if self.barriers < (1 << self.mask.count_ones()) {
                    return true;
                }
                self.barriers = 0;
            }

            self.mask += 1;
            self.mask < (1 << self.params.n_values)
        }
        fn clear(&mut self) {
            *self = Self::new(self.params);
        }
    }

    #[test]
    fn with_2inputs_2keys_2values() {
        test_2inputs(
            GeneratorParams {
                n_keys: 2,
                n_values: 2,
                key_barriers: None,
                value_barriers: None,
                exhaustive_weights: false,
            },
            false,
        );
    }

    #[test]
    fn with_2inputs_3keys_2values_nobarriers_fueled() {
        test_2inputs(
            GeneratorParams {
                n_keys: 3,
                n_values: 2,
                key_barriers: Some(false),
                value_barriers: Some(false),
                exhaustive_weights: false,
            },
            true,
        );
    }

    #[test]
    fn with_2inputs_2keys_2values_nobarriers_weights_fueled() {
        test_2inputs(
            GeneratorParams {
                n_keys: 2,
                n_values: 2,
                key_barriers: Some(false),
                value_barriers: Some(false),
                exhaustive_weights: true,
            },
            true,
        );
    }

    #[test]
    fn with_2inputs_3keys_1value() {
        test_2inputs(
            GeneratorParams {
                n_keys: 3,
                n_values: 1,
                key_barriers: None,
                value_barriers: None,
                exhaustive_weights: false,
            },
            false,
        );
    }

    #[test]
    fn with_2inputs_2keys_1value_weights() {
        test_2inputs(
            GeneratorParams {
                n_keys: 2,
                n_values: 1,
                key_barriers: None,
                value_barriers: None,
                exhaustive_weights: true,
            },
            false,
        );
    }

    fn test_2inputs(params: GeneratorParams, for_all_fuel: bool) {
        let inputs = IndexedZSetGenerator::new(params).generate();
        dbg!(inputs.len());

        for p1 in &inputs {
            for p2 in &inputs {
                test_inputs(&[p1, p2], for_all_fuel);
            }
        }
    }

    #[test]
    fn with_3inputs_3keys_1value_allkeybarriers() {
        test_3inputs(
            GeneratorParams {
                n_keys: 3,
                n_values: 1,
                key_barriers: Some(true),
                value_barriers: None,
                exhaustive_weights: false,
            },
            false,
        );
    }

    #[test]
    fn with_3inputs_3keys_1value_nokeybarriers() {
        test_3inputs(
            GeneratorParams {
                n_keys: 3,
                n_values: 1,
                key_barriers: Some(false),
                value_barriers: None,
                exhaustive_weights: false,
            },
            false,
        );
    }

    #[test]
    fn with_3inputs_2keys_2values_allbarriers() {
        test_3inputs(
            GeneratorParams {
                n_keys: 2,
                n_values: 2,
                key_barriers: Some(true),
                value_barriers: Some(true),
                exhaustive_weights: false,
            },
            false,
        );
    }

    #[test]
    fn with_3inputs_2keys_2values_nobarriers_fueled() {
        test_3inputs(
            GeneratorParams {
                n_keys: 2,
                n_values: 2,
                key_barriers: Some(false),
                value_barriers: Some(false),
                exhaustive_weights: false,
            },
            true,
        );
    }

    fn test_3inputs(params: GeneratorParams, for_all_fuel: bool) {
        let inputs = IndexedZSetGenerator::new(params).generate();
        dbg!(inputs.len());

        for p1 in &inputs {
            for p2 in &inputs {
                for p3 in &inputs {
                    test_inputs(&[p1, p2, p3], for_all_fuel);
                }
            }
        }
    }

    fn test_inputs(inputs: &[&TestInput], for_all_fuel: bool) {
        if for_all_fuel {
            for initial_fuel in 1.. {
                if test_inputs_with_fuel(inputs, initial_fuel) {
                    break;
                }
            }
        } else {
            test_inputs_with_fuel(inputs, isize::MAX);
        }
    }

    fn test_inputs_with_fuel(inputs: &[&TestInput], initial_fuel: isize) -> bool {
        let mut initial_fuel_was_enough = true;

        type T = VecIndexedWSet<DynData, DynData, DynWeight, usize>;
        let factories: <T as BatchReader>::Factories =
            VecIndexedWSetFactories::new::<u32, u32, i32>();
        let mut builder: <T as Batch>::Builder = VecIndexedWSetBuilder::new_builder(&factories);
        let mut push_merger = PushMerger::<_, T>::new(
            &factories,
            inputs
                .iter()
                .map(|input| TestCursor::new(input.0.as_slice()))
                .collect(),
            None,
            None,
        );
        let mut fuel = initial_fuel;
        loop {
            match push_merger.merge(&mut builder, &(), &mut fuel) {
                Ok(()) if fuel > 0 => break,
                Ok(()) => {
                    initial_fuel_was_enough = false;
                    fuel = isize::MAX;
                }
                Err(Pending) => push_merger.run(),
            }
        }
        let actual = builder.done();
        let mut tuples = Vec::new();
        for pattern in inputs {
            for (key, values) in &pattern.0 {
                for (value, weight) in values {
                    tuples.push((key.datum, value.datum, *weight));
                }
            }
        }
        tuples.sort();

        let builder: <T as Batch>::Builder = VecIndexedWSetBuilder::new_builder(&factories);
        let mut builder = TupleBuilder::new(&factories, builder);
        for (key, value, weight) in tuples.into_iter().coalesce(|(k1, v1, w1), (k2, v2, w2)| {
            if k1 == k2 && v1 == v2 {
                Ok((k1, v1, w1 + w2))
            } else {
                Err(((k1, v1, w1), (k2, v2, w2)))
            }
        }) {
            if weight != 0 {
                builder.push_refs(key.erase(), value.erase(), &(), weight.erase());
            }
        }
        let expected = builder.done();

        if actual != expected {
            panic!(
                "merge failed for:
{inputs:#?}
expected: {expected:#?}
  actual: {actual:#?}"
            );
        }

        initial_fuel_was_enough
    }
}

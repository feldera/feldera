use std::{cmp::Ordering, sync::Arc};

use ouroboros::self_referencing;

use crate::{
    algebra::Lattice,
    dynamic::{DynDataTyped, DynWeightedPairs, Weight, WeightTrait},
    time::Timestamp,
    trace::{
        spine_async::index_set::IndexSet, Batch, BatchFactories, BatchReaderFactories, Builder,
        Filter, MergeCursor,
    },
};

pub struct ArcListMerger<B>(ArcMergerInner<B>)
where
    B: Batch;

#[self_referencing]
struct ArcMergerInner<B>
where
    B: Batch,
{
    batches: Vec<Arc<B>>,
    #[borrows(batches)]
    #[not_covariant]
    merger: ListMerger<Box<dyn MergeCursor<B::Key, B::Val, B::Time, B::R> + Send + 'this>, B>,
}

impl<B> ArcListMerger<B>
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
            ArcMergerInnerBuilder {
                batches,
                merger_builder: |batches| {
                    ListMerger::new(
                        factories,
                        batches
                            .iter()
                            .map(|b| b.merge_cursor(key_filter.clone(), value_filter.clone()))
                            .collect(),
                    )
                },
            }
            .build(),
        )
    }

    pub fn work(&mut self, builder: &mut B::Builder, frontier: &B::Time, fuel: &mut isize) {
        self.0
            .with_mut(|fields| fields.merger.work(builder, frontier, fuel))
    }
}

/// Merger that merges up to 64 batches at a time.
pub struct ListMerger<C, B>
where
    C: MergeCursor<B::Key, B::Val, B::Time, B::R>,
    B: Batch,
{
    cursors: Vec<C>,
    any_values: bool,
    has_mut: Vec<bool>,
    tmp_weight: Box<B::R>,
    time_diffs: Option<Box<DynWeightedPairs<DynDataTyped<B::Time>, B::R>>>,
}

impl<C, B> ListMerger<C, B>
where
    C: MergeCursor<B::Key, B::Val, B::Time, B::R>,
    B: Batch,
{
    pub fn merge(factories: &B::Factories, mut builder: B::Builder, cursors: Vec<C>) -> B {
        let mut merger = Self::new(factories, cursors);
        let mut fuel = isize::MAX;
        merger.work(&mut builder, &B::Time::default(), &mut fuel);
        assert!(fuel > 0);
        builder.done()
    }

    /// Creates a new merger for `cursors`.
    pub fn new(factories: &B::Factories, cursors: Vec<C>) -> Self {
        // [IndexSet] supports a maximum of 64 batches.
        assert!(cursors.len() <= 64);

        let time_diffs = factories.time_diffs_factory().map(|f| f.default_box());
        let has_mut = cursors.iter().map(|c| c.has_mut()).collect();

        ListMerger {
            cursors,
            any_values: false,
            has_mut,
            tmp_weight: factories.weight_factory().default_box(),
            time_diffs,
        }
    }

    /// Perform `fuel` amount of work.
    ///
    /// When the function returns and fuel > 0, the batches should be guaranteed to be fully merged.
    pub fn work(&mut self, builder: &mut B::Builder, frontier: &B::Time, fuel: &mut isize) {
        assert!(self.cursors.len() <= 64);
        let mut remaining_cursors = self
            .cursors
            .iter()
            .enumerate()
            .filter_map(|(index, cursor)| cursor.key_valid().then_some(index))
            .collect::<IndexSet>();
        if remaining_cursors.is_empty() {
            return;
        }

        let advance_func = |t: &mut DynDataTyped<B::Time>| t.join_assign(frontier);

        let time_map_func = if frontier == &B::Time::minimum() {
            None
        } else {
            Some(&advance_func as &dyn Fn(&mut DynDataTyped<B::Time>))
        };

        // As long as there are multiple cursors...
        while remaining_cursors.is_long() && *fuel > 0 {
            // Find the indexes of the cursors with minimum keys, among the
            // remaining cursors.
            let orig_min_keys = find_min_indexes(
                remaining_cursors
                    .into_iter()
                    .map(|index| (index, self.cursors[index].key())),
            );

            // If we're resuming after stopping due to running out of fuel in a
            // previous call, then we might have exhausted the values in some of
            // the keys, so drop them.  We still need them in `orig_min_keys` so
            // we can advance the key for all of them later.
            let mut min_keys = if self.any_values {
                orig_min_keys
                    .into_iter()
                    .filter(|index| self.cursors[*index].val_valid())
                    .collect::<IndexSet>()
            } else {
                orig_min_keys
            };

            // As long as there is more than one cursor with minimum keys...
            while min_keys.is_long() {
                // ...Find the indexes of the cursors with minimum values, among
                // those with minimum keys, and copy their time-diff pairs and
                // value into the output.
                let min_vals = find_min_indexes(
                    min_keys
                        .into_iter()
                        .map(|index| (index, self.cursors[index].val())),
                );
                self.any_values =
                    self.copy_times(builder, time_map_func, min_vals, fuel) || self.any_values;

                // Then go on to the next value in each cursor, dropping the keys
                // for which we've exhausted the values.
                for index in min_vals {
                    self.cursors[index].step_val();
                    if !self.cursors[index].val_valid() {
                        min_keys.remove(index);
                    }
                }
                if *fuel <= 0 {
                    return;
                }
            }

            // If there's exactly one cursor left with minimum key, copy its
            // values into the output.
            if let Some(index) = min_keys.first() {
                loop {
                    self.any_values =
                        self.copy_times(builder, time_map_func, min_keys, fuel) || self.any_values;
                    self.cursors[index].step_val();
                    if *fuel <= 0 {
                        return;
                    }
                    if !self.cursors[index].val_valid() {
                        break;
                    }
                }
            }

            // If we wrote any values for these minimum keys, write the key.
            if self.any_values {
                let index = orig_min_keys.first().unwrap();
                if self.has_mut[index] {
                    builder.push_key_mut(self.cursors[index].key_mut());
                } else {
                    builder.push_key(self.cursors[index].key());
                }
                self.any_values = false;
            }

            // Advance each minimum-key cursor, dropping the cursors for which
            // we've exhausted the data.
            for index in orig_min_keys {
                self.cursors[index].step_key();
                if !self.cursors[index].key_valid() {
                    remaining_cursors.remove(index);
                }
            }
        }

        // If there is a cursor left (there's either one or none), copy it
        // directly to the output.
        if let Some(index) = remaining_cursors.first() {
            while *fuel > 0 {
                loop {
                    self.any_values =
                        self.copy_times(builder, time_map_func, remaining_cursors, fuel)
                            || self.any_values;
                    self.cursors[index].step_val();
                    if !self.cursors[index].val_valid() {
                        break;
                    }
                    if *fuel <= 0 {
                        return;
                    }
                }
                debug_assert!(time_map_func.is_some() || self.any_values, "This assertion should fail only if B::Cursor is a spine or a CursorList, but we shouldn't be merging those");
                if self.any_values {
                    self.any_values = false;
                    if self.has_mut[index] {
                        builder.push_key_mut(self.cursors[index].key_mut());
                    } else {
                        builder.push_key(self.cursors[index].key());
                    }
                }
                self.cursors[index].step_key();
                if !self.cursors[index].key_valid() {
                    break;
                }
            }
        }
    }

    fn copy_times(
        &mut self,
        builder: &mut B::Builder,
        map_func: Option<&dyn Fn(&mut DynDataTyped<B::Time>)>,
        indexes: IndexSet,
        fuel: &mut isize,
    ) -> bool {
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
        if self.has_mut[index] {
            builder.push_val_mut(self.cursors[index].val_mut());
        } else {
            builder.push_val(self.cursors[index].val());
        }
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

#[cfg(test)]
mod test {
    use std::fmt::{Debug, Formatter};

    use itertools::Itertools;

    use crate::{
        dynamic::{DynData, DynWeight, Erase},
        trace::{
            ord::vec::indexed_wset_batch::VecIndexedWSetBuilder, spine_async::index_set::IndexSet,
            Batch, BatchReader, BatchReaderFactories, Builder, ListMerger, MergeCursor,
            TupleBuilder, VecIndexedWSet, VecIndexedWSetFactories,
        },
    };

    #[derive(Clone)]
    struct TestCursor<'a> {
        data: &'a [(u32, Vec<(u32, i32)>)],
        key: usize,
        val: usize,
    }

    impl<'a> TestCursor<'a> {
        fn new(data: &'a [(u32, Vec<(u32, i32)>)]) -> Self {
            Self {
                data,
                key: 0,
                val: 0,
            }
        }
    }

    impl MergeCursor<DynData, DynData, (), DynWeight> for TestCursor<'_> {
        fn key_valid(&self) -> bool {
            self.key < self.data.len()
        }
        fn val_valid(&self) -> bool {
            assert!(self.key_valid());
            self.val < self.data[self.key].1.len()
        }
        fn key(&self) -> &DynData {
            self.data[self.key].0.erase()
        }
        fn val(&self) -> &DynData {
            self.data[self.key].1[self.val].0.erase()
        }
        fn map_times(&mut self, logic: &mut dyn FnMut(&(), &DynWeight)) {
            logic(&(), self.weight())
        }
        fn weight(&mut self) -> &DynWeight {
            self.data[self.key].1[self.val].1.erase()
        }
        fn step_key(&mut self) {
            assert!(self.key_valid());
            self.key += 1;
            self.val = 0;
        }
        fn step_val(&mut self) {
            assert!(self.val_valid());
            self.val += 1;
        }
    }

    struct TestInput(Vec<(u32, Vec<(u32, i32)>)>);

    impl Debug for TestInput {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
            writeln!(f, "Pattern:")?;
            for (key, values) in &self.0 {
                write!(f, "  {key:?}(")?;
                for (index, (value, weight)) in values.iter().enumerate() {
                    if index > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "({value:?}, {weight:+})")?;
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
    }

    impl KeysGenerator {
        fn new(params: GeneratorParams) -> Self {
            Self { params, deltas: 0 }
        }
        fn current(&self, row: usize) -> u32 {
            (0..=row).map(|index| self.delta(index)).sum()
        }
        fn delta(&self, row: usize) -> u32 {
            if (self.deltas & (1 << row)) != 0 {
                2
            } else {
                1
            }
        }
        fn next(&mut self) -> bool {
            self.deltas += 1;
            self.deltas < (1 << self.params.n_keys)
        }
    }

    struct ValuesGenerator {
        params: GeneratorParams,
        mask: u32,
        weights: u32,
    }

    impl ValuesGenerator {
        fn new(params: GeneratorParams) -> Self {
            Self {
                params,
                mask: 1,
                weights: 0,
            }
        }
        fn current(&self) -> Vec<(u32, i32)> {
            let weights = if self.params.exhaustive_weights {
                self.weights
            } else {
                0
            };
            IndexSet::for_mask(self.mask)
                .into_iter()
                .enumerate()
                .map(|(index, datum)| {
                    (
                        datum as u32,
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

            self.mask += 1;
            self.mask < (1 << self.params.n_values)
        }
        fn clear(&mut self) {
            *self = Self::new(self.params);
        }
    }

    #[test]
    fn with_2inputs_2keys_2values_weights_fueled() {
        test_2inputs(
            GeneratorParams {
                n_keys: 2,
                n_values: 2,
                exhaustive_weights: true,
            },
            true,
        );
    }

    #[test]
    fn with_2inputs_3keys_1value_weights_fueled() {
        test_2inputs(
            GeneratorParams {
                n_keys: 3,
                n_values: 1,
                exhaustive_weights: true,
            },
            true,
        );
    }

    #[test]
    fn with_2inputs_2keys_1value_weights_fueled() {
        test_2inputs(
            GeneratorParams {
                n_keys: 2,
                n_values: 1,
                exhaustive_weights: true,
            },
            true,
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
    fn with_3inputs_3keys_1value_weights_fueled() {
        test_3inputs(
            GeneratorParams {
                n_keys: 3,
                n_values: 1,
                exhaustive_weights: true,
            },
            true,
        );
    }

    #[test]
    fn with_3inputs_2keys_2values() {
        test_3inputs(
            GeneratorParams {
                n_keys: 2,
                n_values: 2,
                exhaustive_weights: false,
            },
            false,
        );
    }

    #[test]
    fn with_3inputs_2keys_2values_fueled() {
        test_3inputs(
            GeneratorParams {
                n_keys: 2,
                n_values: 2,
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
        let mut list_merger = ListMerger::<_, T>::new(
            &factories,
            inputs
                .iter()
                .map(|input| TestCursor::new(input.0.as_slice()))
                .collect(),
        );
        let mut fuel = initial_fuel;
        loop {
            list_merger.work(&mut builder, &(), &mut fuel);
            if fuel > 0 {
                break;
            }
            initial_fuel_was_enough = false;
            fuel = isize::MAX;
        }
        let actual = builder.done();
        let mut tuples = Vec::new();
        for pattern in inputs {
            for (key, values) in &pattern.0 {
                for (value, weight) in values {
                    tuples.push((*key, *value, *weight));
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

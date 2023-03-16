//! Custom FlatMap operator to reduce the number of allocations utilized

use crate::{
    dataflow::{RowMap, RowSet},
    row::Row,
};
use dbsp::{
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Scope,
    },
    trace::{Batch, BatchReader, Batcher, Cursor},
};
use std::{borrow::Cow, iter};

pub struct FlatMap<F> {
    flat_map: F,
    // TODO: We could keep a running average of the number of tuples
    // yielded from each closure to help estimate pre-allocation
    // TODO: We could maintain some of the buffers we use across invocations
}

impl<F> FlatMap<F> {
    pub fn new(flat_map: F) -> Self {
        Self { flat_map }
    }
}

impl<F> Operator for FlatMap<F>
where
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("FlatMap")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

// Set -> Set
impl<F> UnaryOperator<RowSet, RowSet> for FlatMap<F>
where
    F: FnMut(&Row, &mut Vec<Row>) + 'static,
{
    fn eval(&mut self, input: &RowSet) -> RowSet {
        let (mut keys, mut diffs) = (
            Vec::with_capacity(input.len()),
            Vec::with_capacity(input.len()),
        );

        let mut cursor = input.cursor();

        while cursor.key_valid() {
            // TODO: Sets always have a single valid unit value, we could remove the value loop
            while cursor.val_valid() {
                debug_assert_eq!(keys.len(), diffs.len());

                // Figure out how many keys we had prior to applying the flat map fn
                let len = keys.len();

                // Apply the flat map function and produce an arbitrary number of keys
                (self.flat_map)(cursor.key(), &mut keys);

                // Figure out the number of keys that the closure added and add that number of diffs
                let added_keys = keys.len() - len;
                if added_keys > 0 {
                    let weight = cursor.weight();
                    diffs.extend((0..added_keys).map(|_| weight));
                }

                debug_assert_eq!(keys.len(), diffs.len());
                cursor.step_val();
            }

            cursor.step_key();
        }

        RowSet::from_columns(keys, diffs)
    }
}

// Set -> Map
impl<F> UnaryOperator<RowSet, RowMap> for FlatMap<F>
where
    F: FnMut(&Row, &mut Vec<Row>, &mut Vec<Row>) + 'static,
{
    fn eval(&mut self, input: &RowSet) -> RowMap {
        let mut batch = Vec::new();

        {
            let mut cursor = input.cursor();
            let (mut keys, mut values) = (Vec::new(), Vec::new());

            while cursor.key_valid() {
                // TODO: Sets always have a single valid unit value, we could remove the value loop
                while cursor.val_valid() {
                    debug_assert!(keys.is_empty() && values.is_empty());

                    // Apply the flat map function and produce an arbitrary number of keys
                    (self.flat_map)(cursor.key(), &mut keys, &mut values);
                    assert_eq!(keys.len(), values.len());

                    batch.reserve(keys.len());
                    batch.extend(
                        keys.drain(..)
                            .zip(values.drain(..))
                            .zip(iter::repeat(cursor.weight())),
                    );

                    debug_assert!(keys.is_empty() && values.is_empty());
                    cursor.step_val();
                }

                cursor.step_key();
            }
        }

        // FIXME: Better approach to this
        let mut batcher = <RowMap as Batch>::Batcher::new_batcher(());
        batcher.push_batch(&mut batch);
        batcher.seal()
    }
}

// Map -> Set
impl<F> UnaryOperator<RowMap, RowSet> for FlatMap<F>
where
    F: FnMut(&Row, &Row, &mut Vec<Row>) + 'static,
{
    fn eval(&mut self, input: &RowMap) -> RowSet {
        let (mut keys, mut diffs) = (
            Vec::with_capacity(input.len()),
            Vec::with_capacity(input.len()),
        );

        let mut cursor = input.cursor();

        while cursor.key_valid() {
            while cursor.val_valid() {
                debug_assert_eq!(keys.len(), diffs.len());

                // Figure out how many keys we had prior to applying the flat map fn
                let len = keys.len();

                // Apply the flat map function and produce an arbitrary number of keys
                (self.flat_map)(cursor.key(), cursor.val(), &mut keys);

                // Figure out the number of keys that the closure added and add that number of diffs
                let added_keys = keys.len() - len;
                if added_keys > 0 {
                    let weight = cursor.weight();
                    diffs.extend((0..added_keys).map(|_| weight));
                }

                debug_assert_eq!(keys.len(), diffs.len());
                cursor.step_val();
            }

            cursor.step_key();
        }

        RowSet::from_columns(keys, diffs)
    }
}

// Map -> Map
impl<F> UnaryOperator<RowMap, RowMap> for FlatMap<F>
where
    F: FnMut(&Row, &Row, &mut Vec<Row>, &mut Vec<Row>) + 'static,
{
    fn eval(&mut self, input: &RowMap) -> RowMap {
        let mut batch = Vec::new();

        {
            let mut cursor = input.cursor();
            let (mut keys, mut values) = (Vec::new(), Vec::new());

            while cursor.key_valid() {
                // TODO: Sets always have a single valid unit value, we could remove the value loop
                while cursor.val_valid() {
                    debug_assert!(keys.is_empty() && values.is_empty());

                    // Apply the flat map function and produce an arbitrary number of keys
                    (self.flat_map)(cursor.key(), cursor.val(), &mut keys, &mut values);
                    assert_eq!(keys.len(), values.len());

                    batch.reserve(keys.len());
                    batch.extend(
                        keys.drain(..)
                            .zip(values.drain(..))
                            .zip(iter::repeat(cursor.weight())),
                    );

                    debug_assert!(keys.is_empty() && values.is_empty());
                    cursor.step_val();
                }

                cursor.step_key();
            }
        }

        // FIXME: Better approach to this
        let mut batcher = <RowMap as Batch>::Batcher::new_batcher(());
        batcher.push_batch(&mut batch);
        batcher.seal()
    }
}

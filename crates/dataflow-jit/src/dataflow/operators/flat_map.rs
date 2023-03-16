use crate::{dataflow::RowSet, row::Row};
use dbsp::{
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Scope,
    },
    trace::{Batch, BatchReader, Cursor},
};
use std::borrow::Cow;

/// Internal implementation of `flat_map` methods.
pub struct FlatMap<F> {
    flat_map: F,
    // TODO: We could keep a running average of the number of tuples
    // yielded from each closure to help estimate pre-allocation
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

impl<F> UnaryOperator<RowSet, RowSet> for FlatMap<F>
where
    for<'a> F: FnMut(&'a Row, &mut Vec<Row>) + 'static,
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

        todo!()
    }
}

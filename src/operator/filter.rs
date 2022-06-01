//! Filtering operators.

use crate::{
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, Scope, Stream,
    },
    trace::{Batch, BatchReader, Builder, Cursor},
};
use std::{borrow::Cow, marker::PhantomData};

impl<P, CI> Stream<Circuit<P>, CI>
where
    CI: BatchReader<Time = ()> + Clone + 'static,
    CI::Key: Clone,
    CI::Val: Clone,
    P: Clone + 'static,
{
    /// Apply [`FilterKeys`] operator to `self`.
    pub fn filter_keys<CO, F>(&self, func: F) -> Stream<Circuit<P>, CO>
    where
        CO: Batch<Key = CI::Key, Val = CI::Val, Time = (), R = CI::R> + Clone + 'static,
        F: Fn(&CI::Key) -> bool + 'static,
    {
        self.circuit()
            .add_unary_operator(FilterKeys::new(func), self)
    }
}

/// Operator that filters a collection of key/value pairs based on keys.
///
/// The operator applies a filtering function to each key in the input
/// batch and builds an output batch containing only the elements
/// that satisfy the filter condition.
///
/// # Type arguments
///
/// * `CI` - input collection type.
/// * `CO` - output collection type.
/// * `F` - filtering function type.
pub struct FilterKeys<CI, CO, F>
where
    F: 'static,
{
    filter: F,
    _type: PhantomData<(CI, CO)>,
}

impl<CI, CO, F> FilterKeys<CI, CO, F>
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

impl<CI, CO, F> Operator for FilterKeys<CI, CO, F>
where
    CI: 'static,
    CO: 'static,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("FilterKeys")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
    fn fixedpoint(&self) -> bool {
        true
    }
}

impl<CI, CO, F> UnaryOperator<CI, CO> for FilterKeys<CI, CO, F>
where
    CI: BatchReader<Time = ()> + 'static,
    CI::Key: Clone,
    CI::Val: Clone,
    CO: Batch<Key = CI::Key, Val = CI::Val, Time = (), R = CI::R> + 'static,
    F: Fn(&CI::Key) -> bool + 'static,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut cursor = i.cursor();

        // We can use Builder because cursor yields ordered values.  This
        // is a nice property of the filter operation.

        // This will create waste if most tuples get filtered out, since
        // the buffers allocated here can make it all the way to the output batch.
        // This is probably ok, because the batch will either get freed at the end
        // of the current clock tick or get added to the trace, where it will likely
        // get merged with other batches soon, at which point the waste is gone.
        let mut builder = CO::Builder::with_capacity((), i.len());

        while cursor.key_valid(i) {
            let k = cursor.key(i);
            if (self.filter)(k) {
                while cursor.val_valid(i) {
                    let val = cursor.val(i);
                    let w = cursor.weight(i);
                    builder.push((k.clone(), val.clone(), w.clone()));
                    cursor.step_val(i);
                }
            }
            cursor.step_key(i);
        }
        builder.done()
    }

    fn eval_owned(&mut self, i: CI) -> CO {
        // TODO: owned implementation
        self.eval(&i)
    }
}

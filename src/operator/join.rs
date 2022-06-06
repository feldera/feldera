//! Relational join operator.

use crate::{
    algebra::{IndexedZSet, MulByRef, ZSet},
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        Circuit, Scope, Stream,
    },
    lattice::Lattice,
    time::{NestedTimestamp32, Timestamp},
    trace::{
        cursor::Cursor as TraceCursor, ord::OrdValSpine, BatchReader, Batcher, Trace, TraceReader,
    },
};
use deepsize::DeepSizeOf;
use std::{
    borrow::Cow,
    cmp::{min, Ordering},
    collections::HashMap,
    fmt::Write,
    marker::PhantomData,
    mem::take,
};
use timely::PartialOrder;

impl<P, IZ1> Stream<Circuit<P>, IZ1>
where
    P: Clone + 'static,
{
    /// Apply [`Join`] operator to `self` and `other`.
    ///
    /// See [`Join`] operator for more info.
    pub fn join<F, IZ2, Z>(&self, other: &Stream<Circuit<P>, IZ2>, f: F) -> Stream<Circuit<P>, Z>
    where
        IZ1: BatchReader<Time = (), R = Z::R> + Clone + 'static,
        IZ2: BatchReader<Key = IZ1::Key, Time = (), R = Z::R> + Clone + 'static,
        IZ1::Key: Ord,
        Z: Clone + ZSet + 'static,
        Z::R: MulByRef,
        F: Fn(&IZ1::Key, &IZ1::Val, &IZ2::Val) -> Z::Key + 'static,
    {
        self.circuit()
            .add_binary_operator(Join::new(f), self, other)
    }
}

impl<P, I1> Stream<Circuit<P>, I1>
where
    P: Clone + 'static,
{
    /// Incremental join of two streams.
    ///
    /// Given streams `a` and `b` of changes to relations `A` and `B`
    /// respectively, computes a stream of changes to `A <> B` (where `<>`
    /// is the join operator):
    ///
    /// ```text
    /// delta(A <> B) = A <> B - z^-1(A) <> z^-1(B) = a <> z^-1(B) + z^-1(A) <> b + a <> b
    /// ```
    pub fn join_incremental<F, I2, Z>(
        &self,
        other: &Stream<Circuit<P>, I2>,
        join_func: F,
    ) -> Stream<Circuit<P>, Z>
    where
        I1: IndexedZSet + DeepSizeOf,
        I1::Key: Ord + DeepSizeOf,
        I1::Val: Ord,
        I1::R: DeepSizeOf,
        I2: IndexedZSet<Key = I1::Key, R = I1::R> + DeepSizeOf,
        I2::Val: Ord,
        F: Clone + Fn(&I1::Key, &I1::Val, &I2::Val) -> Z::Key + 'static,
        Z: ZSet<R = I1::R>,
        Z::R: MulByRef,
    {
        self.integrate_trace()
            .delay_trace()
            .join(other, join_func.clone())
            .plus(&self.join(&other.integrate_trace(), join_func))
    }

    /*
    /// Incremental join of two nested streams.
    ///
    /// Given nested streams `a` and `b` of changes to relations `A` and `B`,
    /// computes `(↑((↑(a <> b))∆))∆` using the following formula:
    ///
    /// ```text
    /// (↑((↑(a <> b))∆))∆ =
    ///     ↑I(z^-1(I(a))) <> b      +
    ///     ↑I(a) <> I(b)            +
    ///     a <> I(↑I(↑z^-1(b)))     +
    ///     I(z^-1(a)) <> ↑I(↑z^-1(b)).
    /// ```
    pub fn join_incremental_nested<F, I2, Z>(
        &self,
        other: &Stream<Circuit<P>, I2>,
        join_func: F,
    ) -> Stream<Circuit<P>, Z>
    where
        I1: IndexedZSet + DeepSizeOf,
        I1::Key: Ord,
        I2: IndexedZSet<Key = I1::Key, R = I1::R> + DeepSizeOf,
        F: Clone + Fn(&I1::Key, &I1::Val, &I2::Val) -> Z::Key + 'static,
        Z: ZSet<R = I1::R>,
        Z::R: MulByRef,
    {
        let join1: Stream<_, Z> = self
            .integrate_nested()
            .delay_nested()
            .integrate()
            .join(other, join_func.clone());
        let join2 = self
            .integrate()
            .join(&other.integrate_nested(), join_func.clone());
        let join3 = self.join(
            &other.integrate_nested().integrate().delay(),
            join_func.clone(),
        );
        let join4 = self
            .integrate_nested()
            .delay_nested()
            .join(&other.integrate().delay(), join_func);

        // Note: I would use `join.sum(...)` but for some reason
        //       Rust Analyzer tries to resolve it to `Iterator::sum()`
        Stream::sum(&join1, &[join2, join3, join4])
    }
    */
}

impl<P, I1> Stream<Circuit<P>, I1>
where
    P: Clone + 'static,
    I1: IndexedZSet,
{
    /// Incremental join of two nested streams.
    ///
    /// Given nested streams `self` and `other` of changes to relations `A` and
    /// `B`, computes `(↑((↑(self <> other))∆))∆` by first assembling traces
    /// of both streams:
    pub fn join_trace<I2, F, Z>(
        &self,
        other: &Stream<Circuit<P>, I2>,
        join_func: F,
    ) -> Stream<Circuit<P>, Z>
    where
        I1::Key: DeepSizeOf + Clone + Ord,
        I1::Val: DeepSizeOf + Clone + Ord,
        I1::R: DeepSizeOf,
        I2::Val: DeepSizeOf + Clone + Ord,
        I2: IndexedZSet<Key = I1::Key, R = I1::R>,
        Z: ZSet<R = I1::R>,
        Z::Batcher: DeepSizeOf,
        Z::Key: Clone + Default,
        Z::R: MulByRef + Default,
        F: Fn(&I1::Key, &I1::Val, &I2::Val) -> Z::Key + Clone + 'static,
    {
        // Writing out the definition of the operator and applying distributivity,
        // we end up with the following four terms:
        //
        //          self                    other
        //
        // ┌─────────────────┬─┐    ┌─────────────────┬─┐
        // │                 │ │    │                 │ │
        // │                 │ │ <> │                 │ │
        // ├─────────────────┼─┤    ├─────────────────┼─┤
        // │                 │x│    │                 │x│
        // └─────────────────┴─┘    └─────────────────┴─┘
        //                       +
        // ┌─────────────────┬─┐    ┌─────────────────┬─┐
        // │                 │ │    │xxxxxxxxxxxxxxxxx│x│
        // │                 │ │ <> │xxxxxxxxxxxxxxxxx│x│
        // ├─────────────────┼─┤    ├─────────────────┼─┤
        // │                 │x│    │xxxxxxxxxxxxxxxxx│ │
        // └─────────────────┴─┘    └─────────────────┴─┘
        //                       +
        // ┌─────────────────┬─┐    ┌─────────────────┬─┐
        // │                 │ │    │                 │x│
        // │                 │ │ <> │                 │x│
        // ├─────────────────┼─┤    ├─────────────────┼─┤
        // │xxxxxxxxxxxxxxxxx│ │    │                 │ │
        // └─────────────────┴─┘    └─────────────────┴─┘
        //                       +
        // ┌─────────────────┬─┐    ┌─────────────────┬─┐
        // │xxxxxxxxxxxxxxxxx│x│    │                 │ │
        // │xxxxxxxxxxxxxxxxx│x│ <> │                 │ │
        // ├─────────────────┼─┤    ├─────────────────┼─┤
        // │xxxxxxxxxxxxxxxxx│ │    │                 │x│
        // └─────────────────┴─┘    └─────────────────┴─┘
        //                       +
        // ┌─────────────────┬─┐    ┌─────────────────┬─┐
        // │                 │x│    │                 │ │
        // │                 │x│ <> │                 │ │
        // ├─────────────────┼─┤    ├─────────────────┼─┤
        // │                 │ │    │xxxxxxxxxxxxxxxxx│ │
        // └─────────────────┴─┘    └─────────────────┴─┘
        //
        // Terms 2 + 3 and 4 + 5 are symmetric and are implemented by the `JoinTrace`
        // operator. We sneak the first term into one of them, but not the other
        // by delaying one of the traces.
        let self_trace = self.trace::<OrdValSpine<I1::Key, I1::Val, NestedTimestamp32, I1::R>>();
        let other_trace = other.trace::<OrdValSpine<I1::Key, I2::Val, NestedTimestamp32, I1::R>>();
        let join_func_clone = join_func.clone();

        // Terms 1+2+3:
        //
        //        self                        other
        // ┌─────────────────┬─┐    ┌─────────────────┬─┐
        // │                 │ │    │xxxxxxxxxxxxxxxxx│x│
        // │                 │ │ <> │xxxxxxxxxxxxxxxxx│x│
        // ├─────────────────┼─┤    ├─────────────────┼─┤
        // │                 │x│    │xxxxxxxxxxxxxxxxx│x│
        // └─────────────────┴─┘    └─────────────────┴─┘
        //                       +
        // ┌─────────────────┬─┐    ┌─────────────────┬─┐
        // │                 │ │    │                 │x│
        // │                 │ │ <> │                 │x│
        // ├─────────────────┼─┤    ├─────────────────┼─┤
        // │xxxxxxxxxxxxxxxxx│ │    │                 │ │
        // └─────────────────┴─┘    └─────────────────┴─┘
        let left =
            self.circuit()
                .add_binary_operator(JoinTrace::new(join_func), self, &other_trace);

        // This function does not do anything, it's only needed to tell the compiler
        // about the type of `f`.
        fn assert_type<F, K, V1, V2, V>(f: F) -> F
        where
            F: Fn(&K, &V1, &V2) -> V,
        {
            f
        }

        // Terms 4+5:
        //
        //        self                        other
        // ┌─────────────────┬─┐    ┌─────────────────┬─┐
        // │xxxxxxxxxxxxxxxxx│x│    │                 │ │
        // │xxxxxxxxxxxxxxxxx│x│ <> │                 │ │
        // ├─────────────────┼─┤    ├─────────────────┼─┤
        // │xxxxxxxxxxxxxxxxx│ │    │                 │x│
        // └─────────────────┴─┘    └─────────────────┴─┘
        //                       +
        // ┌─────────────────┬─┐    ┌─────────────────┬─┐
        // │                 │x│    │                 │ │
        // │                 │x│ <> │                 │ │
        // ├─────────────────┼─┤    ├─────────────────┼─┤
        // │                 │ │    │xxxxxxxxxxxxxxxxx│ │
        // └─────────────────┴─┘    └─────────────────┴─┘
        let right = self.circuit().add_binary_operator(
            JoinTrace::new(assert_type(move |k, v2, v1| join_func_clone(k, v1, v2))),
            other,
            &self_trace.delay_trace(),
        );

        left.plus(&right)
    }
}

/*
impl<P, I1> Stream<Circuit<P>, I1>
where
    P: Clone + 'static,
    I1: IndexedZSet,
{
    pub fn join_incremental2<I2, T1, T2, F, Z>(
        &self,
        other: &Stream<Circuit<P>, I2>,
        join_func: F,
    ) -> Stream<Circuit<P>, Z>
    where
        I1::Key: DeepSizeOf,
        I1::Value: DeepSizeOf,
        I1::Weight: DeepSizeOf,
        I2: IndexedZSet<Key = I1::Key, Weight = I1::Weight>,
        I2::Value: DeepSizeOf,
        T1::Batch: From<I1>,
        T2::Batch: From<I2>,
        Z: ZSet<Weight = I1::Weight>,
        Z::TupleBuilder: DeepSizeOf,
        F: Fn(&I1::Key, &I1::Value, &I2::Value) -> Z::Data + Clone + 'static,
    {
        let self_trace = self.integrate_trace::<T1>();
        let other_trace = other.integrate_trace::<T2>();
        let join_func_clone = join_func.clone();

        let left = self.circuit().add_binary_operator(
            JoinTrace::new(join_func),
            self,
            &other_trace,
        );

        fn flip_args<F, K, V1, V2, V>(f: F) -> F
        where
            F: Fn(&K, &V1, &V2) -> V,
        {
            f
        }

        let right = self.circuit().add_binary_operator(
            JoinTrace::new(flip_args(move |k, v2, v1| join_func_clone(k, v1, v2))),
            other,
            &self_trace.delay_trace(),
        );

        left.plus(&right)
    }
}
*/

/// Join two indexed Z-sets.
///
/// The operator takes two streams of indexed Z-sets and outputs
/// a stream obtained by joining each pair of inputs.
///
/// An indexed Z-set is a map from keys to a Z-set of values associated
/// with each key.  Both input streams must use the same key type `K`.
/// Indexed Z-sets are produced for example by the
/// [`Index`](`crate::operator::Index`) operator.
///
/// # Type arguments
///
/// * `F` - join function type: maps key and a pair of values from input Z-sets
///   to an output value.
/// * `I1` - indexed Z-set type in the first input stream.
/// * `I2` - indexed Z-set type in the second input stream.
/// * `Z` - output Z-set type.
pub struct Join<F, I1, I2, Z> {
    join_func: F,
    _types: PhantomData<(I1, I2, Z)>,
}

impl<F, I1, I2, Z> Join<F, I1, I2, Z> {
    pub fn new(join_func: F) -> Self {
        Self {
            join_func,
            _types: PhantomData,
        }
    }
}

impl<F, I1, I2, Z> Operator for Join<F, I1, I2, Z>
where
    I1: 'static,
    I2: 'static,
    F: 'static,
    Z: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Join")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<F, I1, I2, Z> BinaryOperator<I1, I2, Z> for Join<F, I1, I2, Z>
where
    I1: BatchReader<Time = (), R = Z::R> + 'static,
    I1::Key: Ord,
    I2: BatchReader<Key = I1::Key, Time = (), R = Z::R> + 'static,
    F: Fn(&I1::Key, &I1::Val, &I2::Val) -> Z::Key + 'static,
    Z: ZSet + 'static,
    Z::R: MulByRef,
{
    fn eval(&mut self, i1: &I1, i2: &I2) -> Z {
        let mut cursor1 = i1.cursor();
        let mut cursor2 = i2.cursor();

        // Choose capacity heuristically.
        let mut batch = Vec::with_capacity(min(i1.len(), i2.len()));

        while cursor1.key_valid(i1) && cursor2.key_valid(i2) {
            match cursor1.key(i1).cmp(cursor2.key(i2)) {
                Ordering::Less => cursor1.seek_key(i1, cursor2.key(i2)),
                Ordering::Greater => cursor2.seek_key(i2, cursor1.key(i1)),
                Ordering::Equal => {
                    while cursor1.val_valid(i1) {
                        let w1 = cursor1.weight(i1);
                        let v1 = cursor1.val(i1);
                        while cursor2.val_valid(i2) {
                            let v2 = cursor2.val(i2);
                            let w2 = cursor2.weight(i2);

                            batch.push((
                                ((self.join_func)(cursor1.key(i1), v1, v2), ()),
                                w1.mul_by_ref(&w2),
                            ));
                            cursor2.step_val(i2);
                        }

                        cursor2.rewind_vals(i2);
                        cursor1.step_val(i1);
                    }

                    cursor1.step_key(i1);
                    cursor2.step_key(i2);
                }
            }
        }

        Z::from_tuples((), batch)
    }
}

// Computes one half of nested incremental join:
//
//        self                       other
// ┌─────────────────┬─┐    ┌─────────────────┬─┐
// │                 │ │    │xxxxxxxxxxxxxxxxx│x│
// │                 │ │ <> │xxxxxxxxxxxxxxxxx│x│
// ├─────────────────┼─┤    ├─────────────────┼─┤
// │                 │x│    │xxxxxxxxxxxxxxxxx│x│
// └─────────────────┴─┘    └─────────────────┴─┘
//                       +
// ┌─────────────────┬─┐    ┌─────────────────┬─┐
// │                 │ │    │                 │x│
// │                 │ │ <> │                 │x│
// ├─────────────────┼─┤    ├─────────────────┼─┤
// │xxxxxxxxxxxxxxxxx│ │    │                 │ │
// └─────────────────┴─┘    └─────────────────┴─┘
//       ^            ^                        ^
//       |            |                        |
//       t1           t2                       t2
//
// The first term is a standard join between a single batch
// and a full trace.  The second term joins all updates
// observed in the current epoch with all updates observed in
// past epochs at the same clock cycle.  It is bit tricky to
// compute efficiently, as the trace API does not allow
// selecting updates for a particular time.  The trick is
// to compute the second term ahead of time: at time t1,
// for each key in indexed ZSet `self`, lookup matching key
// in trace `other` scan the trace and scan associated
// (time, weight) pairs.  We already perform this scan to
// compute the first term above, but instead of stopping at
// time `t1`, we continue scanning and record computed output
// tuples for time `t2 > t1` inside the operator so that we can
// output them at time `t2`.
pub struct JoinTrace<F, I, T, Z>
where
    T: TraceReader,
    Z: ZSet,
{
    join_func: F,
    // TODO: not needed once timekeeping is handled by the circuit.
    time: T::Time,
    // Future update batches computed ahead of time, indexed by time
    // when each batch should be output.
    output_batchers: HashMap<T::Time, Z::Batcher>,
    // True if empty input batch was received at the current clock cycle.
    empty_input: bool,
    // True if empty output was produced at the current clock cycle.
    empty_output: bool,
    _types: PhantomData<(I, T, Z)>,
}

impl<F, I, T, Z> JoinTrace<F, I, T, Z>
where
    T: TraceReader,
    Z: ZSet,
{
    pub fn new(join_func: F) -> Self {
        Self {
            join_func,
            time: T::Time::minimum(),
            output_batchers: HashMap::new(),
            empty_input: false,
            empty_output: false,
            _types: PhantomData,
        }
    }
}

impl<F, I, T, Z> Operator for JoinTrace<F, I, T, Z>
where
    F: 'static,
    I: 'static,
    T: TraceReader + 'static,
    Z: ZSet,
    Z::Batcher: DeepSizeOf,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("JoinTrace")
    }
    fn clock_start(&mut self, scope: Scope) {
        self.time = self.time.advance(scope + 1);
        if scope == 0 {
            self.empty_input = false;
            self.empty_output = false;
        }
    }
    fn clock_end(&mut self, _scope: Scope) {
        debug_assert!(self
            .output_batchers
            .iter()
            .all(|(time, _)| !time.less_equal(&self.time)));
    }

    fn summary(&self, summary: &mut String) {
        let sizes: Vec<usize> = self
            .output_batchers
            .iter()
            .map(|(_, batcher)| batcher.tuples())
            .collect();
        writeln!(summary, "sizes: {:?}", sizes).unwrap();
        writeln!(summary, "total size: {}", sizes.iter().sum::<usize>()).unwrap();

        let bytes: Vec<usize> = self
            .output_batchers
            .iter()
            .map(|(_, batcher)| batcher.deep_size_of())
            .collect();
        writeln!(summary, "bytes: {:?}", bytes).unwrap();
        writeln!(summary, "total bytes: {}", bytes.iter().sum::<usize>()).unwrap();
        //println!("zbytes:{}", bytes);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        let epoch_end = self.time.epoch_end(scope);
        // We're in a stable state if input and output at the current clock cycle are
        // both empty, and there are no precomputed outputs before the end of the
        // clock epoch.
        self.empty_input
            && self.empty_output
            && self
                .output_batchers
                .iter()
                .all(|(time, _)| !time.less_equal(&epoch_end))
    }
}

impl<F, I, T, Z> BinaryOperator<I, T, Z> for JoinTrace<F, I, T, Z>
where
    I: IndexedZSet,
    I::Key: Ord + Clone,
    T: Trace<Key = I::Key, R = I::R> + 'static,
    F: Clone + Fn(&I::Key, &I::Val, &T::Val) -> Z::Key + 'static,
    Z: ZSet<R = I::R>,
    Z::Key: Clone + Default,
    Z::Batcher: DeepSizeOf,
    Z::R: MulByRef + Default,
{
    fn eval(&mut self, index: &I, trace: &T) -> Z {
        /*println!("JoinTrace::eval@{}:\n  index:\n{}\n  trace:\n{}",
        self.time,
        textwrap::indent(&index.to_string(), "    "),
        textwrap::indent(&trace.to_string(), "    "));*/

        self.empty_input = index.is_empty();

        // Buffer to collect output tuples.
        // One allocation per clock tick is acceptable; however the actual output can be
        // larger than `index.len()`.  If re-allocations becomes a problem, we
        // may need to do something smarter, like a chain of buffers.
        let mut output_tuples = Vec::with_capacity(index.len());

        let mut index_cursor = index.cursor();
        let mut trace_cursor = trace.cursor();

        while index_cursor.key_valid(index) && trace_cursor.key_valid(trace) {
            match index_cursor.key(index).cmp(trace_cursor.key(trace)) {
                Ordering::Less => {
                    index_cursor.seek_key(index, trace_cursor.key(trace));
                }
                Ordering::Greater => {
                    trace_cursor.seek_key(trace, index_cursor.key(index));
                }
                Ordering::Equal => {
                    //println!("key: {}", index_cursor.key(index));

                    while index_cursor.val_valid(index) {
                        let v1 = index_cursor.val(index);
                        let w1 = index_cursor.weight(index);
                        //println!("v1: {}, w1: {}", v1, w1);

                        while trace_cursor.val_valid(trace) {
                            let output = (self.join_func)(
                                index_cursor.key(index),
                                v1,
                                trace_cursor.val(trace),
                            );
                            trace_cursor.map_times(trace, |ts, w2| {
                                // TODO: I think this is correct, but we should write a proper proof
                                // that this formula implements an arbitrarily nested incremental
                                // join operator.
                                output_tuples.push((
                                    ts.join(&self.time),
                                    ((output.clone(), ()), w1.mul_by_ref(w2)),
                                ));
                                //println!("  tuple@{}: ({:?}, {})", off,
                                // output, w1.clone() * w2.clone());
                            });
                            trace_cursor.step_val(trace);
                        }
                        trace_cursor.rewind_vals(trace);
                        index_cursor.step_val(index);
                    }

                    index_cursor.step_key(index);
                    trace_cursor.step_key(trace);
                }
            }
        }

        // Sort `output_tuples` by timestamp and push all tuples for each unique
        // timestamp to the appropriate batcher.
        output_tuples.sort_by(|(t1, _), (t2, _)| t1.cmp(t2));
        let mut start = 0;
        while start < output_tuples.len() {
            let end = start
                + output_tuples[start..].partition_point(|(t, _)| *t == output_tuples[start].0);
            let mut batch = output_tuples[start..end]
                .iter_mut()
                .map(|(_, tuple)| take(tuple))
                .collect();
            self.output_batchers
                .entry(output_tuples[start].0.clone())
                .or_insert_with(|| Z::Batcher::new(()))
                .push_batch(&mut batch);
            start = end;
        }

        // Finalize the batch for the current timestamp (`self.time`) and return it.
        let batcher = self
            .output_batchers
            .remove(&self.time)
            .unwrap_or_else(|| Z::Batcher::new(()));
        self.time = self.time.advance(0);
        let result = batcher.seal();
        self.empty_output = result.is_empty();
        result
    }
}

#[cfg(test)]
mod test {
    use crate::{
        circuit::{Root, Stream},
        operator::{DelayedFeedback, Generator},
        trace::ord::{OrdIndexedZSet, OrdZSet},
        zset,
    };
    use std::vec;

    #[test]
    fn join_test() {
        let root = Root::build(move |circuit| {
            let mut input1 = vec![
                zset! {
                    (1, "a") => 1,
                    (1, "b") => 2,
                    (2, "c") => 3,
                    (2, "d") => 4,
                    (3, "e") => 5,
                    (3, "f") => -2,
                },
                zset! {(1, "a") => 1},
                zset! {(1, "a") => 1},
                zset! {(4, "n") => 2},
                zset! {(1, "a") => 0},
            ]
            .into_iter();
            let mut input2 = vec![
                zset! {
                    (2, "g") => 3,
                    (2, "h") => 4,
                    (3, "i") => 5,
                    (3, "j") => -2,
                    (4, "k") => 5,
                    (4, "l") => -2,
                },
                zset! {(1, "b") => 1},
                zset! {(4, "m") => 1},
                zset! {},
                zset! {},
            ]
            .into_iter();
            let mut outputs = vec![
                zset! {
                    (2, "c g".to_string()) => 9,
                    (2, "c h".to_string()) => 12,
                    (2, "d g".to_string()) => 12,
                    (2, "d h".to_string()) => 16,
                    (3, "e i".to_string()) => 25,
                    (3, "e j".to_string()) => -10,
                    (3, "f i".to_string()) => -10,
                    (3, "f j".to_string()) => 4
                },
                zset! {
                    (1, "a b".to_string()) => 1,
                },
                zset! {},
                zset! {},
                zset! {},
            ]
            .into_iter();
            let mut inc_outputs = vec![
                zset! {
                    (2, "c g".to_string()) => 9,
                    (2, "c h".to_string()) => 12,
                    (2, "d g".to_string()) => 12,
                    (2, "d h".to_string()) => 16,
                    (3, "e i".to_string()) => 25,
                    (3, "e j".to_string()) => -10,
                    (3, "f i".to_string()) => -10,
                    (3, "f j".to_string()) => 4
                },
                zset! {
                    (1, "a b".to_string()) => 2,
                    (1, "b b".to_string()) => 2,
                },
                zset! {
                    (1, "a b".to_string()) => 1,
                },
                zset! {
                    (4, "n k".to_string()) => 10,
                    (4, "n l".to_string()) => -4,
                    (4, "n m".to_string()) => 2,
                },
                zset! {},
            ]
            .into_iter();

            let index1: Stream<_, OrdIndexedZSet<usize, &'static str, isize>> = circuit
                .add_source(Generator::new(move || input1.next().unwrap()))
                .index();
            let index2: Stream<_, OrdIndexedZSet<usize, &'static str, isize>> = circuit
                .add_source(Generator::new(move || input2.next().unwrap()))
                .index();
            index1
                .join(&index2, |&k: &usize, s1, s2| (k, format!("{} {}", s1, s2)))
                .inspect(move |fm: &OrdZSet<(usize, String), _>| {
                    assert_eq!(fm, &outputs.next().unwrap())
                });
            index1
                .join_incremental(&index2, |&k: &usize, s1, s2| (k, format!("{} {}", s1, s2)))
                .inspect(move |fm: &OrdZSet<(usize, String), _>| {
                    assert_eq!(fm, &inc_outputs.next().unwrap())
                });
        })
        .unwrap();

        for _ in 0..5 {
            root.step().unwrap();
        }
    }

    /*
    // Nested incremental reachability algorithm.
    #[test]
    fn join_incremental_nested_test() {
        let root = Root::build(move |circuit| {
            // Changes to the edges relation.
            let mut edges: vec::IntoIter<OrdZSet<(usize, usize), isize>> = vec![
                zset! { (1, 2) => 1 },
                zset! { (2, 3) => 1},
                zset! { (1, 3) => 1},
                zset! { (3, 1) => 1},
                zset! { (3, 1) => -1},
                zset! { (1, 2) => -1},
                zset! { (2, 4) => 1, (4, 1) => 1 },
                zset! { (2, 3) => -1, (3, 2) => 1 },
            ]
            .into_iter();

            // Expected content of the reachability relation.
            let mut outputs: vec::IntoIter<OrdZSet<(usize, usize), isize>> = vec![
                zset! { (1, 2) => 1 },
                zset! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                zset! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                zset! { (1, 1) => 1, (2, 2) => 1, (3, 3) => 1, (1, 2) => 1, (1, 3) => 1, (2, 3) => 1, (2, 1) => 1, (3, 1) => 1, (3, 2) => 1},
                zset! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                zset! { (2, 3) => 1, (1, 3) => 1 },
                zset! { (1, 3) => 1, (2, 3) => 1, (2, 4) => 1, (2, 1) => 1, (4, 1) => 1, (4, 3) => 1 },
                zset! { (1, 1) => 1, (2, 2) => 1, (3, 3) => 1, (4, 4) => 1,
                              (1, 2) => 1, (1, 3) => 1, (1, 4) => 1,
                              (2, 1) => 1, (2, 3) => 1, (2, 4) => 1,
                              (3, 1) => 1, (3, 2) => 1, (3, 4) => 1,
                              (4, 1) => 1, (4, 2) => 1, (4, 3) => 1 },
            ]
            .into_iter();

            let edges: Stream<_, OrdZSet<(usize, usize), isize>> =
                circuit
                    .add_source(Generator::new(move || edges.next().unwrap()));

            let paths = circuit.iterate_with_conditions(|child| {
                // ```text
                //                      distinct_incremental_nested
                //               ┌───┐          ┌───┐
                // edges         │   │          │   │  paths
                // ────┬────────►│ + ├──────────┤   ├────────┬───►
                //     │         │   │          │   │        │
                //     │         └───┘          └───┘        │
                //     │           ▲                         │
                //     │           │                         │
                //     │         ┌─┴─┐                       │
                //     │         │   │                       │
                //     └────────►│ X │ ◄─────────────────────┘
                //               │   │
                //               └───┘
                //      join_incremental_nested
                // ```
                let edges = edges.delta0(child);
                let paths_delayed = <DelayedFeedback<_, OrdZSet<_, _>>>::new(child);

                let paths_inverted: Stream<_, OrdZSet<(usize, usize), isize>> = paths_delayed
                    .stream()
                    .map_keys(|&(x, y)| (y, x));

                let paths_inverted_indexed: Stream<_, OrdIndexedZSet<usize, usize, isize>> = paths_inverted.index();
                let edges_indexed: Stream<_, OrdIndexedZSet<usize, usize, isize>> = edges.index();

                let paths = edges.plus(&paths_inverted_indexed.join_incremental_nested(&edges_indexed, |_via, from, to| (*from, *to)))
                    .distinct_incremental_nested();
                paths_delayed.connect(&paths);
                let output = paths.integrate();
                Ok((
                    vec![
                        paths.condition(HasZero::is_zero),
                        paths.integrate_nested().condition(HasZero::is_zero)
                    ],
                    output.export(),
                ))
            })
            .unwrap();

            paths.integrate().distinct().inspect(move |ps| {
                assert_eq!(*ps, outputs.next().unwrap());
            })
        })
        .unwrap();

        for _ in 0..8 {
            root.step().unwrap();
        }
    }
    */

    // Compute pairwise reachability relation between graph nodes as the
    // transitive closure of the edge relation.
    #[test]
    fn join_trace_test() {
        let root = Root::build(move |circuit| {
            // Changes to the edges relation.
            let mut edges: vec::IntoIter<OrdZSet<(usize, usize), isize>> = vec![
                zset! { (1, 2) => 1 },
                zset! { (2, 3) => 1},
                zset! { (1, 3) => 1},
                zset! { (3, 1) => 1},
                zset! { (3, 1) => -1},
                zset! { (1, 2) => -1},
                zset! { (2, 4) => 1, (4, 1) => 1 },
                zset! { (2, 3) => -1, (3, 2) => 1 },
            ]
            .into_iter();

            // Expected content of the reachability relation.
            let mut outputs: vec::IntoIter<OrdZSet<(usize, usize), isize>> = vec![
                zset! { (1, 2) => 1 },
                zset! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                zset! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                zset! { (1, 1) => 1, (2, 2) => 1, (3, 3) => 1, (1, 2) => 1, (1, 3) => 1, (2, 3) => 1, (2, 1) => 1, (3, 1) => 1, (3, 2) => 1},
                zset! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                zset! { (2, 3) => 1, (1, 3) => 1 },
                zset! { (1, 3) => 1, (2, 3) => 1, (2, 4) => 1, (2, 1) => 1, (4, 1) => 1, (4, 3) => 1 },
                zset! { (1, 1) => 1, (2, 2) => 1, (3, 3) => 1, (4, 4) => 1,
                              (1, 2) => 1, (1, 3) => 1, (1, 4) => 1,
                              (2, 1) => 1, (2, 3) => 1, (2, 4) => 1,
                              (3, 1) => 1, (3, 2) => 1, (3, 4) => 1,
                              (4, 1) => 1, (4, 2) => 1, (4, 3) => 1 },
            ]
            .into_iter();

            let edges: Stream<_, OrdZSet<(usize, usize), isize>> =
                circuit
                    .add_source(Generator::new(move || edges.next().unwrap()));

            let paths = circuit.fixedpoint(|child| {
                // ```text
                //                          distinct_trace
                //               ┌───┐          ┌───┐
                // edges         │   │          │   │  paths
                // ────┬────────►│ + ├──────────┤   ├────────┬───►
                //     │         │   │          │   │        │
                //     │         └───┘          └───┘        │
                //     │           ▲                         │
                //     │           │                         │
                //     │         ┌─┴─┐                       │
                //     │         │   │                       │
                //     └────────►│ X │ ◄─────────────────────┘
                //               │   │
                //               └───┘
                //             join_trace
                // ```
                let edges = edges.delta0(child);
                let paths_delayed = <DelayedFeedback<_, OrdZSet<_, _>>>::new(child);

                let paths_inverted: Stream<_, OrdZSet<(usize, usize), isize>> = paths_delayed
                    .stream()
                    .map_keys(|&(x, y)| (y, x));

                let paths_inverted_indexed: Stream<_, OrdIndexedZSet<usize, usize, isize>> = paths_inverted.index();
                let edges_indexed: Stream<_, OrdIndexedZSet<usize, usize, isize>> = edges.index();

                let paths = edges.plus(&paths_inverted_indexed.join_trace(&edges_indexed, |_via, from, to| (*from, *to)))
                    .distinct_trace();
                paths_delayed.connect(&paths);
                let output = paths.integrate_trace();
                Ok(output.export())
            })
            .unwrap();

            paths.consolidate::<OrdZSet<_, _>>().integrate().distinct().inspect(move |ps| {
                assert_eq!(*ps, outputs.next().unwrap());
            })
        })
        .unwrap();

        for _ in 0..8 {
            //eprintln!("{}", i);
            root.step().unwrap();
        }
    }
}

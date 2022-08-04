//! Relational join operator.

use crate::{
    algebra::{IndexedZSet, MulByRef, ZSet},
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        Circuit, Scope, Stream,
    },
    lattice::Lattice,
    time::Timestamp,
    trace::{
        cursor::Cursor as TraceCursor, ord::OrdValSpine, Batch, BatchReader, Batcher, Trace,
        TraceReader,
    },
};
use deepsize::DeepSizeOf;
use std::{
    borrow::Cow,
    cmp::{min, Ordering},
    collections::HashMap,
    fmt::Write,
    hash::Hash,
    marker::PhantomData,
};
use timely::PartialOrder;

impl<P, I1> Stream<Circuit<P>, I1>
where
    P: Clone + 'static,
{
    /// Join two streams of batches.
    ///
    /// The operator takes two streams of batches indexed with the same key type
    /// (`I1::Key = I2::Key`) and outputs a stream obtained by joining each pair
    /// of inputs.
    ///
    /// Input streams will typically be produced by
    /// [`index`](`crate::circuit::Stream::index`) or
    /// [`index_with`](`crate::circuit::Stream::index_with`) operators.
    ///
    /// # Type arguments
    ///
    /// * `F` - join function type: maps key and a pair of values from input
    ///   batches to an output value.
    /// * `I1` - batch type in the first input stream.
    /// * `I2` - batch type in the second input stream.
    /// * `Z` - output Z-set type.
    // TODO: Allow taking two different `R` types for each collection
    pub fn stream_join<F, I2, Z>(
        &self,
        other: &Stream<Circuit<P>, I2>,
        f: F,
    ) -> Stream<Circuit<P>, Z>
    where
        I1: Batch<Time = (), R = Z::R> + Clone + Send + 'static,
        I1::Key: Ord + Hash + Clone,
        I1::Val: Ord + Clone,
        I2: Batch<Key = I1::Key, Time = (), R = Z::R> + Send + Clone + 'static,
        I2::Val: Ord + Clone,
        Z: Clone + ZSet + 'static,
        Z::R: MulByRef<Output = Z::R>,
        F: Fn(&I1::Key, &I1::Val, &I2::Val) -> Z::Key + 'static,
    {
        self.circuit()
            .add_binary_operator(Join::new(f), &self.shard(), &other.shard())
    }

    pub fn monotonic_stream_join<F, I2, Z>(
        &self,
        other: &Stream<Circuit<P>, I2>,
        f: F,
    ) -> Stream<Circuit<P>, Z>
    where
        I1: Batch<Time = (), R = Z::R> + Clone + Send + 'static,
        I1::Key: Ord + Hash + Clone,
        I1::Val: Ord + Clone,
        I2: Batch<Key = I1::Key, Time = (), R = Z::R> + Send + Clone + 'static,
        I2::Val: Ord + Clone,
        Z: Clone + ZSet + 'static,
        Z::R: MulByRef<Output = Z::R>,
        F: Fn(&I1::Key, &I1::Val, &I2::Val) -> Z::Key + 'static,
    {
        self.circuit()
            .add_binary_operator(MonotonicJoin::new(f), &self.shard(), &other.shard())
    }

    fn stream_join_inner<F, I2, Z>(
        &self,
        other: &Stream<Circuit<P>, I2>,
        f: F,
    ) -> Stream<Circuit<P>, Z>
    where
        I1: BatchReader<Time = (), R = Z::R> + Clone + 'static,
        I2: BatchReader<Key = I1::Key, Time = (), R = Z::R> + Clone + 'static,
        I1::Key: Ord,
        Z: Clone + ZSet + 'static,
        Z::R: MulByRef<Output = Z::R>,
        F: Fn(&I1::Key, &I1::Val, &I2::Val) -> Z::Key + 'static,
    {
        self.circuit()
            .add_binary_operator(Join::new(f), self, other)
    }
}

impl<I1> Stream<Circuit<()>, I1> {
    /// Incremental join of two streams of batches.
    ///
    /// Given streams `a` and `b` of changes to relations `A` and `B`
    /// respectively, computes a stream of changes to `A <> B` (where `<>`
    /// is the join operator):
    ///
    /// ```text
    /// delta(A <> B) = A <> B - z^-1(A) <> z^-1(B) = a <> z^-1(B) + z^-1(A) <> b + a <> b
    /// ```
    ///
    /// This method only works in the top-level scope.  It is superseded by
    /// [`join`](`crate::circuit::Stream::join`), which works in arbitrary
    /// nested scopes.  We keep this implementation for testing and
    /// benchmarking purposes.
    pub fn join_incremental<F, I2, Z>(
        &self,
        other: &Stream<Circuit<()>, I2>,
        join_func: F,
    ) -> Stream<Circuit<()>, Z>
    where
        I1: IndexedZSet + DeepSizeOf + Send,
        I1::Key: Ord + Clone + Hash + DeepSizeOf,
        I1::Val: Ord + Clone,
        I1::R: DeepSizeOf,
        I2: IndexedZSet<Key = I1::Key, R = I1::R> + DeepSizeOf + Send,
        I2::Val: Ord + Clone,
        F: Clone + Fn(&I1::Key, &I1::Val, &I2::Val) -> Z::Key + 'static,
        Z: ZSet<R = I1::R>,
        Z::R: MulByRef<Output = Z::R>,
    {
        let left = self.shard();
        let right = other.shard();
        left.integrate_trace()
            .delay_trace()
            .stream_join_inner(&right, join_func.clone())
            .plus(&left.stream_join_inner(&right.integrate_trace(), join_func))
    }
}

impl<P, I1> Stream<Circuit<P>, I1>
where
    P: Clone + 'static,
    I1: IndexedZSet + Send,
{
    // TODO: Derive `TS` type from circuit.
    /// Incremental join two streams of batches.
    ///
    /// Given streams `self` and `other` of batches that represent changes to
    /// relations `A` and `B` respectively, computes a stream of changes to
    /// `A <> B` (where `<>` is the join operator):
    ///
    /// # Type arguments
    ///
    /// * `I1` - batch type in the first input stream.
    /// * `I2` - batch type in the second input stream.
    /// * `F` - join function type: maps key and a pair of values from input
    ///   batches to an output value.
    /// * `Z` - output Z-set type.
    pub fn join<TS, I2, F, Z>(
        &self,
        other: &Stream<Circuit<P>, I2>,
        join_func: F,
    ) -> Stream<Circuit<P>, Z>
    where
        TS: Timestamp + DeepSizeOf, /* + ::std::fmt::Display */
        //I1: ::std::fmt::Display,
        I1::Key: DeepSizeOf + Clone + Ord + Hash, /* + ::std::fmt::Display */
        I1::Val: DeepSizeOf + Clone + Ord,        /* + ::std::fmt::Display */
        I1::R: DeepSizeOf,                        /* + ::std::fmt::Display */
        I2::Val: DeepSizeOf + Clone + Ord,        /* + ::std::fmt::Display */
        I2: IndexedZSet<Key = I1::Key, R = I1::R> + Send, /* + ::std::fmt::Display */
        Z: ZSet<R = I1::R>,                       /* + ::std::fmt::Display */
        Z::Batcher: DeepSizeOf,
        Z::Key: Clone + Default,
        Z::R: MulByRef<Output = Z::R> + Default,
        Z::Item: Default,
        F: Fn(&I1::Key, &I1::Val, &I2::Val) -> Z::Key + Clone + 'static,
    {
        // TODO: I think this is correct, but we need a proper proof.

        // We use the following formula for nested incremental join with arbitrary
        // of nesting depth:
        //
        // ```
        // (↑(a <> b))∆))[t] =
        //      __         __            __
        //      ╲          ╲             ╲
        //      ╱          ╱             ╱  {f(k,v1,v2), w1*w2}
        //      ‾‾         ‾‾            ‾‾
        //     k∈K  (t1,t2).t1\/t2=t  (k,v1,w1)∈a[t1]
        //                            (k,v2,w2)∈b[t2]
        // ```
        // where `t1\/t2 = t1.join(t2)` is the least upper bound of logical timestamps
        // t1 and t2, `f` is the join function that combines values from input streams
        // `a` and `b`.  This sum can be split into two terms `left + right`:
        //
        // ```
        //           __         __            __
        //           ╲          ╲             ╲
        // left=     ╱          ╱             ╱  {f(k,v1,v2), w1*w2}
        //           ‾‾         ‾‾            ‾‾
        //          k∈K  (t1,t2).t1\/t2=t  (k,v1,w1)∈a[t1]
        //                 and t2<t1       (k,v2,w2)∈b[t2]
        //           __         __            __
        //           ╲          ╲             ╲
        // right=    ╱          ╱             ╱  {f(k,v1,v2), w1*w2}
        //           ‾‾         ‾‾            ‾‾
        //          k∈K  (t1,t2).t1\/t2=t  (k,v1,w1)∈a[t1]
        //                 and t2>=t1      (k,v2,w2)∈b[t2]
        // ```
        // where `t2<t1` and `t2>=t1` refer to the total order in which timestamps are
        // observed during the execution of the circuit, not their logical partial
        // order.  In particular, all iterations of an earlier clock epoch preceed the
        // first iteration of a newer epoch.
        //
        // The advantage of this representation is that each term can be computed
        // as a join of one of the input streams with the trace of the other stream,
        // implemented by the `JoinTrace` operator.
        let left = self.shard();
        let right = other.shard();

        let left_trace = left.trace::<OrdValSpine<I1::Key, I1::Val, TS, I1::R>>();
        let right_trace = right.trace::<OrdValSpine<I1::Key, I2::Val, TS, I1::R>>();

        let left = self.circuit().add_binary_operator(
            JoinTrace::new(join_func.clone()),
            &left,
            &right_trace,
        );

        let right = self.circuit().add_binary_operator(
            JoinTrace::new(move |k: &I1::Key, v2: &I2::Val, v1: &I1::Val| join_func(k, v1, v2)),
            &right,
            &left_trace.delay_trace(),
        );

        left.plus(&right)
    }
}

/// Join two streams of batches.
///
/// See [`Stream::join`](`crate::circuit::Stream::join`).
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
    Z::R: MulByRef<Output = Z::R>,
{
    fn eval(&mut self, i1: &I1, i2: &I2) -> Z {
        let mut cursor1 = i1.cursor();
        let mut cursor2 = i2.cursor();

        // Choose capacity heuristically.
        let mut batch = Vec::with_capacity(min(i1.len(), i2.len()));

        while cursor1.key_valid() && cursor2.key_valid() {
            match cursor1.key().cmp(cursor2.key()) {
                Ordering::Less => cursor1.seek_key(cursor2.key()),
                Ordering::Greater => cursor2.seek_key(cursor1.key()),
                Ordering::Equal => {
                    while cursor1.val_valid() {
                        let w1 = cursor1.weight();
                        let v1 = cursor1.val();
                        while cursor2.val_valid() {
                            let w2 = cursor2.weight();
                            let v2 = cursor2.val();

                            batch.push((
                                (self.join_func)(cursor1.key(), v1, v2),
                                w1.mul_by_ref(&w2),
                            ));
                            cursor2.step_val();
                        }

                        cursor2.rewind_vals();
                        cursor1.step_val();
                    }

                    cursor1.step_key();
                    cursor2.step_key();
                }
            }
        }

        Z::from_keys((), batch)
    }
}

pub struct MonotonicJoin<F, I1, I2, Z> {
    join_func: F,
    _types: PhantomData<(I1, I2, Z)>,
}

impl<F, I1, I2, Z> MonotonicJoin<F, I1, I2, Z> {
    pub fn new(join_func: F) -> Self {
        Self {
            join_func,
            _types: PhantomData,
        }
    }
}

impl<F, I1, I2, Z> Operator for MonotonicJoin<F, I1, I2, Z>
where
    I1: 'static,
    I2: 'static,
    F: 'static,
    Z: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("MonotonicJoin")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<F, I1, I2, Z> BinaryOperator<I1, I2, Z> for MonotonicJoin<F, I1, I2, Z>
where
    I1: BatchReader<Time = (), R = Z::R> + 'static,
    I1::Key: Ord,
    I2: BatchReader<Key = I1::Key, Time = (), R = Z::R> + 'static,
    F: Fn(&I1::Key, &I1::Val, &I2::Val) -> Z::Key + 'static,
    Z: ZSet + 'static,
    Z::R: MulByRef<Output = Z::R>,
{
    fn eval(&mut self, i1: &I1, i2: &I2) -> Z {
        let mut cursor1 = i1.cursor();
        let mut cursor2 = i2.cursor();

        // Choose capacity heuristically.
        let mut batch = Vec::with_capacity(min(i1.len(), i2.len()));

        while cursor1.key_valid() && cursor2.key_valid() {
            match cursor1.key().cmp(cursor2.key()) {
                Ordering::Less => cursor1.seek_key(cursor2.key()),
                Ordering::Greater => cursor2.seek_key(cursor1.key()),
                Ordering::Equal => {
                    while cursor1.val_valid() {
                        let w1 = cursor1.weight();
                        let v1 = cursor1.val();
                        while cursor2.val_valid() {
                            let w2 = cursor2.weight();
                            let v2 = cursor2.val();

                            batch.push((
                                Z::item_from((self.join_func)(cursor1.key(), v1, v2), ()),
                                w1.mul_by_ref(&w2),
                            ));
                            cursor2.step_val();
                        }

                        cursor2.rewind_vals();
                        cursor1.step_val();
                    }

                    cursor1.step_key();
                    cursor2.step_key();
                }
            }
        }

        Z::from_tuples((), batch)
    }
}

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
            time: T::Time::clock_start(),
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
        if scope == 0 {
            self.empty_input = false;
            self.empty_output = false;
        }
    }
    fn clock_end(&mut self, scope: Scope) {
        debug_assert!(self
            .output_batchers
            .iter()
            .all(|(time, _)| !time.less_equal(&self.time)));
        self.time = self.time.advance(scope + 1);
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
    I: IndexedZSet, /* + ::std::fmt::Display */
    I::Key: Ord + Clone,
    T: Trace<Key = I::Key, R = I::R> + 'static, /* + ::std::fmt::Display */
    //T::Time: ::std::fmt::Display,
    F: Clone + Fn(&I::Key, &I::Val, &T::Val) -> Z::Key + 'static,
    Z: ZSet<R = I::R>, /* + ::std::fmt::Display */
    Z::Key: Clone + Default,
    Z::Batcher: DeepSizeOf,
    Z::R: MulByRef<Output = Z::R> + Default,
    Z::Item: Default,
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

        while index_cursor.key_valid() && trace_cursor.key_valid() {
            match index_cursor.key().cmp(trace_cursor.key()) {
                Ordering::Less => {
                    index_cursor.seek_key(trace_cursor.key());
                }
                Ordering::Greater => {
                    trace_cursor.seek_key(index_cursor.key());
                }
                Ordering::Equal => {
                    //println!("key: {}", index_cursor.key(index));

                    while index_cursor.val_valid() {
                        let w1 = index_cursor.weight();
                        let v1 = index_cursor.val();
                        //println!("v1: {}, w1: {}", v1, w1);

                        while trace_cursor.val_valid() {
                            let output =
                                (self.join_func)(index_cursor.key(), v1, trace_cursor.val());
                            trace_cursor.map_times(|ts, w2| {
                                output_tuples.push((
                                    ts.join(&self.time),
                                    (Z::item_from(output.clone(), ()), w1.mul_by_ref(w2)),
                                ));
                                //println!("  tuple@{}: ({:?}, {})", off,
                                // output, w1.clone() * w2.clone());
                            });
                            trace_cursor.step_val();
                        }
                        trace_cursor.rewind_vals();
                        index_cursor.step_val();
                    }

                    index_cursor.step_key();
                    trace_cursor.step_key();
                }
            }
        }

        // Sort `output_tuples` by timestamp and push all tuples for each unique
        // timestamp to the appropriate batcher.
        output_tuples.sort_by(|(t1, _), (t2, _)| t1.cmp(t2));

        let mut batch = Vec::new();
        while !output_tuples.is_empty() {
            let batch_time = output_tuples[0].0.clone();

            let end = output_tuples.partition_point(|(time, _)| *time == batch_time);
            batch.extend(output_tuples.drain(..end).map(|(_, tuple)| tuple));

            self.output_batchers
                .entry(batch_time)
                .or_insert_with(|| Z::Batcher::new(()))
                .push_batch(&mut batch);
            batch.clear();
        }

        // Finalize the batch for the current timestamp (`self.time`) and return it.
        let batcher = self
            .output_batchers
            .remove(&self.time)
            .unwrap_or_else(|| Z::Batcher::new(()));
        self.time = self.time.advance(0);
        let result = batcher.seal();
        //println!("JoinTrace output:\n{}", result);

        self.empty_output = result.is_empty();
        result
    }
}

#[cfg(test)]
mod test {
    use crate::{
        operator::{DelayedFeedback, FilterMap, Generator},
        time::{NestedTimestamp32, Product, Timestamp},
        trace::{
            ord::{OrdIndexedZSet, OrdZSet},
            Batch,
        },
        zset, Circuit, Runtime, Stream,
    };
    use deepsize::DeepSizeOf;
    use std::{
        fmt::{Display, Formatter},
        vec,
    };

    #[test]
    fn join_test() {
        let circuit = Circuit::build(move |circuit| {
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
            let inc_outputs_vec = vec![
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
            ];

            let mut inc_outputs = inc_outputs_vec.clone().into_iter();
            let mut inc_outputs2 = inc_outputs_vec.into_iter();

            let index1: Stream<_, OrdIndexedZSet<usize, &'static str, isize>> = circuit
                .add_source(Generator::new(move || {
                    if Runtime::worker_index() == 0 {
                        input1.next().unwrap()
                    } else {
                        <OrdZSet<_, _>>::empty(())
                    }
                }))
                .index();
            let index2: Stream<_, OrdIndexedZSet<usize, &'static str, isize>> = circuit
                .add_source(Generator::new(move || {
                    if Runtime::worker_index() == 0 {
                        input2.next().unwrap()
                    } else {
                        <OrdZSet<_, _>>::empty(())
                    }
                }))
                .index();
            index1
                .stream_join(&index2, |&k: &usize, s1, s2| (k, format!("{} {}", s1, s2)))
                .gather(0)
                .inspect(move |fm: &OrdZSet<(usize, String), _>| {
                    if Runtime::worker_index() == 0 {
                        assert_eq!(fm, &outputs.next().unwrap())
                    }
                });
            index1
                .join_incremental(&index2, |&k: &usize, s1, s2| (k, format!("{} {}", s1, s2)))
                .gather(0)
                .inspect(move |fm: &OrdZSet<(usize, String), _>| {
                    if Runtime::worker_index() == 0 {
                        assert_eq!(fm, &inc_outputs.next().unwrap())
                    }
                });

            index1
                .join::<(), _, _, _>(&index2, |&k: &usize, s1, s2| (k, format!("{} {}", s1, s2)))
                .gather(0)
                .inspect(move |fm: &OrdZSet<(usize, String), _>| {
                    if Runtime::worker_index() == 0 {
                        assert_eq!(fm, &inc_outputs2.next().unwrap())
                    }
                });
        })
        .unwrap()
        .0;

        for _ in 0..5 {
            circuit.step().unwrap();
        }
    }

    fn do_join_test_mt(workers: usize) {
        let hruntime = Runtime::run(workers, || {
            join_test();
        });

        hruntime.join().unwrap();
    }

    #[test]
    fn join_test_mt() {
        do_join_test_mt(1);
        do_join_test_mt(2);
        do_join_test_mt(4);
        do_join_test_mt(16);
    }

    // Compute pairwise reachability relation between graph nodes as the
    // transitive closure of the edge relation.
    #[test]
    fn join_trace_test() {
        let circuit = Circuit::build(move |circuit| {
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

            let paths = circuit.recursive(|child, paths_delayed: Stream<_, OrdZSet<(usize, usize), isize>>| {
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
                //               join
                // ```
                let edges = edges.delta0(child);

                let paths_inverted: Stream<_, OrdZSet<(usize, usize), isize>> = paths_delayed
                    .map(|&(x, y)| (y, x));

                let paths_inverted_indexed = paths_inverted.index();
                let edges_indexed = edges.index();

                Ok(edges.plus(&paths_inverted_indexed.join::<NestedTimestamp32, _, _, _>(&edges_indexed, |_via, from, to| (*from, *to))))
            })
            .unwrap();

            paths.integrate().distinct().inspect(move |ps| {
                assert_eq!(*ps, outputs.next().unwrap());
            });
        })
        .unwrap().0;

        for _ in 0..8 {
            //eprintln!("{}", i);
            circuit.step().unwrap();
        }
    }

    #[derive(Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, DeepSizeOf)]
    struct Label(pub usize, pub u16);

    impl Display for Label {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
            write!(f, "L({},{})", self.0, self.1)
        }
    }

    #[derive(Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, DeepSizeOf)]
    struct Edge(pub usize, pub usize);

    impl Display for Edge {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
            write!(f, "E({},{})", self.0, self.1)
        }
    }
    // Recursively propagate node labels in an acyclic graph.
    // The reason for supporting acyclic graphs only is that we use this to test
    // the join operator in isolation, so we don't want to use `distinct`.
    fn propagate<P, TS>(
        circuit: &Circuit<P>,
        edges: &Stream<Circuit<P>, OrdZSet<Edge, isize>>,
        labels: &Stream<Circuit<P>, OrdZSet<Label, isize>>,
    ) -> Stream<Circuit<P>, OrdZSet<Label, isize>>
    where
        P: Clone + 'static,
        TS: Timestamp + DeepSizeOf + ::std::fmt::Display,
    {
        let computed_labels = circuit
            .fixedpoint(|child| {
                let edges = edges.delta0(child);
                let labels = labels.delta0(child);

                let computed_labels = <DelayedFeedback<_, OrdZSet<_, _>>>::new(child);
                let result: Stream<_, OrdZSet<Label, isize>> =
                    labels.plus(computed_labels.stream());

                computed_labels.connect(
                    &result
                        .index_with(|label| (label.0, label.1))
                        .join::<TS, _, _, _>(
                            &edges.index_with(|edge| (edge.0, edge.1)),
                            |_from, label, to| Label(*to, *label),
                        ),
                );

                Ok(result.integrate_trace().export())
            })
            .unwrap();

        computed_labels.consolidate()
    }

    #[test]
    fn propagate_test() {
        let circuit = Circuit::build(move |circuit| {
            let mut edges: vec::IntoIter<OrdZSet<Edge, isize>> = vec![
                zset! { Edge(1, 2) => 1, Edge(1, 3) => 1, Edge(2, 4) => 1, Edge(3, 4) => 1 },
                zset! { Edge(5, 7) => 1, Edge(6, 7) => 1 },
                zset! { Edge(4, 5) => 1, Edge(4, 6) => 1, },
                zset! { Edge(3, 8) => 1, Edge(8, 9) => 1 },
                zset! { Edge(2, 4) => -1, Edge(7, 10) => 1 },
                zset! { Edge(3, 4) => -1 },
                zset! { Edge(1, 4) => 1 },
                zset! { Edge(9, 7) => 1 },
            ]
            .into_iter();

            let mut labels: vec::IntoIter<OrdZSet<Label, isize>> = vec![
                zset! { Label(1, 0) => 1 },
                zset! { Label(4, 1) => 1 },
                zset! { },
                zset! { Label(1, 0) => -1, Label(1, 2) => 1 },
                zset! { },
                zset! { Label(8, 3) => 1 },
                zset! { Label(4, 1) => -1 },
                zset! { },
            ]
            .into_iter();

            let mut outputs: vec::IntoIter<OrdZSet<Label, isize>> = vec![
                zset! { Label(1, 0) => 1, Label(2, 0) => 1, Label(3, 0) => 1, Label(4, 0) => 2 },
                zset! { Label(4, 1) => 1 },
                zset! { Label(5, 0) => 2, Label(5, 1) => 1, Label(6, 0) => 2, Label(6, 1) => 1, Label(7, 0) => 4, Label(7, 1) => 2 },
                zset! { Label(1, 0) => -1, Label(1, 2) => 1, Label(2, 0) => -1, Label(2, 2) => 1, Label(3, 0) => -1, Label(3, 2) => 1, Label(4, 0) => -2, Label(4, 2) => 2, Label(5, 0) => -2, Label(5, 2) => 2, Label(6, 0) => -2, Label(6, 2) => 2, Label(7, 0) => -4, Label(7, 2) => 4, Label(8, 2) => 1, Label(9, 2) => 1 },
                zset! { Label(4, 2) => -1, Label(5, 2) => -1, Label(6, 2) => -1, Label(7, 2) => -2, Label(10, 1) => 2, Label(10, 2) => 2 },
                zset! { Label(4, 2) => -1, Label(5, 2) => -1, Label(6, 2) => -1, Label(7, 2) => -2, Label(8, 3) => 1, Label(9, 3) => 1, Label(10, 2) => -2 },
                zset! { Label(4, 1) => -1, Label(4, 2) => 1, Label(5, 1) => -1, Label(5, 2) => 1, Label(6, 1) => -1, Label(6, 2) => 1, Label(7, 1) => -2, Label(7, 2) => 2, Label(10, 1) => -2, Label(10, 2) => 2 },
                zset! { Label(7, 2) => 1, Label(7, 3) => 1, Label(10, 2) => 1, Label(10, 3) => 1 },
            ]
            .into_iter();

            let edges: Stream<_, OrdZSet<Edge, isize>> =
                circuit
                    .add_source(Generator::new(move || edges.next().unwrap()));

            let labels: Stream<_, OrdZSet<Label, isize>> =
                circuit
                    .add_source(Generator::new(move || labels.next().unwrap()));

            propagate::<_, NestedTimestamp32>(circuit, &edges, &labels).inspect(move |labeled| {
                assert_eq!(*labeled, outputs.next().unwrap());
            });
        })
        .unwrap().0;

        for _ in 0..8 {
            //eprintln!("{}", i);
            circuit.step().unwrap();
        }
    }

    #[test]
    fn propagate_nested_test() {
        let circuit = Circuit::build(move |circuit| {
            let mut edges: vec::IntoIter<OrdZSet<Edge, isize>> = vec![
                zset! { Edge(1, 2) => 1, Edge(1, 3) => 1, Edge(2, 4) => 1, Edge(3, 4) => 1 },
                zset! { Edge(5, 7) => 1, Edge(6, 7) => 1 },
                zset! { Edge(4, 5) => 1, Edge(4, 6) => 1 },
                zset! { Edge(3, 8) => 1, Edge(8, 9) => 1 },
                zset! { Edge(2, 4) => -1, Edge(7, 10) => 1 },
                zset! { Edge(3, 4) => -1 },
                zset! { Edge(1, 4) => 1 },
                zset! { Edge(9, 7) => 1 },
            ]
            .into_iter();

            let mut labels: vec::IntoIter<OrdZSet<Label, isize>> = vec![
                zset! { Label(1, 0) => 1 },
                zset! { Label(4, 1) => 1 },
                zset! { },
                zset! { Label(1, 0) => -1, Label(1, 2) => 1 },
                zset! { },
                zset! { Label(8, 3) => 1 },
                zset! { Label(4, 1) => -1 },
                zset! { },
            ]
            .into_iter();

            let mut outputs: vec::IntoIter<OrdZSet<Label, isize>> = vec![
                zset!{ Label(1,0) => 2, Label(2,0) => 3, Label(3,0) => 3, Label(4,0) => 8 },
                zset!{ Label(4,1) => 2 },
                zset!{ Label(5,0) => 10, Label(5,1) => 3, Label(6,0) => 10, Label(6,1) => 3, Label(7,0) => 24, Label(7,1) => 8 },
                zset!{ Label(1,0) => -2, Label(1,2) => 2, Label(2,0) => -3, Label(2,2) => 3, Label(3,0) => -3, Label(3,2) => 3,
                       Label(4,0) => -8, Label(4,2) => 8, Label(5,0) => -10, Label(5,2) => 10, Label(6,0) => -10, Label(6,2) => 10,
                       Label(7,0) => -24, Label(7,2) => 24, Label(8,2) => 4, Label(9,2) => 5 },
                zset!{ Label(4,2) => -4, Label(5,2) => -5, Label(6,2) => -5, Label(7,2) => -12, Label(10,1) => 10, Label(10,2) => 14 },
                zset!{ Label(4,2) => -4, Label(5,2) => -5, Label(6,2) => -5, Label(7,2) => -12, Label(8,3) => 2,
                       Label(9,3) => 3, Label(10,2) => -14 },
                zset!{ Label(4,1) => -2, Label(4,2) => 3, Label(5,1) => -3, Label(5,2) => 4, Label(6,1) => -3, Label(6,2) => 4,
                       Label(7,1) => -8, Label(7,2) => 10, Label(10,1) => -10, Label(10,2) => 12 },
                zset!{ Label(7,2) => 6, Label(7,3) => 4, Label(10,2) => 7, Label(10,3) => 5 },
            ].into_iter();

            let edges: Stream<_, OrdZSet<Edge, isize>> =
                circuit
                    .add_source(Generator::new(move || edges.next().unwrap()));

            let labels: Stream<_, OrdZSet<Label, isize>> =
                circuit
                    .add_source(Generator::new(move || labels.next().unwrap()));

            let result = circuit.iterate(|child| {

                let counter = std::cell::RefCell::new(0);
                let edges = edges.delta0(child);
                let labels = labels.delta0(child);

                let computed_labels = <DelayedFeedback<_, OrdZSet<_, _>>>::new(child);
                let result = propagate::<_, Product<NestedTimestamp32, u32>>(child, &edges, &labels.plus(computed_labels.stream()));
                computed_labels.connect(&result);

                //result.inspect(|res: &OrdZSet<Label, isize>| println!("delta: {}", res));
                Ok((move || {
                    let mut counter = counter.borrow_mut();
                    // reset to 0 on each outer loop iteration.
                    if *counter == 2 {
                        *counter = 0;
                    }
                    *counter += 1;
                    //println!("counter: {}", *counter);
                    Ok(*counter == 2)
                },
                result.integrate_trace().export()))
            }).unwrap();

            result.consolidate().inspect(move |res: &OrdZSet<Label, isize>| {
                assert_eq!(*res, outputs.next().unwrap());
            });
        })
        .unwrap().0;

        for _ in 0..8 {
            //eprintln!("{}", i);
            circuit.step().unwrap();
        }
    }
}

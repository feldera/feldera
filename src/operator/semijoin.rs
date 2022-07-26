use crate::{
    algebra::{HasZero, IndexedZSet, MulByRef, ZSet},
    circuit::{
        operator_traits::{BinaryOperator, Data, Operator},
        Scope,
    },
    lattice::Lattice,
    trace::{ord::OrdValSpine, Batch, BatchReader, Batcher, Builder, Cursor, Trace, TraceReader},
    Circuit, Stream, Timestamp,
};
use deepsize::DeepSizeOf;
use hashbrown::HashMap;
use std::{
    borrow::Cow,
    cmp::{min, Ordering},
    fmt::Write,
    hash::Hash,
    marker::PhantomData,
};
use timely::PartialOrder;

impl<S, Pairs> Stream<Circuit<S>, Pairs>
where
    S: Clone + 'static,
{
    /// Semijoin two streams of batches.
    ///
    /// The operator takes two streams of batches indexed with the same key type
    /// (`Pairs::Key = Keys::Key`) and outputs a stream obtained by joining each
    /// pair of inputs.
    ///
    /// Input streams will typically be produced by [`Stream::index()`] or
    /// [`Stream::index_with()`]
    ///
    /// #### Type arguments
    ///
    /// * `Pairs` - batch type in the first input stream.
    /// * `Keys` - batch type in the second input stream.
    /// * `Out` - output Z-set type.
    pub fn semijoin_stream<Keys, Out>(
        &self,
        keys: &Stream<Circuit<S>, Keys>,
    ) -> Stream<Circuit<S>, Out>
    where
        // TODO: Associated type bounds (rust/#52662) really simplify things
        // TODO: Allow non-unit timestamps
        Pairs: Batch<Time = ()> + Data + Send,
        Pairs::Key: Hash + Ord + Clone,
        Pairs::Val: Ord + Clone,
        Pairs::R: HasZero + MulByRef,
        Keys: Batch<Key = Pairs::Key, Val = (), Time = (), R = Pairs::R> + Data + Send,
        // TODO: Should this be `IndexedZSet<Key = Pairs::Key, Val = Pairs::Val>`?
        Out: ZSet<Key = (Pairs::Key, Pairs::Val), R = Pairs::R> + 'static,
    {
        self.shard().semijoin_stream_inner(&keys.shard())
    }

    fn semijoin_stream_inner<Keys, Out>(
        &self,
        keys: &Stream<Circuit<S>, Keys>,
    ) -> Stream<Circuit<S>, Out>
    where
        // TODO: Associated type bounds (rust/#52662) really simplify things
        // TODO: Allow non-unit timestamps
        Pairs: BatchReader<Time = ()> + Data,
        Pairs::Key: Ord + Clone,
        Pairs::Val: Clone,
        Pairs::R: HasZero + MulByRef,
        Keys: BatchReader<Key = Pairs::Key, Val = (), Time = (), R = Pairs::R> + Data,
        // TODO: Should this be `IndexedZSet<Key = Pairs::Key, Val = Pairs::Val>`?
        Out: ZSet<Key = (Pairs::Key, Pairs::Val), R = Pairs::R> + 'static,
    {
        self.circuit()
            .add_binary_operator(SemiJoinStreamMonotonic::new(), self, keys)
    }

    /// Semijoin two streams of batches.
    ///
    /// The operator takes two streams of batches indexed with the same key type
    /// (`Pairs::Key = Keys::Key`) and outputs a stream obtained by joining each
    /// pair of inputs.
    ///
    /// Input streams will typically be produced by [`Stream::index()`] or
    /// [`Stream::index_with()`]
    ///
    /// #### Type arguments
    ///
    /// * `Pairs` - batch type in the first input stream.
    /// * `Keys` - batch type in the second input stream.
    /// * `Out` - output Z-set type.
    pub fn semijoin_stream_core<F, Keys, Out>(
        &self,
        keys: &Stream<Circuit<S>, Keys>,
        semijoin_func: F,
    ) -> Stream<Circuit<S>, Out>
    where
        F: Fn(&Pairs::Key, &Pairs::Val) -> Out::Key + 'static,
        // TODO: Associated type bounds (rust/#52662) really simplify things
        // TODO: Allow non-unit timestamps
        Pairs: Batch<Time = ()> + Data + Send,
        Pairs::Key: Hash + Ord + Clone,
        Pairs::Val: Ord + Clone,
        Pairs::R: HasZero + MulByRef,
        Keys: Batch<Key = Pairs::Key, Val = (), Time = (), R = Pairs::R> + Data + Send,
        // TODO: Should this be `IndexedZSet<Key = Pairs::Key, Val = Pairs::Val>`?
        Out: ZSet<R = Pairs::R> + 'static,
    {
        self.shard()
            .semijoin_stream_core_inner(&keys.shard(), semijoin_func)
    }

    fn semijoin_stream_core_inner<F, Keys, Out>(
        &self,
        keys: &Stream<Circuit<S>, Keys>,
        semijoin_func: F,
    ) -> Stream<Circuit<S>, Out>
    where
        F: Fn(&Pairs::Key, &Pairs::Val) -> Out::Key + 'static,
        // TODO: Associated type bounds (rust/#52662) really simplify things
        // TODO: Allow non-unit timestamps
        Pairs: BatchReader<Time = ()> + Data,
        Pairs::Key: Clone + Ord,
        Pairs::Val: Clone,
        Pairs::R: HasZero + MulByRef,
        Keys: BatchReader<Key = Pairs::Key, Val = (), Time = (), R = Pairs::R> + Data,
        // TODO: Should this be `IndexedZSet<Key = Pairs::Key, Val = Pairs::Val>`?
        Out: ZSet<R = Pairs::R> + 'static,
    {
        self.circuit()
            .add_binary_operator(SemiJoinStream::new(semijoin_func), self, keys)
    }
}

/// Semijoin two streams of batches, see [`Stream::semijoin_stream`]
pub struct SemiJoinStreamMonotonic<Pairs, Keys, Out> {
    _types: PhantomData<(Pairs, Keys, Out)>,
}

impl<Pairs, Keys, Out> SemiJoinStreamMonotonic<Pairs, Keys, Out> {
    pub const fn new() -> Self {
        Self {
            _types: PhantomData,
        }
    }
}

impl<Pairs, Keys, Out> Operator for SemiJoinStreamMonotonic<Pairs, Keys, Out>
where
    Pairs: 'static,
    Keys: 'static,
    Out: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("SemiJoinStreamMonotonic")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<Pairs, Keys, Out> BinaryOperator<Pairs, Keys, Out>
    for SemiJoinStreamMonotonic<Pairs, Keys, Out>
where
    Pairs: BatchReader<Time = ()> + 'static,
    Pairs::Key: Clone + Ord,
    Pairs::Val: Clone,
    Pairs::R: HasZero + MulByRef,
    Keys: BatchReader<Key = Pairs::Key, Val = (), Time = (), R = Pairs::R> + 'static,
    Out: ZSet<Key = (Pairs::Key, Pairs::Val), R = Pairs::R> + 'static,
{
    fn eval(&mut self, pairs: &Pairs, keys: &Keys) -> Out {
        let mut pair_cursor = pairs.cursor();
        let mut key_cursor = keys.cursor();

        // Choose capacity heuristically.
        let mut builder = Out::Builder::with_capacity((), min(pairs.len(), keys.len()));

        // While both keys are valid
        // TODO: Is there a better way to iterate here? `keys_cursor` is the
        //       thing really driving this, so can we just use it as the
        //       source of iteration to do the least work possible?
        while key_cursor.key_valid() && pair_cursor.key_valid() {
            match key_cursor.key().cmp(pair_cursor.key()) {
                // Match up both the cursors
                Ordering::Less => key_cursor.seek_key(pair_cursor.key()),
                Ordering::Greater => pair_cursor.seek_key(key_cursor.key()),

                Ordering::Equal => {
                    // TODO: Can the value of `()` ever be invalid? Do we need an `if
                    // key_cursor.val_valid()` check?
                    let key_weight = key_cursor.weight();
                    while pair_cursor.val_valid() {
                        // Get the weight of the output kv pair by multiplying them together
                        // TODO: Can either weights possibly be zero? If so, we can check if
                        // `key_weight`       is zero outside of the loop to
                        // skip redundant work
                        let pair_weight = pair_cursor.weight();
                        let kv_weight = pair_weight.mul_by_ref(&key_weight);

                        // Add to our output batch
                        builder.push((
                            (pair_cursor.key().clone(), pair_cursor.val().clone()),
                            (),
                            kv_weight,
                        ));
                        pair_cursor.step_val();
                    }

                    pair_cursor.step_key();
                    key_cursor.step_key();
                }
            }
        }

        // Create the output stream
        builder.done()
    }
}

/// Semijoin two streams of batches, see [`Stream::semijoin_stream_core`]
pub struct SemiJoinStream<F, Pairs, Keys, Out> {
    semijoin_func: F,
    _types: PhantomData<(Pairs, Keys, Out)>,
}

impl<F, Pairs, Keys, Out> SemiJoinStream<F, Pairs, Keys, Out> {
    pub const fn new(semijoin_func: F) -> Self {
        Self {
            semijoin_func,
            _types: PhantomData,
        }
    }
}

impl<F, Pairs, Keys, Out> Operator for SemiJoinStream<F, Pairs, Keys, Out>
where
    F: 'static,
    Pairs: 'static,
    Keys: 'static,
    Out: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("SemiJoinStream")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<F, Pairs, Keys, Out> BinaryOperator<Pairs, Keys, Out> for SemiJoinStream<F, Pairs, Keys, Out>
where
    F: Fn(&Pairs::Key, &Pairs::Val) -> Out::Key + 'static,
    Pairs: BatchReader<Time = ()> + 'static,
    Pairs::Key: Clone + Ord,
    Pairs::Val: Clone,
    Pairs::R: HasZero + MulByRef,
    Keys: BatchReader<Key = Pairs::Key, Val = (), Time = (), R = Pairs::R> + 'static,
    Out: ZSet<R = Pairs::R> + 'static,
{
    fn eval(&mut self, pairs: &Pairs, keys: &Keys) -> Out {
        let mut pair_cursor = pairs.cursor();
        let mut key_cursor = keys.cursor();

        // Choose capacity heuristically.
        let mut batch = Vec::with_capacity(min(pairs.len(), keys.len()));

        // While both keys are valid
        // TODO: Is there a better way to iterate here? `keys_cursor` is the
        //       thing really driving this, so can we just use it as the
        //       source of iteration to do the least work possible?
        while key_cursor.key_valid() && pair_cursor.key_valid() {
            match key_cursor.key().cmp(pair_cursor.key()) {
                // Match up both the cursors
                Ordering::Less => key_cursor.seek_key(pair_cursor.key()),
                Ordering::Greater => pair_cursor.seek_key(key_cursor.key()),

                Ordering::Equal => {
                    // TODO: Can the value of `()` ever be invalid? Do we need an `if
                    // key_cursor.val_valid()` check?
                    let key_weight = key_cursor.weight();
                    while pair_cursor.val_valid() {
                        // Get the weight of the output kv pair by multiplying them together
                        // TODO: Can either weights possibly be zero? If so, we can check if
                        // `key_weight`       is zero outside of the loop to
                        // skip redundant work
                        let pair_weight = pair_cursor.weight();
                        let kv_weight = pair_weight.mul_by_ref(&key_weight);

                        // Produce the output value
                        let value = (self.semijoin_func)(pair_cursor.key(), pair_cursor.val());

                        // Add it to our output batch
                        batch.push(((value, ()), kv_weight));
                        pair_cursor.step_val();
                    }

                    pair_cursor.step_key();
                    key_cursor.step_key();
                }
            }
        }

        // Create the output stream
        Out::from_tuples((), batch)
    }
}

impl<Pairs> Stream<Circuit<()>, Pairs> {
    /// Incremental semijoin of two streams of batches.
    ///
    /// Given streams `a` and `b` of changes to relations `A` and `B`
    /// respectively, computes a stream of changes to `A <> B` (where `<>`
    /// is the semijoin operator):
    ///
    /// ```text
    /// delta(A <> B) = A <> B - z^-1(A) <> z^-1(B) = a <> z^-1(B) + z^-1(A) <> b + a <> b
    /// ```
    ///
    /// This method only works in the top-level scope.  It is superseded by
    /// [`Stream::semijoin()`], which works in arbitrary nested scopes. We
    /// keep this implementation for testing and benchmarking purposes.
    pub fn semijoin_incremental<Keys, Out>(
        &self,
        keys: &Stream<Circuit<()>, Keys>,
    ) -> Stream<Circuit<()>, Out>
    where
        Pairs: IndexedZSet + DeepSizeOf + Send,
        Pairs::Key: Hash + Ord + Clone + DeepSizeOf,
        Pairs::Val: Ord + Clone,
        Pairs::R: MulByRef + DeepSizeOf,
        Keys: ZSet<Key = Pairs::Key, R = Pairs::R> + DeepSizeOf + Send,
        Out: ZSet<Key = (Pairs::Key, Pairs::Val), R = Pairs::R>,
    {
        let (pairs, keys) = (self.shard(), keys.shard());
        pairs
            .integrate_trace()
            .delay_trace()
            .semijoin_stream_inner(&keys)
            .plus(&pairs.semijoin_stream_inner(&keys.integrate_trace()))
    }

    /// Incremental semijoin of two streams of batches.
    ///
    /// Given streams `a` and `b` of changes to relations `A` and `B`
    /// respectively, computes a stream of changes to `A <> B` (where `<>`
    /// is the semijoin operator):
    ///
    /// ```text
    /// delta(A <> B) = A <> B - z^-1(A) <> z^-1(B) = a <> z^-1(B) + z^-1(A) <> b + a <> b
    /// ```
    ///
    /// This method only works in the top-level scope.  It is superseded by
    /// [`Stream::semijoin()`], which works in arbitrary nested scopes. We
    /// keep this implementation for testing and benchmarking purposes.
    pub fn semijoin_incremental_core<F, Keys, Out>(
        &self,
        keys: &Stream<Circuit<()>, Keys>,
        semijoin_func: F,
    ) -> Stream<Circuit<()>, Out>
    where
        F: Fn(&Pairs::Key, &Pairs::Val) -> Out::Key + Clone + 'static,
        Pairs: IndexedZSet + DeepSizeOf + Send,
        Pairs::Key: Hash + Ord + Clone + DeepSizeOf,
        Pairs::Val: Ord + Clone,
        Pairs::R: MulByRef + DeepSizeOf,
        Keys: ZSet<Key = Pairs::Key, R = Pairs::R> + DeepSizeOf + Send,
        Out: ZSet<R = Pairs::R>,
    {
        let (pairs, keys) = (self.shard(), keys.shard());
        pairs
            .integrate_trace()
            .delay_trace()
            .semijoin_stream_core_inner(&keys, semijoin_func.clone())
            .plus(&pairs.semijoin_stream_core_inner(&keys.integrate_trace(), semijoin_func))
    }
}

impl<S, Pairs> Stream<Circuit<S>, Pairs>
where
    S: Clone + 'static,
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
    /// * `Pairs` - batch type in the first input stream.
    /// * `Keys` - batch type in the second input stream.
    /// * `Out` - output Z-set type.
    pub fn semijoin<Time, F, Keys, Out>(
        &self,
        keys: &Stream<Circuit<S>, Keys>,
        semijoin_func: F,
    ) -> Stream<Circuit<S>, Out>
    where
        Time: Timestamp + DeepSizeOf,
        F: Fn(&Pairs::Key, &Pairs::Val) -> Out::Key + 'static,
        Pairs: IndexedZSet + Send,
        Pairs::Key: Hash + Ord + DeepSizeOf + Clone,
        Pairs::Val: Ord + DeepSizeOf + Clone,
        Pairs::R: MulByRef + Default + DeepSizeOf,
        Keys: ZSet<Key = Pairs::Key, R = Pairs::R> + Send,
        Out: ZSet<R = Pairs::R>,
        Out::Key: Clone,
        Out::Batcher: DeepSizeOf,
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
        let key_trace = keys
            .shard()
            .trace::<OrdValSpine<Pairs::Key, Keys::Val, Time, Pairs::R>>();
        self.circuit().add_binary_operator(
            SemiJoinTrace::new(semijoin_func),
            &self.shard(),
            &key_trace,
        )
    }
}

pub struct SemiJoinTrace<F, Pairs, Keys, Out>
where
    Keys: TraceReader,
    Out: ZSet,
{
    semijoin_func: F,
    // TODO: not needed once timekeeping is handled by the circuit.
    time: Keys::Time,
    // Future update batches computed ahead of time, indexed by time
    // when each batch should be output.
    output_batchers: HashMap<Keys::Time, Out::Batcher>,
    // True if empty input batch was received at the current clock cycle.
    empty_input: bool,
    // True if empty output was produced at the current clock cycle.
    empty_output: bool,
    _types: PhantomData<(Pairs, Keys, Out)>,
}

impl<F, Pairs, Keys, Out> SemiJoinTrace<F, Pairs, Keys, Out>
where
    Keys: TraceReader,
    Out: ZSet,
{
    pub fn new(semijoin_func: F) -> Self {
        Self {
            semijoin_func,
            time: Keys::Time::clock_start(),
            output_batchers: HashMap::new(),
            empty_input: false,
            empty_output: false,
            _types: PhantomData,
        }
    }
}

impl<F, Pairs, Keys, Out> Operator for SemiJoinTrace<F, Pairs, Keys, Out>
where
    F: 'static,
    Pairs: 'static,
    Keys: TraceReader + 'static,
    Out: ZSet,
    Out::Batcher: DeepSizeOf,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("SemiJoinTrace")
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

impl<F, Pairs, Keys, Out> BinaryOperator<Pairs, Keys, Out> for SemiJoinTrace<F, Pairs, Keys, Out>
where
    F: Fn(&Pairs::Key, &Pairs::Val) -> Out::Key + 'static,
    Pairs: IndexedZSet,
    Pairs::Key: Ord + Clone,
    Pairs::Val: Clone,
    Pairs::R: MulByRef + Default,
    Keys: Trace<Key = Pairs::Key, Val = (), R = Pairs::R> + 'static,
    Out: ZSet<R = Pairs::R>,
    Out::Key: Clone,
    Out::Batcher: DeepSizeOf,
{
    fn eval(&mut self, pairs: &Pairs, keys: &Keys) -> Out {
        self.empty_input = pairs.is_empty();

        // Buffer to collect output tuples.
        // One allocation per clock tick is acceptable; however the actual output can be
        // larger than `index.len()`.  If re-allocations becomes a problem, we
        // may need to do something smarter, like a chain of buffers.
        let mut output_tuples = Vec::with_capacity(pairs.len());

        let mut pairs_cursor = pairs.cursor();
        let mut keys_cursor = keys.cursor();

        // TODO: Is there a better way to iterate here? `keys_cursor` is the
        //       thing really driving this, so can we just use it as the
        //       source of iteration to do the least work possible?
        while pairs_cursor.key_valid() && keys_cursor.key_valid() {
            match pairs_cursor.key().cmp(keys_cursor.key()) {
                Ordering::Less => pairs_cursor.seek_key(keys_cursor.key()),
                Ordering::Greater => keys_cursor.seek_key(pairs_cursor.key()),

                Ordering::Equal => {
                    // TODO: Can the value of `()` ever be invalid? Do we need an `if
                    // keys_cursor.val_valid()` check?
                    while pairs_cursor.val_valid() {
                        let pair_weight = pairs_cursor.weight();
                        let pair_value = pairs_cursor.val();

                        let output = (self.semijoin_func)(pairs_cursor.key(), pair_value);

                        // TODO: We know there'll always be exactly one value but `.map_times()`
                        //       doesn't let us express that
                        keys_cursor.map_times(|time, key_weight| {
                            output_tuples.push((
                                time.join(&self.time),
                                ((output.clone(), ()), pair_weight.mul_by_ref(key_weight)),
                            ));
                        });

                        pairs_cursor.step_val();
                    }

                    pairs_cursor.step_key();
                    keys_cursor.step_key();
                }
            }
        }

        // Sort `output_tuples` by timestamp and push all tuples for each unique
        // timestamp to the appropriate batcher.
        output_tuples.sort_by(|(t1, _), (t2, _)| t1.cmp(t2));

        let mut batch = Vec::new();
        while !output_tuples.is_empty() {
            let time = output_tuples[0].0.clone();
            let end = output_tuples.partition_point(|(t, _)| *t == time);
            batch.extend(output_tuples.drain(..end).map(|(_, tuple)| tuple));

            self.output_batchers
                .entry(time)
                .or_insert_with(|| Out::Batcher::new(()))
                .push_batch(&mut batch);
        }

        // Finalize the batch for the current timestamp (`self.time`) and return it.
        let batcher = self
            .output_batchers
            .remove(&self.time)
            .unwrap_or_else(|| Out::Batcher::new(()));
        self.time = self.time.advance(0);
        let result = batcher.seal();

        self.empty_output = result.is_empty();
        result
    }
}

// FIXME: More semijoin tests
#[cfg(test)]
mod test {
    use crate::{
        circuit::{Root, Stream},
        operator::Generator,
        trace::ord::{OrdIndexedZSet, OrdZSet},
        zset,
    };

    #[test]
    fn semijoin_test() {
        type Ids<S> = Stream<S, OrdZSet<usize, isize>>;
        type StringSet = OrdZSet<(usize, &'static str), isize>;
        type Strings<S> = Stream<S, OrdIndexedZSet<usize, &'static str, isize>>;

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
                zset! { (1, "a") => 1 },
                zset! { (1, "a") => 1 },
                zset! { (4, "n") => 2 },
                zset! { (1, "a") => 0 },
            ]
            .into_iter();

            let mut input2 = vec![
                zset! {
                    2 => 3,
                    2 => 4,
                    3 => 5,
                    3 => -2,
                    4 => 5,
                    4 => -2,
                },
                zset! { 1 => 1 },
                zset! { 4 => 1 },
                zset! {},
                zset! {},
            ]
            .into_iter();

            let mut outputs = vec![
                zset! {
                    (2, "c") => 9,
                    (2, "c") => 12,
                    (2, "d") => 12,
                    (2, "d") => 16,
                    (3, "e") => 25,
                    (3, "e") => -10,
                    (3, "f") => -10,
                    (3, "f") => 4
                },
                zset! { (1, "a") => 1 },
                zset! {},
                zset! {},
                zset! {},
            ]
            .into_iter();

            let inc_outputs_vec = vec![
                zset! {
                    (2, "c") => 9,
                    (2, "c") => 12,
                    (2, "d") => 12,
                    (2, "d") => 16,
                    (3, "e") => 25,
                    (3, "e") => -10,
                    (3, "f") => -10,
                    (3, "f") => 4
                },
                zset! {
                    (1, "a") => 2,
                    (1, "b") => 2,
                },
                zset! { (1, "a") => 1 },
                zset! {
                    (4, "n") => 10,
                    (4, "n") => -4,
                    (4, "n") => 2,
                },
                zset! {},
            ];

            let mut inc_outputs = inc_outputs_vec.clone().into_iter();
            let mut inc_outputs2 = inc_outputs_vec.into_iter();

            let index1: Strings<_> = circuit
                .add_source(Generator::new(move || input1.next().unwrap()))
                .index();
            let index2: Ids<_> = circuit.add_source(Generator::new(move || input2.next().unwrap()));

            index1
                .semijoin_stream_core(&index2, |&k, &s| (k, s))
                .inspect(move |fm: &StringSet| assert_eq!(fm, &outputs.next().unwrap()));

            index1
                .semijoin_incremental_core(&index2, |&k, &s| (k, s))
                .inspect(move |fm: &StringSet| assert_eq!(fm, &inc_outputs.next().unwrap()));

            index1
                .semijoin::<(), _, _, _>(&index2, |&k, &s| (k, s))
                .inspect(move |fm: &StringSet| assert_eq!(fm, &inc_outputs2.next().unwrap()));
        })
        .unwrap();

        for _ in 0..5 {
            root.step().unwrap();
        }
    }
}

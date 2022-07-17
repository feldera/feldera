use crate::{
    algebra::{HasZero, IndexedZSet, MulByRef, ZSet},
    circuit::{
        operator_traits::{BinaryOperator, Data, Operator},
        Scope,
    },
    lattice::Lattice,
    trace::{ord::OrdValSpine, BatchReader, Batcher, Cursor, Trace, TraceReader},
    Circuit, Stream, Timestamp,
};
use deepsize::DeepSizeOf;
use hashbrown::HashMap;
use std::{
    borrow::Cow,
    cmp::{min, Ordering},
    fmt::Write,
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
    /// (`Pairs::Key = Keys::Key`) and outputs a stream obtained by joining each pair
    /// of inputs.
    ///
    /// Input streams will typically be produced by [`Stream::index()`] or [`Stream::index_with()`]
    ///
    /// #### Type arguments
    ///
    /// * `Pairs` - batch type in the first input stream.
    /// * `Keys` - batch type in the second input stream.
    /// * `Out` - output Z-set type.
    ///
    pub fn semijoin_stream<F, Keys, Out>(
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
        while key_cursor.key_valid() && pair_cursor.key_valid() {
            match key_cursor.key().cmp(pair_cursor.key()) {
                // Match up both the cursors
                Ordering::Less => key_cursor.seek_key(pair_cursor.key()),
                Ordering::Greater => pair_cursor.seek_key(key_cursor.key()),

                Ordering::Equal => {
                    // For each value within the pair stream,
                    if key_cursor.val_valid() {
                        let key_weight = key_cursor.weight();
                        while pair_cursor.val_valid() {
                            // Get the weight of the output kv pair by multiplying them together
                            let pair_weight = pair_cursor.weight();
                            let kv_weight = pair_weight.mul_by_ref(&key_weight);

                            // Produce the output value
                            let value = (self.semijoin_func)(pair_cursor.key(), pair_cursor.val());

                            // Add it to our output batch
                            batch.push(((value, ()), kv_weight));
                            pair_cursor.step_val();
                        }
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
    /// This method only works in the top-level scope.  It is superseded by [`Stream::semijoin()`],
    /// which works in arbitrary nested scopes. We keep this implementation for testing and
    /// benchmarking purposes.
    ///
    pub fn semijoin_incremental<F, Keys, Out>(
        &self,
        keys: &Stream<Circuit<()>, Keys>,
        semijoin_func: F,
    ) -> Stream<Circuit<()>, Out>
    where
        F: Fn(&Pairs::Key, &Pairs::Val) -> Out::Key + Clone + 'static,
        Pairs: IndexedZSet + DeepSizeOf,
        Pairs::Key: Clone + Ord + DeepSizeOf,
        Pairs::Val: Clone + Ord,
        Pairs::R: MulByRef + DeepSizeOf,
        Keys: ZSet<Key = Pairs::Key, R = Pairs::R> + DeepSizeOf,
        Out: ZSet<R = Pairs::R>,
    {
        self.integrate_trace()
            .delay_trace()
            .semijoin_stream(keys, semijoin_func.clone())
            .plus(&self.semijoin_stream(&keys.integrate_trace(), semijoin_func))
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
    ///
    pub fn semijoin<Time, F, Keys, Out>(
        &self,
        keys: &Stream<Circuit<S>, Keys>,
        semijoin_func: F,
    ) -> Stream<Circuit<S>, Out>
    where
        Time: Timestamp + DeepSizeOf,
        F: Fn(&Pairs::Key, &Pairs::Val) -> Out::Key + 'static,
        Pairs: IndexedZSet,
        Pairs::Key: DeepSizeOf + Clone + Ord,
        Pairs::Val: DeepSizeOf + Clone + Ord,
        Pairs::R: MulByRef + Default + DeepSizeOf,
        Keys: ZSet<Key = Pairs::Key, R = Pairs::R>,
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
        let key_trace = keys.trace::<OrdValSpine<Pairs::Key, Keys::Val, Time, Pairs::R>>();
        self.circuit()
            .add_binary_operator(SemiJoinTrace::new(semijoin_func), self, &key_trace)
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

        while pairs_cursor.key_valid() && keys_cursor.key_valid() {
            match pairs_cursor.key().cmp(keys_cursor.key()) {
                Ordering::Less => pairs_cursor.seek_key(keys_cursor.key()),
                Ordering::Greater => keys_cursor.seek_key(pairs_cursor.key()),

                Ordering::Equal => {
                    if keys_cursor.val_valid() {
                        while pairs_cursor.val_valid() {
                            let pair_weight = pairs_cursor.weight();
                            let pair_value = pairs_cursor.val();

                            let output = (self.semijoin_func)(pairs_cursor.key(), pair_value);

                            keys_cursor.map_times(|time, key_weight| {
                                output_tuples.push((
                                    time.join(&self.time),
                                    ((output.clone(), ()), pair_weight.mul_by_ref(key_weight)),
                                ));
                            });

                            pairs_cursor.step_val();
                        }
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

#[cfg(test)]
mod test {
    use crate::{
        circuit::{Circuit, Root, Stream},
        operator::{DelayedFeedback, Generator},
        time::{NestedTimestamp32, Product, Timestamp},
        trace::ord::{OrdIndexedZSet, OrdZSet},
        zset,
    };
    use deepsize::DeepSizeOf;
    use std::{
        fmt::{Display, Formatter},
        vec,
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
                .semijoin_stream(&index2, |&k, &s| (k, s))
                .inspect(move |fm: &StringSet| assert_eq!(fm, &outputs.next().unwrap()));

            index1
                .semijoin_incremental(&index2, |&k, &s| (k, s))
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
                //               join 
                // ```
                let edges = edges.delta0(child);
                let paths_delayed = <DelayedFeedback<_, OrdZSet<_, _>>>::new(child);

                let paths_inverted: Stream<_, OrdZSet<(usize, usize), isize>> = paths_delayed
                    .stream()
                    .map_keys(|&(x, y)| (y, x));

                let paths_inverted_indexed = paths_inverted.index();
                let edges_indexed = edges.index();

                let paths = edges.plus(&paths_inverted_indexed.join::<NestedTimestamp32, _, _, _>(&edges_indexed, |_via, from, to| (*from, *to)))
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

        computed_labels.consolidate::<OrdZSet<_, _>>()
    }

    #[test]
    fn propagate_test() {
        let root = Root::build(move |circuit| {
            let mut edges = vec![
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

            let mut labels = vec![
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

            let mut outputs = vec![
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
        .unwrap();

        for _ in 0..8 {
            root.step().unwrap();
        }
    }

    #[test]
    fn propagate_nested_test() {
        let root = Root::build(move |circuit| {
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

            let mut outputs = vec![
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
            ]
            .into_iter();

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
                Ok((
                    move || {
                        let mut counter = counter.borrow_mut();
                        // reset to 0 on each outer loop iteration.
                        if *counter == 2 {
                            *counter = 0;
                        }
                        *counter += 1;
                        *counter == 2
                    },
                    result.integrate_trace().export(),
                ))
            })
            .unwrap();

            result.consolidate().inspect(move |res: &OrdZSet<Label, isize>| {
                assert_eq!(*res, outputs.next().unwrap())
            });
        })
        .unwrap();

        for _ in 0..8 {
            root.step().unwrap();
        }
    }
}

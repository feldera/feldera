//! Distinct operator.

use std::{
    borrow::Cow,
    cmp::{max, Ordering},
    collections::BTreeSet,
    fmt::Write,
    marker::PhantomData,
    mem::take,
    ops::{Add, Neg},
};

use crate::{
    algebra::{AddAssignByRef, AddByRef, HasOne, HasZero, ZRingValue, ZSet},
    circuit::{
        operator_traits::{BinaryOperator, Operator, UnaryOperator},
        Circuit, GlobalNodeId, Scope, Stream,
    },
    circuit_cache_key,
    time::NestedTimestamp32,
    trace::{ord::OrdKeySpine, BatchReader, Builder, Cursor as TraceCursor, Trace, TraceReader},
    NumEntries, Timestamp,
};
use deepsize::DeepSizeOf;

circuit_cache_key!(DistinctId<C, D>(GlobalNodeId => Stream<C, D>));
circuit_cache_key!(DistinctIncrementalId<C, D>(GlobalNodeId => Stream<C, D>));
circuit_cache_key!(DistinctTraceId<C, D>(GlobalNodeId => Stream<C, D>));

impl<P, Z> Stream<Circuit<P>, Z>
where
    P: Clone + 'static,
{
    /// Apply [`Distinct`] operator to `self`.
    pub fn distinct(&self) -> Stream<Circuit<P>, Z>
    where
        Z: ZSet,
    {
        self.circuit()
            .cache_get_or_insert_with(DistinctId::new(self.origin_node_id().clone()), || {
                self.circuit().add_unary_operator(Distinct::new(), self)
            })
            .clone()
    }

    /// Incremental version of the [`Distinct`] operator.
    ///
    /// This is equivalent to `self.integrate().distinct().differentiate()`, but
    /// is more efficient.
    pub fn distinct_incremental(&self) -> Stream<Circuit<P>, Z>
    where
        Z: DeepSizeOf + NumEntries + ZSet,
        Z::Key: Clone + PartialEq + Ord,
        Z::R: ZRingValue,
    {
        self.circuit()
            .cache_get_or_insert_with(
                DistinctIncrementalId::new(self.origin_node_id().clone()),
                || {
                    self.circuit().add_binary_operator(
                        DistinctIncremental::new(),
                        self,
                        &self.integrate_trace().delay_trace(),
                    )
                },
            )
            .clone()
    }

    /// Incremental nested version of the [`Distinct`] operator.
    // TODO: remove this method.
    pub fn distinct_incremental_nested(&self) -> Stream<Circuit<P>, Z>
    where
        Z: DeepSizeOf + NumEntries + ZSet,
        Z::Key: Clone + PartialEq + Ord,
        Z::R: ZRingValue,
    {
        self.integrate_nested()
            .distinct_incremental()
            .differentiate_nested()
    }
}

impl<P, Z> Stream<Circuit<P>, Z>
where
    P: Clone + 'static,
{
    // TODO: Rename this method.
    // TODO: Document it better (we need a better framework to explain nested
    // incremental computations).
    /// Incremental nested version of the [`Distinct`] operator.
    ///
    /// This implementation integrates the input stream into a trace and should
    /// be more CPU and memory efficient than
    /// [`Stream::distinct_incremental_nested`].
    pub fn distinct_trace(&self) -> Stream<Circuit<P>, Z>
    where
        Z: NumEntries + ZSet + DeepSizeOf,
        Z::Key: Clone + Ord + DeepSizeOf,
        Z::R: ZRingValue + DeepSizeOf,
    {
        self.circuit()
            .cache_get_or_insert_with(DistinctTraceId::new(self.origin_node_id().clone()), || {
                self.circuit().add_binary_operator(
                    DistinctTrace::new(),
                    self,
                    &self
                        .trace::<OrdKeySpine<Z::Key, NestedTimestamp32, Z::R>>()
                        .delay_trace(),
                )
            })
            .clone()
    }
}

/// `Distinct` operator changes all weights in the support of a Z-set to 1.
pub struct Distinct<Z> {
    _type: PhantomData<Z>,
}

impl<Z> Distinct<Z> {
    pub fn new() -> Self {
        Self { _type: PhantomData }
    }
}

impl<Z> Default for Distinct<Z> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Z> Operator for Distinct<Z>
where
    Z: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Distinct")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<Z> UnaryOperator<Z, Z> for Distinct<Z>
where
    Z: ZSet,
{
    fn eval(&mut self, i: &Z) -> Z {
        i.distinct()
    }

    fn eval_owned(&mut self, i: Z) -> Z {
        i.distinct_owned()
    }
}

/// Incremental version of the distinct operator.
///
/// Takes a stream `a` of changes to relation `A` and a stream with delayed
/// value of `A`: `z^-1(A) = a.integrate().delay()` and computes
/// `distinct(A) - distinct(z^-1(A))` incrementally, by only considering
/// values in the support of `a`.
struct DistinctIncremental<Z, I> {
    _type: PhantomData<(Z, I)>,
}

impl<Z, I> DistinctIncremental<Z, I> {
    pub fn new() -> Self {
        Self { _type: PhantomData }
    }
}

impl<Z, I> Default for DistinctIncremental<Z, I> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Z, I> Operator for DistinctIncremental<Z, I>
where
    Z: 'static,
    I: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("DistinctIncremental")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<Z, I> BinaryOperator<Z, I, Z> for DistinctIncremental<Z, I>
where
    Z: ZSet,
    Z::Key: Clone + PartialEq,
    Z::R: ZRingValue,
    I: BatchReader<Key = Z::Key, Val = (), Time = (), R = Z::R> + 'static,
{
    fn eval(&mut self, delta: &Z, delayed_integral: &I) -> Z {
        let mut builder = Z::Builder::with_capacity((), delta.len());
        let mut delta_cursor = delta.cursor();
        let mut integral_cursor = delayed_integral.cursor();

        while delta_cursor.key_valid() {
            let w = delta_cursor.weight();
            let v = delta_cursor.key();
            integral_cursor.seek_key(v);
            let old_weight = if integral_cursor.key_valid() && integral_cursor.key() == v {
                integral_cursor.weight().clone()
            } else {
                HasZero::zero()
            };

            let new_weight = old_weight.add_by_ref(&w);

            if old_weight.le0() {
                // Weight changes from non-positive to positive.
                if new_weight.ge0() && !new_weight.is_zero() {
                    builder.push((v.clone(), (), HasOne::one()));
                }
            } else if new_weight.le0() {
                // Weight changes from positive to non-positive.
                builder.push((v.clone(), (), Z::R::one().neg()));
            }
            delta_cursor.step_key();
        }

        builder.done()
    }

    // TODO: owned implementation.
    fn eval_owned_and_ref(&mut self, delta: Z, delayed_integral: &I) -> Z {
        self.eval(&delta, delayed_integral)
    }

    fn eval_owned(&mut self, delta: Z, delayed_integral: I) -> Z {
        self.eval_owned_and_ref(delta, &delayed_integral)
    }
}

pub struct DistinctTrace<Z, T>
where
    Z: ZSet,
    T: TraceReader<Key = Z::Key, Val = (), R = Z::R> + 'static,
{
    // Keeps track of keys that need to be considered at future times.
    // Specifically, `future_updates[i]` accumulates all keys observed during
    // the current epoch whose weight can change at time `i`.
    future_updates: Vec<BTreeSet<Z::Key>>,
    // TODO: not needed once timekeeping is handled by the circuit.
    time: u32,
    empty_input: bool,
    empty_output: bool,
    _type: PhantomData<(Z, T)>,
}

impl<Z, T> DistinctTrace<Z, T>
where
    Z: ZSet,
    T: TraceReader<Key = Z::Key, Val = (), R = Z::R> + 'static,
    T::Time: Timestamp,
{
    fn new() -> Self {
        Self {
            future_updates: Vec::new(),
            time: HasZero::zero(),
            empty_input: false,
            empty_output: false,
            _type: PhantomData,
        }
    }
}

impl<Z, T> DistinctTrace<Z, T>
where
    Z: ZSet,
    Z::Key: Clone + Ord + PartialEq,
    Z::R: ZRingValue,
    T: TraceReader<Key = Z::Key, Val = (), Time = NestedTimestamp32, R = Z::R> + 'static,
{
    // Evaluate nested incremental distinct for a single value.
    //
    // In the following diagram,
    //
    // * `w1` - the sum of weights with which `value` appears in previous epochs at
    //   times `t < self.time`.
    // * `w2` - the sum of weight with which `value` appers in previous epochs at
    //   time `self.time`.
    // * `w3` - the sum of weights with which `value` appears in the current epoch
    //   at times `t < self.time`.
    // * `w4` - the weight with which `value` appears in the current epoch at times
    //   `self.time`, i.e., in the current input `delta` being processed.
    //
    // ```
    // ┌─────────────────────┬─────┐
    // │                     │     │
    // │                     │     │
    // │         w1          │  w2 │
    // │                     │     │
    // │                     │     │
    // ├─────────────────────┼─────┤
    // │         w3          │  w4 │
    // └─────────────────────┴─────┘
    // ```
    //
    // Then `value` is added to the output batch with weight
    // `w_output = delta_new - delta_old` where
    //
    // ```
    // delta_old = if w1 <= 0 && w1 + w2 > 0 {
    //      1
    // } else if w1 > 0 && w1 + w2 <= 0 {
    //     -1
    // } else {
    //      0
    // }
    //
    // delta_new = if w1 + w3 <= 0 && w1 + w2 + w3 + w4 > 0 {
    //      1
    // } else if w1 + w3 > 0 && w1 + w2 + w3 + w4 <= 0 {
    //      -1
    // } else {
    //      0
    // }
    // ```
    //
    // This is just the definition of `(↑((↑distinct)∆))∆`.
    #[allow(clippy::type_complexity)]
    fn eval_value<'s>(
        &mut self,
        trace_cursor: &mut T::Cursor<'s>,
        _trace: &'s T,
        value: &Z::Key,
        weight: Z::R,
        output: &mut Vec<((Z::Key, ()), Z::R)>,
    ) {
        //eprintln!("value: {:?}, weight: {:?}", value, weight);
        trace_cursor.seek_key(value);

        if trace_cursor.key_valid() && trace_cursor.key() == value {
            let mut w1: Z::R = HasZero::zero();
            let mut w2: Z::R = HasZero::zero();
            let mut w3: Z::R = HasZero::zero();
            let mut next_ts: Option<T::Time> = None;
            trace_cursor.map_times(|t, w| {
                if !t.epoch() {
                    if t.inner() < self.time {
                        w1.add_assign_by_ref(w);
                    } else if t.inner() == self.time {
                        w2.add_assign_by_ref(w);
                    } else if next_ts.is_none() || t < next_ts.as_ref().unwrap() {
                        next_ts = Some(t.clone());
                    }
                } else if t.inner() < self.time {
                    w3.add_assign_by_ref(w);
                }
            });

            //eprintln!("w1: {:?}, w2: {:?}, w3: {:?}", w1, w2, w3);
            // w1 + w2
            let w12 = w1.add_by_ref(&w2);
            // w1 + w3
            let w13 = w1.add_by_ref(&w3);
            // w1 + w2 + w3 + w4
            let w1234 = w12.add_by_ref(&w3).add(weight);

            let delta_old = if w1.le0() && w12.ge0() && !w12.is_zero() {
                HasOne::one()
            } else if w1.ge0() && !w1.is_zero() && w12.le0() {
                Z::R::one().neg()
            } else {
                HasZero::zero()
            };

            let delta_new = if w13.le0() && w1234.ge0() && !w1234.is_zero() {
                HasOne::one()
            } else if w13.ge0() && !w13.is_zero() && w1234.le0() {
                Z::R::one().neg()
            } else {
                HasZero::zero()
            };

            // Update output.
            if delta_old != delta_new {
                output.push(((value.clone(), ()), delta_new + delta_old.neg()));
            }

            // Record next_ts in `self.future_updates`.
            if let Some(next_ts) = next_ts {
                let idx: usize = next_ts.inner() as usize;
                self.future_updates[idx].insert(value.clone());
            }
        } else if weight.ge0() && !weight.is_zero() {
            output.push(((value.clone(), ()), HasOne::one()));
        }
    }
}

impl<Z, T> Operator for DistinctTrace<Z, T>
where
    Z: ZSet,
    Z::Key: DeepSizeOf + Clone + Ord + PartialEq,
    T: TraceReader<Key = Z::Key, Val = (), Time = NestedTimestamp32, R = Z::R> + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("DistinctTrace")
    }
    fn clock_start(&mut self, scope: Scope) {
        if scope == 0 {
            self.time = 0;
        }
    }
    fn clock_end(&mut self, scope: Scope) {
        if scope == 0 {
            self.future_updates.clear();
            self.empty_input = false;
            self.empty_output = false;
        }
    }
    fn fixedpoint(&self, scope: Scope) -> bool {
        // TODO: generalize `DistinctTrace` to support arbitrarily nested scopes.
        assert_eq!(scope, 0);
        self.empty_input
            && self.empty_output
            && self
                .future_updates
                .iter()
                .skip(self.time as usize)
                .all(|vals| vals.is_empty())
    }

    fn summary(&self, summary: &mut String) {
        let size: usize = self.future_updates.iter().map(|vals| vals.len()).sum();
        writeln!(summary, "size: {}", size).unwrap();

        let bytes = self.future_updates.deep_size_of();
        writeln!(summary, "bytes: {}", bytes).unwrap();
    }
}

impl<Z, T> BinaryOperator<Z, T, Z> for DistinctTrace<Z, T>
where
    Z: ZSet,
    Z::Key: Clone + Ord + PartialEq + DeepSizeOf,
    Z::R: ZRingValue,
    T: Trace<Key = Z::Key, Val = (), Time = NestedTimestamp32, R = Z::R> + 'static,
{
    // Distinct does not have nice properties like linearity that help with
    // incremental evaluation: computing an update for each key requires inspecting
    // the entire history of this key.  Fortunately, we do not need to look at all
    // keys in the trace.  In particular, only those keys that appeared in input
    // deltas during the current clock epoch can appear in the output of this
    // operator with non-zero weights.  So, as a simple heuristic, we could track
    // the set of keys that appeared since the start of the epoch and only consider
    // those.
    //
    // But we can restrict the set of keys to consider even more by using a stronger
    // property: only the keys `k` that satisfy one of the following conditions can
    // appear in the output of the operator at local time `t`:
    // * keys in the current input `delta`
    // * keys from earlier inputs observed during the current clock epoch that
    //   appeared in one of the previous epochs at time `t`.
    //
    // To efficiently compute keys that satisfy the second condition, we use the
    // `future_updates` vector, where for each key observed in the current epoch
    // at time `t1` we lookup the smallest time `t2 > t1` (if any) at which we saw
    // the key during any previous epochs and record this key in
    // `future_updates[t2]`. Then when evaluating an operatpr at time `t` we
    // simply scan all keys in `delta` and all keys in `future_updates[t]` for
    // candidates.
    //
    // TODO: Add example.
    //
    fn eval(&mut self, delta: &Z, trace: &T) -> Z {
        //eprintln!("distinct_trace {}", self.time);

        self.empty_input = delta.is_zero();

        // Make sure we have enough room in `future_updates` to
        // accommodate the largest timestamp in the trace, so we don't
        // need to worry about growing `future_updates` later on.
        let mut new_len: u32 = self.time + 1;
        trace.map_batches(|batch| {
            for ts in batch.upper().elements().iter() {
                new_len = max(new_len, ts.inner() + 1);
            }
        });

        self.future_updates
            .resize(new_len as usize, BTreeSet::new());

        let mut batch = Vec::with_capacity(delta.len());

        let mut trace_cursor = trace.cursor();

        // For all keys in delta, for all keys in future_updates[time].
        let mut delta_cursor = delta.cursor();
        let candidates = take(&mut self.future_updates[self.time as usize]);
        let mut cand_iterator = candidates.iter();

        let mut candidate = cand_iterator.next();

        // Iterate over keys that appear in either `future_updates[self.time]` or
        // `delta`.
        while delta_cursor.key_valid() && candidate.is_some() {
            let cand_val = candidate.unwrap();
            let w = delta_cursor.weight();
            let k = delta_cursor.key();
            match k.cmp(cand_val) {
                // Key only appears in `delta`.
                Ordering::Less => {
                    self.eval_value(&mut trace_cursor, trace, k, w, &mut batch);
                    delta_cursor.step_key();
                }
                // Key only appears in `future_updates`.
                Ordering::Greater => {
                    self.eval_value(
                        &mut trace_cursor,
                        trace,
                        cand_val,
                        HasZero::zero(),
                        &mut batch,
                    );
                    candidate = cand_iterator.next();
                }
                // Key appears in both `delta` and `future_updates`.
                Ordering::Equal => {
                    self.eval_value(&mut trace_cursor, trace, k, w, &mut batch);
                    delta_cursor.step_key();
                    candidate = cand_iterator.next();
                }
            }
        }

        // One of the cursors is empty; iterate over whatever remains in the other
        // cursor.

        while delta_cursor.key_valid() {
            let w = delta_cursor.weight();
            let k = delta_cursor.key();

            self.eval_value(&mut trace_cursor, trace, k, w, &mut batch);
            delta_cursor.step_key();
        }
        while candidate.is_some() {
            self.eval_value(
                &mut trace_cursor,
                trace,
                candidate.unwrap(),
                HasZero::zero(),
                &mut batch,
            );
            candidate = cand_iterator.next();
        }

        self.time += 1;

        let result = Z::from_tuples((), batch);
        self.empty_output = result.is_zero();
        result
    }
}

#[cfg(test)]
mod test {
    use std::{cell::RefCell, rc::Rc};

    use crate::{circuit::Root, operator::GeneratorNested, trace::ord::OrdZSet, zset};

    #[test]
    fn distinct_incremental_nested_test() {
        let root = Root::build(move |circuit| {
            let mut inputs = vec![
                vec![zset! { 1 => 1, 2 => 1 }, zset! { 2 => -1, 3 => 2, 4 => 2 }],
                vec![zset! { 2 => 1, 3 => 1 }, zset! { 3 => -2, 4 => -1 }],
                vec![
                    zset! { 5 => 1, 6 => 1 },
                    zset! { 2 => -1, 7 => 1 },
                    zset! { 2 => 1, 7 => -1, 8 => 2, 9 => 1 },
                ],
            ]
            .into_iter();

            circuit
                .iterate(|child| {
                    let counter = Rc::new(RefCell::new(0));
                    let counter_clone = counter.clone();

                    let input = child.add_source(GeneratorNested::new(Box::new(move || {
                        *counter_clone.borrow_mut() = 0;
                        let mut deltas = inputs.next().unwrap_or_default().into_iter();
                        Box::new(move || deltas.next().unwrap_or_else(|| zset! {}))
                    })));

                    let distinct_inc = input.distinct_incremental_nested();
                    let distinct_noninc = input
                        // Non-incremental implementation of distinct_nested_incremental.
                        .integrate()
                        .integrate_nested()
                        .distinct()
                        .differentiate()
                        .differentiate_nested();

                    distinct_inc
                        .apply2(
                            &distinct_noninc,
                            |d1: &OrdZSet<usize, isize>, d2: &OrdZSet<usize, isize>| {
                                (d1.clone(), d2.clone())
                            },
                        )
                        .inspect(|(d1, d2)| assert_eq!(d1, d2));

                    Ok((
                        move || {
                            *counter.borrow_mut() += 1;
                            *counter.borrow() == 4
                        },
                        (),
                    ))
                })
                .unwrap();
        })
        .unwrap();

        for _ in 0..3 {
            root.step().unwrap();
        }
    }

    #[test]
    fn distinct_trace_test() {
        let root = Root::build(move |circuit| {
            let mut inputs = vec![
                vec![zset! { 1 => 1, 2 => 1 }, zset! { 2 => -1, 3 => 2, 4 => 2 }],
                vec![zset! { 2 => 1, 3 => 1 }, zset! { 3 => -2, 4 => -1 }],
                vec![
                    zset! { 5 => 1, 6 => 1 },
                    zset! { 2 => -1, 7 => 1 },
                    zset! { 2 => 1, 7 => -1, 8 => 2, 9 => 1 },
                ],
            ]
            .into_iter();

            circuit
                .iterate(|child| {
                    let counter = Rc::new(RefCell::new(0));
                    let counter_clone = counter.clone();

                    let input = child.add_source(GeneratorNested::new(Box::new(move || {
                        *counter_clone.borrow_mut() = 0;
                        let mut deltas = inputs.next().unwrap_or_default().into_iter();
                        Box::new(move || deltas.next().unwrap_or_else(|| zset! {}))
                    })));

                    let distinct_inc = input.distinct_trace();
                    let distinct_noninc = input
                        // Non-incremental implementation of distinct_nested_incremental.
                        .integrate()
                        .integrate_nested()
                        .distinct()
                        .differentiate()
                        .differentiate_nested();

                    distinct_inc
                        .apply2(
                            &distinct_noninc,
                            |d1: &OrdZSet<usize, isize>, d2: &OrdZSet<usize, isize>| {
                                (d1.clone(), d2.clone())
                            },
                        )
                        .inspect(|(d1, d2)| assert_eq!(d1, d2));

                    Ok((
                        move || {
                            *counter.borrow_mut() += 1;
                            *counter.borrow() == 4
                        },
                        (),
                    ))
                })
                .unwrap();
        })
        .unwrap();

        for _ in 0..3 {
            root.step().unwrap();
        }
    }
}

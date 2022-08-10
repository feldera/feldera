//! Aggregation operators.

use std::{borrow::Cow, hash::Hash, marker::PhantomData, ops::Neg};

use crate::{
    algebra::{GroupValue, HasOne, HasZero, IndexedZSet, MonoidValue, MulByRef, ZRingValue, ZSet},
    circuit::{
        operator_traits::{BinaryOperator, Operator, UnaryOperator},
        Circuit, Scope, Stream,
    },
    trace::{cursor::Cursor, Batch, BatchReader, Builder},
    NumEntries, OrdZSet,
};
use deepsize::DeepSizeOf;

impl<P, Z> Stream<Circuit<P>, Z>
where
    P: Clone + 'static,
    Z: Clone + 'static,
{
    /// Aggregate each indexed Z-set in the input stream.
    ///
    /// Values in the input stream are
    /// [indexed Z-sets](`crate::algebra::IndexedZSet`). The aggregation
    /// function `agg_func` takes a single key and a sorted array of (value,
    /// weight) tuples associated with this key and transforms them into a
    /// single aggregate value.  The output of the operator is a Z-set
    /// computed as a sum of aggregates across all keys with weight `+1`
    /// each.
    ///
    /// # Type arguments
    ///
    /// * `Z` - input indexed Z-set type.
    /// * `O` - output Z-set type.
    pub fn aggregate<F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        Z: IndexedZSet + Send + 'static,
        Z::Key: Ord + Clone + Hash,
        Z::Val: Ord + Clone,
        F: Fn(&Z::Key, &mut Vec<(&Z::Val, Z::R)>) -> O::Key + 'static,
        O: Clone + ZSet + 'static,
        O::R: ZRingValue,
    {
        self.circuit()
            .add_unary_operator(Aggregate::new(f), &self.shard())
    }

    /// Incremental version of the [`Self::aggregate`] operator.
    ///
    /// This is equivalent to `self.integrate().aggregate(f).differentiate()`,
    /// but is more efficient.
    pub fn aggregate_incremental<F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        Z: IndexedZSet + DeepSizeOf + NumEntries + Send,
        Z::Key: PartialEq + Ord + Hash + Clone,
        Z::Val: Ord + Clone,
        F: Fn(&Z::Key, &mut Vec<(&Z::Val, Z::R)>) -> O::Key + Clone + 'static,
        O: Clone + ZSet + 'static,
        O::R: ZRingValue,
    {
        self.shard().aggregate_incremental_inner(f)
    }

    fn aggregate_incremental_inner<F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        Z: IndexedZSet + DeepSizeOf + NumEntries,
        Z::Key: PartialEq + Ord,
        Z::Val: Ord,
        F: Fn(&Z::Key, &mut Vec<(&Z::Val, Z::R)>) -> O::Key + Clone + 'static,
        O: Clone + ZSet + 'static,
        O::R: ZRingValue,
    {
        // Retract old values of the aggregate for affected keys.
        let retract_old = self.circuit().add_binary_operator(
            AggregateIncremental::new(false, f.clone()),
            self,
            &self.integrate_trace().delay_trace(),
        );

        // Insert new aggregates.
        let insert_new = self.circuit().add_binary_operator(
            AggregateIncremental::new(true, f),
            self,
            &self.integrate_trace(),
        );

        retract_old.plus(&insert_new)
    }

    /// Incremental nested version of the [`Self::aggregate`] operator.
    ///
    /// This is equivalent to
    /// `self.integrate().integrate_nested().aggregate(f).differentiate_nested.
    /// differentiate()`, but is more efficient.
    pub fn aggregate_incremental_nested<F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        Z: IndexedZSet + DeepSizeOf + NumEntries + Send,
        Z::Key: PartialEq + Ord + Clone + Hash,
        Z::Val: Ord + Clone,
        F: Fn(&Z::Key, &mut Vec<(&Z::Val, Z::R)>) -> O::Key + Clone + 'static,
        O: Clone + ZSet + DeepSizeOf + 'static,
        O::R: ZRingValue,
    {
        self.shard()
            .integrate_nested()
            .aggregate_incremental_inner(f)
            .differentiate_nested()
    }

    /// A version of [`Self::aggregate_incremental`] optimized for linear
    /// aggregation functions.
    ///
    /// This method only works for linear aggregation functions `f`, i.e.,
    /// functions that satisfy `f(a+b) = f(a) + f(b)`.  It will produce
    /// incorrect results if `f` is not linear.  Linearity means that
    /// `f` can be defined per `(key, value)` tuple.
    ///
    /// Note that this method adds the value of the key from the input indexed
    /// Z-set to the output Z-set, i.e., given an input key-value pair `(k,
    /// v)`, the output Z-set contains value `(k, f(k, v))`.  In contrast,
    /// [`Self::aggregate_incremental`] does not automatically include key
    /// in the output, since a user-defined aggregation function can be
    /// designed to return the key if necessary.  However,
    /// such an aggregation function can be non-linear (in fact, the plus
    /// operation may not even be defined for its output type).
    pub fn aggregate_linear_incremental<F, A, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        Z: IndexedZSet,
        Z::Key: PartialEq + Ord + Hash + Clone + DeepSizeOf + Send,
        Z::Val: Ord + Clone,
        F: Fn(&Z::Key, &Z::Val) -> A + Clone + 'static,
        O: Clone + ZSet<Key = (Z::Key, A), R = Z::R> + 'static,
        O::R: ZRingValue,
        A: MulByRef<Z::R> + GroupValue + DeepSizeOf + Send,
    {
        self.weigh(f).aggregate_incremental(|key, weights| {
            debug_assert_eq!(weights.len(), 1);
            (key.clone(), weights.pop().unwrap().1)
        })
    }

    /// A version of [`Self::aggregate_incremental_nested`] optimized for linear
    /// aggregation functions.
    pub fn aggregate_linear_incremental_nested<F, A, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        Z: IndexedZSet,
        Z::Key: PartialEq + Ord + Hash + Clone + DeepSizeOf + Send,
        Z::Val: Ord + Clone,
        F: Fn(&Z::Key, &Z::Val) -> A + Clone + 'static,
        O: Clone + ZSet<Key = (Z::Key, A), R = Z::R> + DeepSizeOf + 'static,
        O::R: ZRingValue,
        A: MulByRef<Z::R> + GroupValue + DeepSizeOf + Send,
    {
        self.weigh(f).aggregate_incremental_nested(|key, weights| {
            debug_assert_eq!(weights.len(), 1);
            (key.clone(), weights.pop().unwrap().1)
        })
    }

    /// Convert indexed Z-set `Z` into a Z-set where the weight of each key
    /// is computed as:
    ///
    /// ```text
    ///    __
    ///    ╲
    ///    ╱ f(k,v) * w
    ///    ‾‾
    /// (k,v,w) ∈ Z
    /// ```
    ///
    /// This is a linear operator.
    pub fn weigh<F, T>(&self, f: F) -> Stream<Circuit<P>, OrdZSet<Z::Key, T>>
    where
        Z: IndexedZSet,
        Z::Key: Ord + Clone,
        Z::Val: Ord + Clone,
        F: Fn(&Z::Key, &Z::Val) -> T + 'static,
        T: MulByRef<Z::R> + MonoidValue,
    {
        self.weigh_generic::<_, OrdZSet<_, _>>(f)
    }

    /// Like [`Self::weigh`], but can return any batch type.
    pub fn weigh_generic<F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        Z: IndexedZSet,
        Z::Key: Ord + Clone,
        Z::Val: Ord + Clone,
        F: Fn(&Z::Key, &Z::Val) -> O::R + 'static,
        O: Clone + Batch<Key = Z::Key, Val = (), Time = ()> + 'static,
        O::R: MulByRef<Z::R>,
    {
        self.apply(move |batch| {
            let mut delta = <O::Builder>::with_capacity((), batch.key_count());
            let mut cursor = batch.cursor();
            while cursor.key_valid() {
                let mut agg = HasZero::zero();
                while cursor.val_valid() {
                    agg += f(cursor.key(), cursor.val()).mul_by_ref(&cursor.weight());
                    cursor.step_val();
                }
                delta.push((O::item_from(cursor.key().clone(), ()), agg));
                cursor.step_key();
            }
            delta.done()
        })
    }
}

pub struct Aggregate<Z, F, O> {
    agg_func: F,
    _type: PhantomData<(Z, O)>,
}

impl<Z, F, O> Aggregate<Z, F, O> {
    pub fn new(agg_func: F) -> Self {
        Self {
            agg_func,
            _type: PhantomData,
        }
    }
}

impl<Z, F, O> Operator for Aggregate<Z, F, O>
where
    Z: 'static,
    F: 'static,
    O: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Aggregate")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<Z, F, O> UnaryOperator<Z, O> for Aggregate<Z, F, O>
where
    Z: IndexedZSet + 'static,
    F: Fn(&Z::Key, &mut Vec<(&Z::Val, Z::R)>) -> O::Key + 'static,
    O: Clone + ZSet + 'static,
    O::R: ZRingValue,
{
    fn eval(&mut self, i: &Z) -> O {
        let mut elements = Vec::with_capacity(i.len());
        let mut cursor = i.cursor();
        let mut vals: Vec<(&Z::Val, Z::R)> = Vec::with_capacity(i.len());

        while cursor.key_valid() {
            cursor.values(&mut vals);
            // Skip keys that only contain values with weight zero.
            if !vals.is_empty() {
                elements.push(((self.agg_func)(cursor.key(), &mut vals), O::R::one()));
            }
            vals.clear();
            cursor.step_key();
        }
        O::from_keys((), elements)
    }
}

/// Incremental version of the `Aggregate` operator.
///
/// Takes a stream `a` of changes to relation `A` and a stream with delayed
/// value of `A`: `z^-1(A) = a.integrate().delay()` and computes
/// `integrate(A) - integrate(z^-1(A))` incrementally, by only considering
/// values in the support of `a`.
pub struct AggregateIncremental<Z, I, F, O> {
    polarity: bool,
    agg_func: F,
    _type: PhantomData<(Z, I, O)>,
}

impl<Z, I, F, O> AggregateIncremental<Z, I, F, O> {
    pub fn new(polarity: bool, agg_func: F) -> Self {
        Self {
            polarity,
            agg_func,
            _type: PhantomData,
        }
    }
}

impl<Z, I, F, O> Operator for AggregateIncremental<Z, I, F, O>
where
    Z: 'static,
    I: 'static,
    F: 'static,
    O: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("AggregateIncremental")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<Z, I, F, O> BinaryOperator<Z, I, O> for AggregateIncremental<Z, I, F, O>
where
    Z: IndexedZSet + 'static,
    Z::Key: PartialEq,
    I: BatchReader<Key = Z::Key, Val = Z::Val, Time = (), R = Z::R> + 'static,
    F: Fn(&Z::Key, &mut Vec<(&Z::Val, Z::R)>) -> O::Key + 'static,
    O: Clone + ZSet + 'static,
    O::R: ZRingValue,
{
    fn eval(&mut self, delta: &Z, integral: &I) -> O {
        let mut result = Vec::with_capacity(delta.len());

        let mut delta_cursor = delta.cursor();
        let mut integral_cursor = integral.cursor();
        let weight = if self.polarity {
            O::R::one()
        } else {
            O::R::one().neg()
        };

        // This could be an overkill.
        let mut vals: Vec<(&Z::Val, Z::R)> = Vec::with_capacity(delta.len());

        while delta_cursor.key_valid() {
            let key = delta_cursor.key();

            integral_cursor.seek_key(key);

            if integral_cursor.key_valid() && integral_cursor.key() == key {
                integral_cursor.values(&mut vals);
                // Skip keys that only contain values with weight 0.
                if !vals.is_empty() {
                    result.push(((self.agg_func)(key, &mut vals), weight.clone()));
                }
                vals.clear();
            }
            delta_cursor.step_key();
        }

        O::from_keys((), result)
    }
}

#[cfg(test)]
mod test {
    use std::{
        cell::RefCell,
        rc::Rc,
        sync::{Arc, Mutex},
    };

    use crate::{operator::GeneratorNested, zset, zset_set, Circuit, OrdZSet, Runtime, Stream};

    fn do_aggregate_test_mt(workers: usize) {
        let hruntime = Runtime::run(workers, || {
            aggregate_test();
        });

        hruntime.join().unwrap();
    }

    #[test]
    fn aggregate_test_mt() {
        do_aggregate_test_mt(1);
        do_aggregate_test_mt(2);
        do_aggregate_test_mt(4);
        do_aggregate_test_mt(16);
    }

    #[test]
    fn aggregate_test() {
        let root = Circuit::build(move |circuit| {
            let mut inputs = vec![
                vec![
                    zset_set! { (1, 10), (1, 20), (5, 1) },
                    zset! { (2, 10) => 1, (1, 10) => -1, (1, 20) => 1, (3, 10) => 1, (5, 1) => 1 },
                ],
                vec![
                    zset! { (4, 20) => 1, (2, 10) => -1 },
                    zset_set! { (5, 10), (6, 10) },
                ],
                vec![],
            ]
            .into_iter();

            circuit
                .iterate(|child| {
                    let counter = Rc::new(RefCell::new(0));
                    let counter_clone = counter.clone();

                    let input = child
                        .add_source(GeneratorNested::new(Box::new(move || {
                            *counter_clone.borrow_mut() = 0;
                            if Runtime::worker_index() == 0 {
                                let mut deltas = inputs.next().unwrap_or_default().into_iter();
                                Box::new(move || deltas.next().unwrap_or_else(|| zset! {}))
                            } else {
                                Box::new(|| zset! {})
                            }
                        })))
                        .index();

                    // Weighted sum aggregate.  Returns `(key, weighted_sum)`.
                    let sum = |key: &usize, vals: &mut Vec<(&usize, isize)>| -> (usize, isize) {
                        let result: isize = vals.drain(..).map(|(v, w)| (*v as isize) * w).sum();
                        (*key, result)
                    };

                    // Weighted sum aggregate that returns only the weighted sum
                    // value and is therefore linear.
                    let sum_linear = |_key: &usize, val: &usize| -> isize { *val as isize };

                    let sum_inc = input.aggregate_incremental_nested(sum).gather(0);
                    let sum_inc_linear: Stream<_, OrdZSet<(usize, isize), isize>> = input
                        .aggregate_linear_incremental_nested(sum_linear)
                        .gather(0);
                    let sum_noninc = input
                        .integrate_nested()
                        .integrate()
                        .aggregate(sum)
                        .differentiate()
                        .differentiate_nested()
                        .gather(0);

                    // Compare outputs of all three implementations.
                    sum_inc
                        .apply2(
                            &sum_noninc,
                            |d1: &OrdZSet<(usize, isize), isize>,
                             d2: &OrdZSet<(usize, isize), isize>| {
                                (d1.clone(), d2.clone())
                            },
                        )
                        .inspect(|(d1, d2)| {
                            //println!("{}: incremental: {:?}", Runtime::worker_index(), d1);
                            //println!("{}: non-incremental: {:?}", Runtime::worker_index(), d2);
                            assert_eq!(d1, d2);
                        });

                    sum_inc.apply2(
                        &sum_inc_linear,
                        |d1: &OrdZSet<(usize, isize), isize>,
                         d2: &OrdZSet<(usize, isize), isize>| {
                            assert_eq!(d1, d2);
                        },
                    );

                    // Min aggregate (non-linear).
                    let min = |&key: &usize, vals: &mut Vec<(&usize, isize)>| -> (usize, usize) {
                        let result = vals
                            .drain(..)
                            .map(|(&value, _)| value)
                            .min()
                            .unwrap_or(usize::MAX);

                        (key, result)
                    };

                    let min_inc = input.aggregate_incremental_nested(min).gather(0);
                    let min_noninc = input
                        .integrate_nested()
                        .integrate()
                        .aggregate(min)
                        .differentiate()
                        .differentiate_nested()
                        .gather(0);

                    min_inc
                        .apply2(
                            &min_noninc,
                            |d1: &OrdZSet<(usize, usize), isize>,
                             d2: &OrdZSet<(usize, usize), isize>| {
                                (d1.clone(), d2.clone())
                            },
                        )
                        .inspect(|(d1, d2)| {
                            assert_eq!(d1, d2);
                        });

                    Ok((
                        move || {
                            *counter.borrow_mut() += 1;
                            Ok(*counter.borrow() == 4)
                        },
                        (),
                    ))
                })
                .unwrap();
        })
        .unwrap()
        .0;

        for _ in 0..3 {
            root.step().unwrap();
        }
    }

    fn count_test(workers: usize) {
        let count_weighted_output: Arc<Mutex<OrdZSet<(usize, isize), isize>>> =
            Arc::new(Mutex::new(zset! {}));
        let sum_weighted_output: Arc<Mutex<OrdZSet<(usize, isize), isize>>> =
            Arc::new(Mutex::new(zset! {}));
        let count_distinct_output: Arc<Mutex<OrdZSet<(usize, usize), isize>>> =
            Arc::new(Mutex::new(zset! {}));
        let sum_distinct_output: Arc<Mutex<OrdZSet<(usize, usize), isize>>> =
            Arc::new(Mutex::new(zset! {}));

        let count_weighted_output_clone = count_weighted_output.clone();
        let count_distinct_output_clone = count_distinct_output.clone();
        let sum_weighted_output_clone = sum_weighted_output.clone();
        let sum_distinct_output_clone = sum_distinct_output.clone();

        let (mut dbsp, mut input_handle) = Runtime::init_circuit(workers, move |circuit| {
            let (input_stream, input_handle) = circuit.add_input_indexed_zset();
            input_stream
                .aggregate_linear_incremental::<_, _, OrdZSet<(usize, isize), isize>>(
                    |_key, _value: &usize| 1isize,
                )
                .gather(0)
                .inspect(move |batch| {
                    if Runtime::worker_index() == 0 {
                        *count_weighted_output.lock().unwrap() = batch.clone();
                    }
                });

            input_stream
                .aggregate_linear_incremental::<_, _, OrdZSet<(usize, isize), isize>>(
                    |_key, value: &usize| *value as isize,
                )
                .gather(0)
                .inspect(move |batch| {
                    if Runtime::worker_index() == 0 {
                        *sum_weighted_output.lock().unwrap() = batch.clone();
                    }
                });

            input_stream
                .aggregate_incremental::<_, OrdZSet<(usize, usize), isize>>(|key, weights| {
                    (*key, weights.len())
                })
                .gather(0)
                .inspect(move |batch| {
                    if Runtime::worker_index() == 0 {
                        *count_distinct_output.lock().unwrap() = batch.clone();
                    }
                });

            input_stream
                .aggregate_incremental::<_, OrdZSet<(usize, usize), isize>>(|key, weights| {
                    let mut sum = 0;
                    for (v, _) in weights.iter() {
                        sum += *v;
                    }
                    (*key, sum)
                })
                .gather(0)
                .inspect(move |batch| {
                    if Runtime::worker_index() == 0 {
                        *sum_distinct_output.lock().unwrap() = batch.clone();
                    }
                });
            input_handle
        })
        .unwrap();

        input_handle.append(&mut vec![(1, (1, 1)), (1, (2, 2))]);
        dbsp.step().unwrap();
        assert_eq!(
            &*count_distinct_output_clone.lock().unwrap(),
            &zset! {(1, 2) => 1}
        );
        assert_eq!(
            &*sum_distinct_output_clone.lock().unwrap(),
            &zset! {(1, 3) => 1}
        );
        assert_eq!(
            &*count_weighted_output_clone.lock().unwrap(),
            &zset! {(1, 3) => 1}
        );
        assert_eq!(
            &*sum_weighted_output_clone.lock().unwrap(),
            &zset! {(1, 5) => 1}
        );

        input_handle.append(&mut vec![(2, (2, 1)), (2, (4, 1)), (1, (2, -1))]);
        dbsp.step().unwrap();
        assert_eq!(
            &*count_distinct_output_clone.lock().unwrap(),
            &zset! {(2, 2) => 1}
        );
        assert_eq!(
            &*sum_distinct_output_clone.lock().unwrap(),
            &zset! {(2, 6) => 1}
        );
        assert_eq!(
            &*count_weighted_output_clone.lock().unwrap(),
            &zset! {(1, 3) => -1, (1, 2) => 1, (2, 2) => 1}
        );
        assert_eq!(
            &*sum_weighted_output_clone.lock().unwrap(),
            &zset! {(2, 6) => 1, (1, 5) => -1, (1, 3) => 1}
        );

        input_handle.append(&mut vec![(1, (3, 1)), (1, (2, -1))]);
        dbsp.step().unwrap();
        assert_eq!(&*count_distinct_output_clone.lock().unwrap(), &zset! {});
        assert_eq!(
            &*sum_distinct_output_clone.lock().unwrap(),
            &zset! {(1, 3) => -1, (1, 4) => 1}
        );
        assert_eq!(&*count_weighted_output_clone.lock().unwrap(), &zset! {});
        assert_eq!(
            &*sum_weighted_output_clone.lock().unwrap(),
            &zset! {(1, 3) => -1, (1, 4) => 1}
        );

        dbsp.kill().unwrap();
    }

    #[test]
    fn count_test1() {
        count_test(1);
    }

    #[test]
    fn count_test4() {
        count_test(4);
    }
}
